//! Postgres source: streams the outbox publication through a logical replication slot.
//! Why `pgwire-replication`: docs/adr/0001-replication-client.md.

mod pgoutput;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, anyhow, bail, ensure};
use percent_encoding::percent_decode_str;
use pgwire_replication::client::ReplicationEvent;
use pgwire_replication::{PgWireError, ReplicationClient, ReplicationConfig, SslMode, TlsConfig};
use serde::Deserialize;
use tokio::sync::{mpsc, watch};

use crate::app::Health;
use crate::domain::{Lsn, SourceMsg};
use crate::ports::EventSource;
use pgoutput::{Decoder, Row, outbox_event, pg_time};

/// Wait between attempts while another relay holds the slot or the database is down.
const CONNECT_RETRY: Duration = Duration::from_secs(5);
/// How often acks reach Postgres, which bounds the duplicates a crash can cause.
const FEEDBACK_INTERVAL: Duration = Duration::from_secs(1);
const SLOT_LAG_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Deserialize)]
pub struct PgConfig {
    /// `postgres://user:password@host:5432/dbname?sslmode=verify-full&sslrootcert=/ca.pem`
    pub dsn: String,
    pub slot: String,
    pub publication: String,
}

pub struct PgSource {
    config: PgConfig,
    health: Arc<Health>,
}

impl PgSource {
    pub fn new(config: PgConfig, health: Arc<Health>) -> Self {
        Self { config, health }
    }
}

impl EventSource for PgSource {
    async fn run(
        self,
        out: mpsc::Sender<SourceMsg>,
        acked: watch::Receiver<Lsn>,
    ) -> anyhow::Result<()> {
        let dsn = Dsn::parse(&self.config.dsn)?;
        let (client, first) = connect(dsn.replication(&self.config)).await?;
        tracing::info!(slot = %self.config.slot, "streaming from the replication slot");
        self.health.source_ready.store(true, Ordering::Relaxed);

        let result = tokio::select! {
            result = stream(client, first, out, acked) => result,
            () = poll_slot_lag(&dsn, &self.config.slot) => Ok(()),
        };
        self.health.source_ready.store(false, Ordering::Relaxed);
        result
    }
}

/// Opens the stream. Waits while another relay holds the slot (this one is the HA standby)
/// or the database is unreachable. Returns the client and its first event.
async fn connect(
    config: ReplicationConfig,
) -> anyhow::Result<(ReplicationClient, ReplicationEvent)> {
    loop {
        // `connect` returns at once; failures surface on the first `recv`.
        let mut client = ReplicationClient::connect(config.clone()).await?;
        let error = match client.recv().await {
            Ok(Some(first)) => return Ok((client, first)),
            Ok(None) => bail!("the replication stream ended before it started"),
            Err(error) => error,
        };
        match sqlstate(&error) {
            Some("42704") => bail!(
                "{error}. Create it with sql/slot.sql; the relay never creates slots, \
                 because a recreated slot silently skips everything committed before it"
            ),
            Some("55006" | "57P03") => {} // slot active elsewhere, database starting up
            None if error.is_io() => {}
            _ => return Err(error.into()),
        }
        tracing::warn!(%error, "replication slot unavailable, retrying in {CONNECT_RETRY:?}");
        tokio::time::sleep(CONNECT_RETRY).await;
    }
}

/// The SQLSTATE of a server error, e.g. `55006`. The client only exposes the message text.
fn sqlstate(error: &PgWireError) -> Option<&str> {
    match error {
        PgWireError::Server(message) => message.rsplit_once("(SQLSTATE ")?.1.strip_suffix(')'),
        _ => None,
    }
}

/// Forwards events to the relay and the relay's acks to Postgres, until either side stops.
/// Any stream error ends the process: Postgres replays everything unacked on restart.
async fn stream(
    mut client: ReplicationClient,
    first: ReplicationEvent,
    out: mpsc::Sender<SourceMsg>,
    mut acked: watch::Receiver<Lsn>,
) -> anyhow::Result<()> {
    let mut transactions = Transactions {
        out,
        decoder: Decoder::default(),
        rows: None,
    };
    transactions.handle(first).await?;
    loop {
        tokio::select! {
            changed = acked.changed() => {
                if changed.is_err() {
                    return Ok(()); // the relay core stopped
                }
                let lsn = *acked.borrow_and_update();
                client.update_applied_lsn(pgwire_replication::Lsn(lsn.0));
            }
            event = client.recv() => match event? {
                Some(event) => transactions.handle(event).await?,
                None => bail!("the replication stream ended"),
            },
        }
    }
}

/// Turns replication events into source messages, one committed transaction at a time.
struct Transactions {
    out: mpsc::Sender<SourceMsg>,
    decoder: Decoder,
    /// Outbox rows of the transaction being received; `None` between transactions.
    // ponytail: a transaction's rows wait in memory for its Commit; fine for outbox-sized transactions
    rows: Option<Vec<Row>>,
}

impl Transactions {
    async fn handle(&mut self, event: ReplicationEvent) -> anyhow::Result<()> {
        match event {
            ReplicationEvent::Begin { .. } => self.rows = Some(Vec::new()),
            ReplicationEvent::XLogData { data, .. } => {
                if let Some(row) = self.decoder.decode(&data)? {
                    self.rows
                        .as_mut()
                        .context("insert outside a transaction")?
                        .push(row);
                }
            }
            ReplicationEvent::Commit {
                end_lsn,
                commit_time_micros,
                ..
            } => {
                let lsn = Lsn(end_lsn.0);
                let committed_at = pg_time(commit_time_micros);
                for row in self.rows.take().context("commit without a begin")? {
                    self.send(SourceMsg::Event(outbox_event(row, lsn, committed_at)?))
                        .await?;
                }
                self.send(SourceMsg::Progress(lsn)).await?;
            }
            // Only between transactions: mid-transaction, `wal_end` can be behind the commit.
            ReplicationEvent::KeepAlive { wal_end, .. } if self.rows.is_none() => {
                self.send(SourceMsg::Progress(Lsn(wal_end.0))).await?;
            }
            _ => {} // mid-transaction keepalives, pg_logical_emit_message, StoppedAt
        }
        Ok(())
    }

    async fn send(&self, msg: SourceMsg) -> anyhow::Result<()> {
        self.out
            .send(msg)
            .await
            .map_err(|_| anyhow!("the relay core stopped"))
    }
}

/// Exports how much WAL the slot holds back, i.e. what a stuck relay costs the database.
// ponytail: plain-text SQL connection only; TLS for SQL connections lands with M2's dead-letter table
async fn poll_slot_lag(dsn: &Dsn, slot: &str) {
    if dsn.sslmode != SslMode::Disable {
        tracing::warn!(
            "pg_outbox_slot_lag_bytes needs sslmode=disable until SQL connections get TLS (M2); \
             watch pg_replication_slots from your database monitoring meanwhile"
        );
        return std::future::pending().await;
    }
    let mut config = tokio_postgres::Config::new();
    config
        .host(&dsn.host)
        .port(dsn.port)
        .user(&dsn.user)
        .password(&dsn.password)
        .dbname(&dsn.dbname);
    loop {
        match slot_lag(&config, slot).await {
            Ok(bytes) => metrics::gauge!("pg_outbox_slot_lag_bytes").set(bytes as f64),
            Err(error) => tracing::warn!(%error, "could not read the slot lag"),
        }
        tokio::time::sleep(SLOT_LAG_INTERVAL).await;
    }
}

async fn slot_lag(config: &tokio_postgres::Config, slot: &str) -> anyhow::Result<i64> {
    let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
    tokio::spawn(connection);
    let row = client
        .query_opt(
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await?
        .with_context(|| format!("replication slot {slot} does not exist"))?;
    Ok(row.get(0))
}

/// Connection settings from a libpq-style URL.
#[derive(Debug, PartialEq)]
struct Dsn {
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: String,
    sslmode: SslMode,
    sslrootcert: Option<PathBuf>,
}

impl Dsn {
    fn parse(dsn: &str) -> anyhow::Result<Self> {
        let url = url::Url::parse(dsn)
            .context("source.dsn must look like postgres://user:password@host:5432/dbname")?;
        ensure!(
            matches!(url.scheme(), "postgres" | "postgresql"),
            "source.dsn must start with postgres://"
        );
        let decode = |part: &str| -> anyhow::Result<String> {
            Ok(percent_decode_str(part).decode_utf8()?.into_owned())
        };
        let mut parsed = Dsn {
            host: url.host_str().context("source.dsn has no host")?.to_owned(),
            port: url.port().unwrap_or(5432),
            user: decode(url.username())?,
            password: decode(url.password().unwrap_or_default())?,
            dbname: decode(url.path().trim_start_matches('/'))?,
            sslmode: SslMode::Prefer, // libpq's default
            sslrootcert: None,
        };
        ensure!(!parsed.user.is_empty(), "source.dsn has no user");
        ensure!(!parsed.dbname.is_empty(), "source.dsn has no database name");
        for (key, value) in url.query_pairs() {
            match &*key {
                "sslmode" => {
                    parsed.sslmode = match &*value {
                        "disable" => SslMode::Disable,
                        "prefer" => SslMode::Prefer,
                        "require" => SslMode::Require,
                        "verify-ca" => SslMode::VerifyCa,
                        "verify-full" => SslMode::VerifyFull,
                        other => bail!("source.dsn: unsupported sslmode `{other}`"),
                    }
                }
                "sslrootcert" => parsed.sslrootcert = Some(PathBuf::from(&*value)),
                "replication" => {} // accepted for libpq compatibility; the relay sets it itself
                other => bail!("source.dsn: unsupported parameter `{other}`"),
            }
        }
        Ok(parsed)
    }

    fn replication(&self, config: &PgConfig) -> ReplicationConfig {
        let tls = TlsConfig {
            mode: self.sslmode,
            ca_pem_path: self.sslrootcert.clone(),
            ..TlsConfig::default()
        };
        ReplicationConfig::new(
            self.host.as_str(),
            self.user.as_str(),
            self.password.as_str(),
            self.dbname.as_str(),
            config.slot.as_str(),
            config.publication.as_str(),
        )
        .with_port(self.port)
        .with_tls(tls)
        // pgoutput sends values as text; pin their format so `created_at` parses the same everywhere.
        .with_options("-c TimeZone=UTC -c DateStyle=ISO")
        .with_status_interval(FEEDBACK_INTERVAL)
        .with_wakeup_interval(FEEDBACK_INTERVAL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_full_dsn() {
        let dsn = Dsn::parse(
            "postgres://relay:p%40ss@db.internal:6543/app?sslmode=verify-full&sslrootcert=/ca.pem",
        )
        .unwrap();
        assert_eq!(
            dsn,
            Dsn {
                host: "db.internal".into(),
                port: 6543,
                user: "relay".into(),
                password: "p@ss".into(),
                dbname: "app".into(),
                sslmode: SslMode::VerifyFull,
                sslrootcert: Some("/ca.pem".into()),
            }
        );
    }

    #[test]
    fn applies_libpq_defaults_and_accepts_the_replication_parameter() {
        let dsn = Dsn::parse("postgresql://relay@db/app?replication=database").unwrap();
        assert_eq!(dsn.port, 5432);
        assert_eq!(dsn.sslmode, SslMode::Prefer);
        assert_eq!(dsn.password, "");
    }

    #[test]
    fn rejects_bad_dsns() {
        for bad in [
            "mysql://relay@db/app",
            "postgres://db/app",
            "postgres://relay@db",
            "postgres://relay@db/app?sslmode=maybe",
            "postgres://relay@db/app?typo=1",
        ] {
            assert!(Dsn::parse(bad).is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn reads_sqlstate_from_server_errors() {
        let busy = PgWireError::Server(
            "replication slot \"outbox_relay\" is active for PID 114 (SQLSTATE 55006)".into(),
        );
        assert_eq!(sqlstate(&busy), Some("55006"));
        assert_eq!(
            sqlstate(&PgWireError::Protocol("x (SQLSTATE 55006)".into())),
            None
        );
        assert_eq!(sqlstate(&PgWireError::Server("no code".into())), None);
    }
}
