//! Postgres source: streams the outbox publication through a logical replication slot.
//! Why `pgwire-replication`: docs/adr/0001-replication-client.md.

mod dead_letter;
mod pgoutput;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, anyhow, bail, ensure};
use metrics::Gauge;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use pgwire_replication::client::ReplicationEvent;
use pgwire_replication::tls::rustls::maybe_upgrade_to_tls;
use pgwire_replication::{PgWireError, ReplicationClient, ReplicationConfig, SslMode, TlsConfig};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};

use crate::app::{Health, stopped};
use crate::domain::{Lsn, SourceMsg};
use crate::ports::EventSource;
pub use dead_letter::PgDeadLetters;
use pgoutput::{Decoder, Row, outbox_event, pg_time};

/// Wait between attempts while another relay holds the slot or the database is down.
const CONNECT_RETRY: Duration = Duration::from_secs(5);
/// How often acks reach Postgres, which bounds the duplicates a crash can cause.
const FEEDBACK_INTERVAL: Duration = Duration::from_secs(1);
const SLOT_LAG_INTERVAL: Duration = Duration::from_secs(10);
/// How long a drain waits for the worker to send the final ack (it reports every FEEDBACK_INTERVAL).
const FINAL_ACK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Deserialize)]
pub struct PgConfig {
    /// `postgres://user:password@host:5432/dbname?sslmode=verify-full&sslrootcert=/ca.pem`
    pub dsn: String,
    pub slot: String,
    pub publication: String,
    /// Relay these databases, one pipeline each; `{database}` in `dsn` and `slot` stands
    /// for each name.
    #[serde(default)]
    pub databases: Vec<String>,
}

/// Stands for each entry of `databases` in the DSN and slot templates.
const PLACEHOLDER: &str = "{database}";

pub struct PgSource {
    config: PgConfig,
    health: Arc<Health>,
    /// Turns `true` when the relay should drain and stop.
    stop: watch::Receiver<bool>,
}

impl PgConfig {
    /// The source's name: its database. It tags events and labels metrics.
    pub fn database(&self) -> anyhow::Result<String> {
        Ok(Dsn::parse(&self.dsn)?.dbname)
    }

    /// One concrete config per source: each entry of `databases`, or this config alone.
    /// Checked up front, so a bad entry fails the start instead of one pipeline.
    pub fn sources(&self) -> anyhow::Result<Vec<PgConfig>> {
        ensure!(
            !self.publication.contains(PLACEHOLDER),
            "source.publication cannot contain {PLACEHOLDER}: every database has its own, under the same name"
        );
        // As the environment splits them: `acme, globex,` or an empty variable.
        let databases: Vec<&str> = self
            .databases
            .iter()
            .map(|database| database.trim())
            .filter(|database| !database.is_empty())
            .collect();
        if databases.is_empty() {
            ensure!(
                !self.dsn.contains(PLACEHOLDER) && !self.slot.contains(PLACEHOLDER),
                "source.dsn or source.slot contain {PLACEHOLDER}, but source.databases is empty"
            );
            check_slot_name(&self.slot)?;
            Dsn::parse(&self.dsn)?;
            return Ok(vec![self.clone()]);
        }
        ensure!(
            self.dsn.contains(PLACEHOLDER),
            "source.dsn must contain {PLACEHOLDER} when source.databases is set"
        );
        ensure!(
            self.slot.contains(PLACEHOLDER),
            "source.slot must contain {PLACEHOLDER}: slot names are unique across the whole server"
        );
        let (mut seen, mut names) = (HashSet::new(), HashSet::new());
        databases
            .into_iter()
            .map(|database| {
                ensure!(
                    seen.insert(database),
                    "source.databases lists {database} twice"
                );
                let encoded = utf8_percent_encode(database, NON_ALPHANUMERIC).to_string();
                let source = PgConfig {
                    dsn: self.dsn.replace(PLACEHOLDER, &encoded),
                    slot: self.slot.replace(PLACEHOLDER, database),
                    publication: self.publication.clone(),
                    databases: Vec::new(),
                };
                check_slot_name(&source.slot).with_context(|| format!("database {database}"))?;
                // The database name names the source: it tags its events and labels its metrics.
                let name = Dsn::parse(&source.dsn)?.dbname;
                ensure!(
                    names.insert(name.clone()),
                    "source.dsn must put {PLACEHOLDER} in the database name, which names each source: several would be named `{name}`"
                );
                Ok(source)
            })
            .collect()
    }
}

/// Postgres slot names: lowercase letters, digits and underscores, at most 63 bytes.
fn check_slot_name(slot: &str) -> anyhow::Result<()> {
    ensure!(
        !slot.is_empty()
            && slot.len() <= 63
            && slot
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "`{slot}` is not a valid slot name: use lowercase letters, digits and underscores (at most 63)"
    );
    Ok(())
}

impl PgSource {
    pub fn new(config: PgConfig, health: Arc<Health>, stop: watch::Receiver<bool>) -> Self {
        Self {
            config,
            health,
            stop,
        }
    }
}

impl EventSource for PgSource {
    async fn run(
        mut self,
        out: mpsc::Sender<SourceMsg>,
        acked: watch::Receiver<Lsn>,
    ) -> anyhow::Result<()> {
        let dsn = Dsn::parse(&self.config.dsn)?;
        // 1 while this relay streams the slot: across replicas, `sum by (source)` shows whether anyone does.
        let up = metrics::gauge!("pg_outbox_source_up", "source" => dsn.dbname.clone());
        let lag = metrics::gauge!("pg_outbox_slot_lag_bytes", "source" => dsn.dbname.clone());
        up.set(0.0);
        let (client, first) = tokio::select! {
            connected = connect(dsn.replication(&self.config)) => connected?,
            // A standby waiting for the slot has nothing to drain.
            () = stopped(&mut self.stop) => return Ok(()),
        };
        dead_letter::check(&dsn).await?;
        tracing::info!(slot = %self.config.slot, "streaming from the replication slot");
        self.health.source_ready.store(true, Ordering::Relaxed);
        up.set(1.0);

        let transactions = Transactions {
            out,
            decoder: Decoder::default(),
            rows: None,
            source: dsn.dbname.clone(),
        };
        let result = tokio::select! {
            result = stream(client, first, transactions, acked, self.stop) => result,
            () = poll_slot_lag(&dsn, &self.config.slot, &lag) => Ok(()),
        };
        self.health.source_ready.store(false, Ordering::Relaxed);
        up.set(0.0);
        // Only the relay streaming the slot reports its lag: after a restart or a takeover,
        // a stale value here would page forever.
        lag.set(0.0);
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

/// Forwards events to the relay and its acks to Postgres. On `stop`, it stops reading, lets
/// the core publish what it was sent, and reports the final ack.
/// Any stream error ends this source's pipeline: Postgres replays everything unacked when it restarts.
async fn stream(
    mut client: ReplicationClient,
    first: ReplicationEvent,
    mut transactions: Transactions,
    mut acked: watch::Receiver<Lsn>,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
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
            // Checked between replication events: a commit's messages all go out together,
            // and a transaction without its commit yet is dropped and replays later.
            () = stopped(&mut stop) => break,
            event = client.recv() => match event? {
                Some(event) => transactions.handle(event).await?,
                None => bail!("the replication stream ended"),
            },
        }
    }

    // Closing `out` lets the core publish what it has; it acks as it goes, then drops `acked`.
    drop(transactions);
    while acked.changed().await.is_ok() {
        let lsn = *acked.borrow_and_update();
        client.update_applied_lsn(pgwire_replication::Lsn(lsn.0));
    }
    let last = *acked.borrow();
    final_ack(client, last).await
}

/// `ReplicationClient::stop` sends CopyDone without a last status update, so wait until the
/// worker has sent `lsn` itself; otherwise the next start replays what was just published.
async fn final_ack(mut client: ReplicationClient, lsn: Lsn) -> anyhow::Result<()> {
    client.update_applied_lsn(pgwire_replication::Lsn(lsn.0));
    let metrics = client.metrics();
    tokio::time::timeout(FINAL_ACK_TIMEOUT, async {
        while metrics.last_applied_lsn().0 < lsn.0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context(
        "the final ack did not reach Postgres; the next start replays what it would have covered",
    )?;
    if let Err(error) = client.shutdown().await {
        tracing::warn!(%error, "closing the replication stream");
    }
    tracing::info!(%lsn, "drained; final ack sent");
    Ok(())
}

/// Turns replication events into source messages, one committed transaction at a time.
struct Transactions {
    out: mpsc::Sender<SourceMsg>,
    decoder: Decoder,
    /// Outbox rows of the transaction being received; `None` between transactions.
    // ponytail: a transaction's rows wait in memory for its Commit; fine for outbox-sized transactions
    rows: Option<Vec<Row>>,
    /// The database, which every event is tagged with.
    source: String,
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
                    self.send(SourceMsg::Event(outbox_event(
                        row,
                        &self.source,
                        lsn,
                        committed_at,
                    )?))
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
async fn poll_slot_lag(dsn: &Dsn, slot: &str, lag: &Gauge) {
    loop {
        match slot_lag(dsn, slot).await {
            Ok(bytes) => lag.set(bytes as f64),
            Err(error) => {
                tracing::warn!(error = format!("{error:#}"), "could not read the slot lag")
            }
        }
        tokio::time::sleep(SLOT_LAG_INTERVAL).await;
    }
}

async fn slot_lag(dsn: &Dsn, slot: &str) -> anyhow::Result<i64> {
    let client = sql_connect(dsn).await?;
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

/// A SQL connection with the replication connection's TLS behavior, for every sslmode:
/// pgwire-replication negotiates TLS, then tokio-postgres talks over that stream.
// ponytail: couples SQL TLS to pgwire-replication; switch to tokio-postgres-rustls if it is ever replaced
async fn sql_connect(dsn: &Dsn) -> anyhow::Result<tokio_postgres::Client> {
    let tcp = TcpStream::connect((dsn.host.as_str(), dsn.port)).await?;
    let stream = maybe_upgrade_to_tls(tcp, &dsn.tls(), &dsn.host).await?;
    let (client, connection) = tokio_postgres::Config::new()
        .user(&dsn.user)
        .password(&dsn.password)
        .dbname(&dsn.dbname)
        .ssl_mode(tokio_postgres::config::SslMode::Disable) // already negotiated above
        .connect_raw(stream, tokio_postgres::NoTls)
        .await?;
    tokio::spawn(connection);
    Ok(client)
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

    fn tls(&self) -> TlsConfig {
        TlsConfig {
            mode: self.sslmode,
            ca_pem_path: self.sslrootcert.clone(),
            ..TlsConfig::default()
        }
    }

    fn replication(&self, config: &PgConfig) -> ReplicationConfig {
        ReplicationConfig::new(
            self.host.as_str(),
            self.user.as_str(),
            self.password.as_str(),
            self.dbname.as_str(),
            config.slot.as_str(),
            config.publication.as_str(),
        )
        .with_port(self.port)
        .with_tls(self.tls())
        // pgoutput sends values as text; pin their format so `created_at` parses the same everywhere.
        .with_options("-c TimeZone=UTC -c DateStyle=ISO")
        // ponytail: bounds memory per source (N × 1024 here, and as many in the channels); raise if a benchmark says so
        .with_buffer_size(1024)
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

    fn config(dsn: &str, slot: &str, databases: &[&str]) -> PgConfig {
        PgConfig {
            dsn: dsn.into(),
            slot: slot.into(),
            publication: "outbox_pub".into(),
            databases: databases.iter().map(|name| name.to_string()).collect(),
        }
    }

    #[test]
    fn one_source_per_database_from_the_templates() {
        let sources = config(
            "postgres://relay:pw@db:5432/{database}?sslmode=require",
            "outbox_{database}",
            &["acme", " acme_corp", ""], // RELAY__SOURCE__DATABASES=acme, acme_corp,
        )
        .sources()
        .unwrap();
        let got: Vec<(String, String)> = sources
            .iter()
            .map(|s| (s.database().unwrap(), s.slot.clone()))
            .collect();
        assert_eq!(
            got,
            [
                ("acme".to_owned(), "outbox_acme".to_owned()),
                ("acme_corp".to_owned(), "outbox_acme_corp".to_owned()),
            ]
        );
        assert!(
            sources
                .iter()
                .all(|s| s.dsn.ends_with("?sslmode=require") && s.databases.is_empty())
        );
    }

    #[test]
    fn without_databases_the_config_is_its_own_single_source() {
        // [""] is an empty RELAY__SOURCE__DATABASES, as templated deployments set it.
        for databases in [&[][..], &[""]] {
            let sources = config("postgres://relay@db/app", "outbox_relay", databases)
                .sources()
                .unwrap();
            assert_eq!(sources.len(), 1);
            assert_eq!(sources[0].database().unwrap(), "app");
            assert_eq!(sources[0].slot, "outbox_relay");
        }
    }

    #[test]
    fn rejects_database_lists_that_cannot_work() {
        let dsn = "postgres://relay@db/{database}";
        for (bad, why) in [
            (
                config("postgres://relay@db/app", "outbox_{database}", &["acme"]),
                "{database}",
            ),
            (config(dsn, "outbox_relay", &["acme"]), "server"),
            (config(dsn, "outbox_{database}", &["acme", "acme"]), "twice"),
            (
                config(dsn, "outbox_{database}", &["acme-corp"]),
                "acme-corp",
            ),
            (config(dsn, "outbox_{database}", &[]), "databases"),
            // Every source would be named app: its events and metrics would be indistinguishable.
            (
                config(
                    "postgres://relay@{database}.internal/app",
                    "outbox_{database}",
                    &["acme", "globex"],
                ),
                "database name",
            ),
            (
                PgConfig {
                    publication: "outbox_pub_{database}".into(),
                    ..config(dsn, "outbox_{database}", &["acme"])
                },
                "publication",
            ),
        ] {
            let error = bad.sources().unwrap_err();
            assert!(format!("{error:#}").contains(why), "{error:#}");
        }
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
