//! Docker setup shared by the end-to-end tests (`mod common;`).
#![allow(dead_code)] // each test binary uses a different part

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_sqs::config::Credentials;
use aws_sdk_sqs::types::{MessageSystemAttributeName, QueueAttributeName};
use pg_outbox_relay::adapters::postgres::PgConfig;
use serde_json::Value;
use testcontainers_modules::elasticmq::ElasticMq;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::time::{Instant, sleep};
use tokio_postgres::NoTls;

pub const SLOT: &str = "outbox_relay";

/// Postgres 17 with TLS on, set up by the repo's scripts in the order an operator runs them.
pub struct Pg {
    pub container: ContainerAsync<Postgres>,
    pub port: u16,
    /// The application's side: a superuser connection.
    pub db: tokio_postgres::Client,
}

pub async fn postgres() -> anyhow::Result<Pg> {
    let container = Postgres::default()
        .with_init_sql(include_bytes!("../../sql/outbox.sql").to_vec())
        .with_init_sql(
            b"CREATE ROLE relay WITH LOGIN REPLICATION PASSWORD 'relay';
              GRANT INSERT ON outbox_dead_letter TO relay;"
                .to_vec(),
        )
        .with_init_sql(include_bytes!("../../sql/slot.sql").to_vec())
        .with_tag("17")
        .with_cmd([
            "-c",
            "wal_level=logical",
            "-c",
            "fsync=off",
            // The image's snakeoil certificate is enough for sslmode=require.
            "-c",
            "ssl=on",
            "-c",
            "ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem",
            "-c",
            "ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key",
        ])
        .start()
        .await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let (db, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres"),
        NoTls,
    )
    .await?;
    tokio::spawn(connection);
    Ok(Pg {
        container,
        port,
        db,
    })
}

impl Pg {
    /// The relay's source settings: its own role, over TLS.
    pub fn config(&self) -> PgConfig {
        PgConfig {
            dsn: format!(
                "postgres://relay:relay@127.0.0.1:{}/postgres?sslmode=require",
                self.port
            ),
            slot: SLOT.into(),
            publication: "outbox_pub".into(),
            databases: Vec::new(),
        }
    }

    /// Inserts events `first..first + count` (payload `{"n": n}`) over `aggregates`
    /// aggregates, `per_tx` rows per transaction.
    pub async fn insert(
        &self,
        first: usize,
        count: usize,
        aggregates: usize,
        per_tx: usize,
    ) -> anyhow::Result<()> {
        let numbers: Vec<usize> = (first..first + count).collect();
        for chunk in numbers.chunks(per_tx) {
            let rows: Vec<String> = chunk
                .iter()
                .map(|n| {
                    format!(
                        "(gen_random_uuid(), 'policy', '{}', 'policy.updated', '{{\"n\": {n}}}', '{{\"tenant\": \"acme\"}}')",
                        n % aggregates
                    )
                })
                .collect();
            self.db
                .batch_execute(&format!(
                    "INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, headers) VALUES {}",
                    rows.join(", ")
                ))
                .await?;
        }
        Ok(())
    }

    pub async fn ids(&self) -> anyhow::Result<HashSet<String>> {
        let rows = self.db.query("SELECT id::text FROM outbox", &[]).await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    /// Waits until the slot confirms everything written so far.
    pub async fn wait_until_acked(&self) -> anyhow::Result<()> {
        let written: String = self
            .db
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await?
            .get(0);
        self.wait_for(&format!(
            "SELECT confirmed_flush_lsn >= '{written}'::pg_lsn FROM pg_replication_slots WHERE slot_name = '{SLOT}'"
        ))
        .await
    }

    /// Waits until a relay holds the slot.
    pub async fn wait_until_streaming(&self) -> anyhow::Result<()> {
        self.wait_for(&format!(
            "SELECT active FROM pg_replication_slots WHERE slot_name = '{SLOT}'"
        ))
        .await
    }

    async fn wait_for(&self, query: &str) -> anyhow::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !self.db.query_one(query, &[]).await?.get::<_, bool>(0) {
            anyhow::ensure!(Instant::now() < deadline, "timed out waiting for: {query}");
            sleep(Duration::from_millis(200)).await;
        }
        Ok(())
    }
}

/// ElasticMQ, a local SQS, and its endpoint URL.
pub async fn elasticmq() -> anyhow::Result<(ContainerAsync<ElasticMq>, String)> {
    let container = ElasticMq::default()
        .with_name("softwaremill/elasticmq-native")
        .with_tag("1.7.1")
        .start()
        .await?;
    let endpoint = format!(
        "http://127.0.0.1:{}",
        container.get_host_port_ipv4(9324).await?
    );
    Ok((container, endpoint))
}

/// AWS SDK settings for a local endpoint (ElasticMQ or moto).
pub async fn aws(endpoint: &str) -> SdkConfig {
    aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("e2e", "e2e", None, None, "e2e"))
        .load()
        .await
}

/// Creates a queue; a `.fifo` name makes a FIFO queue.
pub async fn create_queue(sqs: &aws_sdk_sqs::Client, name: &str) -> anyhow::Result<String> {
    let mut request = sqs.create_queue().queue_name(name);
    if name.ends_with(".fifo") {
        request = request.attributes(QueueAttributeName::FifoQueue, "true");
    }
    Ok(request
        .send()
        .await?
        .queue_url()
        .expect("created queues have a URL")
        .to_owned())
}

/// One consumed message: its FIFO group, if any, and its body.
pub struct Received {
    pub group: Option<String>,
    pub envelope: Value,
}

/// A consumer: receives and deletes into `received` until `done` holds or `within` passes.
pub async fn receive(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    received: &mut Vec<Received>,
    done: impl Fn(&[Received]) -> bool,
    within: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + within;
    while !done(received) && Instant::now() < deadline {
        let batch = sqs
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .message_system_attribute_names(MessageSystemAttributeName::MessageGroupId)
            .send()
            .await?;
        for message in batch.messages() {
            received.push(Received {
                group: message
                    .attributes()
                    .and_then(|a| a.get(&MessageSystemAttributeName::MessageGroupId))
                    .cloned(),
                envelope: serde_json::from_str(message.body().expect("has a body"))?,
            });
            sqs.delete_message()
                .queue_url(queue_url)
                .receipt_handle(message.receipt_handle().expect("has a receipt handle"))
                .send()
                .await?;
        }
    }
    Ok(())
}

pub fn ids(received: &[Received]) -> HashSet<String> {
    received
        .iter()
        .map(|r| r.envelope["id"].as_str().unwrap().to_owned())
        .collect()
}

/// Within each aggregate, events arrived in commit order (`payload.n` increases).
pub fn assert_ordered_per_aggregate(received: &[Received]) {
    let mut last = HashMap::new();
    for r in received {
        let aggregate = format!("{}:{}", r.envelope["source"], r.envelope["aggregate_id"]);
        let n = r.envelope["payload"]["n"].as_u64().unwrap();
        if let Some(previous) = last.insert(aggregate.clone(), n) {
            assert!(
                previous < n,
                "aggregate {aggregate}: {n} arrived after {previous}"
            );
        }
    }
}
