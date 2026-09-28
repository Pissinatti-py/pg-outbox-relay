//! End to end: Postgres 17 (logical replication) → relay → SQS FIFO API (ElasticMQ).
//! Needs Docker: `cargo test -- --ignored`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_sqs::Client;
use aws_sdk_sqs::config::Credentials;
use aws_sdk_sqs::types::{MessageSystemAttributeName, QueueAttributeName};
use pg_outbox_relay::adapters::postgres::{PgConfig, PgSource};
use pg_outbox_relay::adapters::sqs::{SqsConfig, SqsSink};
use pg_outbox_relay::app::Health;
use pg_outbox_relay::app::relay::{self, Batching, Retry};
use serde_json::Value;
use testcontainers_modules::elasticmq::ElasticMq;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::time::{Instant, sleep};
use tokio_postgres::NoTls;

const EVENTS: usize = 60;
const AGGREGATES: usize = 6;

#[tokio::test]
#[ignore = "needs Docker"]
async fn relays_outbox_inserts_to_a_fifo_queue_in_per_aggregate_order() -> anyhow::Result<()> {
    // Postgres with the repo's own setup scripts, in the order an operator runs them.
    let pg = Postgres::default()
        .with_init_sql(include_bytes!("../sql/outbox.sql").to_vec())
        .with_init_sql(b"CREATE ROLE relay WITH LOGIN REPLICATION PASSWORD 'relay';".to_vec())
        .with_init_sql(include_bytes!("../sql/slot.sql").to_vec())
        .with_tag("17")
        .with_cmd(["-c", "wal_level=logical", "-c", "fsync=off"])
        .start()
        .await?;
    let elasticmq = ElasticMq::default()
        .with_name("softwaremill/elasticmq-native")
        .with_tag("1.7.1")
        .start()
        .await?;
    let pg_port = pg.get_host_port_ipv4(5432).await?;
    let sqs_endpoint = format!(
        "http://127.0.0.1:{}",
        elasticmq.get_host_port_ipv4(9324).await?
    );

    let sqs = Client::new(
        &aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(&sqs_endpoint)
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("e2e", "e2e", None, None, "e2e"))
            .load()
            .await,
    );
    let queue_url = sqs
        .create_queue()
        .queue_name("events.fifo")
        .attributes(QueueAttributeName::FifoQueue, "true")
        .send()
        .await?
        .queue_url()
        .expect("created queues have a URL")
        .to_owned();

    // The relay, wired exactly like main.rs does.
    let health = Arc::new(Health::default());
    let source = PgSource::new(
        PgConfig {
            dsn: format!("postgres://relay:relay@127.0.0.1:{pg_port}/postgres?sslmode=disable"),
            slot: "outbox_relay".into(),
            publication: "outbox_pub".into(),
        },
        health.clone(),
    );
    let sink = SqsSink::with_client(
        sqs.clone(),
        SqsConfig {
            queue_url: queue_url.clone(),
        },
    )
    .await?;
    let relay = tokio::spawn(relay::run(
        source,
        sink,
        Batching::default(),
        Retry::default(),
        health,
    ));

    // The application: 60 events over 6 aggregates, three rows per transaction.
    let (db, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres password=postgres dbname=postgres"),
        NoTls,
    )
    .await?;
    tokio::spawn(connection);
    for first in (0..EVENTS).step_by(3) {
        let rows: Vec<String> = (first..first + 3)
            .map(|n| {
                format!(
                    "(gen_random_uuid(), 'policy', '{}', 'policy.updated', '{{\"n\": {n}}}', '{{\"tenant\": \"acme\"}}')",
                    n % AGGREGATES
                )
            })
            .collect();
        db.batch_execute(&format!(
            "INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, headers) VALUES {}",
            rows.join(", ")
        ))
        .await?;
    }
    let written_up_to: String = db
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    let inserted: HashSet<String> = db
        .query("SELECT id::text FROM outbox", &[])
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();

    // A consumer: receive and delete until every event arrived (FIFO holds a group while in flight).
    let mut received: Vec<(String, Value)> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while received.len() < EVENTS && Instant::now() < deadline {
        let batch = sqs
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .message_system_attribute_names(MessageSystemAttributeName::MessageGroupId)
            .send()
            .await?;
        for message in batch.messages() {
            let group = message.attributes().expect("asked for the group id")
                [&MessageSystemAttributeName::MessageGroupId]
                .clone();
            received.push((
                group,
                serde_json::from_str(message.body().expect("has a body"))?,
            ));
            sqs.delete_message()
                .queue_url(&queue_url)
                .receipt_handle(message.receipt_handle().expect("has a receipt handle"))
                .send()
                .await?;
        }
    }
    assert!(!relay.is_finished(), "the relay stopped: {:?}", relay.await);

    // Every event arrived once, as the documented envelope, grouped by aggregate.
    assert_eq!(
        received.len(),
        EVENTS,
        "received {} of {EVENTS} events",
        received.len()
    );
    let ids: HashSet<String> = received
        .iter()
        .map(|(_, e)| e["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(ids, inserted);
    let (group, envelope) = &received[0];
    assert_eq!(
        group,
        &format!("policy:{}", envelope["aggregate_id"].as_str().unwrap())
    );
    assert_eq!(envelope["headers"]["tenant"], "acme");
    assert!(
        envelope["occurred_at"].as_str().unwrap().ends_with('Z'),
        "{envelope}"
    );

    // Within each aggregate, events arrive in commit order.
    let mut by_group: HashMap<&str, Vec<u64>> = HashMap::new();
    for (group, envelope) in &received {
        by_group
            .entry(group)
            .or_default()
            .push(envelope["payload"]["n"].as_u64().unwrap());
    }
    assert_eq!(by_group.len(), AGGREGATES);
    for (group, ns) in &by_group {
        assert!(ns.is_sorted(), "{group} arrived out of order: {ns:?}");
    }

    // The relay acknowledged everything back to the slot.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let acked: bool = db
            .query_one(
                "SELECT confirmed_flush_lsn >= $1::text::pg_lsn FROM pg_replication_slots WHERE slot_name = 'outbox_relay'",
                &[&written_up_to],
            )
            .await?
            .get(0);
        if acked {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the slot never confirmed {written_up_to}"
        );
        sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}
