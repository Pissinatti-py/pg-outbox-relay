//! End to end: Postgres 17 (logical replication, over TLS) → relay → SQS FIFO API (ElasticMQ).
//! Needs Docker: `cargo test -- --ignored`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use pg_outbox_relay::adapters::http;
use pg_outbox_relay::adapters::postgres::PgSource;
use pg_outbox_relay::adapters::sqs::{SqsConfig, SqsSink};
use pg_outbox_relay::app::Health;
use pg_outbox_relay::app::relay::{self, Batching, Retry};

const EVENTS: usize = 60;
const AGGREGATES: usize = 6;

#[tokio::test]
#[ignore = "needs Docker"]
async fn relays_outbox_inserts_to_a_fifo_queue_in_per_aggregate_order() -> anyhow::Result<()> {
    let metrics = http::install_metrics()?;
    let pg = common::postgres().await?;
    let (_elasticmq, endpoint) = common::elasticmq().await?;
    let sqs = aws_sdk_sqs::Client::new(&common::aws(&endpoint).await);
    let queue_url = common::create_queue(&sqs, "events.fifo").await?;

    // The relay, wired like main.rs.
    let health = Arc::new(Health::default());
    let source = PgSource::new(pg.config(), health.clone());
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
    pg.insert(0, EVENTS, AGGREGATES, 3).await?;
    let mut received = Vec::new();
    common::receive(
        &sqs,
        &queue_url,
        &mut received,
        |r| r.len() >= EVENTS,
        Duration::from_secs(60),
    )
    .await?;
    assert!(!relay.is_finished(), "the relay stopped: {:?}", relay.await);

    // Every event arrived once, as the documented envelope, in order per aggregate.
    assert_eq!(received.len(), EVENTS);
    assert_eq!(common::ids(&received), pg.ids().await?);
    let first = &received[0];
    let group = format!(
        "policy:{}",
        first.envelope["aggregate_id"].as_str().unwrap()
    );
    assert_eq!(first.group.as_deref(), Some(group.as_str()));
    assert_eq!(first.envelope["headers"]["tenant"], "acme");
    assert!(
        first.envelope["occurred_at"]
            .as_str()
            .unwrap()
            .ends_with('Z'),
        "{}",
        first.envelope
    );
    common::assert_ordered_per_aggregate(&received);

    // The relay acknowledged everything back to the slot.
    pg.wait_until_acked().await?;
    // The slot-lag poller's SQL connection works over TLS too.
    assert!(
        metrics.render().contains("pg_outbox_slot_lag_bytes"),
        "no slot lag exported over TLS"
    );
    Ok(())
}
