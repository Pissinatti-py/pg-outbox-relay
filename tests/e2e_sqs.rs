//! End to end: Postgres 17 (logical replication, over TLS) → relay → SQS FIFO API (ElasticMQ).
//! Needs Docker: `cargo test -- --ignored`.

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pg_outbox_relay::adapters::http;
use pg_outbox_relay::adapters::postgres::{PgDeadLetters, PgSource};
use pg_outbox_relay::adapters::sqs::{SqsConfig, SqsSink};
use pg_outbox_relay::app::Health;
use pg_outbox_relay::app::relay::{self, Batching, Retry};
use tokio::sync::watch;

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
    let (stop, stopped) = watch::channel(false);
    let source = PgSource::new(pg.config(), health.clone(), stopped);
    let sink = SqsSink::with_client(
        sqs.clone(),
        SqsConfig {
            queue_url: queue_url.clone(),
        },
    )
    .await?;
    let relay = tokio::spawn(relay::run(
        "postgres",
        source,
        sink,
        PgDeadLetters::new(&pg.config())?,
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
        "postgres:policy:{}",
        first.envelope["aggregate_id"].as_str().unwrap()
    );
    assert_eq!(first.group.as_deref(), Some(group.as_str()));
    // The broker's send time: what the benchmark measures latency against.
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
    assert!(
        received
            .iter()
            .all(|r| r.sent_ms.is_some_and(|sent| now_ms.abs_diff(sent) < 60_000)),
        "a message without a recent SentTimestamp"
    );
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
    // Every metric is labelled with its source. (The SQL connections' TLS is proven by the
    // dead-letter check every start runs.)
    let rendered = metrics.render();
    assert!(
        exported(&rendered, "pg_outbox_events_published_total", " 60"),
        "{rendered}"
    );
    assert!(
        exported(&rendered, "pg_outbox_source_up", " 1"),
        "{rendered}"
    );
    assert!(
        exported(&rendered, "pg_outbox_slot_lag_bytes", ""),
        "no slot lag exported: {rendered}"
    );

    // Once its pipeline stops, a relay no longer reports the slot: with two replicas, a
    // stale lag from the one that lost it would page forever.
    stop.send_replace(true);
    relay.await??;
    let rendered = metrics.render();
    assert!(
        exported(&rendered, "pg_outbox_source_up", " 0"),
        "{rendered}"
    );
    assert!(
        exported(&rendered, "pg_outbox_slot_lag_bytes", " 0"),
        "{rendered}"
    );
    Ok(())
}

/// Whether `rendered` has a sample of `metric` for the source `postgres` ending in `value`.
fn exported(rendered: &str, metric: &str, value: &str) -> bool {
    rendered.lines().any(|line| {
        line.starts_with(metric) && line.contains(r#"source="postgres""#) && line.ends_with(value)
    })
}
