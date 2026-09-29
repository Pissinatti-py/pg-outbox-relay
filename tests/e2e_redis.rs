//! End to end: Postgres 17 → relay → Redis Streams.
//! Needs Docker: `cargo test -- --ignored`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use pg_outbox_relay::adapters::postgres::{PgDeadLetters, PgSource};
use pg_outbox_relay::adapters::redis::{RedisConfig, RedisSink};
use pg_outbox_relay::app::Health;
use pg_outbox_relay::app::relay::{self, Batching, Retry};
use redis::AsyncCommands;
use redis::streams::StreamRangeReply;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::sync::watch;
use tokio::time::{Instant, sleep};

#[tokio::test]
#[ignore = "needs Docker"]
async fn relays_outbox_inserts_to_one_stream_per_aggregate_type() -> anyhow::Result<()> {
    let pg = common::postgres().await?;
    let redis = Redis::default().with_tag("8.4.2").start().await?;
    let url = format!(
        "redis://127.0.0.1:{}",
        redis.get_host_port_ipv4(6379).await?
    );

    // The relay, wired like main.rs.
    let health = Arc::new(Health::default());
    let (_stop, stopped) = watch::channel(false);
    let config = pg.config();
    let source = PgSource::new(config.clone(), health.clone(), stopped);
    let sink = RedisSink::connect(RedisConfig {
        url: url.clone(),
        stream_prefix: "outbox:".into(),
    })
    .await?;
    let dead_letters = PgDeadLetters::new(&config)?;
    let relay = tokio::spawn(relay::run(
        "postgres",
        source,
        sink,
        dead_letters,
        Batching::default(),
        Retry::default(),
        health,
    ));
    pg.insert(0, 60, 6, 3).await?;

    // A consumer: read the whole stream until every event is there.
    let mut con = redis::Client::open(url)?
        .get_multiplexed_async_connection()
        .await?;
    let deadline = Instant::now() + Duration::from_secs(60);
    let entries = loop {
        let reply: StreamRangeReply = con.xrange_all("outbox:policy").await?;
        if reply.ids.len() >= 60 || Instant::now() > deadline {
            break reply.ids;
        }
        sleep(Duration::from_millis(200)).await;
    };
    assert!(!relay.is_finished(), "the relay stopped: {:?}", relay.await);

    let received: Vec<common::Received> = entries
        .iter()
        .map(|entry| {
            let id: String = entry.get("id").expect("has an id");
            let source: String = entry.get("source").expect("has a source");
            assert_eq!(source, "postgres");
            let envelope: String = entry.get("envelope").expect("has an envelope");
            let envelope: serde_json::Value = serde_json::from_str(&envelope).unwrap();
            assert_eq!(envelope["id"], id);
            common::Received {
                group: None,
                envelope,
            }
        })
        .collect();
    assert_eq!(received.len(), 60);
    assert_eq!(common::ids(&received), pg.ids().await?);
    common::assert_ordered_per_aggregate(&received);
    Ok(())
}
