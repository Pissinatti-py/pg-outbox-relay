//! Several tenant databases → the relay binary → one SQS FIFO queue.
//! Needs Docker: `cargo test -- --ignored`.

mod common;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use pg_outbox_relay::adapters::postgres::PgConfig;

#[tokio::test]
#[ignore = "needs Docker"]
async fn relays_every_tenant_database_and_isolates_a_broken_one() -> anyhow::Result<()> {
    let pg = common::postgres().await?;
    let acme = pg.tenant("acme", true).await?;
    let globex = pg.tenant("globex", true).await?;
    let initech = pg.tenant("initech", false).await?; // listed, but its slot does not exist yet
    let (_elasticmq, endpoint) = common::elasticmq().await?;
    let sqs = aws_sdk_sqs::Client::new(&common::aws(&endpoint).await);
    let queue_url = common::create_queue(&sqs, "events.fifo").await?;
    let source = PgConfig {
        dsn: format!(
            "postgres://relay:relay@127.0.0.1:{}/{{database}}?sslmode=require",
            pg.port
        ),
        slot: "outbox_{database}".into(),
        publication: "outbox_pub".into(),
        databases: vec!["acme".into(), "globex".into(), "initech".into()],
    };
    let mut relay = common::RelayProcess::start(&source, &queue_url, &endpoint)?;

    // The same aggregate ids in both tenants: they must not share FIFO groups.
    common::insert(&acme, 0, 30, 3, 3).await?;
    common::insert(&globex, 100, 30, 3, 3).await?;
    let mut tenant_of = HashMap::new();
    for (name, db) in [("acme", &acme), ("globex", &globex)] {
        for id in common::ids_in(db).await? {
            tenant_of.insert(id, name);
        }
    }
    let mut received = Vec::new();
    common::receive(
        &sqs,
        &queue_url,
        &mut received,
        |r| r.len() >= 60,
        Duration::from_secs(60),
    )
    .await?;

    assert_eq!(received.len(), 60);
    for r in &received {
        let id = r.envelope["id"].as_str().unwrap();
        let tenant = tenant_of[id];
        assert_eq!(r.envelope["source"], tenant, "{id}");
        let group = format!(
            "{tenant}:policy:{}",
            r.envelope["aggregate_id"].as_str().unwrap()
        );
        assert_eq!(r.group.as_deref(), Some(group.as_str()));
    }
    common::assert_ordered_per_aggregate(&received);

    // Broken sources recover on their own, while the relay keeps running: acme's stream
    // breaks with writes in flight, and initech's slot appears.
    common::insert(&acme, 200, 30, 3, 3).await?;
    let broken: Option<bool> = pg
        .db
        .query_one(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = 'outbox_acme'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(broken, Some(true), "acme was not streaming");
    initech
        .batch_execute("SELECT pg_create_logical_replication_slot('outbox_initech', 'pgoutput')")
        .await?;
    common::insert(&globex, 300, 30, 3, 3).await?;
    common::insert(&initech, 400, 30, 3, 3).await?;
    for (name, db) in [("acme", &acme), ("globex", &globex), ("initech", &initech)] {
        for id in common::ids_in(db).await? {
            tenant_of.insert(id, name);
        }
    }
    let inserted: HashSet<String> = tenant_of.keys().cloned().collect();
    // Long enough for initech's restart backoff, which has grown since the relay started.
    common::receive(
        &sqs,
        &queue_url,
        &mut received,
        |r| common::ids(r) == inserted,
        Duration::from_secs(90),
    )
    .await?;
    let missing = inserted.difference(&common::ids(&received)).count();
    assert_eq!(missing, 0, "{missing} events lost");
    // Duplicates are allowed: acme replays what it had not acknowledged.
    for r in &received {
        let id = r.envelope["id"].as_str().unwrap();
        assert_eq!(r.envelope["source"], tenant_of[id], "{id}");
    }

    // The broken sources never stopped the relay, and a deploy still drains cleanly.
    assert!(
        relay.exited(Duration::ZERO).await?.is_none(),
        "a broken source stopped the relay"
    );
    relay.terminate()?;
    let status = relay.exited(Duration::from_secs(15)).await?;
    assert!(status.is_some_and(|s| s.success()), "{status:?}");
    Ok(())
}
