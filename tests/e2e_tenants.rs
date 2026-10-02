//! Several tenant databases → the relay binary → one SQS FIFO queue.
//! Needs Docker: `cargo test -- --ignored`.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use pg_outbox_relay::adapters::postgres::PgConfig;

#[tokio::test]
#[ignore = "needs Docker"]
async fn relays_every_tenant_database_and_isolates_a_broken_one() -> anyhow::Result<()> {
    let pg = common::postgres().await?;
    let acme = pg.tenant("acme", true).await?;
    let globex = pg.tenant("globex", true).await?;
    pg.tenant("initech", false).await?; // listed, but its slot does not exist yet
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

    // initech keeps failing without stopping the others, and a deploy still drains cleanly.
    assert!(
        relay.exited(Duration::ZERO).await?.is_none(),
        "one broken source stopped the relay"
    );
    relay.terminate()?;
    let status = relay.exited(Duration::from_secs(15)).await?;
    assert!(status.is_some_and(|s| s.success()), "{status:?}");
    Ok(())
}
