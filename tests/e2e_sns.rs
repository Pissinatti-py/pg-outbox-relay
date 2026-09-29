//! End to end: Postgres 17 → relay → SNS FIFO topic → SQS FIFO queue, on moto (a local AWS).
//! Needs Docker: `cargo test -- --ignored`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use aws_sdk_sqs::types::QueueAttributeName;
use pg_outbox_relay::adapters::postgres::{PgDeadLetters, PgSource};
use pg_outbox_relay::adapters::sns::{SnsConfig, SnsSink};
use pg_outbox_relay::app::Health;
use pg_outbox_relay::app::relay::{self, Batching, Retry};
use testcontainers_modules::testcontainers::GenericImage;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::sync::watch;

const MOTO_TAG: &str = "5.2.2";

#[tokio::test]
#[ignore = "needs Docker"]
async fn relays_outbox_inserts_through_a_fifo_topic() -> anyhow::Result<()> {
    let pg = common::postgres().await?;
    let moto = GenericImage::new("motoserver/moto", MOTO_TAG)
        .with_exposed_port(5000.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Running on"))
        .start()
        .await?;
    let aws = common::aws(&format!(
        "http://127.0.0.1:{}",
        moto.get_host_port_ipv4(5000).await?
    ))
    .await;
    let (sns, sqs) = (
        aws_sdk_sns::Client::new(&aws),
        aws_sdk_sqs::Client::new(&aws),
    );

    // A FIFO queue subscribed to a FIFO topic with raw delivery: the body is the envelope.
    let queue_url = common::create_queue(&sqs, "events.fifo").await?;
    let attributes = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await?;
    let queue_arn = attributes
        .attributes()
        .and_then(|a| a.get(&QueueAttributeName::QueueArn))
        .cloned()
        .expect("queues have an ARN");
    let topic_arn = sns
        .create_topic()
        .name("events.fifo")
        .attributes("FifoTopic", "true")
        .send()
        .await?
        .topic_arn()
        .expect("created topics have an ARN")
        .to_owned();
    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(queue_arn)
        .attributes("RawMessageDelivery", "true")
        .send()
        .await?;

    // The relay, wired like main.rs.
    let health = Arc::new(Health::default());
    let (_stop, stopped) = watch::channel(false);
    let config = pg.config();
    let source = PgSource::new(config.clone(), health.clone(), stopped);
    let sink = SnsSink::with_client(sns, SnsConfig { topic_arn }).await?;
    let dead_letters = PgDeadLetters::new(&config)?;
    let relay = tokio::spawn(relay::run(
        source,
        sink,
        dead_letters,
        Batching::default(),
        Retry::default(),
        health,
    ));

    pg.insert(0, 60, 6, 3).await?;
    let mut received = Vec::new();
    common::receive(
        &sqs,
        &queue_url,
        &mut received,
        |r| r.len() >= 60,
        Duration::from_secs(60),
    )
    .await?;
    assert!(!relay.is_finished(), "the relay stopped: {:?}", relay.await);
    assert_eq!(received.len(), 60);
    assert_eq!(common::ids(&received), pg.ids().await?);
    common::assert_ordered_per_aggregate(&received);
    Ok(())
}
