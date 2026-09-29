//! The relay binary under signals, the way an orchestrator runs it: SIGTERM drains,
//! SIGKILL loses nothing.
//! Needs Docker: `cargo test -- --ignored`.

mod common;

use std::process::{Child, Command, ExitStatus};
use std::time::Duration;

use tokio::time::{Instant, sleep};

/// A relay process configured through the environment alone, like in a container.
struct RelayProcess(Child);

impl RelayProcess {
    fn start(pg: &common::Pg, queue_url: &str, sqs_endpoint: &str) -> anyhow::Result<Self> {
        let source = pg.config();
        Ok(Self(
            Command::new(env!("CARGO_BIN_EXE_pg-outbox-relay"))
                .arg("no-such-file.toml") // the environment alone configures it
                .env("RELAY__SOURCE__DSN", &source.dsn)
                .env("RELAY__SOURCE__SLOT", &source.slot)
                .env("RELAY__SOURCE__PUBLICATION", &source.publication)
                .env("RELAY__SINK__KIND", "sqs")
                .env("RELAY__SINK__QUEUE_URL", queue_url)
                .env("RELAY__SERVER__LISTEN", "127.0.0.1:0") // several relays at once
                .env("AWS_ENDPOINT_URL", sqs_endpoint)
                .env("AWS_REGION", "us-east-1")
                .env("AWS_ACCESS_KEY_ID", "e2e")
                .env("AWS_SECRET_ACCESS_KEY", "e2e")
                .env("RUST_LOG", "warn")
                .spawn()?,
        ))
    }

    /// What a deploy sends.
    fn terminate(&self) -> anyhow::Result<()> {
        let sent = Command::new("kill")
            .args(["-TERM", &self.0.id().to_string()])
            .status()?;
        anyhow::ensure!(sent.success(), "kill -TERM failed");
        Ok(())
    }

    /// No chance to clean up.
    fn kill(&mut self) -> anyhow::Result<()> {
        self.0.kill()?;
        self.0.wait()?;
        Ok(())
    }

    /// The exit status, or `None` if it still runs after `within`.
    async fn exited(&mut self, within: Duration) -> anyhow::Result<Option<ExitStatus>> {
        let deadline = Instant::now() + within;
        loop {
            if let Some(status) = self.0.try_wait()? {
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for RelayProcess {
    fn drop(&mut self) {
        let _ = self.0.kill(); // no orphans when an assertion fails
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn sigterm_drains_so_the_next_start_publishes_no_duplicates() -> anyhow::Result<()> {
    let pg = common::postgres().await?;
    let (_elasticmq, endpoint) = common::elasticmq().await?;
    let sqs = aws_sdk_sqs::Client::new(&common::aws(&endpoint).await);
    let queue_url = common::create_queue(&sqs, "events").await?; // standard: duplicates stay visible

    let mut active = RelayProcess::start(&pg, &queue_url, &endpoint)?;
    pg.wait_until_streaming().await?;

    // A standby waiting for the slot stops at once.
    let mut standby = RelayProcess::start(&pg, &queue_url, &endpoint)?;
    sleep(Duration::from_secs(1)).await;
    standby.terminate()?;
    let status = standby.exited(Duration::from_secs(3)).await?;
    assert!(status.is_some_and(|s| s.success()), "standby: {status:?}");

    // A deploy mid-stream, while the application keeps writing.
    pg.insert(0, 500, 50, 25).await?;
    let mut received = Vec::new();
    common::receive(
        &sqs,
        &queue_url,
        &mut received,
        |r| r.len() >= 100,
        Duration::from_secs(30),
    )
    .await?;
    active.terminate()?;
    pg.insert(500, 500, 50, 25).await?;
    let status = active.exited(Duration::from_secs(15)).await?;
    assert!(
        status.is_some_and(|s| s.success()),
        "the drain did not finish cleanly: {status:?}"
    );

    // The next instance starts exactly where the final ack left off.
    let _next = RelayProcess::start(&pg, &queue_url, &endpoint)?;
    common::receive(
        &sqs,
        &queue_url,
        &mut received,
        |r| r.len() >= 1_000,
        Duration::from_secs(60),
    )
    .await?;
    // Late duplicates would arrive now.
    common::receive(
        &sqs,
        &queue_url,
        &mut received,
        |_| false,
        Duration::from_secs(3),
    )
    .await?;
    assert_eq!(common::ids(&received), pg.ids().await?);
    assert_eq!(received.len(), 1_000, "a clean stop must not cause replays");
    Ok(())
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_second_signal_stops_a_drain_stuck_on_a_dead_broker() -> anyhow::Result<()> {
    let pg = common::postgres().await?;
    let (elasticmq, endpoint) = common::elasticmq().await?;
    let sqs = aws_sdk_sqs::Client::new(&common::aws(&endpoint).await);
    let queue_url = common::create_queue(&sqs, "events").await?;
    let mut relay = RelayProcess::start(&pg, &queue_url, &endpoint)?;
    pg.wait_until_streaming().await?;

    elasticmq.pause().await?; // the broker hangs
    pg.insert(0, 20, 20, 20).await?;
    sleep(Duration::from_secs(2)).await;
    relay.terminate()?;
    assert!(
        relay.exited(Duration::from_secs(3)).await?.is_none(),
        "the drain gave up on unpublished events"
    );
    relay.terminate()?;
    let status = relay.exited(Duration::from_secs(5)).await?;
    assert!(status.is_some_and(|s| s.success()), "{status:?}");

    // Nothing is lost: once the broker is back, the next instance publishes it all.
    elasticmq.unpause().await?;
    let _next = RelayProcess::start(&pg, &queue_url, &endpoint)?;
    let inserted = pg.ids().await?;
    let mut received = Vec::new();
    common::receive(
        &sqs,
        &queue_url,
        &mut received,
        |r| common::ids(r) == inserted,
        Duration::from_secs(60),
    )
    .await?;
    assert_eq!(common::ids(&received), inserted);
    Ok(())
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn sigkill_mid_stream_loses_nothing() -> anyhow::Result<()> {
    let pg = common::postgres().await?;
    let (elasticmq, endpoint) = common::elasticmq().await?;
    let sqs = aws_sdk_sqs::Client::new(&common::aws(&endpoint).await);
    let queue_url = common::create_queue(&sqs, "events").await?;
    let mut received = Vec::new();

    // Hard kills with writes in flight. 50-row transactions over 50 aggregates span
    // several batches: the case M1 could lose.
    for round in 0..2 {
        let mut relay = RelayProcess::start(&pg, &queue_url, &endpoint)?;
        pg.insert(round * 500, 500, 50, 50).await?;
        let seen = received.len();
        common::receive(
            &sqs,
            &queue_url,
            &mut received,
            |r| r.len() >= seen + 100,
            Duration::from_secs(30),
        )
        .await?;
        relay.kill()?;
    }

    // The hardest case: the relay has read whole transactions it cannot publish, then dies.
    let mut relay = RelayProcess::start(&pg, &queue_url, &endpoint)?;
    let so_far = pg.ids().await?;
    common::receive(
        &sqs,
        &queue_url,
        &mut received,
        |r| common::ids(r) == so_far,
        Duration::from_secs(60),
    )
    .await?;
    elasticmq.pause().await?;
    pg.insert(1_000, 100, 20, 5).await?;
    // Longer than the ack interval: an ack that ran ahead of publishing would reach Postgres.
    sleep(Duration::from_secs(3)).await;
    relay.kill()?;
    elasticmq.unpause().await?;

    let _relay = RelayProcess::start(&pg, &queue_url, &endpoint)?;
    let inserted = pg.ids().await?;
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
    // Duplicates are allowed; each one repeats an id that was already delivered.
    Ok(())
}
