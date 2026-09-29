//! Composition root: the only place that knows every layer.

use std::process::ExitCode;
use std::sync::Arc;

use pg_outbox_relay::adapters::http;
use pg_outbox_relay::adapters::postgres::{PgDeadLetters, PgSource};
use pg_outbox_relay::adapters::sns::SnsSink;
use pg_outbox_relay::adapters::sqs::SqsSink;
use pg_outbox_relay::app::{Health, relay};
use pg_outbox_relay::config::{Config, SinkConfig};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        // pgwire_replication logs errors it also returns to us, and we log those ourselves.
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pgwire_replication=off".into()),
        )
        .init();

    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = format!("{error:#}"), "relay stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "relay.toml".into());
    let config = Config::load(&path)?;
    let metrics = http::install_metrics()?;
    let health = Arc::new(Health::default());
    let (stop, stopped) = watch::channel(false);

    let dead_letters = PgDeadLetters::new(&config.source)?;
    let source = PgSource::new(config.source, health.clone(), stopped);
    let relay = async {
        // One arm per sink: relay::run is compiled for each concrete sink, with no trait objects.
        let (batching, retry, health) = (config.batching, config.retry, health.clone());
        match config.sink {
            SinkConfig::Sqs(sqs) => {
                let sink = SqsSink::connect(sqs).await?;
                relay::run(source, sink, dead_letters, batching, retry, health).await
            }
            SinkConfig::Sns(sns) => {
                let sink = SnsSink::connect(sns).await?;
                relay::run(source, sink, dead_letters, batching, retry, health).await
            }
        }
    };
    tokio::pin!(relay);

    // Whichever finishes first ends the process. Crashing is safe: Postgres replays
    // everything unacknowledged on the next start.
    tokio::select! {
        result = &mut relay => result,
        result = http::serve(config.server.listen, metrics, health.clone()) => result,
        signal = shutdown_signal() => {
            tracing::info!("{} received, draining: publishing what was read, then sending a final ack", signal?);
            stop.send_replace(true);
            // During a broker outage the drain waits; a second signal, or the orchestrator's
            // SIGKILL, ends it, and unacknowledged events replay on the next start.
            tokio::select! {
                result = &mut relay => result,
                signal = shutdown_signal() => {
                    tracing::warn!("{} received again, stopping without a final ack", signal?);
                    Ok(())
                }
            }
        }
    }
}

async fn shutdown_signal() -> anyhow::Result<&'static str> {
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok("SIGINT"),
        _ = terminate.recv() => Ok("SIGTERM"),
    }
}
