//! Composition root: the only place that knows every layer.

use std::process::ExitCode;
use std::sync::Arc;

use pg_outbox_relay::adapters::http;
use pg_outbox_relay::adapters::postgres::{PgDeadLetters, PgSource};
use pg_outbox_relay::adapters::sqs::SqsSink;
use pg_outbox_relay::app::{Health, relay};
use pg_outbox_relay::config::{Config, SinkConfig};
use tokio::signal::unix::{SignalKind, signal};
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

    let dead_letters = PgDeadLetters::new(&config.source)?;
    let source = PgSource::new(config.source, health.clone());
    let relay = async {
        let sink = match config.sink {
            SinkConfig::Sqs(sqs) => SqsSink::connect(sqs).await?,
        };
        relay::run(
            source,
            sink,
            dead_letters,
            config.batching,
            config.retry,
            health.clone(),
        )
        .await
    };

    // Whichever finishes first ends the process. Crashing is safe: Postgres replays
    // everything unacknowledged on the next start.
    tokio::select! {
        result = relay => result,
        result = http::serve(config.server.listen, metrics, health.clone()) => result,
        signal = shutdown_signal() => {
            tracing::info!("{} received, stopping; unacknowledged events replay on the next start", signal?);
            Ok(())
        }
    }
}

// ponytail: stops right away; M2 drains in-flight batches and sends a final ack first
async fn shutdown_signal() -> anyhow::Result<&'static str> {
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok("SIGINT"),
        _ = terminate.recv() => Ok("SIGTERM"),
    }
}
