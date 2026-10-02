//! Composition root: the only place that knows every layer.

use std::process::ExitCode;
use std::sync::Arc;

use pg_outbox_relay::adapters::http;
use pg_outbox_relay::adapters::postgres::{PgConfig, PgDeadLetters, PgSource};
use pg_outbox_relay::adapters::redis::RedisSink;
use pg_outbox_relay::adapters::sns::SnsSink;
use pg_outbox_relay::adapters::sqs::SqsSink;
use pg_outbox_relay::app::relay::{Batching, Retry};
use pg_outbox_relay::app::{Health, relay};
use pg_outbox_relay::config::{Config, SinkConfig};
use pg_outbox_relay::ports::EventSink;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tokio::task::JoinSet;
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
    let sources = config.source.sources()?;
    let metrics = http::install_metrics()?;
    let healths: Vec<Arc<Health>> = sources.iter().map(|_| Arc::default()).collect();
    let (stop, stopped) = watch::channel(false);

    let relay = async {
        let (healths, batching, retry) = (healths.clone(), config.batching, config.retry);
        // One arm per sink: relay_all is compiled for each concrete sink, with no trait objects.
        match config.sink {
            SinkConfig::Sqs(sqs) => {
                let sink = SqsSink::connect(sqs).await?;
                relay_all(sink, sources, healths, batching, retry, stopped).await
            }
            SinkConfig::Sns(sns) => {
                let sink = SnsSink::connect(sns).await?;
                relay_all(sink, sources, healths, batching, retry, stopped).await
            }
            SinkConfig::Redis(redis) => {
                let sink = RedisSink::connect(redis).await?;
                relay_all(sink, sources, healths, batching, retry, stopped).await
            }
        }
    };
    tokio::pin!(relay);

    // Whichever finishes first ends the process. Crashing is safe: Postgres replays
    // everything unacknowledged on the next start.
    tokio::select! {
        result = &mut relay => result,
        result = http::serve(config.server.listen, metrics, healths.clone()) => result,
        signal = shutdown_signal() => {
            tracing::info!("{} received, draining: publishing what was read, then sending a final ack", signal?);
            stop.send_replace(true);
            // During a broker outage the drain waits; a second signal, or the orchestrator's
            // SIGKILL, ends it, and unacknowledged events replay on the next start.
            // ponytail: no drain timeout of our own; add a setting if grace periods prove too short
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

/// Relays every source until the relay stops, one supervised pipeline each, all publishing
/// through clones of `sink`: a failing source restarts alone and the others keep going.
async fn relay_all<K: EventSink + Clone>(
    sink: K,
    sources: Vec<PgConfig>,
    healths: Vec<Arc<Health>>,
    batching: Batching,
    retry: Retry,
    stopped: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut pipelines = JoinSet::new();
    for (config, health) in sources.into_iter().zip(healths) {
        let name = config.database()?;
        let (sink, batching, retry, stopped) = (
            sink.clone(),
            batching.clone(),
            retry.clone(),
            stopped.clone(),
        );
        pipelines.spawn(async move {
            relay::supervise(&name, stopped.clone(), || {
                // Each attempt starts fresh: the source owns the replication connection.
                let source = PgSource::new(config.clone(), health.clone(), stopped.clone());
                let dead_letters = PgDeadLetters::new(&config);
                let (name, sink, batching, retry, health) = (
                    name.clone(),
                    sink.clone(),
                    batching.clone(),
                    retry.clone(),
                    health.clone(),
                );
                async move {
                    relay::run(&name, source, sink, dead_letters?, batching, retry, health).await
                }
            })
            .await
        });
    }
    // Pipelines end only when the relay stops, each finishing its drain even if another one's
    // failed. A panic is a bug: crash, and let the orchestrator restart.
    let mut failed = 0;
    while let Some(ended) = pipelines.join_next().await {
        if let Err(error) = ended? {
            tracing::error!(error = format!("{error:#}"), "drain failed");
            failed += 1;
        }
    }
    anyhow::ensure!(
        failed == 0,
        "{failed} of the sources could not finish draining; the next start replays what they had not acknowledged"
    );
    Ok(())
}

async fn shutdown_signal() -> anyhow::Result<&'static str> {
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok("SIGINT"),
        _ = terminate.recv() => Ok("SIGTERM"),
    }
}
