//! The relay pipeline:
//! source → bounded channel → buffer → batch → publish/retry → checkpoint → ack → source.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep, sleep_until};

use super::Health;
use crate::domain::{Checkpoint, Lsn, OutboxEvent, SourceMsg, backoff, take_batch};
use crate::ports::{EventSink, EventSource, PublishError};

/// How far the source may run ahead of the sink before it has to wait.
// ponytail: fixed; make it configurable if a benchmark says it matters
const CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Batching {
    /// Events per publish call. Sinks split further if their API needs it.
    pub max_events: usize,
    /// How long a buffered event waits for company before it is flushed anyway.
    pub max_wait_ms: u64,
}

impl Default for Batching {
    fn default() -> Self {
        Self {
            max_events: 10,
            max_wait_ms: 20,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Retry {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for Retry {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 100,
            max_backoff_ms: 30_000,
        }
    }
}

/// Relays events from `source` to `sink` until the source stops.
///
/// A source error is returned right away: nothing unpublished was acked, so a
/// restart replays it. A clean stop first publishes everything the source sent.
pub async fn run(
    source: impl EventSource,
    sink: impl EventSink,
    batching: Batching,
    retry: Retry,
    health: Arc<Health>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        batching.max_events > 0,
        "batching.max_events must be at least 1"
    );

    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (ack, acked) = watch::channel(Lsn::default());
    // A sink that could be constructed counts as reachable until a publish fails.
    health.sink_ready.store(true, Ordering::Relaxed);

    let core = Core {
        sink,
        batching,
        retry,
        health,
        ack,
        checkpoint: Checkpoint::default(),
    };
    let core = core.run(rx);
    tokio::pin!(core);
    tokio::select! {
        result = source.run(tx, acked) => {
            result?;
            core.await;
            Ok(())
        }
        () = &mut core => Ok(()),
    }
}

struct Core<K> {
    sink: K,
    batching: Batching,
    retry: Retry,
    health: Arc<Health>,
    ack: watch::Sender<Lsn>,
    checkpoint: Checkpoint,
}

impl<K: EventSink> Core<K> {
    async fn run(mut self, mut rx: mpsc::Receiver<SourceMsg>) {
        let max_wait = Duration::from_millis(self.batching.max_wait_ms);
        let mut buffer = VecDeque::new();
        // When the oldest buffered event has waited long enough.
        let mut deadline: Option<Instant> = None;

        loop {
            metrics::gauge!("pg_outbox_channel_depth").set(rx.len() as f64);

            let waited_enough = deadline.is_some_and(|d| d <= Instant::now());
            if buffer.len() >= self.batching.max_events || waited_enough {
                self.flush(&mut buffer).await;
                deadline = (!buffer.is_empty()).then(|| Instant::now() + max_wait);
                continue;
            }

            tokio::select! {
                msg = rx.recv() => match msg {
                    Some(SourceMsg::Event(event)) => {
                        self.checkpoint.track(event.commit_lsn);
                        deadline.get_or_insert_with(|| Instant::now() + max_wait);
                        buffer.push_back(event);
                    }
                    Some(SourceMsg::Progress(lsn)) => {
                        self.checkpoint.observe(lsn);
                        self.send_ack();
                    }
                    None => break,
                },
                () = sleep_until(deadline.unwrap_or_else(Instant::now)), if deadline.is_some() => {}
            }
        }

        // The source stopped cleanly: publish what it sent.
        while !buffer.is_empty() {
            self.flush(&mut buffer).await;
        }
    }

    /// Publishes one batch, retrying until every event is published or permanently rejected.
    // ponytail: one batch in flight at a time; pipeline batches with disjoint keys if throughput needs it
    async fn flush(&mut self, buffer: &mut VecDeque<OutboxEvent>) {
        let initial = Duration::from_millis(self.retry.initial_backoff_ms);
        let max = Duration::from_millis(self.retry.max_backoff_ms);
        let mut pending = take_batch(buffer, self.batching.max_events);
        let mut attempt = 0;

        loop {
            let results = self.sink.publish(&pending).await;
            assert_eq!(
                results.len(),
                pending.len(),
                "sink {} must return one result per event",
                K::NAME
            );

            let mut failed = Vec::new();
            let mut last_error = String::new();
            for (event, result) in pending.into_iter().zip(results) {
                match result {
                    Ok(()) => {
                        metrics::counter!("pg_outbox_events_published_total", "sink" => K::NAME)
                            .increment(1);
                        let latency = SystemTime::now()
                            .duration_since(event.committed_at)
                            .unwrap_or_default();
                        metrics::histogram!("pg_outbox_publish_latency_seconds")
                            .record(latency.as_secs_f64());
                        self.checkpoint.confirm(event.commit_lsn);
                    }
                    Err(PublishError::Permanent(reason)) => {
                        // ponytail: M1 keeps a poison event only in this log line; M2 adds outbox_dead_letter
                        tracing::error!(
                            event_id = %event.id,
                            lsn = %event.commit_lsn,
                            %reason,
                            envelope = %event.envelope(),
                            "event rejected permanently, skipping it"
                        );
                        metrics::counter!("pg_outbox_publish_errors_total", "sink" => K::NAME, "kind" => "permanent")
                            .increment(1);
                        metrics::counter!("pg_outbox_dead_letters_total").increment(1);
                        self.checkpoint.confirm(event.commit_lsn);
                    }
                    Err(PublishError::Retryable(reason)) => {
                        metrics::counter!("pg_outbox_publish_errors_total", "sink" => K::NAME, "kind" => "retryable")
                            .increment(1);
                        last_error = reason;
                        failed.push(event);
                    }
                }
            }
            self.send_ack();

            if failed.is_empty() {
                break;
            }
            self.health.sink_ready.store(false, Ordering::Relaxed);
            let delay = backoff(attempt, initial, max, fastrand::f64());
            tracing::warn!(
                failed = failed.len(),
                attempt,
                retry_in_ms = delay.as_millis() as u64,
                error = %last_error,
                "publish failed, retrying"
            );
            sleep(delay).await;
            attempt = attempt.saturating_add(1);
            pending = failed;
        }
        self.health.sink_ready.store(true, Ordering::Relaxed);
    }

    fn send_ack(&mut self) {
        let lsn = self.checkpoint.safe_lsn();
        self.ack
            .send_if_modified(|acked| std::mem::replace(acked, lsn) != lsn);
    }
}
