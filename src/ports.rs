//! The contract between the relay core and the outside world. Adapters implement these.

use std::future::Future;

use tokio::sync::{mpsc, watch};

use crate::domain::{Lsn, OutboxEvent, SourceMsg};

/// Where outbox events come from: the Postgres replication stream in production.
pub trait EventSource: Send + 'static {
    /// Pushes messages into `out` in commit order, and reports the latest value of
    /// `acked` back to the database as its confirmed position.
    ///
    /// Contract: when started again after a crash, the source resumes from the last
    /// position it reported, so every unacknowledged event is delivered again.
    fn run(
        self,
        out: mpsc::Sender<SourceMsg>,
        acked: watch::Receiver<Lsn>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

/// Where events are published: SQS in M1.
pub trait EventSink: Send + Sync + 'static {
    /// Metrics label, e.g. `"sqs"`.
    const NAME: &'static str;

    /// Publishes `events` in order. Returns exactly one result per event, in the same order.
    fn publish(
        &self,
        events: &[OutboxEvent],
    ) -> impl Future<Output = Vec<Result<(), PublishError>>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishError {
    /// May succeed later: throttling, network, broker down, bad credentials. Retried forever;
    /// the replication slot keeps the WAL meanwhile.
    Retryable(String),
    /// Will never succeed, e.g. the broker rejected the message itself. Skipped so one
    /// bad row cannot block the stream.
    Permanent(String),
}
