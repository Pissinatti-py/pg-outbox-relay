//! Redis Streams sink: `XADD` to one stream per aggregate type.

use anyhow::Context;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::Deserialize;

use crate::domain::OutboxEvent;
use crate::ports::{EventSink, PublishError};

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    /// `redis://[user:password@]host:6379[/db]`, or `rediss://` for TLS.
    pub url: String,
    /// Events of aggregate type `t` go to the stream `<stream_prefix>t`.
    #[serde(default = "default_stream_prefix")]
    pub stream_prefix: String,
}

fn default_stream_prefix() -> String {
    "outbox:".into()
}

#[derive(Clone)]
pub struct RedisSink {
    redis: ConnectionManager,
    stream_prefix: String,
}

impl RedisSink {
    /// Connects at startup, so a wrong URL or password fails right away.
    pub async fn connect(config: RedisConfig) -> anyhow::Result<Self> {
        // rustls is built with two crypto backends, so it needs a process-wide choice before
        // redis builds its TLS config. pgwire-replication makes the same choice.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = redis::Client::open(config.url.as_str())
            .context("sink.url must look like redis://host:6379")?;
        let redis = ConnectionManager::new(client)
            .await
            .context("cannot connect to Redis")?;
        Ok(Self {
            redis,
            stream_prefix: config.stream_prefix,
        })
    }
}

impl EventSink for RedisSink {
    const NAME: &'static str = "redis";

    /// Redis never rejects an entry for its content, so every failure is retryable.
    // ponytail: one XADD round trip per event; pipeline the batch if a benchmark of this sink asks for it
    async fn publish(&self, events: &[OutboxEvent]) -> Vec<Result<(), PublishError>> {
        let mut redis = self.redis.clone();
        let mut results = Vec::with_capacity(events.len());
        for event in events {
            let added: redis::RedisResult<String> = redis
                .xadd(stream(&self.stream_prefix, event), "*", &fields(event))
                .await;
            results.push(
                added
                    .map(drop)
                    .map_err(|error| PublishError::Retryable(error.to_string())),
            );
        }
        results
    }
}

/// An aggregate's events all go to one stream, in commit order, so they stay in order.
fn stream(prefix: &str, event: &OutboxEvent) -> String {
    format!("{prefix}{}", event.aggregate_type)
}

/// `envelope` is the JSON the SQS and SNS sinks send as the body; `id` and `event_type`
/// allow deduplication and filtering without parsing it.
fn fields(event: &OutboxEvent) -> [(&'static str, String); 4] {
    [
        ("id", event.id.clone()),
        ("source", event.source.clone()),
        ("event_type", event.event_type.clone()),
        ("envelope", event.envelope()),
    ]
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use serde_json::value::RawValue;

    use super::*;
    use crate::domain::Lsn;

    fn event(aggregate_id: &str) -> OutboxEvent {
        OutboxEvent {
            id: "7c9e6679-7425-40de-944b-e07fc1f90ae7".into(),
            source: "acme".into(),
            aggregate_type: "policy".into(),
            aggregate_id: aggregate_id.into(),
            event_type: "policy.approved".into(),
            occurred_at: "2026-09-28T14:03:11Z".into(),
            headers: RawValue::from_string("{}".into()).unwrap(),
            payload: RawValue::from_string("{}".into()).unwrap(),
            commit_lsn: Lsn(1),
            committed_at: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn one_stream_per_aggregate_type_with_the_envelope_as_a_field() {
        let event = event("42");
        assert_eq!(stream("outbox:", &event), "outbox:policy");
        assert_eq!(
            fields(&event),
            [
                ("id", event.id.clone()),
                ("source", "acme".into()),
                ("event_type", "policy.approved".into()),
                ("envelope", event.envelope()),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tls_urls_fail_cleanly_without_a_server() {
        // Without a process-wide rustls provider, redis panics while building its TLS config.
        let config = RedisConfig {
            url: "rediss://127.0.0.1:1".into(),
            stream_prefix: "outbox:".into(),
        };
        assert!(RedisSink::connect(config).await.is_err());
    }
}
