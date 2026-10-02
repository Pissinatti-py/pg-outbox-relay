//! SNS sink: publishes envelopes with `PublishBatch`. SNS has SQS's batch limits and id
//! rules, so splitting, id mapping and error classification come from `sqs.rs`.

use anyhow::Context;
use aws_sdk_sns::Client;
use aws_sdk_sns::error::DisplayErrorContext;
use aws_sdk_sns::types::{MessageAttributeValue, PublishBatchRequestEntry};
use serde::Deserialize;

use super::sqs::{batches, classify, size, sqs_id};
use crate::domain::OutboxEvent;
use crate::ports::{EventSink, PublishError};

#[derive(Debug, Clone, Deserialize)]
pub struct SnsConfig {
    /// A `.fifo` topic keeps per-aggregate order and deduplicates on the event id.
    pub topic_arn: String,
}

#[derive(Clone)]
pub struct SnsSink {
    client: Client,
    topic_arn: String,
    fifo: bool,
}

impl SnsSink {
    /// Uses the standard AWS configuration chain, like the SQS sink.
    pub async fn connect(config: SnsConfig) -> anyhow::Result<Self> {
        Self::with_client(Client::new(&aws_config::load_from_env().await), config).await
    }

    /// Checks the topic first, so a wrong ARN or a missing permission fails at startup.
    pub async fn with_client(client: Client, config: SnsConfig) -> anyhow::Result<Self> {
        client
            .get_topic_attributes()
            .topic_arn(&config.topic_arn)
            .send()
            .await
            .map_err(|error| anyhow::anyhow!("{}", DisplayErrorContext(error)))
            .with_context(|| format!("cannot use SNS topic {}", config.topic_arn))?;
        Ok(Self {
            client,
            fifo: config.topic_arn.ends_with(".fifo"),
            topic_arn: config.topic_arn,
        })
    }

    async fn send_batch(
        &self,
        events: &[OutboxEvent],
        bodies: &[String],
    ) -> Vec<Result<(), PublishError>> {
        let entries = events
            .iter()
            .zip(bodies)
            .enumerate()
            .map(|(index, (event, body))| entry(index, event, body, self.fifo))
            .collect();
        let sent = self
            .client
            .publish_batch()
            .topic_arn(&self.topic_arn)
            .set_publish_batch_request_entries(Some(entries))
            .send()
            .await;

        let output = match sent {
            Ok(output) => output,
            Err(error) => {
                // As in SQS: the call failed, so retry it all, except a lone event too large to ever fit.
                let too_large = events.len() == 1
                    && error
                        .as_service_error()
                        .is_some_and(|e| e.is_batch_request_too_long_exception());
                let reason = DisplayErrorContext(&error).to_string();
                let failure = if too_large {
                    PublishError::Permanent(reason)
                } else {
                    PublishError::Retryable(reason)
                };
                return vec![Err(failure); events.len()];
            }
        };

        // Anything SNS does not report back is retried, never assumed published.
        let mut results = vec![
            Err(PublishError::Retryable(
                "missing from the SNS response".into()
            ));
            events.len()
        ];
        for ok in output.successful() {
            if let Some(result) = ok
                .id()
                .and_then(|id| id.parse().ok())
                .and_then(|i: usize| results.get_mut(i))
            {
                *result = Ok(());
            }
        }
        for failed in output.failed() {
            if let Some(result) = failed
                .id()
                .parse()
                .ok()
                .and_then(|i: usize| results.get_mut(i))
            {
                *result = Err(classify(
                    failed.sender_fault(),
                    failed.code(),
                    failed.message().unwrap_or_default(),
                ));
            }
        }
        results
    }
}

impl EventSink for SnsSink {
    const NAME: &'static str = "sns";

    async fn publish(&self, events: &[OutboxEvent]) -> Vec<Result<(), PublishError>> {
        let bodies: Vec<String> = events.iter().map(OutboxEvent::envelope).collect();
        let sizes: Vec<usize> = events
            .iter()
            .zip(&bodies)
            .map(|(e, body)| size(e, body))
            .collect();
        let mut results = Vec::with_capacity(events.len());
        for range in batches(&sizes) {
            results.extend(
                self.send_batch(&events[range.clone()], &bodies[range])
                    .await,
            );
        }
        results
    }
}

fn entry(index: usize, event: &OutboxEvent, body: &str, fifo: bool) -> PublishBatchRequestEntry {
    let attribute = |value: &str| {
        MessageAttributeValue::builder()
            .data_type("String")
            .string_value(value)
            .build()
            .expect("data type is set")
    };
    let mut entry = PublishBatchRequestEntry::builder()
        .id(index.to_string())
        .message(body)
        // Subscribers can deduplicate on `id` and filter on `event_type` without parsing the body.
        .message_attributes("id", attribute(&event.id))
        .message_attributes("source", attribute(&event.source));
    if !event.event_type.is_empty() {
        entry = entry.message_attributes("event_type", attribute(&event.event_type));
    }
    if fifo {
        entry = entry
            .message_group_id(sqs_id(&event.ordering_key()))
            .message_deduplication_id(sqs_id(&event.dedup_key()));
    }
    entry.build().expect("id and message are set")
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use serde_json::value::RawValue;

    use super::*;
    use crate::domain::Lsn;

    fn event() -> OutboxEvent {
        OutboxEvent {
            id: "7c9e6679-7425-40de-944b-e07fc1f90ae7".into(),
            source: "acme".into(),
            aggregate_type: "policy".into(),
            aggregate_id: "42".into(),
            event_type: "policy.approved".into(),
            occurred_at: "2026-09-28T14:03:11Z".into(),
            headers: RawValue::from_string("{}".into()).unwrap(),
            payload: RawValue::from_string("{}".into()).unwrap(),
            commit_lsn: Lsn(1),
            committed_at: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn fifo_entries_carry_group_and_dedup_ids() {
        let event = event();
        let entry = entry(3, &event, &event.envelope(), true);
        assert_eq!(entry.id(), "3");
        assert_eq!(entry.message(), event.envelope());
        assert_eq!(entry.message_group_id(), Some("acme:policy:42"));
        // An id is unique only within its database, so tenants never share a dedup id.
        assert_eq!(
            entry.message_deduplication_id(),
            Some("acme:7c9e6679-7425-40de-944b-e07fc1f90ae7")
        );
        let attributes = entry.message_attributes().unwrap();
        assert_eq!(attributes["id"].string_value(), Some(event.id.as_str()));
        assert_eq!(attributes["source"].string_value(), Some("acme"));
        assert_eq!(
            attributes["event_type"].string_value(),
            Some("policy.approved")
        );
    }

    #[test]
    fn standard_entries_have_no_fifo_fields() {
        let event = event();
        let entry = entry(0, &event, &event.envelope(), false);
        assert_eq!(entry.message_group_id(), None);
        assert_eq!(entry.message_deduplication_id(), None);
    }
}
