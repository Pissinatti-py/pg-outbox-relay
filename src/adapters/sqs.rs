//! SQS sink: publishes envelopes with `SendMessageBatch`.

use std::ops::Range;

use anyhow::Context;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::error::DisplayErrorContext;
use aws_sdk_sqs::types::{MessageAttributeValue, SendMessageBatchRequestEntry};
use serde::Deserialize;

use crate::domain::OutboxEvent;
use crate::ports::{EventSink, PublishError};

/// `SendMessageBatch` limits: entries per call, and bytes per call (the classic 256 KiB;
/// queues allowing 1 MiB messages still accept it, it just means more calls).
const MAX_ENTRIES: usize = 10;
const MAX_BATCH_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Deserialize)]
pub struct SqsConfig {
    /// A `.fifo` URL enables per-aggregate ordering and deduplication.
    pub queue_url: String,
}

#[derive(Clone)]
pub struct SqsSink {
    client: Client,
    queue_url: String,
    fifo: bool,
}

impl SqsSink {
    /// Uses the standard AWS configuration chain (env, profile, IAM role; `AWS_ENDPOINT_URL`
    /// for a local SQS such as ElasticMQ).
    pub async fn connect(config: SqsConfig) -> anyhow::Result<Self> {
        Self::with_client(Client::new(&aws_config::load_from_env().await), config).await
    }

    /// Checks the queue first, so a wrong URL or a missing permission fails at startup.
    pub async fn with_client(client: Client, config: SqsConfig) -> anyhow::Result<Self> {
        client
            .get_queue_attributes()
            .queue_url(&config.queue_url)
            .send()
            .await
            .map_err(|error| anyhow::anyhow!("{}", DisplayErrorContext(error)))
            .with_context(|| format!("cannot use SQS queue {}", config.queue_url))?;
        Ok(Self {
            client,
            fifo: config.queue_url.ends_with(".fifo"),
            queue_url: config.queue_url,
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
            .send_message_batch()
            .queue_url(&self.queue_url)
            .set_entries(Some(entries))
            .send()
            .await;

        let output = match sent {
            Ok(output) => output,
            Err(error) => {
                // The whole call failed, so nothing was judged on its content: retry it all.
                // Except a lone event too large to ever fit, which is skipped instead of stalling.
                let too_large = events.len() == 1
                    && error
                        .as_service_error()
                        .is_some_and(|e| e.is_batch_request_too_long());
                let reason = DisplayErrorContext(&error).to_string();
                let failure = if too_large {
                    PublishError::Permanent(reason)
                } else {
                    PublishError::Retryable(reason)
                };
                return vec![Err(failure); events.len()];
            }
        };

        // Anything SQS does not report back is retried, never assumed published.
        let mut results = vec![
            Err(PublishError::Retryable(
                "missing from the SQS response".into()
            ));
            events.len()
        ];
        for ok in output.successful() {
            if let Some(result) = ok.id().parse().ok().and_then(|i: usize| results.get_mut(i)) {
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

impl EventSink for SqsSink {
    const NAME: &'static str = "sqs";

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

fn entry(
    index: usize,
    event: &OutboxEvent,
    body: &str,
    fifo: bool,
) -> SendMessageBatchRequestEntry {
    let attribute = |value: &str| {
        MessageAttributeValue::builder()
            .data_type("String")
            .string_value(value)
            .build()
            .expect("data type is set")
    };
    let mut entry = SendMessageBatchRequestEntry::builder()
        .id(index.to_string())
        .message_body(body)
        // Lets consumers of standard queues deduplicate without parsing the body.
        .message_attributes("id", attribute(&event.id))
        .message_attributes("source", attribute(&event.source));
    if !event.event_type.is_empty() {
        entry = entry.message_attributes("event_type", attribute(&event.event_type));
    }
    if fifo {
        entry = entry
            .message_group_id(sqs_id(&event.ordering_key()))
            .message_deduplication_id(sqs_id(&event.id));
    }
    entry.build().expect("id and body are set")
}

/// SQS and SNS group and deduplication ids allow up to 128 printable ASCII characters. The mapping
/// is deterministic, so a clipped key can only merge groups (less parallelism), never
/// split an aggregate across groups (lost ordering).
pub(super) fn sqs_id(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_ascii_graphic() { c } else { '_' })
        .take(128)
        .collect()
}

/// Bytes SQS and SNS count for an entry: the body plus its message attributes, with some slack.
pub(super) fn size(event: &OutboxEvent, body: &str) -> usize {
    // Values of the id, event_type and source attributes; 64 covers their names and types.
    body.len() + event.id.len() + event.event_type.len() + event.source.len() + 64
}

/// Splits entries into calls within `SendMessageBatch` (and `PublishBatch`) limits. An entry too large for
/// any call still gets one of its own, so SQS rejects only that entry.
pub(super) fn batches(sizes: &[usize]) -> Vec<Range<usize>> {
    let mut batches = Vec::new();
    let (mut start, mut bytes) = (0, 0);
    for (i, &size) in sizes.iter().enumerate() {
        if i > start && (i - start == MAX_ENTRIES || bytes + size > MAX_BATCH_BYTES) {
            batches.push(start..i);
            (start, bytes) = (i, 0);
        }
        bytes += size;
    }
    if start < sizes.len() {
        batches.push(start..sizes.len());
    }
    batches
}

/// Only content SQS or SNS will never accept is permanent. Other entry errors are retried,
/// even with `sender_fault` set (e.g. KMS permissions): a stall is visible and loses nothing,
/// while skipping would silently drop events. The codes differ per service, so one list
/// serves both: no SQS code ends in SNS's `InvalidParameter`.
pub(super) fn classify(sender_fault: bool, code: &str, message: &str) -> PublishError {
    const CONTENT_ERRORS: [&str; 5] = [
        "InvalidParameterValue",
        "InvalidMessageContents",
        "InvalidAttributeValue",
        "MessageTooLong",
        "InvalidParameter", // SNS
    ];
    let reason = format!("{code}: {message}");
    if sender_fault && CONTENT_ERRORS.iter().any(|content| code.ends_with(content)) {
        PublishError::Permanent(reason)
    } else {
        PublishError::Retryable(reason)
    }
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
    fn fifo_entries_carry_group_and_dedup_ids() {
        let event = event("42");
        let entry = entry(3, &event, &event.envelope(), true);
        assert_eq!(entry.id(), "3");
        assert_eq!(entry.message_body(), event.envelope());
        assert_eq!(entry.message_group_id(), Some("acme:policy:42"));
        assert_eq!(entry.message_deduplication_id(), Some(event.id.as_str()));
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
        let event = event("42");
        let entry = entry(0, &event, &event.envelope(), false);
        assert_eq!(entry.message_group_id(), None);
        assert_eq!(entry.message_deduplication_id(), None);
    }

    #[test]
    fn group_ids_are_valid_and_deterministic() {
        assert_eq!(sqs_id("policy:42"), "policy:42");
        assert_eq!(sqs_id("policy:São Paulo"), "policy:S_o_Paulo");
        assert_eq!(sqs_id(&"x".repeat(300)).len(), 128);
    }

    #[test]
    fn size_covers_every_byte_sqs_and_sns_count() {
        let event = OutboxEvent {
            source: "s".repeat(63), // the longest a database name gets
            ..event("42")
        };
        let body = event.envelope();
        // They count the body, then each attribute's name, data type and value.
        let counted = body.len()
            + [
                ("id", event.id.as_str()),
                ("event_type", event.event_type.as_str()),
                ("source", event.source.as_str()),
            ]
            .iter()
            .map(|(name, value)| name.len() + "String".len() + value.len())
            .sum::<usize>();
        let estimated = size(&event, &body);
        assert!(
            estimated >= counted,
            "estimated {estimated} < counted {counted}"
        );
    }

    #[test]
    fn splits_calls_by_entries_and_bytes() {
        assert_eq!(batches(&[1; 25]), [0..10, 10..20, 20..25]);
        assert_eq!(batches(&[200_000, 100_000, 10_000]), [0..1, 1..3]);
        assert_eq!(batches(&[10_000, 300_000, 10_000]), [0..1, 1..2, 2..3]);
        assert_eq!(batches(&[]), Vec::<Range<usize>>::new());
    }

    #[test]
    fn only_rejected_content_is_permanent() {
        let permanent = |e| matches!(e, PublishError::Permanent(_));
        assert!(permanent(classify(
            true,
            "InvalidParameterValue",
            "Message must be shorter than 262144 bytes"
        )));
        assert!(permanent(classify(
            true,
            "AWS.SimpleQueueService.InvalidMessageContents",
            ""
        )));
        assert!(permanent(classify(
            true,
            "InvalidParameter",
            "Invalid parameter: Message too long"
        )));
        assert!(!permanent(classify(true, "KMS.AccessDeniedException", "")));
        assert!(!permanent(classify(false, "InternalError", "")));
        assert!(!permanent(classify(false, "InvalidParameterValue", "")));
    }
}
