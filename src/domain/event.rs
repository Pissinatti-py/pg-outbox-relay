use std::fmt;
use std::time::SystemTime;

use serde::Serialize;
use serde_json::value::RawValue;

/// A position in the PostgreSQL write-ahead log.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

impl fmt::Display for Lsn {
    /// Same `X/Y` form Postgres prints, so logs can be matched against `pg_replication_slots`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 as u32)
    }
}

/// One committed row of the `outbox` table.
#[derive(Debug, Clone)]
pub struct OutboxEvent {
    /// Idempotency key: consumers deduplicate on it.
    pub id: String,
    /// The database the event was committed in.
    pub source: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    /// RFC 3339, UTC.
    pub occurred_at: String,
    pub headers: Box<RawValue>,
    pub payload: Box<RawValue>,
    /// End LSN of the transaction that inserted the row: acking it tells
    /// Postgres the whole transaction is delivered.
    pub commit_lsn: Lsn,
    /// Commit time, for the commit → broker ack latency metric.
    pub committed_at: SystemTime,
}

impl OutboxEvent {
    /// Events sharing this key reach the broker in commit order.
    pub fn ordering_key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.source, self.aggregate_type, self.aggregate_id
        )
    }

    /// The JSON document consumers receive.
    pub fn envelope(&self) -> String {
        #[derive(Serialize)]
        struct Envelope<'a> {
            id: &'a str,
            source: &'a str,
            aggregate_type: &'a str,
            aggregate_id: &'a str,
            event_type: &'a str,
            occurred_at: &'a str,
            headers: &'a RawValue,
            payload: &'a RawValue,
        }
        serde_json::to_string(&Envelope {
            id: &self.id,
            source: &self.source,
            aggregate_type: &self.aggregate_type,
            aggregate_id: &self.aggregate_id,
            event_type: &self.event_type,
            occurred_at: &self.occurred_at,
            headers: &self.headers,
            payload: &self.payload,
        })
        .expect("strings and already-valid JSON always serialize")
    }
}

/// What a source hands to the relay core, in commit order.
#[derive(Debug, Clone)]
pub enum SourceMsg {
    /// A committed outbox row.
    Event(OutboxEvent),
    /// Everything up to this LSN has been sent (a commit or a keepalive).
    Progress(Lsn),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_prints_like_postgres() {
        assert_eq!(Lsn(0x16_B374_D848).to_string(), "16/B374D848");
        assert_eq!(Lsn(0).to_string(), "0/0");
    }

    #[test]
    fn envelope_has_the_documented_shape() {
        let event = OutboxEvent {
            id: "0b7e5c1e-2f4a-4c33-9d7e-9a4f1c2b3d4e".into(),
            source: "acme".into(),
            aggregate_type: "policy".into(),
            aggregate_id: "42".into(),
            event_type: "policy.approved".into(),
            occurred_at: "2026-09-28T14:03:11Z".into(),
            headers: RawValue::from_string(r#"{"tenant": "acme"}"#.into()).unwrap(),
            payload: RawValue::from_string(r#"{"policy_id": 42}"#.into()).unwrap(),
            commit_lsn: Lsn(1),
            committed_at: SystemTime::UNIX_EPOCH,
        };

        assert_eq!(event.ordering_key(), "acme:policy:42");
        assert_eq!(
            event.envelope(),
            r#"{"id":"0b7e5c1e-2f4a-4c33-9d7e-9a4f1c2b3d4e","source":"acme","aggregate_type":"policy","aggregate_id":"42","event_type":"policy.approved","occurred_at":"2026-09-28T14:03:11Z","headers":{"tenant": "acme"},"payload":{"policy_id": 42}}"#
        );
    }
}
