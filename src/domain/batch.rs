use std::collections::{HashSet, VecDeque};

use super::OutboxEvent;

/// Takes up to `max` events from `buffer`, oldest first, at most one per ordering key.
///
/// Later events of a key already in the batch stay buffered for the next one.
/// A batch can fail partially, so if two events of one aggregate shared a batch
/// the second could be published while the first waits for a retry.
// ponytail: a hot aggregate ships one event per batch; allow same-key runs if throughput needs it
pub fn take_batch(buffer: &mut VecDeque<OutboxEvent>, max: usize) -> Vec<OutboxEvent> {
    let mut keys = HashSet::new();
    let mut batch = Vec::new();
    let mut rest = VecDeque::with_capacity(buffer.len());
    for event in buffer.drain(..) {
        if batch.len() < max && keys.insert(event.ordering_key()) {
            batch.push(event);
        } else {
            rest.push_back(event);
        }
    }
    *buffer = rest;
    batch
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use serde_json::value::RawValue;

    use super::*;
    use crate::domain::Lsn;

    fn event(id: &str, aggregate_id: &str) -> OutboxEvent {
        OutboxEvent {
            id: id.into(),
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

    fn ids(events: impl IntoIterator<Item = OutboxEvent>) -> Vec<String> {
        events.into_iter().map(|e| e.id).collect()
    }

    #[test]
    fn holds_at_most_one_event_per_aggregate() {
        let mut buffer = VecDeque::from([event("a1", "a"), event("a2", "a"), event("b1", "b")]);
        assert_eq!(ids(take_batch(&mut buffer, 10)), ["a1", "b1"]);
        assert_eq!(ids(buffer), ["a2"]);
    }

    #[test]
    fn stops_at_max() {
        let mut buffer = VecDeque::from([event("a1", "a"), event("b1", "b"), event("c1", "c")]);
        assert_eq!(ids(take_batch(&mut buffer, 2)), ["a1", "b1"]);
        assert_eq!(ids(buffer), ["c1"]);
    }

    #[test]
    fn leftovers_keep_their_order() {
        let mut buffer = VecDeque::from([
            event("a1", "a"),
            event("a2", "a"),
            event("b1", "b"),
            event("a3", "a"),
            event("b2", "b"),
        ]);
        assert_eq!(ids(take_batch(&mut buffer, 10)), ["a1", "b1"]);
        assert_eq!(ids(take_batch(&mut buffer, 10)), ["a2", "b2"]);
        assert_eq!(ids(buffer), ["a3"]);
    }
}
