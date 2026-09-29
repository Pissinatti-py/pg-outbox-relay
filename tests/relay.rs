//! Relay core behavior, driven through the ports with in-memory fakes. No Docker needed.
//!
//! Time is paused, so batching windows and retry backoffs complete instantly.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use pg_outbox_relay::app::Health;
use pg_outbox_relay::app::relay::{self, Batching, Retry};
use pg_outbox_relay::domain::{Lsn, OutboxEvent, SourceMsg};
use pg_outbox_relay::ports::{DeadLetterStore, EventSink, EventSource, PublishError};
use serde_json::value::RawValue;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::time::timeout;

// ---- fakes ----------------------------------------------------------------

/// Streams a scripted WAL, starting after `resume_after` the way Postgres does after a restart.
struct FakeSource {
    wal: Vec<SourceMsg>,
    resume_after: Lsn,
    probe: Arc<SourceProbe>,
}

#[derive(Default)]
struct SourceProbe {
    /// The relay's ack channel, kept so tests can read the last acked LSN.
    acked: Mutex<Option<watch::Receiver<Lsn>>>,
    /// Messages the relay has accepted.
    sent: AtomicUsize,
    /// The ack the source held when the core finished: what it reports to Postgres last.
    final_ack: Mutex<Option<Lsn>>,
}

impl SourceProbe {
    fn last_ack(&self) -> Lsn {
        *self
            .acked
            .lock()
            .unwrap()
            .as_ref()
            .expect("source never ran")
            .borrow()
    }
}

impl FakeSource {
    fn new(wal: &[SourceMsg], resume_after: Lsn) -> Self {
        Self {
            wal: wal.to_vec(),
            resume_after,
            probe: Arc::default(),
        }
    }
}

impl EventSource for FakeSource {
    async fn run(
        self,
        out: mpsc::Sender<SourceMsg>,
        mut acked: watch::Receiver<Lsn>,
    ) -> anyhow::Result<()> {
        let FakeSource {
            wal,
            resume_after,
            probe,
        } = self;
        *probe.acked.lock().unwrap() = Some(acked.clone());
        for msg in wal.into_iter().filter(|msg| lsn_of(msg) > resume_after) {
            if out.send(msg).await.is_err() {
                break;
            }
            probe.sent.fetch_add(1, Ordering::SeqCst);
        }
        // A clean stop, as PgSource does it: close `out`, then follow the acks until the core is done.
        drop(out);
        while acked.changed().await.is_ok() {}
        *probe.final_ack.lock().unwrap() = Some(*acked.borrow());
        Ok(())
    }
}

/// An in-memory broker with scriptable failures.
#[derive(Clone, Default)]
struct FakeSink {
    /// Every event accepted, in acceptance order, duplicates included.
    published: Arc<Mutex<Vec<OutboxEvent>>>,
    /// The input of every publish call.
    calls: Arc<Mutex<Vec<Vec<OutboxEvent>>>>,
    /// Errors to return for an event id, one per attempt, before it succeeds.
    failures: Arc<Mutex<HashMap<String, VecDeque<PublishError>>>>,
    /// When set, every publish call waits for a permit.
    gate: Option<Arc<Semaphore>>,
    /// On this call (1-based), accept the first half of the batch, then hang: the relay "dies".
    crash_on_call: Option<usize>,
}

impl FakeSink {
    fn fail(&self, id: &str, errors: impl IntoIterator<Item = PublishError>) {
        self.failures
            .lock()
            .unwrap()
            .insert(id.into(), errors.into_iter().collect());
    }

    fn published(&self) -> Vec<OutboxEvent> {
        self.published.lock().unwrap().clone()
    }

    fn calls(&self) -> Vec<Vec<OutboxEvent>> {
        self.calls.lock().unwrap().clone()
    }
}

impl EventSink for FakeSink {
    const NAME: &'static str = "fake";

    async fn publish(&self, events: &[OutboxEvent]) -> Vec<Result<(), PublishError>> {
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        let call = {
            let mut calls = self.calls.lock().unwrap();
            calls.push(events.to_vec());
            calls.len()
        };
        if self.crash_on_call == Some(call) {
            let half = &events[..events.len() / 2];
            self.published.lock().unwrap().extend(half.iter().cloned());
            std::future::pending::<()>().await;
        }
        events
            .iter()
            .map(|event| {
                let failure = self
                    .failures
                    .lock()
                    .unwrap()
                    .get_mut(&event.id)
                    .and_then(VecDeque::pop_front);
                match failure {
                    Some(error) => Err(error),
                    None => {
                        self.published.lock().unwrap().push(event.clone());
                        Ok(())
                    }
                }
            })
            .collect()
    }
}

/// Records dead letters; every store fails while `broken` is set.
#[derive(Clone, Default)]
struct FakeDeadLetters {
    stored: Arc<Mutex<Vec<(String, String)>>>,
    broken: Arc<AtomicBool>,
}

impl FakeDeadLetters {
    fn stored(&self) -> Vec<(String, String)> {
        self.stored.lock().unwrap().clone()
    }
}

impl DeadLetterStore for FakeDeadLetters {
    async fn store(&self, event: &OutboxEvent, reason: &str) -> anyhow::Result<()> {
        anyhow::ensure!(!self.broken.load(Ordering::SeqCst), "database unreachable");
        self.stored
            .lock()
            .unwrap()
            .push((event.id.clone(), reason.to_owned()));
        Ok(())
    }
}

// ---- helpers ----------------------------------------------------------------

fn raw(json: &str) -> Box<RawValue> {
    RawValue::from_string(json.into()).unwrap()
}

/// `count` events spread round-robin over `aggregates` aggregates.
/// Event `n` commits in its own transaction, at LSN `n * 100`.
fn events(count: u64, aggregates: u64) -> Vec<OutboxEvent> {
    (1..=count)
        .map(|n| OutboxEvent {
            id: format!("evt-{n}"),
            aggregate_type: "policy".into(),
            aggregate_id: (n % aggregates).to_string(),
            event_type: "policy.updated".into(),
            occurred_at: "2026-09-28T14:03:11Z".into(),
            headers: raw("{}"),
            payload: raw(&format!(r#"{{"n":{n}}}"#)),
            commit_lsn: Lsn(n * 100),
            committed_at: SystemTime::now(),
        })
        .collect()
}

/// What the replication stream carries for `events`: each event followed by its commit.
fn wal(events: &[OutboxEvent]) -> Vec<SourceMsg> {
    events
        .iter()
        .flat_map(|e| {
            [
                SourceMsg::Event(e.clone()),
                SourceMsg::Progress(e.commit_lsn),
            ]
        })
        .collect()
}

fn lsn_of(msg: &SourceMsg) -> Lsn {
    match msg {
        SourceMsg::Event(event) => event.commit_lsn,
        SourceMsg::Progress(lsn) => *lsn,
    }
}

fn ids(events: &[OutboxEvent]) -> HashSet<String> {
    events.iter().map(|e| e.id.clone()).collect()
}

async fn relay(source: FakeSource, sink: FakeSink) -> anyhow::Result<()> {
    relay_with(source, sink, FakeDeadLetters::default()).await
}

async fn relay_with(
    source: FakeSource,
    sink: FakeSink,
    dead_letters: FakeDeadLetters,
) -> anyhow::Result<()> {
    let health = Arc::new(Health::default());
    relay::run(
        source,
        sink,
        dead_letters,
        Batching::default(),
        Retry::default(),
        health,
    )
    .await
}

fn assert_ordered_per_aggregate(published: &[OutboxEvent]) {
    let mut last = HashMap::new();
    for event in published {
        if let Some(previous) = last.insert(event.ordering_key(), event.commit_lsn) {
            assert!(
                previous < event.commit_lsn,
                "{} overtook an earlier event of its aggregate",
                event.id
            );
        }
    }
}

// ---- tests ------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn publishes_every_event_once_in_per_aggregate_order() {
    let events = events(100, 3);
    let source = FakeSource::new(&wal(&events), Lsn(0));
    let probe = source.probe.clone();
    let sink = FakeSink::default();

    relay(source, sink.clone()).await.unwrap();

    let published = sink.published();
    assert_eq!(published.len(), events.len());
    assert_eq!(ids(&published), ids(&events));
    assert_ordered_per_aggregate(&published);
    for call in sink.calls() {
        let keys: HashSet<_> = call.iter().map(OutboxEvent::ordering_key).collect();
        assert!(call.len() <= Batching::default().max_events);
        assert_eq!(
            keys.len(),
            call.len(),
            "two events of one aggregate shared a batch"
        );
    }
    assert_eq!(probe.last_ack(), Lsn(10_000));
}

#[tokio::test(start_paused = true)]
async fn acks_only_after_the_broker_confirms() {
    let events = events(5, 5);
    let gate = Arc::new(Semaphore::new(0));
    let sink = FakeSink {
        gate: Some(gate.clone()),
        ..Default::default()
    };
    let source = FakeSource::new(&wal(&events), Lsn(0));
    let probe = source.probe.clone();

    let relay = tokio::spawn(relay(source, sink.clone()));
    tokio::time::sleep(Duration::from_secs(1)).await; // the relay is now stuck inside publish
    assert_eq!(probe.last_ack(), Lsn(0));

    gate.add_permits(100);
    relay.await.unwrap().unwrap();
    assert_eq!(probe.last_ack(), Lsn(500));
}

#[tokio::test(start_paused = true)]
async fn retries_retryable_failures_without_reordering() {
    let events = events(30, 3);
    let sink = FakeSink::default();
    let throttled = || PublishError::Retryable("throttled".into());
    sink.fail(&events[0].id, [throttled(), throttled(), throttled()]);
    sink.fail(&events[4].id, [throttled()]);
    let source = FakeSource::new(&wal(&events), Lsn(0));
    let probe = source.probe.clone();

    relay(source, sink.clone()).await.unwrap();

    let published = sink.published();
    assert_eq!(published.len(), events.len());
    assert_eq!(ids(&published), ids(&events));
    assert_ordered_per_aggregate(&published);
    assert_eq!(probe.last_ack(), Lsn(3_000));
}

#[tokio::test(start_paused = true)]
async fn dead_letters_a_permanently_rejected_event_and_moves_on() {
    let events = events(10, 2);
    let sink = FakeSink::default();
    sink.fail(
        &events[3].id,
        [PublishError::Permanent("message too long".into())],
    );
    let dead_letters = FakeDeadLetters::default();
    let source = FakeSource::new(&wal(&events), Lsn(0));
    let probe = source.probe.clone();

    relay_with(source, sink.clone(), dead_letters.clone())
        .await
        .unwrap();

    let published = ids(&sink.published());
    assert_eq!(published.len(), 9);
    assert!(!published.contains(&events[3].id));
    assert_eq!(
        dead_letters.stored(),
        [(events[3].id.clone(), "message too long".to_owned())]
    );
    assert_eq!(
        probe.last_ack(),
        Lsn(1_000),
        "the poison event must not block the ack"
    );
}

#[tokio::test(start_paused = true)]
async fn a_rejected_event_is_not_acked_until_it_is_dead_lettered() {
    let events = events(5, 5);
    let sink = FakeSink::default();
    sink.fail(
        &events[2].id,
        [PublishError::Permanent("message too long".into())],
    );
    let dead_letters = FakeDeadLetters::default();
    dead_letters.broken.store(true, Ordering::SeqCst);
    let source = FakeSource::new(&wal(&events), Lsn(0));
    let probe = source.probe.clone();

    let relay = tokio::spawn(relay_with(source, sink, dead_letters.clone()));
    tokio::time::sleep(Duration::from_secs(60)).await; // the store keeps failing meanwhile
    assert!(!relay.is_finished());
    assert!(
        probe.last_ack() < events[2].commit_lsn,
        "acked an event that is nowhere"
    );

    dead_letters.broken.store(false, Ordering::SeqCst);
    relay.await.unwrap().unwrap();
    assert_eq!(dead_letters.stored().len(), 1);
    assert_eq!(probe.last_ack(), Lsn(500));
}

#[tokio::test(start_paused = true)]
async fn idle_progress_advances_the_ack() {
    let source = FakeSource::new(
        &[SourceMsg::Progress(Lsn(7)), SourceMsg::Progress(Lsn(42))],
        Lsn(0),
    );
    let probe = source.probe.clone();
    let sink = FakeSink::default();

    relay(source, sink.clone()).await.unwrap();

    assert!(sink.calls().is_empty());
    assert_eq!(probe.last_ack(), Lsn(42));
}

#[tokio::test(start_paused = true)]
async fn a_crash_mid_batch_loses_nothing() {
    let events = events(50, 12);
    let wal = wal(&events);

    // First run: the broker takes two batches and half of the third, then the relay dies.
    let sink = FakeSink {
        crash_on_call: Some(3),
        ..Default::default()
    };
    let source = FakeSource::new(&wal, Lsn(0));
    let probe = source.probe.clone();
    let first_run = timeout(Duration::from_secs(60), relay(source, sink.clone())).await;
    assert!(
        first_run.is_err(),
        "the relay should still be stuck in the crashing publish"
    );
    let resume_after = probe.last_ack();
    assert!(resume_after < Lsn(5_000));

    // Second run resumes where Postgres would: right after the last acked LSN.
    let sink = FakeSink {
        published: sink.published.clone(),
        ..Default::default()
    };
    relay(FakeSource::new(&wal, resume_after), sink.clone())
        .await
        .unwrap();

    let published = sink.published();
    assert_eq!(
        ids(&published),
        ids(&events),
        "every event is published at least once"
    );
    assert!(
        published.len() > events.len(),
        "the unacked half batch is delivered again"
    );
}

#[tokio::test(start_paused = true)]
async fn a_stalled_sink_holds_the_source_back() {
    let events = events(5_000, 100);
    let wal = wal(&events);
    let sink = FakeSink {
        gate: Some(Arc::new(Semaphore::new(0))),
        ..Default::default()
    };
    let source = FakeSource::new(&wal, Lsn(0));
    let probe = source.probe.clone();

    let stalled = timeout(Duration::from_secs(60), relay(source, sink)).await;

    assert!(stalled.is_err());
    let sent = probe.sent.load(Ordering::SeqCst);
    assert!(
        sent < 1_100,
        "the source ran {sent} messages ahead of a stalled sink"
    );
}

#[tokio::test(start_paused = true)]
async fn a_transaction_split_across_batches_is_acked_only_once_complete() {
    // One transaction, 15 rows for 15 aggregates: more than one batch holds.
    let events: Vec<OutboxEvent> = events(15, 15)
        .into_iter()
        .map(|event| OutboxEvent {
            commit_lsn: Lsn(100),
            ..event
        })
        .collect();
    let mut wal: Vec<SourceMsg> = events.iter().cloned().map(SourceMsg::Event).collect();
    wal.push(SourceMsg::Progress(Lsn(100)));

    // The first batch is published, then the relay dies inside the second.
    let sink = FakeSink {
        crash_on_call: Some(2),
        ..Default::default()
    };
    let source = FakeSource::new(&wal, Lsn(0));
    let probe = source.probe.clone();
    let first_run = timeout(Duration::from_secs(60), relay(source, sink.clone())).await;
    assert!(first_run.is_err());
    assert_eq!(
        probe.last_ack(),
        Lsn(0),
        "acked a transaction whose events were still in flight"
    );

    let sink = FakeSink {
        published: sink.published.clone(),
        ..Default::default()
    };
    relay(FakeSource::new(&wal, probe.last_ack()), sink.clone())
        .await
        .unwrap();
    assert_eq!(ids(&sink.published()), ids(&events));
}

#[tokio::test(start_paused = true)]
async fn a_clean_stop_hands_the_final_ack_to_the_source() {
    let events = events(25, 5);
    let source = FakeSource::new(&wal(&events), Lsn(0));
    let probe = source.probe.clone();

    relay(source, FakeSink::default()).await.unwrap();

    assert_eq!(*probe.final_ack.lock().unwrap(), Some(Lsn(2_500)));
}
