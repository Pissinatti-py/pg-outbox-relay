use std::collections::BTreeMap;

use super::Lsn;

/// Decides which WAL position is safe to acknowledge to Postgres.
///
/// Acking an LSN tells Postgres never to send anything at or before it again,
/// so it may only be acked once every event committed at or before it has been
/// published. Each commit LSN the source reports gets an entry counting its
/// unconfirmed events; the safe LSN is the highest entry with nothing
/// unconfirmed at or below it, and at or below the last progress the source
/// reported: until then, more events of that transaction may still be on
/// their way.
#[derive(Debug, Default)]
pub struct Checkpoint {
    unconfirmed: BTreeMap<Lsn, usize>,
    /// The source has sent every event committed at or before this LSN.
    observed: Lsn,
    acked: Lsn,
}

impl Checkpoint {
    /// An event committed at `lsn` entered the pipeline.
    pub fn track(&mut self, lsn: Lsn) {
        *self.unconfirmed.entry(lsn).or_default() += 1;
    }

    /// The source has sent everything up to `lsn` (a commit or a keepalive).
    /// Lets the slot advance while the outbox is idle.
    pub fn observe(&mut self, lsn: Lsn) {
        self.observed = self.observed.max(lsn);
        self.unconfirmed.entry(lsn).or_default();
    }

    /// An event committed at `lsn` was published, or given up on for good.
    pub fn confirm(&mut self, lsn: Lsn) {
        let count = self
            .unconfirmed
            .get_mut(&lsn)
            .expect("confirmed an event that was never tracked");
        *count -= 1;
    }

    /// Highest LSN with every event at or before it confirmed. Never moves backwards.
    pub fn safe_lsn(&mut self) -> Lsn {
        while let Some(entry) = self.unconfirmed.first_entry() {
            if *entry.get() > 0 || *entry.key() > self.observed {
                break;
            }
            self.acked = self.acked.max(*entry.key());
            entry.remove();
        }
        self.acked
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_passes_an_unconfirmed_event() {
        let mut cp = Checkpoint::default();
        cp.track(Lsn(10));
        cp.observe(Lsn(10));
        cp.track(Lsn(20));
        cp.observe(Lsn(20));

        cp.confirm(Lsn(20));
        assert_eq!(cp.safe_lsn(), Lsn(0), "10 is still in flight");

        cp.confirm(Lsn(10));
        assert_eq!(cp.safe_lsn(), Lsn(20));
    }

    #[test]
    fn stops_at_the_first_unconfirmed_event() {
        let mut cp = Checkpoint::default();
        for lsn in [10, 20, 30] {
            cp.track(Lsn(lsn));
            cp.observe(Lsn(lsn));
        }
        cp.confirm(Lsn(10));
        cp.confirm(Lsn(30));
        assert_eq!(cp.safe_lsn(), Lsn(10));
    }

    #[test]
    fn a_transaction_needs_all_of_its_events_confirmed() {
        let mut cp = Checkpoint::default();
        cp.track(Lsn(10));
        cp.track(Lsn(10));
        cp.observe(Lsn(10));

        cp.confirm(Lsn(10));
        assert_eq!(cp.safe_lsn(), Lsn(0));
        cp.confirm(Lsn(10));
        assert_eq!(cp.safe_lsn(), Lsn(10));
    }

    #[test]
    fn waits_for_the_commit_before_acking_a_transaction() {
        let mut cp = Checkpoint::default();
        cp.track(Lsn(10));
        cp.confirm(Lsn(10));
        assert_eq!(
            cp.safe_lsn(),
            Lsn(0),
            "more events of this transaction may follow"
        );
        cp.track(Lsn(10));
        cp.observe(Lsn(10));
        assert_eq!(cp.safe_lsn(), Lsn(0));
        cp.confirm(Lsn(10));
        assert_eq!(cp.safe_lsn(), Lsn(10));
    }

    #[test]
    fn idle_progress_advances_the_ack() {
        let mut cp = Checkpoint::default();
        cp.observe(Lsn(5));
        cp.observe(Lsn(9));
        assert_eq!(cp.safe_lsn(), Lsn(9));
    }

    #[test]
    fn never_moves_backwards() {
        let mut cp = Checkpoint::default();
        cp.observe(Lsn(50));
        assert_eq!(cp.safe_lsn(), Lsn(50));
        cp.observe(Lsn(30));
        assert_eq!(cp.safe_lsn(), Lsn(50));
    }
}
