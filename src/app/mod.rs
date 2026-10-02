//! Use cases: the relay pipeline, built only on domain rules and ports.

pub mod relay;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::watch;

/// Readiness for `/readyz`. The source adapter flips `source_ready`, the relay core `sink_ready`.
#[derive(Debug, Default)]
pub struct Health {
    pub source_ready: AtomicBool,
    pub sink_ready: AtomicBool,
}

/// Whether `/readyz` reports ready: a source streams from its slot, and no pipeline's last
/// publish failed. Any source, since two replicas share the slots; every pipeline, since they
/// share the sink and an idle one never notices it failing.
pub fn ready(sources: &[Arc<Health>]) -> bool {
    sources
        .iter()
        .any(|h| h.source_ready.load(Ordering::Relaxed))
        && sources.iter().all(|h| h.sink_ready.load(Ordering::Relaxed))
}

/// Resolves once the relay is asked to stop; never if nobody can ask any more.
pub async fn stopped(stop: &mut watch::Receiver<bool>) {
    if stop.wait_for(|stop| *stop).await.is_err() {
        std::future::pending().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health(source_ready: bool, sink_ready: bool) -> Arc<Health> {
        Arc::new(Health {
            source_ready: AtomicBool::new(source_ready),
            sink_ready: AtomicBool::new(sink_ready),
        })
    }

    #[test]
    fn ready_while_a_source_streams_and_every_pipeline_publishes() {
        // This replica streams one slot and is the standby for the other.
        assert!(ready(&[health(true, true), health(false, true)]));
        // It is the standby for every slot.
        assert!(!ready(&[health(false, true), health(false, true)]));
        // The broker is down: a busy source's publishes fail, and an idle one never notices.
        assert!(!ready(&[health(true, false), health(true, true)]));
    }
}
