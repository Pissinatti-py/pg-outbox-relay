//! Use cases: the relay pipeline, built only on domain rules and ports.

pub mod relay;

use std::sync::atomic::{AtomicBool, Ordering};

/// Readiness for `/readyz`. The source adapter flips `source_ready`, the relay core `sink_ready`.
#[derive(Debug, Default)]
pub struct Health {
    pub source_ready: AtomicBool,
    pub sink_ready: AtomicBool,
}

impl Health {
    pub fn is_ready(&self) -> bool {
        self.source_ready.load(Ordering::Relaxed) && self.sink_ready.load(Ordering::Relaxed)
    }
}
