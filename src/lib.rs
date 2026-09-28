//! pg-outbox-relay: publishes PostgreSQL outbox rows to message brokers.
//!
//! Hexagonal layers, dependencies pointing inward (enforced by `tests/architecture.rs`):
//! - [`domain`]: pure rules (events, envelope, checkpointing, batching, backoff).
//! - [`ports`]: the traits the core needs from the outside world.
//! - [`app`]: the relay pipeline, built only on `domain` and `ports`.

pub mod app;
pub mod domain;
pub mod ports;
