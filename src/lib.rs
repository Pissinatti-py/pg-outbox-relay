//! pg-outbox-relay: publishes PostgreSQL outbox rows to message brokers.
//!
//! Hexagonal layers, dependencies pointing inward (enforced by `tests/architecture.rs`):
//! - [`domain`]: pure rules (events, envelope, checkpointing, batching, backoff).
//! - [`ports`]: the traits the core needs from the outside world.
//! - [`app`]: the relay pipeline, built only on `domain` and `ports`.
//! - [`adapters`]: Postgres, SQS and HTTP behind those ports.
//! - [`config`]: loads every layer's settings; used by `main`.

pub mod adapters;
pub mod app;
pub mod config;
pub mod domain;
pub mod ports;
