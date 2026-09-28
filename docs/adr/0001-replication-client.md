# ADR 0001: Replication client — `pgwire-replication`

- **Status:** accepted
- **Date:** 2026-09-28
- **Context owner:** the Postgres source adapter (`src/adapters/postgres/`)

## Context

The relay has to consume a logical replication stream. That means:
- `START_REPLICATION ... LOGICAL` over a `CopyBoth` stream;
- decoding the `pgoutput` messages;
- sending standby status updates that carry our flush LSN.

The spec (§12) listed this as the open question to settle before M1.

Upstream `tokio-postgres` still has no replication support: the 0.7.18 source has no `CopyBoth` and no replication API.

## Options

| Option | Notes |
|---|---|
| **`pgwire-replication` 0.4.1** (crates.io) | Replication-only pgwire client. SCRAM-SHA-256 and MD5 auth, rustls TLS (`disable` … `verify-full`, mTLS). Parses `Begin` and `Commit`; hands every other pgoutput message over as raw bytes. Explicit, forward-only `update_applied_lsn`. Keeps sending status feedback while its event buffer is full. MIT OR Apache-2.0, MSRV 1.88. |
| `pg_walstream` 0.9.0 (crates.io) | BSD-3-Clause. Pulls `aws-lc-rs` and an optional libpq backend. Not evaluated in depth. |
| Materialize's `rust-postgres` fork | Git dependency only, which blocks publishing to crates.io. Not evaluated in depth. |
| Supabase `etl` | A full pipeline framework with its own destinations and state store, which would overlap our core. Not evaluated in depth. |
| Hand-rolled on `postgres-protocol` | Startup, SCRAM, TLS and `CopyBoth` handling are the bulk of the work, and `pgwire-replication` already does all of it. |

`pgwire-replication` met every criterion on the first try, so the other options were not built out.

## Spike evidence

The test ran against Postgres 17 in Docker, using `sql/outbox.sql` and `sql/slot.sql` from this repo. The throwaway program is not kept.

- **Auth:** SCRAM-SHA-256 (the image default) works. A role with only `LOGIN REPLICATION` streams the outbox; it needs no `SELECT` grant.
- **Stream shape:** `Begin { final_lsn, commit_time }` → raw `R` (Relation) → raw `I` (Insert) × N → `Commit { end_lsn, commit_time }`. Keepalives arrive between transactions.
- **Decoding:** Insert values arrive in text format. With the startup options `-c TimeZone=UTC -c DateStyle=ISO`, `created_at` arrives as `2026-09-28 16:03:22.128012+00`. `jsonb` arrives normalized, e.g. `{"x": [1, 2]}`.
- **Acks:**
  - `update_applied_lsn(commit end_lsn)` moved the slot's `confirmed_flush_lsn` to exactly that LSN.
  - Killing the consumer before the ack made Postgres replay the transaction on restart (at-least-once).
  - Restarting after the ack replayed nothing.
- **Failures:** `connect()` returns at once; errors surface on the first `recv()` as `PgWireError::Server("... (SQLSTATE xxxxx)")`:
  - `42704` — the slot does not exist;
  - `55006` — the slot is active for another PID (the HA standby case);
  - `28P01` — wrong password.

## Decision

Use `pgwire-replication` for the replication connection.

## Consequences

- **We own the pgoutput decoding** of `Relation` and `Insert` (`src/adapters/postgres/pgoutput.rs`), which is about 100 lines with fixture tests.
- **Connect-retry policy by SQLSTATE,** parsed from the error text because the crate exposes only strings:
  - retry on I/O errors, `55006` and `57P03`;
  - fail fast on anything else;
  - `42704` gets a hint pointing to `sql/slot.sql`.
- **DSN parsing uses the `url` crate** (already in the tree through `aws-config`), because `tokio-postgres`'s parser rejects `sslmode=verify-ca` and `sslmode=verify-full`.
- **SQL connections still use `tokio-postgres`.** That means the slot-lag poller now and the dead-letter table in M2. TLS for SQL connections arrives with M2. Until then the poller runs only with `sslmode=disable` and otherwise stays off, with a warning, so credentials are never sent in clear.
- **Single-maintainer risk** is contained by the `EventSource` port: only `src/adapters/postgres/` knows this crate, and the decoder is ours.
