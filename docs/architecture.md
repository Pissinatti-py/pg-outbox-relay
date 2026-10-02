# Architecture

This page is for people changing the code. It covers:
- how the pieces fit together;
- why each guarantee holds;
- how to extend the relay.

For what the relay is and how to run it, see the [README](../README.md). The original design document is [spec.md](spec.md).

## Hexagonal layers

```
            ┌──────────────── adapters (I/O) ─────────────────┐
            │  postgres/ (source)  sqs·sns·redis (sinks) http │
            └───────┬─────────────────▲──────────────▲────────┘
                    │ implements      │ implements   │ reads
            ┌───────▼─────────────────┴──────────────┴────────┐
            │  ports.rs   EventSource · EventSink ·           │
            │             DeadLetterStore                     │
            ├─────────────────────────────────────────────────┤
            │  app/       relay::run: the pipeline use case   │
            │             relay::supervise: one per source    │
            ├─────────────────────────────────────────────────┤
            │  domain/    events, envelope, checkpoint,       │
            │             batching rule, backoff (pure)       │
            └─────────────────────────────────────────────────┘
   main.rs + config.rs = composition root (the only code that knows every layer)
```

**The rule: dependencies point inward only.**
- `domain` knows nothing else, not even `tokio`.
- `ports` knows `domain`.
- `app` knows `ports` and `domain`.
- `adapters` may use everything inside them.

`tests/architecture.rs` enforces this: `cargo test` fails if the core imports an adapter, the config, or an infrastructure crate (`aws_*`, `axum`, `tokio_postgres`, …).

The core may use two facades, much like `log`: `tracing` for logs and `metrics` for counters. The Prometheus exporter behind `metrics` lives only in `adapters/http.rs`.

**Why this shape pays off here:**
- **The hard guarantees are testable in isolation.** At-least-once delivery, ordering and backpressure live in `domain/` and `app/`, and `tests/relay.rs` checks them in milliseconds with in-memory fakes, with no Docker.
- **The replication client is a replaceable detail.** Only `adapters/postgres/` knows `pgwire-replication` (see [ADR 0001](adr/0001-replication-client.md)).
- **New brokers plug in** without touching the core (see [Adding a sink](#adding-a-sink)).

## Where things live

| File | Responsibility | Depends on |
|---|---|---|
| `src/domain/event.rs` | `Lsn`, `OutboxEvent`, the JSON envelope, `ordering_key()`, `dedup_key()`, `SourceMsg` | serde |
| `src/domain/checkpoint.rs` | `Checkpoint`: the highest LSN that is safe to acknowledge | — |
| `src/domain/batch.rs` | `take_batch`: at most one event per aggregate per batch | — |
| `src/domain/backoff.rs` | Exponential backoff with full jitter (pure: jitter is an argument) | — |
| `src/ports.rs` | `EventSource`, `EventSink`, `DeadLetterStore`, `PublishError` | domain, tokio channels |
| `src/app/relay.rs` | `relay::run`: channel → buffer → batch → publish/retry/dead-letter → checkpoint → ack; `relay::supervise`: restarts a failed source's pipeline | domain, ports |
| `src/app/mod.rs` | `Health` and `ready`: what `/readyz` reports; `stopped`: resolves when the relay is asked to stop | — |
| `src/adapters/postgres/mod.rs` | `PgSource`: connect/retry, the replication stream, acks, the slot-lag poller, DSN parsing, SQL connections over the same TLS | pgwire-replication, tokio-postgres |
| `src/adapters/postgres/pgoutput.rs` | Decodes pgoutput `Relation` and `Insert`; maps a row to an `OutboxEvent` | domain |
| `src/adapters/postgres/dead_letter.rs` | `PgDeadLetters`: stores rejected events in `outbox_dead_letter`; the startup check | tokio-postgres |
| `src/adapters/sqs.rs` | `SqsSink`: envelope → `SendMessageBatch`, call splitting, error classification (shared with SNS) | aws-sdk-sqs |
| `src/adapters/sns.rs` | `SnsSink`: envelope → `PublishBatch`, reusing the SQS splitting, id mapping and classification | aws-sdk-sns |
| `src/adapters/redis.rs` | `RedisSink`: `XADD` of `id`, `source`, `event_type`, `envelope` to one stream per aggregate type | redis |
| `src/adapters/http.rs` | `/metrics`, `/healthz`, `/readyz` | axum, metrics-exporter-prometheus |
| `src/config.rs` | TOML file + `RELAY__…` env overrides → each layer's own settings struct | config |
| `src/main.rs` | Wires everything together: one supervised pipeline per source (`relay_all`); logging; signals | everything |

## The life of one event

1. **The application commits.** A transaction inserts a row into `outbox`. Postgres writes it to the WAL, and the `outbox_pub` publication makes it visible to logical decoding.
2. **`PgSource` receives the transaction** (`adapters/postgres/mod.rs`). `pgwire-replication` yields `Begin`, then raw pgoutput bytes, then `Commit`.
   - `pgoutput::Decoder` remembers column names from the `Relation` message and decodes each `Insert` into a row.
   - Rows wait until `Commit`. They are then sent as `SourceMsg::Event`, tagged with their database (`source`), the commit's `end_lsn` and commit time, followed by `SourceMsg::Progress(end_lsn)`.
   - A keepalive between transactions becomes `Progress(wal_end)`.
3. **A bounded channel** (1024 messages) carries them to the core. When it is full, `PgSource` waits. The replication client keeps sending status updates meanwhile, so Postgres does not time the connection out.
4. **The core buffers the event** (`app/relay.rs`). `checkpoint.track(lsn)` records it as unconfirmed. A flush happens when the buffer holds `max_events` events or when the oldest one has waited `max_wait_ms`.
5. **`take_batch`** picks up to `max_events` events, oldest first, **at most one per aggregate**.
6. **`sink.publish(batch)`** returns one result per event.
   - `Ok` → `checkpoint.confirm(lsn)`.
   - `Retryable` → retried alone after `backoff(attempt)`, before any other batch goes out.
   - `Permanent` → stored in `outbox_dead_letter` (retried until it is), logged with the full envelope, counted as a dead letter, then confirmed so the stream moves on.
7. **The ack flows back.** `checkpoint.safe_lsn()` goes into a `watch` channel. `PgSource` passes it to `update_applied_lsn`, and within about 1 s Postgres records it as the slot's `confirmed_flush_lsn`.

## Why the guarantees hold

| Guarantee | Mechanism | Code | Test |
|---|---|---|---|
| **At-least-once** | The checkpoint keeps a `BTreeMap<commit LSN, unconfirmed count>`. `safe_lsn` pops fully confirmed entries from the lowest LSN up, and only up to the last `Progress` the source sent, so a transaction split across batches is never acked early. It returns the last one popped, so the ack can never pass an unconfirmed event. A crash only loses unacked progress, which Postgres replays. | `domain/checkpoint.rs` | `checkpoint` unit tests; `a_crash_mid_batch_loses_nothing`; `a_transaction_split_across_batches_is_acked_only_once_complete`; e2e ack check |
| **Idle slots advance** | With nothing pending, a `Progress` LSN (commit or keepalive) becomes the ack, the same as Postgres' own apply worker. Keepalives received mid-transaction are ignored, because their `wal_end` can sit before that transaction's commit. | `checkpoint.rs`, `postgres/mod.rs` | `idle_progress_advances_the_ack` |
| **Per-aggregate order** | pgoutput delivers in commit order and the channel is FIFO. A batch holds at most one event per aggregate, and it finishes, retries included, before the next batch starts. So a partial failure cannot let a later event of an aggregate overtake an earlier one. | `domain/batch.rs`, `app/relay.rs` | `batch` unit tests; `retries_retryable_failures_without_reordering`; e2e order check |
| **Idempotency key** | The outbox `id` becomes an `id` message attribute. The SQS or SNS `MessageDeduplicationId` is `source:id` (`dedup_key()`), since an `id` is unique only within its database, and `MessageGroupId` is `source:aggregate_type:aggregate_id`. Both are mapped to their character set deterministically. | `adapters/sqs.rs`, `adapters/sns.rs` | `sqs` and `sns` unit tests; e2e |
| **Backpressure** | The core stops reading when a batch is due, the channel fills up, and the source waits. | `app/relay.rs` | `a_stalled_sink_holds_the_source_back` |
| **One source cannot stall another** | Each database runs its own pipeline (channel, checkpoint, batches, dead letters) in its own task, all sharing one sink. `relay::supervise` restarts a failed pipeline alone, after a backoff, and a stop ends its backoff at once. | `app/relay.rs` (`supervise`), `main.rs` (`relay_all`) | `a_failed_source_restarts_and_catches_up`; `stopping_during_a_restart_backoff_returns_at_once`; `one_failing_source_does_not_hold_back_another`; `e2e_tenants` |
| **Clean stop without replays** | On SIGTERM the source stops reading between transactions and closes the channel. The core publishes what it was sent, and `relay::run` waits for the source to report the final ack. The source confirms the replication worker has sent it (`last_applied_lsn`) before closing the stream. | `postgres/mod.rs` (`stream`, `final_ack`), `app/relay.rs`, `main.rs` | `a_clean_stop_hands_the_final_ack_to_the_source`; `sigterm_drains_so_the_next_start_publishes_no_duplicates`; `a_second_signal_stops_a_drain_stuck_on_a_dead_broker` |
| **No poison-pill stall** | Only rejected *content* is `Permanent`. Call-level failures and unknown entry errors are retried, so a stall is visible and loses nothing. A rejected event is stored before it is confirmed, so the ack never passes an event that is nowhere. | `adapters/sqs.rs` (`classify`), `app/relay.rs` (`dead_letter`) | `dead_letters_a_permanently_rejected_event_and_moves_on`; `a_rejected_event_is_not_acked_until_it_is_dead_lettered`; `classify` tests; `e2e_postgres` |

## Failure model

- **Crash-only, per source.** Any error in a source's stream ends that source's pipeline with a JSON `ERROR` log, and `relay::supervise` starts it again after a backoff (1 s, growing to 60 s). Postgres replays everything that source had not acknowledged, exactly as after a process restart, so a restart is always safe, and the other sources keep streaming. The process itself exits only on a startup error, a signal or a panic.
- **Connecting waits instead of crashing** in these cases:
  - the slot is held by another relay (`55006`): this instance is the HA standby;
  - the database is starting (`57P03`) or unreachable.

  Meanwhile that source is not ready: `/readyz` returns 503 unless another source streams.
- **A missing slot fails that source**, with a hint to run `sql/slot.sql`, and it is retried with the backoff. The relay never creates slots: a recreated slot silently skips everything committed before it existed.
- **A missing dead-letter table or grant fails that source** as it starts, once the database is reachable, instead of stalling at the first rejected event, and it is retried with the backoff.
- **Broker errors never crash.** They are retried forever with backoff (100 ms up to 30 s), during which `sink_ready` is false.
- **Shutdown drains.** On SIGTERM/SIGINT every source stops reading its WAL at a transaction boundary, its core publishes everything already read, its final ack reaches Postgres, and the process exits 0: the next start replays nothing. If a source's final ack cannot reach Postgres, the other sources still finish their drains, and the process then exits 1. A source waiting for its slot, or in a restart backoff, stops at once. During a broker outage the drain waits; a second signal (or the orchestrator's SIGKILL) ends it, and unacknowledged events replay.

## Tests

| Kind | Where | Runs with | Needs |
|---|---|---|---|
| Unit: domain rules | `#[cfg(test)]` in `src/domain/*` | `cargo test` | nothing |
| Unit: adapter logic (pgoutput fixtures captured from Postgres 17, DSN, SQS and SNS mapping and splitting, Redis stream mapping and `rediss://` setup, config) | `#[cfg(test)]` in `src/adapters/*`, `src/config.rs` | `cargo test` | nothing |
| Core behavior through the ports, with fakes and paused time | `tests/relay.rs` | `cargo test` | nothing |
| Dependency rule | `tests/architecture.rs` | `cargo test` | nothing |
| End to end: Postgres 17 over TLS → relay → SQS API (ElasticMQ) | `tests/e2e_sqs.rs` | `cargo test -- --ignored` | Docker |
| End to end: Postgres 17 → relay → SNS FIFO topic → SQS FIFO queue (moto) | `tests/e2e_sns.rs` | `cargo test -- --ignored` | Docker |
| End to end: Postgres 17 → relay → Redis Streams | `tests/e2e_redis.rs` | `cargo test -- --ignored` | Docker |
| The Postgres adapter against a real database: the dead-letter table and its startup check | `tests/e2e_postgres.rs` | `cargo test -- --ignored` | Docker |
| End to end: three tenant databases → the relay binary → one SQS FIFO queue: per-tenant tags and groups, and broken tenants (a missing slot, a severed stream) isolated, then recovering | `tests/e2e_tenants.rs` | `cargo test -- --ignored` | Docker |
| The relay binary under signals: SIGTERM drains without replays, a second signal ends a stuck drain, SIGKILL loses nothing | `tests/crash.rs` | `cargo test -- --ignored` | Docker |
| Benchmark: throughput, latency and memory against a Python polling relay ([benchmarks.md](benchmarks.md)) | `benches/relay.rs` | `cargo bench --bench relay` | Docker, `bench/.venv` |

The SQS tests use ElasticMQ, a local SQS, and the SNS test uses moto, because LocalStack now needs an account token. ElasticMQ does not enforce SQS's message-size limit, so the oversized-event path is covered by unit tests only; verify it against real SQS.

## Adding a sink

For a new broker `x` (`sns.rs` is a compact example):
1. Create `src/adapters/x.rs` with an `XConfig` (serde) and an `XSink` implementing `EventSink`:
   - `const NAME: &'static str = "x";`
   - `publish` returns **one result per event, in order**. Classify errors conservatively: only content the broker will never accept is `Permanent`.
2. Add `X(XConfig)` to `SinkConfig` in `src/config.rs`.
3. Add the `SinkConfig::X` arm in `src/main.rs`. Each arm calls `relay::run` with a concrete type, so there are no trait objects.
4. Unit-test the mapping (envelope → broker request, ordering and dedup keys, splitting, error classification) inside the adapter.
5. Add `tests/e2e_<sink>.rs`, marked `#[ignore = "needs Docker"]`.

The core, the domain and every existing test stay untouched.

## Adding a source

Candidates are `pg_logical_emit_message` and a polling fallback for databases without replication rights.

1. Implement `EventSource::run(self, out, acked)`:
   - push `SourceMsg::Event` in commit order, each followed by a `SourceMsg::Progress` for its commit;
   - persist the latest `acked` value wherever the source keeps its position;
   - after a restart, resume from that position;
   - to stop cleanly, drop `out` at a transaction boundary and keep reporting `acked` until it closes: its last value is the final ack.
2. Wire it in `main.rs` behind a new config option.

## Known limits

Each is marked in the code with a `ponytail:` comment naming the upgrade path.

| Limit | Ceiling | Upgrade when needed |
|---|---|---|
| One batch in flight | About 10 events per broker round-trip: 4,800 events/s against a local SQS (M4), more than a FIFO queue without high throughput accepts | Pipeline batches with disjoint aggregates, once a broker takes more |
| One event per aggregate per batch | A single hot aggregate ships one event per request: 1,660 events/s (M4), behind the polling relay's 2,860, and at most 300 on a FIFO queue without high throughput | Allow same-aggregate runs, and resend the rest of a run when one entry fails: the first upgrade M4 points to |
| A transaction's rows wait for its `Commit` in memory | Very large outbox transactions use memory | Stream in-progress transactions (pgoutput protocol v2) |
| Channel capacity fixed at 1024 | Draining 20,000 events peaks at 17 MB of memory (M4) | Make it configurable if a benchmark shows it matters |
| One SQL connection per dead letter | Fine while dead letters are rare | Keep one connection open |
| Redis: one `XADD` round trip per event | About 10 round trips per batch | Pipeline the batch if a benchmark of the Redis sink asks for it (M4 measured SQS) |
| A drain waits for the broker | An outage holds it until SIGKILL or a second signal (safe: unacked events replay) | A drain-timeout setting, if grace periods prove too short |
| The database list is read at startup | Adding a tenant needs a restart (it drains, so nothing replays) | Discover slots by name prefix |
| One replication connection per database | Each reads the server's whole WAL; about 30 per server is comfortable | Fewer databases per server, or a polling source |
