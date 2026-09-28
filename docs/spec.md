# pg-outbox-relay

**Reliable event publishing from PostgreSQL to message brokers, without dual writes.**

A single, small Rust binary that streams rows from a PostgreSQL *outbox* (via logical replication) and publishes them to AWS SQS, AWS SNS or Redis Streams. Delivery is at-least-once, ordering is preserved per aggregate, and every event carries an idempotency key.

---

## 1. Problem

Backend services often need to do two things together: change state in the database and tell other services it happened.

```python
session.add(order)
session.commit()
sqs.send_message(...)   # process dies here → event lost forever
```

This *dual write* cannot be made atomic. If the process crashes between the commit and the publish, the event is lost. Publishing before the commit has the opposite problem: consumers can react to data that was later rolled back. Retries in application code only shrink the window. They never close it.

The **Transactional Outbox** pattern fixes this. The application writes the event to an `outbox` table **in the same transaction** as the business data. A separate relay then reads committed outbox rows and publishes them. The database's transaction becomes the source of truth for "did this happen?".

Existing relays (Debezium + Kafka Connect) are powerful but heavy: JVM, Kafka, and connectors to run. `pg-outbox-relay` targets teams that already run Postgres and SQS/SNS/Redis and want the pattern as **one ~10 MB binary**.

## 2. Goals and non-goals

**Goals**
- Publish every committed outbox event **at least once**, even across relay crashes, restarts and broker outages.
- Keep **per-aggregate ordering**: events for the same `aggregate_id` arrive in commit order.
- Give every event a stable **idempotency key** so consumers can deduplicate.
- Keep latency low (commit → publish in milliseconds) by using logical replication, not polling.
- Be easy to operate: one binary, one config file, Prometheus metrics, health endpoints, graceful shutdown.

**Non-goals**
- Exactly-once delivery end to end. That is the consumer's job, made possible by the idempotency key.
- General-purpose CDC for arbitrary tables. Debezium does that. This relay only reads the outbox.
- Schema registry, event transformation or routing DSLs.

## 3. How it works

```
 ┌──────────────── Application (Django / FastAPI / any) ────────────────┐
 │  BEGIN;                                                              │
 │    UPDATE orders ...;                                                │
 │    INSERT INTO outbox (aggregate_type, aggregate_id, type, payload); │
 │  COMMIT;                                                             │
 └───────────────────────────────┬──────────────────────────────────────┘
                                 │ WAL
                                 ▼
 ┌─────────────────────────── PostgreSQL ───────────────────────────────┐
 │  publication: outbox_pub (outbox table only)                         │
 │  replication slot: outbox_relay (pgoutput)                           │
 └───────────────────────────────┬──────────────────────────────────────┘
                                 │ logical replication stream
                                 ▼
 ┌────────────────────────── pg-outbox-relay ───────────────────────────┐
 │  Source ──► Decoder ──► bounded channel ──► Batcher ──► Sink         │
 │     ▲                                                     │          │
 │     └────── ack LSN (only after sink confirms) ◄──────────┘          │
 └───────────────────────────────┬──────────────────────────────────────┘
                                 ▼
               SQS (FIFO) · SNS (FIFO) · Redis Streams
```

1. **Source** opens a replication connection and consumes the `pgoutput` stream for the `outbox_pub` publication.
2. **Decoder** turns `INSERT` messages into `OutboxEvent` values and ignores everything else. Each event is tagged with the commit LSN of its transaction.
3. A **bounded channel** provides backpressure. If the sink is slow, the relay stops reading WAL instead of buffering without limit.
4. **Batcher** groups events, for example up to 10 per `SendMessageBatch` on SQS, and flushes on size or on a short timeout.
5. **Sink** publishes the batch and retries failed entries with exponential backoff and jitter.
6. **Checkpointing:** the relay reports a new `flush_lsn` to Postgres **only after every event up to that LSN is confirmed by the broker**. After a crash, Postgres replays from the last confirmed LSN. This is the source of the at-least-once guarantee, and no extra state store is needed.

## 4. Data model

### Outbox table

```sql
CREATE TABLE outbox (
    id             uuid        PRIMARY KEY,          -- idempotency key
    aggregate_type text        NOT NULL,             -- e.g. 'policy'
    aggregate_id   text        NOT NULL,             -- ordering key
    event_type     text        NOT NULL,             -- e.g. 'policy.approved'
    payload        jsonb       NOT NULL,
    headers        jsonb       NOT NULL DEFAULT '{}',
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE PUBLICATION outbox_pub FOR TABLE outbox WITH (publish = 'insert');
```

The relay only needs `INSERT`s. Rows can be removed by a retention job (for example, delete rows older than 7 days) or by partitioning `outbox` by day and dropping old partitions. The WAL already carries the event, so deleting a row never affects delivery.

### Event envelope (what consumers receive)

```json
{
  "id": "0b7e…",
  "aggregate_type": "policy",
  "aggregate_id": "42",
  "event_type": "policy.approved",
  "occurred_at": "2026-09-28T14:03:11Z",
  "headers": { "tenant": "acme", "trace_id": "…" },
  "payload": { "…": "…" }
}
```

### Broker mapping

| Sink            | Ordering key                          | Dedup key                    |
|-----------------|---------------------------------------|------------------------------|
| SQS FIFO        | `MessageGroupId = aggregate_type:aggregate_id` | `MessageDeduplicationId = id` |
| SNS FIFO        | same as SQS                           | same as SQS                  |
| SQS / SNS std.  | none (best effort)                    | `id` in message attributes   |
| Redis Streams   | one stream per `aggregate_type`       | `id` field in entry          |

## 5. Delivery semantics

| Situation                          | Outcome                                                                 |
|------------------------------------|-------------------------------------------------------------------------|
| Relay crashes after publish, before ack | Events are re-published on restart (duplicates, same `id`).          |
| Broker unavailable                 | Retries with backoff. WAL is retained by the slot, so nothing is lost.  |
| Transaction rolled back            | Never reaches the WAL stream, so it is never published.                 |
| Event permanently rejected (e.g. payload > 256 KB) | Logged, counted, and written to a `outbox_dead_letter` table. The relay moves on, so one bad row cannot block the stream. |

**Consumers must be idempotent** and deduplicate on `id`. SQS/SNS FIFO does this within their 5-minute window. Beyond that, a processed-events table or a Redis `SET NX` does it.

## 6. Operational concerns

- **Replication slot growth.** An inactive slot makes Postgres retain WAL indefinitely. The relay exports `pg_outbox_slot_lag_bytes`, and the docs ship an alert rule for it. Operators should also set `max_slot_wal_keep_size` as a safety net.
- **Single active instance per slot.** Postgres allows only one consumer per slot. For high availability, run two replicas: the standby retries the connection and takes over when the active one dies. No leader election is needed.
- **Graceful shutdown.** On `SIGTERM`, the relay stops reading, drains in-flight batches, sends a final LSN ack, then exits.
- **Health.** `GET /healthz` reports that the process is alive. `GET /readyz` reports that it is connected to the slot and the sink.

### Metrics (Prometheus, `GET /metrics`)

| Metric                                  | Type      | Why it matters                        |
|-----------------------------------------|-----------|---------------------------------------|
| `pg_outbox_events_published_total{sink}`| counter   | Throughput                            |
| `pg_outbox_publish_errors_total{sink,kind}` | counter | Broker health                        |
| `pg_outbox_publish_latency_seconds`     | histogram | Commit → broker ack latency           |
| `pg_outbox_slot_lag_bytes`              | gauge     | Main alerting signal (WAL piling up)  |
| `pg_outbox_dead_letters_total`          | counter   | Poison events                         |
| `pg_outbox_channel_depth`               | gauge     | Backpressure visibility               |

Logs are structured JSON via `tracing`, including `event_id` and `lsn` for traceability.

## 7. Configuration

```toml
[source]
dsn         = "postgres://relay@db:5432/app?replication=database"
slot        = "outbox_relay"
publication = "outbox_pub"

[sink]
kind      = "sqs"                        # sqs | sns | redis
queue_url = "https://sqs.us-east-1.amazonaws.com/123/events.fifo"

[batching]
max_events   = 10
max_wait_ms  = 20

[retry]
initial_backoff_ms = 100
max_backoff_ms     = 30000

[server]
listen = "0.0.0.0:9090"                  # /metrics, /healthz, /readyz
```

Every value can be overridden with an environment variable (for example, `RELAY__SINK__QUEUE_URL`) for containers.

## 8. Application side (Python example)

The only thing an application needs is to insert into `outbox` in the same transaction:

```python
async with session.begin():
    policy.status = "approved"
    session.add(Outbox(
        id=uuid4(),
        aggregate_type="policy",
        aggregate_id=str(policy.id),
        event_type="policy.approved",
        payload={"policy_id": policy.id, "approved_by": user.id},
        headers={"tenant": tenant.slug},
    ))
```

No SDK and no broker client in the request path.

## 9. Tech stack

| Concern             | Choice                                                      |
|---------------------|-------------------------------------------------------------|
| Runtime             | `tokio`                                                     |
| Replication protocol| Replication-capable Postgres client (see Open questions)    |
| Brokers             | `aws-sdk-sqs`, `aws-sdk-sns`, `redis` (async)               |
| HTTP (metrics/health)| `axum`                                                     |
| Metrics / logs      | `prometheus`, `tracing`, `tracing-subscriber` (JSON)        |
| Config              | `serde` + `toml`, env overrides                             |
| Tests               | `cargo test`, `testcontainers` (Postgres, LocalStack, Redis)|
| Packaging           | Static binary, distroless Docker image, Docker Compose demo |

## 10. Testing strategy

- **Unit:** decoding `pgoutput` messages, envelope building, batch splitting, and backoff math.
- **Integration (testcontainers):** real Postgres + LocalStack. The test inserts N rows and asserts that N messages arrive with correct ordering per `MessageGroupId`.
- **Crash test:** kill the relay mid-batch, restart it, and assert **no missing events**. Duplicates are allowed, and each duplicate must have the same `id`.
- **Chaos:** stop LocalStack for 30 s and assert that the relay recovers, that slot lag grows and then drains, and that nothing is lost.
- **CI:** GitHub Actions runs `cargo fmt --check`, `clippy -D warnings`, and the unit and integration tests.

## 11. Roadmap

| Milestone | Scope |
|-----------|-------|
| **M1: MVP** | Replication source, SQS FIFO sink, LSN checkpointing, `/metrics`, Docker Compose demo (Postgres + LocalStack + Grafana) |
| **M2** | SNS and Redis Streams sinks, dead-letter table, graceful shutdown, crash test in CI |
| **M3** | Multi-source mode: one process relays from N databases (one slot per tenant DB) |
| **M4** | Benchmark report (events/s, p99 latency, memory footprint) vs. a Python polling relay |

## 12. Open questions

- **Replication client.** The upstream `tokio-postgres` crate does not expose the logical replication protocol. The options are a fork that does (for example, Materialize's), the `etl` crate from Supabase, or implementing the small subset of `pgoutput` needed for `INSERT`s directly. This needs a spike before M1.
- **`pg_logical_emit_message`.** On Postgres 14+, applications can write events directly to the WAL without a table. It is worth supporting as an alternative source once M1 is done.
- **Polling fallback.** Some managed databases do not grant replication permissions. A `SELECT … FOR UPDATE SKIP LOCKED` polling mode would cover them, at the cost of higher latency.
