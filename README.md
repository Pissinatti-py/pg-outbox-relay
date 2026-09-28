# pg-outbox-relay

**Reliable event publishing from PostgreSQL to message brokers, without dual writes.**

`pg-outbox-relay` is one small Rust binary. It streams the rows your application inserts into an `outbox` table, using Postgres logical replication, and publishes them to Amazon SQS.

- **At-least-once:** every committed event is published, even across relay crashes, restarts and broker outages.
- **Ordered per aggregate:** events of the same `aggregate_type` + `aggregate_id` arrive in commit order.
- **Idempotent consumers made easy:** every event carries a stable `id` to deduplicate on.

> **Status: milestone 1.** SQS (FIFO and standard) is supported. SNS, Redis Streams and a dead-letter table come next; see the [Roadmap](#roadmap).

---

## Contents

1. [Why](#why)
2. [How it works](#how-it-works)
3. [Quickstart (5 minutes)](#quickstart-5-minutes)
4. [Integrate your application](#integrate-your-application)
5. [Configure](#configure)
6. [Operate](#operate)
7. [Delivery semantics](#delivery-semantics)
8. [Develop](#develop)
9. [Roadmap](#roadmap)

---

## Why

Services often need to change their database *and* tell other services about it:

```python
session.add(order)
session.commit()
sqs.send_message(...)   # the process dies here → the event is lost forever
```

Those two writes cannot be atomic. Publishing after the commit can lose the event. Publishing before it can announce something that later rolls back. Retries only shrink the window; they never close it.

The **transactional outbox** pattern closes it. The application writes the event into an `outbox` table **in the same transaction** as the business change, so the event exists exactly when the change does. A separate relay then publishes committed outbox rows.

Debezium with Kafka Connect does this too, but it brings a JVM, Kafka and connectors. `pg-outbox-relay` is for teams that already run Postgres and SQS: **one binary (about 15 MB), one config, Prometheus metrics.**

## How it works

```
 ┌──────────── Application (Django / FastAPI / anything) ─────────────┐
 │  BEGIN;                                                            │
 │    UPDATE policies SET status = 'approved' ...;                    │
 │    INSERT INTO outbox (id, aggregate_type, aggregate_id, ...);     │
 │  COMMIT;                                                           │
 └──────────────────────────────┬─────────────────────────────────────┘
                                │ WAL
 ┌──────────────────────────────▼────── PostgreSQL ───────────────────┐
 │  publication outbox_pub (outbox inserts only)                      │
 │  replication slot outbox_relay (pgoutput)                          │
 └──────────────────────────────┬─────────────────────────────────────┘
                                │ logical replication stream
 ┌──────────────────────────────▼────── pg-outbox-relay ──────────────┐
 │  Postgres source ──► bounded channel ──► batch ──► SQS sink        │
 │        ▲                                              │            │
 │        └──── ack LSN (only after SQS confirmed) ◄─────┘            │
 └──────────────────────────────┬─────────────────────────────────────┘
                                ▼
                        SQS (FIFO or standard)
```

1. **Stream.** The relay holds a replication slot. Postgres sends every committed outbox insert, in commit order, milliseconds after the commit. There is no polling.
2. **Buffer.** Events pass through a bounded channel. If SQS is slow, the relay stops reading, and Postgres keeps the WAL instead of the relay filling its memory.
3. **Batch.** Up to 10 events per `SendMessageBatch`, flushed on size or after 20 ms. A batch never holds two events of the same aggregate, so a partial failure cannot reorder them.
4. **Publish.** Failed events are retried with exponential backoff and jitter, before anything newer goes out.
5. **Acknowledge.** The relay tells Postgres it has consumed up to LSN *X* **only after SQS confirmed every event up to *X***. After a crash, Postgres replays from the last acknowledged position. That is the whole at-least-once mechanism: no extra state store.

The code is split into hexagonal layers: pure rules, then ports, then the pipeline, then adapters. [docs/architecture.md](docs/architecture.md) walks through it.

## Quickstart (5 minutes)

Requirements: Docker with Compose. The demo runs Postgres 17, the relay, [ElasticMQ](https://github.com/softwaremill/elasticmq) as a local SQS, Prometheus and Grafana.

```bash
docker compose up -d --build        # the first build compiles the relay (a few minutes)
```

**1. Write an event, the way your application would:**

```bash
docker compose exec postgres psql -U app -d app -c "
  INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, headers)
  VALUES (gen_random_uuid(), 'policy', '42', 'policy.approved', '{\"policy_id\": 42}', '{\"tenant\": \"acme\"}');"
```

**2. Read it from the queue, the way a consumer would** (the SQS API, served by ElasticMQ on port 9324):

```bash
curl -s localhost:9324 \
  -H 'Content-Type: application/x-amz-json-1.0' -H 'X-Amz-Target: AmazonSQS.ReceiveMessage' \
  -d '{"QueueUrl": "http://localhost:9324/000000000000/events.fifo", "WaitTimeSeconds": 5,
       "MessageSystemAttributeNames": ["MessageGroupId"], "MessageAttributeNames": ["All"]}'
```

The message body is the event envelope. `MessageGroupId` is `policy:42`, and the event `id` is also the FIFO deduplication id:

```json
{"id":"0fafad7a-fd18-4f77-a13e-3dd92bd7ac61","aggregate_type":"policy","aggregate_id":"42","event_type":"policy.approved","occurred_at":"2026-09-28T16:21:58.774339Z","headers":{"tenant": "acme"},"payload":{"policy_id": 42}}
```

**3. Watch it:**

| What | Where |
|---|---|
| Relay metrics | `curl -s localhost:9090/metrics \| grep pg_outbox` |
| Health / readiness | `curl localhost:9090/healthz`, `curl localhost:9090/readyz` |
| Grafana dashboard | <http://localhost:3001> (anonymous admin, demo only) |
| Prometheus and alerts | <http://localhost:9091/alerts> |
| Relay logs (JSON) | `docker compose logs -f relay` |

**4. Break it on purpose.** Kill the relay mid-stream, insert more rows, then start it again:

```bash
docker compose kill relay
docker compose exec postgres psql -U app -d app -c "
  INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload)
  SELECT gen_random_uuid(), 'policy', (n % 5)::text, 'policy.updated', jsonb_build_object('n', n)
  FROM generate_series(1, 100) n;"
docker compose start relay          # picks up exactly where the slot says it stopped
curl -s localhost:9090/metrics | grep published_total   # the 100 events, plus any replays
```

Every event arrives. Any event that was published but not yet acknowledged is sent again with the same `id`, and the FIFO queue drops it as a duplicate.

Clean up with `docker compose down -v`.

## Integrate your application

### 1. Prepare Postgres (once)

The server needs logical replication:

```ini
wal_level = logical              # needs a restart
max_slot_wal_keep_size = 10GB    # safety net: the most WAL a stuck slot may hold (see Operate)
```

Then run the scripts in `sql/` in this order:

| Script | What it does | Notes |
|---|---|---|
| [`sql/outbox.sql`](sql/outbox.sql) | Creates the `outbox` table and the `outbox_pub` publication | Can go in your normal migrations |
| role (below) | Creates the relay's login | `REPLICATION` is the only privilege it needs |
| [`sql/slot.sql`](sql/slot.sql) | Creates the `outbox_relay` replication slot | Run **outside a transaction**; the relay never creates slots itself |

```sql
CREATE ROLE relay WITH LOGIN REPLICATION PASSWORD '...';
```

The outbox table:

| Column | Type | Meaning |
|---|---|---|
| `id` | `uuid` PK | Idempotency key. Consumers deduplicate on it |
| `aggregate_type` | `text` | For example `policy` |
| `aggregate_id` | `text` | Ordering key: events of one aggregate stay in order |
| `event_type` | `text` | For example `policy.approved` |
| `payload` | `jsonb` | The event data |
| `headers` | `jsonb` | Metadata such as tenant or trace id (default `{}`) |
| `created_at` | `timestamptz` | Becomes `occurred_at` in the envelope (default `now()`) |

### 2. Insert events in the same transaction as your change

No SDK and no broker client in the request path; it is one more insert.

**Django:**

```python
import uuid
from django.db import models, transaction

class Outbox(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    aggregate_type = models.TextField()
    aggregate_id = models.TextField()
    event_type = models.TextField()
    payload = models.JSONField()
    headers = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "outbox"
        managed = False  # created by sql/outbox.sql

with transaction.atomic():
    policy.status = "approved"
    policy.save()
    Outbox.objects.create(
        aggregate_type="policy",
        aggregate_id=str(policy.id),
        event_type="policy.approved",
        payload={"policy_id": policy.id, "approved_by": user.id},
        headers={"tenant": tenant.slug},
    )
```

**SQLAlchemy (async):**

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

If the transaction rolls back, the event never existed. If it commits, it will be published.

### 3. Consume

Each SQS message body is the envelope shown in the [Quickstart](#quickstart-5-minutes). Keep these in mind:
- **Deduplicate on `id`.** Delivery is at-least-once. A FIFO queue drops duplicates within its 5-minute deduplication window. Beyond that, record processed ids, for example with a processed-events table or a Redis `SET NX`.
- **FIFO queues** deliver each aggregate's events in commit order (`MessageGroupId = aggregate_type:aggregate_id`). **Standard queues** also work but do not keep order; the `id` is available as a message attribute there too.
- **Rows can be deleted** once they are published, for example with a nightly job that deletes rows older than 7 days, or with daily partitions you drop. The WAL already carried them. If you partition `outbox`, create the publication `WITH (publish = 'insert', publish_via_partition_root = true)`.

## Configure

The relay reads a TOML file (first argument, default `./relay.toml`), then environment variables named `RELAY__<SECTION>__<KEY>`. In containers the environment alone is enough. [`relay.example.toml`](relay.example.toml) is annotated.

| Setting | Environment variable | Default | Meaning |
|---|---|---|---|
| `source.dsn` | `RELAY__SOURCE__DSN` | required | `postgres://user:password@host:5432/db?sslmode=verify-full&sslrootcert=/ca.pem`. `sslmode`: `disable`, `prefer` (default), `require`, `verify-ca`, `verify-full` |
| `source.slot` | `RELAY__SOURCE__SLOT` | required | Replication slot, for example `outbox_relay` |
| `source.publication` | `RELAY__SOURCE__PUBLICATION` | required | Publication, for example `outbox_pub` |
| `sink.kind` | `RELAY__SINK__KIND` | required | `sqs` |
| `sink.queue_url` | `RELAY__SINK__QUEUE_URL` | required | A `.fifo` URL enables ordering and deduplication |
| `batching.max_events` | `RELAY__BATCHING__MAX_EVENTS` | `10` | Events per publish |
| `batching.max_wait_ms` | `RELAY__BATCHING__MAX_WAIT_MS` | `20` | How long a lone event waits for company |
| `retry.initial_backoff_ms` | `RELAY__RETRY__INITIAL_BACKOFF_MS` | `100` | First retry delay (jittered) |
| `retry.max_backoff_ms` | `RELAY__RETRY__MAX_BACKOFF_MS` | `30000` | Retry delay cap. Broker errors retry forever |
| `server.listen` | `RELAY__SERVER__LISTEN` | `0.0.0.0:9090` | `/metrics`, `/healthz`, `/readyz` |

**AWS credentials and region** come from the standard AWS chain: environment variables, profile, or an IAM role such as IRSA or an ECS task role. To point the relay at a local SQS, set `AWS_ENDPOINT_URL`. The relay needs `sqs:SendMessage` and `sqs:GetQueueAttributes` on the queue.

**Logs** are JSON on stdout. `RUST_LOG` overrides the level (default `info`).

## Operate

**Deploy** the Docker image (`docker build -t pg-outbox-relay .`, a distroless image of about 50 MB) or the binary.
- **High availability:** run **two replicas** against the same slot. Postgres lets only one consume it; the other waits (`/readyz` → 503) and takes over about 5 s after the first one's connection closes. No leader election is needed.
- **Kubernetes:** don't make `/readyz` a readiness probe of a Deployment with `maxUnavailable: 0`. The standby is never ready by design, so a rollout would wait forever. Use `strategy: Recreate`, or keep `/readyz` for monitoring only. `/healthz` is the liveness probe.

**Endpoints:**

| Endpoint | Meaning |
|---|---|
| `GET /healthz` | 200 when the process is alive |
| `GET /readyz` | 200 when it streams from the slot **and** the last publish succeeded; 503 otherwise |
| `GET /metrics` | Prometheus metrics |

**Metrics:**

| Metric | Type | Use it for |
|---|---|---|
| `pg_outbox_events_published_total{sink}` | counter | Throughput |
| `pg_outbox_publish_errors_total{sink,kind}` | counter | Broker health. `kind` is `retryable` or `permanent` |
| `pg_outbox_publish_latency_seconds` | histogram | Commit → broker acknowledgement |
| `pg_outbox_slot_lag_bytes` | gauge | **The main alert signal:** WAL the slot holds back. M1 exports it only with `sslmode=disable`; watch `pg_replication_slots` otherwise |
| `pg_outbox_dead_letters_total` | counter | Events skipped as permanently rejected |
| `pg_outbox_channel_depth` | gauge | Backpressure: near 1024 means the broker is the bottleneck |

**Alerts:** [`deploy/prometheus/alerts.yml`](deploy/prometheus/alerts.yml) ships four rules:
- `OutboxSlotLagHigh`;
- `OutboxRelayDown`, because while the relay is down its lag metric disappears but the slot keeps growing;
- `OutboxPublishStalled`;
- `OutboxPoisonEvents`.

**The replication slot is the one thing to respect.** An unconsumed slot makes Postgres keep WAL forever.
- **Set `max_slot_wal_keep_size`.** It caps that growth. Past the cap, Postgres invalidates the slot, and the relay then stops with an error instead of silently skipping anything.
- **Keep outbox rows longer than your longest tolerable outage.** Then any gap can be republished from the table.
- **Drop the slot if you retire the relay.**

## Delivery semantics

| Situation | What happens |
|---|---|
| The relay crashes after publishing, before acknowledging | On restart Postgres replays from the last ack: duplicates with the **same `id`**. FIFO queues drop them within 5 minutes |
| SQS is down, throttling, or credentials are wrong | Retried forever with backoff. The slot keeps the WAL, `/readyz` turns 503, `OutboxPublishStalled` fires. Nothing is lost |
| The database restarts or the connection drops | The relay exits (crash-only design), the orchestrator restarts it, it waits for the database and resumes from the last ack |
| A transaction rolls back | It never reaches the WAL stream, so it is never published |
| SQS rejects an event for good (invalid content, too large) | Logged at `ERROR` with the full envelope, counted in `pg_outbox_dead_letters_total`, and skipped so one bad row cannot block the stream. *M2 also stores it in an `outbox_dead_letter` table* |
| SIGTERM (deploys) | M1 stops at once; unacknowledged events are replayed as duplicates. *M2 drains in-flight batches and sends a final ack first* |

Exactly-once is not a goal. It is the consumer's job, made possible by `id`.

## Develop

Prerequisites: Rust 1.94.1 or newer (the AWS SDK sets that minimum) and Docker for the end-to-end test.

```bash
cargo test                          # unit, core and architecture tests: fast, no Docker
cargo test -- --ignored             # end to end: Postgres 17 → relay → SQS API (ElasticMQ)
cargo fmt --check && cargo clippy --all-targets -- -D warnings
cargo run -- relay.toml             # against your own Postgres and SQS
```

The code follows a hexagonal architecture: dependencies point inward, and `tests/architecture.rs` enforces that.

```
src/
├── domain/      pure rules: event + envelope, checkpoint (safe ack LSN), batching, backoff
├── ports.rs     EventSource / EventSink: what the core needs from the outside
├── app/         relay::run: the pipeline, and readiness
├── adapters/    postgres/ (replication + pgoutput), sqs.rs, http.rs
├── config.rs    TOML + RELAY__ env → every layer's settings
└── main.rs      composition root
tests/           relay.rs (core, with fakes), architecture.rs, e2e_sqs.rs (Docker)
sql/             outbox table + publication, replication slot
deploy/          demo configs: Postgres init, ElasticMQ, Prometheus + alerts, Grafana
docs/            architecture.md, adr/, spec.md (the original design)
```

[docs/architecture.md](docs/architecture.md) covers:
- the life of one event through the code;
- why each guarantee holds, with the test that proves it;
- how to add a sink in five steps;
- the known limits and their upgrade paths.

Decisions are recorded in [docs/adr/](docs/adr/), for example why the replication client is `pgwire-replication`.

## Roadmap

| Milestone | Scope | Status |
|---|---|---|
| **M1** | Replication source, SQS FIFO sink, LSN checkpointing, metrics and health, Docker Compose demo, end-to-end test | ✅ done |
| **M2** | SNS and Redis Streams sinks, `outbox_dead_letter` table, graceful drain on SIGTERM, TLS for SQL connections, process-kill crash test in CI | next |
| **M3** | Multi-source: one process relays N databases (one slot per tenant database). Until then, run one relay per database | planned |
| **M4** | Benchmarks (events/s, p99 latency, memory) against a Python polling relay | planned |

Non-goals: exactly-once end to end, general-purpose CDC for arbitrary tables (use Debezium), and schema registries or routing DSLs.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
