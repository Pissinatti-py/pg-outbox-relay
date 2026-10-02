# pg-outbox-relay

**Reliable event publishing from PostgreSQL to message brokers, without dual writes.**

`pg-outbox-relay` is one small Rust binary. It streams the rows your application inserts into an `outbox` table, using Postgres logical replication, and publishes them to Amazon SQS, Amazon SNS or Redis Streams.

- **At-least-once:** every committed event is published, even across relay crashes, restarts and broker outages.
- **Ordered per aggregate:** events of the same `aggregate_type` + `aggregate_id` arrive in commit order.
- **Idempotent consumers made easy:** every event carries a stable `id` to deduplicate on.

> **Status: milestone 3.** One relay serves many tenant databases (one slot each), publishing to SQS, SNS or Redis Streams. Benchmarks come next; see the [Roadmap](#roadmap).

---

## Contents

1. [Why](#why)
2. [How it works](#how-it-works)
3. [Quickstart (5 minutes)](#quickstart-5-minutes)
4. [Integrate your application](#integrate-your-application) (including [several databases](#4-several-databases-one-per-tenant))
5. [Examples](#examples)
6. [Configure](#configure)
7. [Operate](#operate)
8. [Delivery semantics](#delivery-semantics)
9. [Upgrading from M2](#upgrading-from-m2)
10. [Develop](#develop)
11. [Roadmap](#roadmap)

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

Debezium with Kafka Connect does this too, but it brings a JVM, Kafka and connectors. `pg-outbox-relay` is for teams that already run Postgres and SQS, SNS or Redis: **one binary (about 15 MB), one config, Prometheus metrics.**

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
 │  Postgres source ──► bounded channel ──► batch ──► sink            │
 │        ▲                                              │            │
 │        └──── ack LSN (after the broker confirmed) ◄───┘            │
 └──────────────────────────────┬─────────────────────────────────────┘
                                ▼
             SQS (FIFO or standard) · SNS (FIFO or standard) · Redis Streams
```

1. **Stream.** The relay holds a replication slot. Postgres sends every committed outbox insert, in commit order, milliseconds after the commit. There is no polling.
2. **Buffer.** Events pass through a bounded channel. If the broker is slow, the relay stops reading, and Postgres keeps the WAL instead of the relay filling its memory.
3. **Batch.** Up to 10 events per call (`SendMessageBatch` or `PublishBatch`), flushed on size or after 20 ms. A batch never holds two events of the same aggregate, so a partial failure cannot reorder them.
4. **Publish.** Failed events are retried with exponential backoff and jitter, before anything newer goes out.
5. **Acknowledge.** The relay tells Postgres it has consumed up to LSN *X* **only after the broker confirmed every event up to *X***. After a crash, Postgres replays from the last acknowledged position. That is the whole at-least-once mechanism: no extra state store.

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

The message body is the event envelope, and `source` is the database it was committed in. `MessageGroupId` is `app:policy:42`, and the event `id` is also the FIFO deduplication id:

```json
{"id":"0fafad7a-fd18-4f77-a13e-3dd92bd7ac61","source":"app","aggregate_type":"policy","aggregate_id":"42","event_type":"policy.approved","occurred_at":"2026-09-28T16:21:58.774339Z","headers":{"tenant": "acme"},"payload":{"policy_id": 42}}
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

`docker compose stop relay` sends SIGTERM instead: the relay drains, and the restart replays nothing.

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
| [`sql/outbox.sql`](sql/outbox.sql) | Creates the `outbox` table, the `outbox_pub` publication and the `outbox_dead_letter` table | Can go in your normal migrations |
| role (below) | Creates the relay's login | `REPLICATION`, plus `INSERT` on `outbox_dead_letter`, are the only privileges it needs |
| [`sql/slot.sql`](sql/slot.sql) | Creates the `outbox_relay` replication slot | Run **outside a transaction**; the relay never creates slots itself |

```sql
CREATE ROLE relay WITH LOGIN REPLICATION PASSWORD '...';
GRANT INSERT ON outbox_dead_letter TO relay;
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
- **FIFO queues** deliver each aggregate's events in commit order (`MessageGroupId = source:aggregate_type:aggregate_id`). **Standard queues** also work but do not keep order; the `id` is available as a message attribute there too.
- **SNS:** subscribe queues with raw message delivery, so the body is the envelope. `id`, `event_type` and `source` are message attributes, usable in subscription filter policies. A `.fifo` topic keeps the same per-aggregate order and delivers to `.fifo` queues.
- **Redis Streams:** each entry has `id`, `source`, `event_type` and `envelope` (the JSON above), in the stream `outbox:<aggregate_type>`. An aggregate's events stay in order within its stream. Read with `XREADGROUP` and deduplicate on `id`. The relay never trims: use `XTRIM <stream> MINID <id>` once every consumer group has passed an entry (`MAXLEN` drops entries a lagging group has not read).
- **Rows can be deleted** once they are published, for example with a nightly job that deletes rows older than 7 days, or with daily partitions you drop. The WAL already carried them. If you partition `outbox`, create the publication `WITH (publish = 'insert', publish_via_partition_root = true)`.

### 4. Several databases (one per tenant)

One relay can serve every tenant database on a Postgres server, each through its own pipeline. List the databases, and use `{database}` in the DSN and in the slot name, since slot names are unique across the whole server:

```bash
RELAY__SOURCE__DSN=postgres://relay:...@db:5432/{database}?sslmode=verify-full
RELAY__SOURCE__SLOT=outbox_{database}
RELAY__SOURCE__PUBLICATION=outbox_pub
RELAY__SOURCE__DATABASES=acme,globex,initech
```

In each tenant database, run `sql/outbox.sql`, grant the relay INSERT on `outbox_dead_letter`, and create the database's slot outside a transaction:

```sql
SELECT pg_create_logical_replication_slot('outbox_' || current_database(), 'pgoutput');
```

- **Tenants are isolated.** A database that is down, or whose slot is missing, is retried with a backoff (1 s, growing to 60 s) while every other tenant keeps streaming. `pg_outbox_source_up{source}` and the `OutboxSourceDown` alert show it.
- **Events carry their database** in `source`, and FIFO group and deduplication ids start with it: two tenants' `policy:42` never share a group, and a FIFO queue never drops one tenant's event as a duplicate of another's. An `id` is unique only within its database, so unless your ids are random UUIDs, consumers deduplicate on `source` and `id`.
- **Adding a tenant** means adding it to the list and restarting the relay. The restart drains, so nothing replays.
- **Postgres limits:** raise `max_replication_slots` and `max_wal_senders` above the number of tenants (both default to 10), with a few spare for a standby's connection attempts. Every slot's connection reads the server's whole WAL, so about 30 tenants per server is comfortable.

## Examples

Three common ways to use the relay with SQS. SNS and Redis Streams work the same way: pick the sink in [Configure](#configure).

| Example | Queue | Why that queue | The idea to take away |
|---|---|---|---|
| [Order paid → fulfillment](#example-1-order-paid--fulfillment-worker) | FIFO | Each order's events must be handled in sequence | One aggregate = one ordered stream |
| [Policy approved → e-mail](#example-2-policy-approved--notification-e-mail) | Standard | No ordering needed, higher throughput | Replace `on_commit` side effects; send each e-mail once |
| [Search index in sync](#example-3-keeping-a-search-index-in-sync) | FIFO | Each document's updates must apply in sequence | State-carrying events, a version guard, fan-out with a second relay |

The consumer snippets are sketches of the `handle(message)` step. Around it runs the usual SQS loop: long-poll `ReceiveMessage`, call `handle`, then `DeleteMessage` once it returns.

### Example 1: Order paid → fulfillment worker

**The problem.** When an order is paid, the fulfillment service reserves stock and ships it.
- **Separate commit and publish can go wrong both ways.** A crash between the commit and the publish leaves a paid order that never ships. Publishing first can ship an order whose payment then rolls back.
- **Order matters.** `order.cancelled` must never be handled before `order.paid`.

**Producer:** plain SQL. Any driver or ORM works the same way.

```sql
BEGIN;
UPDATE orders SET status = 'paid' WHERE id = 1234;
INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload)
VALUES (gen_random_uuid(), 'order', '1234', 'order.paid', '{"order_id": 1234, "amount_cents": 4990}');
COMMIT;
```

A later cancellation writes `order.cancelled` with the same `aggregate_id`, in the same way.

**Relay:** point it at a FIFO queue.

```bash
RELAY__SINK__KIND=sqs
RELAY__SINK__QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/fulfillment.fifo
```

Every event of order 1234 carries `MessageGroupId = app:order:1234` (`app` being its database). SQS FIFO releases the next message of a group only after the previous one is deleted, so `order.cancelled` waits until `order.paid` has been handled. Different orders are still processed in parallel.

**Worker (sketch):** record the event `id` in the same database transaction as the work.

```python
def handle(message):
    event = json.loads(message["Body"])
    with fulfillment_db.transaction():
        # INSERT INTO processed_events (id) VALUES (%s) ON CONFLICT DO NOTHING, True if inserted.
        if not record_processed(event["id"]):
            return  # a replayed duplicate: already handled
        order_id = event["payload"]["order_id"]
        if event["event_type"] == "order.paid":
            reserve_stock(order_id)
        elif event["event_type"] == "order.cancelled":
            release_stock(order_id)
```

**What guarantees what:**
- **The payment and its event commit together.** There is no lost `order.paid` and no event for a payment that rolled back.
- **The group ID keeps each order in sequence.**
- **The `processed_events` row commits together with the stock change.** If the work fails, both roll back and the message is retried. If the relay replays the event after a crash, the unique `id` turns it into a no-op.

### Example 2: Policy approved → notification e-mail

**The problem.** Django apps often trigger side effects with `transaction.on_commit`:

```python
with transaction.atomic():
    policy.status = "approved"
    policy.save()
    transaction.on_commit(lambda: send_approval_email.delay(policy.id))
```

The callback runs in the same process, after the commit. If the process dies at that moment, or the Celery broker is unreachable, the task is never queued: the policy is approved and nobody is told. It is the dual write again, just harder to notice.

**Producer:** one more row in the same transaction. This uses the `Outbox` model from [Integrate your application](#2-insert-events-in-the-same-transaction-as-your-change).

```python
with transaction.atomic():
    policy.status = "approved"
    policy.save()
    Outbox.objects.create(
        aggregate_type="policy",
        aggregate_id=str(policy.id),
        event_type="policy.approved",
        payload={"policy_id": policy.id, "holder_email": policy.holder_email},
        headers={"tenant": tenant.slug},
    )
```

**Relay:** a standard queue is enough, because e-mails need no ordering.

```bash
RELAY__SINK__QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/notifications
```

Standard queues don't deduplicate, and SQS itself can deliver a message twice. The relay therefore also puts the event `id`, `event_type` and `source` in message attributes, so the worker can check them without parsing the body.

**Worker (sketch):**

```python
def handle(message):
    event = json.loads(message["Body"])
    if event["event_type"] != "policy.approved":
        return  # every outbox event reaches this queue; skip the ones that aren't notifications
    key = f"notified:{event['id']}"
    if redis.exists(key):
        return  # this event was already e-mailed
    send_email(
        to=event["payload"]["holder_email"],
        template="policy_approved",
        tenant=event["headers"]["tenant"],
    )
    redis.set(key, 1, ex=7 * 24 * 3600)  # marked after sending, so a failed send is retried
```

A crash between sending and marking can still send one duplicate. If your e-mail provider supports idempotency keys, pass `event["id"]` and that gap closes too.

### Example 3: Keeping a search index in sync

**The problem.** Products are edited in Postgres and searched in Elasticsearch or OpenSearch. The same applies to a cache or a reporting database. Updating the index inside the request is another dual write. A failed index call leaves search stale for good, or fails the user's request over something secondary.

**Producer:** publish the product's *new state*, including a version that increases with every change. One statement does both:

```sql
WITH p AS (
    UPDATE products SET price_cents = 1990, version = version + 1 WHERE id = 7
    RETURNING id, name, price_cents, version
)
INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload)
SELECT gen_random_uuid(), 'product', p.id::text, 'product.updated', to_jsonb(p) FROM p;
```

The consumer receives `"payload": {"id": 7, "name": "Blue mug", "version": 12, "price_cents": 1990}`. The indexer never has to query the database, and the version lets it ignore anything older than what it already has.

**Relay:** a FIFO queue, so each product's updates arrive in sequence.

```bash
RELAY__SINK__QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/search-indexer.fifo
```

**Indexer (sketch):** use Elasticsearch/OpenSearch external versioning.

```python
def handle(message):
    event = json.loads(message["Body"])
    doc = event["payload"]
    try:
        if event["event_type"] == "product.updated":
            es.index(index="products", id=doc["id"], document=doc,
                     version=doc["version"], version_type="external")
        elif event["event_type"] == "product.deleted":
            es.delete(index="products", id=doc["id"],
                      version=doc["version"], version_type="external")
    except ConflictError:
        pass  # the index already holds this version or a newer one: a duplicate or stale event
```

**Feeding several services from the same events (fan-out).** A relay publishes to one queue. To deliver the same events to both the search indexer and the notification service, run **one relay per queue**, each with its own replication slot:

```sql
SELECT pg_create_logical_replication_slot('outbox_search', 'pgoutput');  -- outside a transaction
```

```bash
RELAY__SOURCE__SLOT=outbox_search
RELAY__SINK__QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/search-indexer.fifo
```

Each relay reads the same publication and acknowledges its own slot:
- **Independence:** a slow indexer never delays notifications.
- **Cost:** each slot also keeps WAL until its own relay catches up, so watch `pg_outbox_slot_lag_bytes` for every relay.

Or publish to an SNS topic instead: one relay and one slot, and each service subscribes its own queue (raw message delivery on), with a filter policy on `event_type` if it only needs some events.

```bash
RELAY__SINK__KIND=sns
RELAY__SINK__TOPIC_ARN=arn:aws:sns:us-east-1:123456789012:products.fifo
```

**Building the index the first time.** A slot streams changes from the moment it's created; it isn't a backfill tool. Build the initial index from the `products` table, then let the relay keep it current. The version check makes any overlap between the two harmless.

## Configure

The relay reads a TOML file (first argument, default `./relay.toml`), then environment variables named `RELAY__<SECTION>__<KEY>`. In containers the environment alone is enough. [`relay.example.toml`](relay.example.toml) is annotated.

| Setting | Environment variable | Default | Meaning |
|---|---|---|---|
| `source.dsn` | `RELAY__SOURCE__DSN` | required | `postgres://user:password@host:5432/db?sslmode=verify-full&sslrootcert=/ca.pem`. `sslmode`: `disable`, `prefer` (default), `require`, `verify-ca`, `verify-full`. It applies to every connection: replication, slot lag and dead letters. With `source.databases`, `{database}` stands for each name |
| `source.slot` | `RELAY__SOURCE__SLOT` | required | Replication slot, for example `outbox_relay`. With `source.databases`, a template such as `outbox_{database}`: slot names are unique across the server |
| `source.publication` | `RELAY__SOURCE__PUBLICATION` | required | Publication, for example `outbox_pub` |
| `source.databases` | `RELAY__SOURCE__DATABASES` | empty | Relay these databases, one pipeline each (comma-separated in the environment). See [Several databases](#4-several-databases-one-per-tenant) |
| `sink.kind` | `RELAY__SINK__KIND` | required | `sqs`, `sns` or `redis` |
| `sink.queue_url` | `RELAY__SINK__QUEUE_URL` | required for `sqs` | A `.fifo` URL enables ordering and deduplication |
| `sink.topic_arn` | `RELAY__SINK__TOPIC_ARN` | required for `sns` | A `.fifo` ARN enables ordering and deduplication |
| `sink.url` | `RELAY__SINK__URL` | required for `redis` | `redis://user:password@host:6379/0`; `rediss://` for TLS |
| `sink.stream_prefix` | `RELAY__SINK__STREAM_PREFIX` | `outbox:` | Events go to the stream `<prefix><aggregate_type>` |
| `batching.max_events` | `RELAY__BATCHING__MAX_EVENTS` | `10` | Events per publish |
| `batching.max_wait_ms` | `RELAY__BATCHING__MAX_WAIT_MS` | `20` | How long a lone event waits for company |
| `retry.initial_backoff_ms` | `RELAY__RETRY__INITIAL_BACKOFF_MS` | `100` | First retry delay (jittered) |
| `retry.max_backoff_ms` | `RELAY__RETRY__MAX_BACKOFF_MS` | `30000` | Retry delay cap. Broker errors retry forever |
| `server.listen` | `RELAY__SERVER__LISTEN` | `0.0.0.0:9090` | `/metrics`, `/healthz`, `/readyz` |

**AWS credentials and region** come from the standard AWS chain: environment variables, profile, or an IAM role such as IRSA or an ECS task role. To point the relay at a local SQS or SNS, set `AWS_ENDPOINT_URL`. The relay needs `sqs:SendMessage` and `sqs:GetQueueAttributes` on the queue, or `sns:Publish` and `sns:GetTopicAttributes` on the topic.

**Logs** are JSON on stdout. `RUST_LOG` overrides the level (default `info`).

## Operate

**Deploy** the Docker image (`docker build -t pg-outbox-relay .`, a distroless image of about 50 MB) or the binary.
- **High availability:** run **two replicas** against the same slot. Postgres lets only one consume it; the other waits (`/readyz` → 503) and takes over about 5 s after the first one's connection closes. No leader election is needed. With several databases, the two replicas share the slots: each is streamed by exactly one of them.
- **Shutdown:** SIGTERM drains (see [Delivery semantics](#delivery-semantics)). Against a healthy broker that takes well under a second, so the default grace periods (Kubernetes 30 s, `docker stop` 10 s) are plenty.
- **Kubernetes:** don't make `/readyz` a readiness probe of a Deployment with `maxUnavailable: 0`. The standby is never ready by design, so a rollout would wait forever. Use `strategy: Recreate`, or keep `/readyz` for monitoring only. `/healthz` is the liveness probe.

**Endpoints:**

| Endpoint | Meaning |
|---|---|
| `GET /healthz` | 200 when the process is alive |
| `GET /readyz` | 200 when at least one of its sources streams from its slot **and** no source's last publish failed; 503 otherwise |
| `GET /metrics` | Prometheus metrics |

**Metrics:**

| Metric | Type | Use it for |
|---|---|---|
| `pg_outbox_events_published_total{sink,source}` | counter | Throughput |
| `pg_outbox_publish_errors_total{sink,kind,source}` | counter | Broker health. `kind` is `retryable` or `permanent` |
| `pg_outbox_publish_latency_seconds{source}` | histogram | Commit → broker acknowledgement |
| `pg_outbox_slot_lag_bytes{source}` | gauge | **The main alert signal:** WAL the slot holds back |
| `pg_outbox_source_up{source}` | gauge | 1 while this relay streams the source's slot. `sum by (source)` across replicas shows whether anyone does |
| `pg_outbox_dead_letters_total{source}` | counter | Events rejected for good (stored in `outbox_dead_letter`) |
| `pg_outbox_channel_depth{source}` | gauge | Backpressure: near 1024 means the broker is the bottleneck |

`source` is the database the relay reads, and `sink` the broker (`sqs`, `sns` or `redis`).

**Alerts:** [`deploy/prometheus/alerts.yml`](deploy/prometheus/alerts.yml) ships five rules, each per source where it applies:
- `OutboxSlotLagHigh`;
- `OutboxRelayDown`, because while the relay is down its lag metric disappears but the slot keeps growing;
- `OutboxSourceDown`, when no relay streams a source (its database is down or its slot is missing);
- `OutboxPublishStalled`;
- `OutboxPoisonEvents`.

**The replication slot is the one thing to respect.** An unconsumed slot makes Postgres keep WAL forever.
- **Set `max_slot_wal_keep_size`.** It caps that growth. Past the cap, Postgres invalidates the slot, and the relay then stops with an error instead of silently skipping anything.
- **Keep outbox rows longer than your longest tolerable outage.** Then any gap can be republished from the table.
- **Drop the slot if you retire the relay.**

**Dead letters.** An event the broker rejects for good is stored with the broker's reason:

```sql
SELECT id, reason, failed_at, envelope FROM outbox_dead_letter ORDER BY failed_at;
```

To republish one after fixing the cause, insert a corrected row into `outbox` with a new `id`, then delete the dead letter. The relay refuses to start without the table or its `INSERT` grant. **Upgrading from M1:** run the `CREATE TABLE outbox_dead_letter` statement from [`sql/outbox.sql`](sql/outbox.sql), then the `GRANT`.

## Delivery semantics

| Situation | What happens |
|---|---|
| The relay crashes after publishing, before acknowledging | On restart Postgres replays from the last ack: duplicates with the **same `id`**. FIFO queues drop them within 5 minutes |
| The broker is down, throttling, or credentials are wrong | Retried forever with backoff. The slot keeps the WAL, `/readyz` turns 503, `OutboxPublishStalled` fires. Nothing is lost |
| The database restarts or the connection drops | That source's pipeline restarts after a backoff (1 s, growing to 60 s), waits for the database and resumes from its last ack. The other sources keep streaming |
| A transaction rolls back | It never reaches the WAL stream, so it is never published |
| The broker (SQS or SNS) rejects an event for good (invalid content, too large) | Stored in `outbox_dead_letter` with the broker's reason (retried until it is), logged at `ERROR`, counted in `pg_outbox_dead_letters_total`, then skipped so one bad row cannot block the stream. Redis never rejects content, so the Redis sink never dead-letters |
| SIGTERM (deploys) | Stops reading the WAL at a transaction boundary, publishes everything already read, sends a final ack, and exits 0, so the next start replays nothing. During a broker outage the drain waits. A second SIGTERM/SIGINT, or the orchestrator's SIGKILL, stops it, and unacknowledged events replay |

Exactly-once is not a goal. It is the consumer's job, made possible by `id`.

## Upgrading from M2

A single-database config needs no changes. What consumers and operators see:
- **Events gain `source`**, the database they were committed in: a field in the envelope, a `source` message attribute on SQS and SNS, a `source` field on Redis Streams.
- **FIFO `MessageGroupId` and `MessageDeduplicationId` gain a `<database>:` prefix.** An aggregate's last event before the upgrade and its first event after it land in different groups, so drain the queue before upgrading if that ordering matters. An event replayed across the upgrade is not deduplicated by the queue; consumers deduplicate on `id` anyway.
- **Every metric gains a `source` label.** The shipped alerts and dashboard are updated, and there is a new `pg_outbox_source_up` gauge and `OutboxSourceDown` alert.
- **A source error no longer exits the process.** A database that is down or a missing slot restarts that source's pipeline after a backoff, and `pg_outbox_source_up` shows it.

## Develop

Prerequisites: Rust 1.94.1 or newer (the AWS SDK sets that minimum) and Docker for the end-to-end test.

```bash
cargo test                          # unit, core and architecture tests: fast, no Docker
cargo test -- --ignored             # end to end (SQS, SNS, Redis, dead letters) and the crash tests: the relay binary under SIGTERM and SIGKILL
cargo fmt --check && cargo clippy --all-targets -- -D warnings
cargo run -- relay.toml             # against your own Postgres and broker
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
tests/           relay.rs (core, with fakes), architecture.rs; Docker: e2e_*.rs, crash.rs, common/
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
| **M2** | SNS and Redis Streams sinks, `outbox_dead_letter` table, graceful drain on SIGTERM, TLS for SQL connections, process-kill crash test in CI | ✅ done |
| **M3** | Multi-source: one process relays N databases (one slot per tenant database) | ✅ done |
| **M4** | Benchmarks (events/s, p99 latency, memory) against a Python polling relay | next |

Non-goals: exactly-once end to end, general-purpose CDC for arbitrary tables (use Debezium), and schema registries or routing DSLs.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
