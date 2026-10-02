# Benchmarks

Milestone 4 compares the relay with a Python polling relay, the usual alternative, on the same Postgres and the same local SQS. `cargo bench --bench relay` reproduces it (see [Reproduce](#reproduce)).

## Results

The median of 3 runs, with the range in brackets. The last column compares the medians:

| | pg-outbox-relay | Python polling relay | pg-outbox-relay vs. polling |
|---|--:|--:|--:|
| Throughput: 20,000 events over 1,000 aggregates | **4,820 events/s** (4,430–4,900) | 2,250 events/s (1,900–2,490) | **114% more** |
| Throughput: 2,000 events on one aggregate | 1,660 events/s (1,655–1,661) | **2,860 events/s** (2,500–3,050) | 42% less |
| Latency at 200 events/s: p50 | **12 ms** | 51 ms | **76% lower** |
| Latency: p99 | **23 ms** | 102 ms | **77% lower** |
| Latency: max | **59 ms** (30–67) | 151 ms (116–192) | **61% lower** |
| Peak memory (RSS) | **17.3 MB** | 69.8 MB | **75% less** |
| Memory under load (RSS) | **15.6 MB** | 67.2 MB | **77% less** |

What it shows:
- **Over many aggregates, the relay drains a backlog about twice as fast**, with a quarter of the memory.
- **Its latency is about four times lower.** Its p50 is set by the 20 ms batching window (`batching.max_wait_ms`), and the polling relay's by its 100 ms poll interval.
- **On one hot aggregate, the polling relay is faster.** The relay keeps a batch to one event per aggregate, so that a partly failed batch cannot reorder an aggregate. On one aggregate, every request therefore carries a single event, while the polling relay sends 10.

## Method

Both relays run as processes on the host, so their memory is measured the same way. Each one gets:
- a fresh Postgres 17 in Docker, with TLS (`sslmode=require`) and `fsync=on`, as in production. It is set up by `sql/outbox.sql`, plus the index on `outbox (created_at, id)` that the polling relay needs. Both runs have the index, so inserts cost the same;
- a fresh FIFO queue for each phase, on ElasticMQ, a local SQS;
- the same `relay` role.

**The relay:** the release build, with its default settings (10 events per batch, a 20 ms wait).

**The polling relay** (`bench/polling_relay.py`, about 60 lines) is a single worker. Each transaction:
1. `SELECT … ORDER BY created_at, id LIMIT 10 FOR UPDATE SKIP LOCKED`;
2. `SendMessageBatch`;
3. `DELETE` the rows;
4. commits.

It polls again at once while it finds rows, and waits 100 ms when it finds none. It sends the same envelope, message attributes and FIFO group and deduplication ids as the relay.

**The phases, for each relay:**
1. **Throughput.** Insert a backlog in transactions of 100 rows, then start the relay. Poll the queue's `ApproximateNumberOfMessages` every 20 ms. The rate counts from the first event queued, so the relay's startup doesn't count. The backlog is 20,000 events over 1,000 aggregates, then 2,000 on one aggregate.
2. **Latency.** Start the relay, then insert 200 events per second for 30 seconds, one per transaction. Each payload carries `clock_timestamp()` in milliseconds. The latency is the message's SQS `SentTimestamp` minus that time: from the insert, just before its commit, until the broker accepted the event. All 6,000 events must arrive.
3. **Memory.** The peak RSS (`VmHWM`) is read at the end of every phase. The RSS under load (`VmRSS`) is sampled every 100 ms during phase 2, and its median is reported.

**Setup:**
- Machine: AMD Ryzen 7 5800X (8 cores, 16 threads), 32 GB, Ubuntu 24.04, Linux 7.0, Docker 29.8.
- Relay: version 0.3.0 at `7270db5`, built with Rust 1.99.
- Baseline: Python 3.14.3, psycopg 3.3.6 (binary, libpq 18), boto3 1.43.107.
- Images: Postgres 17.11, ElasticMQ 1.7.1.

## Reading the numbers

- **The broker is local.**
  - ElasticMQ answers in under a millisecond: the relay's one-aggregate run makes about 1,660 requests per second, one at a time.
  - It enforces no quotas. A FIFO queue on SQS without high throughput accepts 300 requests per second for each action, which is 3,000 events per second in full batches.
  - SQS's longer round trips slow both relays.
- **Polling costs the database more than it shows here.**
  - Every batch takes row locks, deletes and commits, so the database writes and vacuums for each one. The relay only reads the WAL.
  - A shorter poll interval cuts the polling relay's latency, at the price of more queries against the table.
- **Polling can publish out of order.**
  - It sees rows by `created_at`, which is the start time of their transaction, not when it committed.
  - So rows of concurrent transactions can go out in a different order than they committed. Rows of one transaction share a `created_at` and go out in `id` order.
  - The benchmark's single inserter never exercises this. The relay follows the commit order of the WAL.
- **Not measured:** CPU use, the SNS and Redis sinks, several databases at once, and several polling workers.

## What it decides

These are the "Known limits" in [architecture.md](architecture.md) that waited for M4:
- **One batch in flight:** 4,800 events per second against a local SQS. That is already more than a FIFO queue without high throughput accepts, so keep it.
- **One event per aggregate per batch:** the relay's weakest case. A hot aggregate drains at 1,660 events per second, behind the polling relay. On a FIFO queue without high throughput, it would be capped at 300 events per second. Allowing runs of one aggregate in a batch is the first improvement worth making.
- **Channel capacity fixed at 1024:** draining 20,000 events peaks at 17 MB. Keep it fixed.

## Reproduce

Docker, plus a virtualenv for the baseline:

```bash
uv venv bench/.venv && uv pip install --python bench/.venv/bin/python -r bench/requirements.txt
# or: python3 -m venv bench/.venv && bench/.venv/bin/pip install -r bench/requirements.txt
cargo bench --bench relay   # about 2 minutes after the build, then prints the table
```

`PYTHON` selects another interpreter. The constants at the top of `benches/relay.rs` set the sizes, the rate and the duration.
