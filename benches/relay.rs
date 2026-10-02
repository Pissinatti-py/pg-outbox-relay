//! M4: the relay against a Python polling relay, on the same Postgres and local SQS.
//! Measures throughput (draining a backlog), latency (commit → SQS under a steady load) and
//! memory. Needs Docker and the baseline's virtualenv (see docs/benchmarks.md):
//! `cargo bench --bench relay`.

#[path = "../tests/common/mod.rs"]
mod common;

use std::process::Command;
use std::time::Duration;

use anyhow::Context;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::QueueAttributeName;
use common::{Pg, RelayProcess};
use tokio::time::{Instant, interval, sleep};

/// A backlog over many aggregates, then one on a single aggregate, since the relay ships at
/// most one event per aggregate per batch.
const BACKLOG: usize = 20_000;
const AGGREGATES: usize = 1_000;
const HOT: usize = 2_000;
/// The steady load latency is measured under: events per second, for how long.
const RATE: usize = 200;
const SECONDS: usize = 30;

#[derive(Clone, Copy)]
enum Relay {
    Rust,
    Python,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let python = std::env::var("PYTHON").unwrap_or_else(|_| "bench/.venv/bin/python".into());
    let ready = Command::new(&python)
        .args(["-c", "import boto3, psycopg"])
        .status()
        .is_ok_and(|status| status.success());
    anyhow::ensure!(
        ready,
        "{python} cannot import boto3 and psycopg: set up bench/.venv as docs/benchmarks.md says"
    );

    let (_elasticmq, endpoint) = common::elasticmq().await?;
    let sqs = Client::new(&common::aws(&endpoint).await);
    println!(
        "| Relay | events/s, {AGGREGATES} aggregates | events/s, 1 aggregate | p50 ms | p99 ms | max ms | peak RSS MB | RSS under load MB |"
    );
    println!("|---|--:|--:|--:|--:|--:|--:|--:|");
    for relay in [Relay::Rust, Relay::Python] {
        println!("{}", measure(relay, &python, &sqs, &endpoint).await?);
    }
    Ok(())
}

/// One row of the table, on a fresh Postgres that flushes every commit, as in production.
async fn measure(
    relay: Relay,
    python: &str,
    sqs: &Client,
    endpoint: &str,
) -> anyhow::Result<String> {
    let pg = common::postgres_with_fsync(true).await?;
    let fsync: String = pg.db.query_one("SHOW fsync", &[]).await?.get(0);
    anyhow::ensure!(fsync == "on", "fsync is {fsync}");
    // The polling relay locks, reads and deletes rows, and finds the oldest through the index.
    // Both runs have the index, so inserts cost the same.
    pg.db
        .batch_execute(
            "GRANT SELECT, UPDATE, DELETE ON outbox TO relay;
             CREATE INDEX ON outbox (created_at, id);",
        )
        .await?;
    let (name, label) = match relay {
        Relay::Rust => ("rust", "pg-outbox-relay"),
        Relay::Python => ("python", "Python polling relay"),
    };
    let mut peak_kb = 0;

    // Throughput: each backlog is inserted first, then drained by a fresh relay.
    let mut rates = Vec::new();
    for (phase, aggregates, count) in [("many", AGGREGATES, BACKLOG), ("hot", 1, HOT)] {
        let queue = common::create_queue(sqs, &format!("{name}-{phase}.fifo")).await?;
        common::insert(&pg.db, 0, count, aggregates, 100).await?;
        let mut process = start(relay, python, &pg, &queue, endpoint)?;
        rates.push(drain_rate(sqs, &queue, count, &mut process).await?);
        peak_kb = peak_kb.max(stop(process).await?);
    }

    // Latency: commit → accepted by SQS, under a steady load the relay keeps up with.
    let queue = common::create_queue(sqs, &format!("{name}-latency.fifo")).await?;
    let mut process = start(relay, python, &pg, &queue, endpoint)?;
    sleep(Duration::from_secs(2)).await; // connected: streaming its slot, or polling
    let (total, rss_kb) = load(&pg.db, process.0.id()).await?;
    drain_rate(sqs, &queue, total, &mut process).await?;
    peak_kb = peak_kb.max(stop(process).await?);
    let mut received = Vec::new();
    common::receive(
        sqs,
        &queue,
        &mut received,
        |r| r.len() >= total,
        Duration::from_secs(120),
    )
    .await?;
    anyhow::ensure!(
        received.len() == total,
        "{} of {total} events arrived",
        received.len()
    );
    let mut latencies = received
        .iter()
        .map(|r| {
            let committed = r.envelope["payload"]["t"]
                .as_u64()
                .context("no payload.t")?;
            let sent = r.sent_ms.context("no SentTimestamp")?;
            Ok(sent.saturating_sub(committed))
        })
        .collect::<anyhow::Result<Vec<u64>>>()?;
    latencies.sort_unstable();
    let percentile = |q: f64| {
        let rank = (q * latencies.len() as f64).ceil() as usize;
        latencies[rank.clamp(1, latencies.len()) - 1]
    };
    let mb = |kb: u64| kb as f64 / 1024.0;
    Ok(format!(
        "| {label} | {:.0} | {:.0} | {} | {} | {} | {:.1} | {:.1} |",
        rates[0],
        rates[1],
        percentile(0.5),
        percentile(0.99),
        latencies[latencies.len() - 1],
        mb(peak_kb),
        mb(median(rss_kb)),
    ))
}

fn start(
    relay: Relay,
    python: &str,
    pg: &Pg,
    queue: &str,
    endpoint: &str,
) -> anyhow::Result<RelayProcess> {
    match relay {
        Relay::Rust => RelayProcess::start(&pg.config(), queue, endpoint),
        Relay::Python => Ok(RelayProcess(
            Command::new(python)
                .arg("bench/polling_relay.py")
                .env("RELAY_DSN", pg.config().dsn)
                .env("RELAY_QUEUE_URL", queue)
                .env("AWS_ENDPOINT_URL", endpoint)
                .env("AWS_DEFAULT_REGION", "us-east-1")
                .env("AWS_ACCESS_KEY_ID", "bench")
                .env("AWS_SECRET_ACCESS_KEY", "bench")
                .spawn()?,
        )),
    }
}

/// Waits until `count` events are queued, and returns the rate from the first one queued on,
/// which leaves out the relay's startup.
async fn drain_rate(
    sqs: &Client,
    queue: &str,
    count: usize,
    process: &mut RelayProcess,
) -> anyhow::Result<f64> {
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut first: Option<(Instant, usize)> = None;
    loop {
        let queued = queued(sqs, queue).await?;
        let now = Instant::now();
        if queued > 0 && first.is_none() {
            first = Some((now, queued));
        }
        if queued >= count {
            let (since, already) = first.expect("set once anything is queued");
            // NaN when everything was queued at the first look: nothing to time.
            return Ok((queued - already) as f64 / (now - since).as_secs_f64());
        }
        anyhow::ensure!(
            process.exited(Duration::ZERO).await?.is_none(),
            "the relay exited"
        );
        anyhow::ensure!(
            now < deadline,
            "{queued} of {count} events queued after 5 minutes"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

async fn queued(sqs: &Client, queue: &str) -> anyhow::Result<usize> {
    let output = sqs
        .get_queue_attributes()
        .queue_url(queue)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
        .send()
        .await?;
    Ok(output
        .attributes()
        .and_then(|a| a.get(&QueueAttributeName::ApproximateNumberOfMessages))
        .and_then(|count| count.parse().ok())
        .unwrap_or(0))
}

/// Inserts RATE events per second for SECONDS, one per transaction, each stamped with when it
/// was inserted, and samples the relay's memory every 100 ms meanwhile.
async fn load(db: &tokio_postgres::Client, pid: u32) -> anyhow::Result<(usize, Vec<u64>)> {
    let insert = db
        .prepare(
            "INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload) \
             VALUES (gen_random_uuid(), 'policy', $1, 'policy.updated', \
                     jsonb_build_object('t', (extract(epoch FROM clock_timestamp()) * 1000)::bigint))",
        )
        .await?;
    let total = RATE * SECONDS;
    let mut ticks = interval(Duration::from_secs(1) / RATE as u32);
    let mut rss_kb = Vec::new();
    for n in 0..total {
        ticks.tick().await;
        db.execute(&insert, &[&(n % 100).to_string()]).await?;
        if n % (RATE / 10) == 0 {
            rss_kb.push(status_kb(pid, "VmRSS:")?);
        }
    }
    Ok((total, rss_kb))
}

/// Stops the relay, and returns its peak memory in kB, read just before.
async fn stop(mut process: RelayProcess) -> anyhow::Result<u64> {
    let peak = status_kb(process.0.id(), "VmHWM:")?;
    process.terminate()?;
    anyhow::ensure!(
        process.exited(Duration::from_secs(15)).await?.is_some(),
        "the relay did not stop"
    );
    Ok(peak)
}

/// A field of /proc/<pid>/status in kB: VmRSS is the resident memory, VmHWM its peak.
// ponytail: Linux only, like the rest of the Docker setup
fn status_kb(pid: u32, field: &str) -> anyhow::Result<u64> {
    std::fs::read_to_string(format!("/proc/{pid}/status"))?
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|value| value.trim().trim_end_matches(" kB").parse().ok())
        .with_context(|| format!("no {field} for process {pid}"))
}

fn median(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}
