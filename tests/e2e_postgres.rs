//! The Postgres adapter against a real database over TLS: the dead-letter table.
//! Needs Docker: `cargo test -- --ignored`.

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use pg_outbox_relay::adapters::postgres::{PgDeadLetters, PgSource};
use pg_outbox_relay::app::Health;
use pg_outbox_relay::domain::{Lsn, OutboxEvent};
use pg_outbox_relay::ports::{DeadLetterStore, EventSource};
use serde_json::value::RawValue;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

#[tokio::test]
#[ignore = "needs Docker"]
async fn the_dead_letter_table_is_checked_at_start_and_written_once() -> anyhow::Result<()> {
    let pg = common::postgres().await?;
    let config = pg.config();

    // Without INSERT on the table (an upgraded M1 database) the relay refuses to start, and says why.
    pg.db
        .batch_execute("REVOKE INSERT ON outbox_dead_letter FROM relay")
        .await?;
    let (out, _events) = mpsc::channel(1);
    let (_ack, acked) = watch::channel(Lsn::default());
    let (_stop, stopped) = watch::channel(false);
    let source = PgSource::new(config.clone(), Arc::new(Health::default()), stopped);
    let error = timeout(Duration::from_secs(30), source.run(out, acked))
        .await
        .expect("the relay started without INSERT on the dead-letter table")
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("GRANT INSERT ON outbox_dead_letter"),
        "{error:#}"
    );

    // With it, a replayed rejection is stored once.
    pg.db
        .batch_execute("GRANT INSERT ON outbox_dead_letter TO relay")
        .await?;
    let dead_letters = PgDeadLetters::new(&config)?;
    let event = OutboxEvent {
        id: "7c9e6679-7425-40de-944b-e07fc1f90ae7".into(),
        aggregate_type: "policy".into(),
        aggregate_id: "42".into(),
        event_type: "policy.approved".into(),
        occurred_at: "2026-09-28T14:03:11Z".into(),
        headers: RawValue::from_string("{}".into())?,
        payload: RawValue::from_string(r#"{"policy_id": 42}"#.into())?,
        commit_lsn: Lsn(1),
        committed_at: SystemTime::UNIX_EPOCH,
    };
    dead_letters.store(&event, "Message too long").await?;
    dead_letters.store(&event, "Message too long").await?;
    let rows = pg
        .db
        .query(
            "SELECT slot, envelope->'payload'->>'policy_id' FROM outbox_dead_letter",
            &[],
        )
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), common::SLOT);
    assert_eq!(rows[0].get::<_, String>(1), "42");
    Ok(())
}
