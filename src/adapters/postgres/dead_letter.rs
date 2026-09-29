//! Keeps permanently rejected events in the `outbox_dead_letter` table (sql/outbox.sql).

use anyhow::{Context, ensure};

use super::{Dsn, PgConfig, sql_connect};
use crate::domain::OutboxEvent;
use crate::ports::DeadLetterStore;

pub struct PgDeadLetters {
    dsn: Dsn,
    /// Identifies the relay: each streams its own slot, and several can share the table.
    slot: String,
}

impl PgDeadLetters {
    pub fn new(config: &PgConfig) -> anyhow::Result<Self> {
        Ok(Self {
            dsn: Dsn::parse(&config.dsn)?,
            slot: config.slot.clone(),
        })
    }
}

impl DeadLetterStore for PgDeadLetters {
    // ponytail: one connection per dead letter; fine while they are rare
    async fn store(&self, event: &OutboxEvent, reason: &str) -> anyhow::Result<()> {
        sql_connect(&self.dsn)
            .await?
            .execute(
                "INSERT INTO outbox_dead_letter (slot, id, envelope, reason) \
                 VALUES ($1, $2::text::uuid, $3::text::jsonb, $4) ON CONFLICT DO NOTHING",
                &[&self.slot, &event.id, &event.envelope(), &reason],
            )
            .await?;
        Ok(())
    }
}

/// Fails the start when a dead letter could not be stored, instead of stalling at the first
/// rejected event. Called once the database is known to be reachable.
pub(super) async fn check(dsn: &Dsn) -> anyhow::Result<()> {
    let can_insert: bool = sql_connect(dsn)
        .await?
        .query_one(
            "SELECT has_table_privilege('outbox_dead_letter', 'INSERT')",
            &[],
        )
        .await
        .context("cannot use outbox_dead_letter; create it with sql/outbox.sql")?
        .get(0);
    ensure!(
        can_insert,
        "the relay's role needs INSERT on outbox_dead_letter: GRANT INSERT ON outbox_dead_letter TO {}",
        dsn.user
    );
    Ok(())
}
