-- The outbox table and its publication. Safe to run inside an application migration.
CREATE TABLE outbox (
    id             uuid        PRIMARY KEY,          -- idempotency key
    aggregate_type text        NOT NULL,             -- e.g. 'policy'
    aggregate_id   text        NOT NULL,             -- ordering key
    event_type     text        NOT NULL,             -- e.g. 'policy.approved'
    payload        jsonb       NOT NULL,
    headers        jsonb       NOT NULL DEFAULT '{}',
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- The relay only needs inserts. If you partition `outbox`, add
-- `, publish_via_partition_root = true` so partitions publish as `outbox`.
CREATE PUBLICATION outbox_pub FOR TABLE outbox WITH (publish = 'insert');

-- Events a broker rejected for good (e.g. too large). The relay stores each one here before
-- moving on, and refuses to start without this table. Not part of outbox_pub.
-- The relay's role needs: GRANT INSERT ON outbox_dead_letter TO relay;
CREATE TABLE outbox_dead_letter (
    slot      text        NOT NULL,              -- the relay that gave up on it (one per slot)
    id        uuid        NOT NULL,              -- the event id
    envelope  jsonb       NOT NULL,              -- exactly what the broker rejected
    reason    text        NOT NULL,              -- the broker's error
    failed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (slot, id)
);
