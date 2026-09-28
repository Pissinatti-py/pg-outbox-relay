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
