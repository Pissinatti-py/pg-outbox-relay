-- The replication slot the relay streams from.
--
-- Run it OUTSIDE a transaction block: Postgres refuses to create a logical slot
-- in a transaction that has already written. The relay never creates the slot
-- itself, because a silently recreated slot would skip every event committed
-- while it was missing.
--
-- Relaying several databases? Slot names are unique across the whole server, so create
-- one per database, e.g. 'outbox_' || current_database(), with slot = "outbox_{database}".
SELECT pg_create_logical_replication_slot('outbox_relay', 'pgoutput');
