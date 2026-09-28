#!/bin/sh
# Demo database setup, run once by the postgres image on first start.
set -e
run() { psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"; }

run -f /sql/outbox.sql
# The relay's login: REPLICATION is the only privilege it needs.
run -c "CREATE ROLE relay WITH LOGIN REPLICATION PASSWORD 'relay'"
run -f /sql/slot.sql
