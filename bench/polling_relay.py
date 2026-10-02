"""The M4 baseline: an outbox relay that polls the table, as Python services commonly do.

Each transaction locks the oldest rows, publishes them to SQS and deletes them. The table
needs an index to find the oldest rows, and the role needs SELECT, UPDATE and DELETE on it:

    CREATE INDEX ON outbox (created_at, id);
    GRANT SELECT, UPDATE, DELETE ON outbox TO relay;

Settings come from the environment: RELAY_DSN, RELAY_QUEUE_URL, RELAY_POLL_MS (how long to
wait when the table is empty, default 100) and the AWS_* variables.
"""

import json
import os
import time
from datetime import timezone

import boto3
import psycopg

BATCH = 10  # SendMessageBatch's limit, and the Rust relay's default batch size

SELECT = """
    SELECT id::text, aggregate_type, aggregate_id, event_type, payload, headers, created_at
    FROM outbox ORDER BY created_at, id LIMIT %s FOR UPDATE SKIP LOCKED
"""


def main():
    queue_url = os.environ["RELAY_QUEUE_URL"]
    idle = int(os.environ.get("RELAY_POLL_MS", "100")) / 1000
    fifo = queue_url.endswith(".fifo")
    sqs = boto3.client("sqs")
    with psycopg.connect(os.environ["RELAY_DSN"], autocommit=True) as db:
        source = db.execute("SELECT current_database()").fetchone()[0]
        while True:
            with db.transaction():
                rows = db.execute(SELECT, (BATCH,)).fetchall()
                if rows:
                    entries = [entry(i, row, source, fifo) for i, row in enumerate(rows)]
                    failed = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries).get("Failed")
                    if failed:
                        raise RuntimeError(failed)  # rolls back: the rows stay for the next run
                    db.execute("DELETE FROM outbox WHERE id = ANY(%s::uuid[])", ([row[0] for row in rows],))
            if not rows:
                time.sleep(idle)


def entry(index, row, source, fifo):
    """The same message the Rust relay sends: envelope, attributes, FIFO group and dedup ids."""
    event_id, aggregate_type, aggregate_id, event_type, payload, headers, created_at = row
    body = json.dumps(
        {
            "id": event_id,
            "source": source,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "event_type": event_type,
            "occurred_at": created_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "headers": headers,
            "payload": payload,
        },
        separators=(",", ":"),
    )
    attributes = {
        name: {"DataType": "String", "StringValue": value}
        for name, value in (("id", event_id), ("source", source), ("event_type", event_type))
    }
    message = {"Id": str(index), "MessageBody": body, "MessageAttributes": attributes}
    if fifo:
        message["MessageGroupId"] = f"{source}:{aggregate_type}:{aggregate_id}"
        message["MessageDeduplicationId"] = f"{source}:{event_id}"
    return message


if __name__ == "__main__":
    main()
