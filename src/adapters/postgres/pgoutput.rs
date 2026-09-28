//! Decodes the two pgoutput messages the relay needs, Relation and Insert, and turns
//! an inserted outbox row into an [`OutboxEvent`]. `pgwire-replication` already parses
//! Begin and Commit; every other message is ignored.
//!
//! Wire format: <https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html>

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use anyhow::{Context, bail, ensure};
use serde_json::value::RawValue;

use crate::domain::{Lsn, OutboxEvent};

/// Column values of one inserted row, by column name. `None` is SQL NULL.
pub type Row = HashMap<String, Option<String>>;

/// Remembers each relation's column names, so Insert values map by name, not position.
#[derive(Debug, Default)]
pub struct Decoder {
    columns: HashMap<u32, Vec<String>>,
}

impl Decoder {
    /// Decodes one pgoutput message: the row for an Insert, `None` for anything else.
    pub fn decode(&mut self, message: &[u8]) -> anyhow::Result<Option<Row>> {
        let mut r = Reader(message);
        match r.u8()? {
            b'R' => {
                let relation = r.u32()?;
                r.cstr()?; // namespace
                r.cstr()?; // table name
                r.u8()?; // replica identity
                let count = r.u16()?;
                let mut names = Vec::with_capacity(count.into());
                for _ in 0..count {
                    r.u8()?; // flags
                    names.push(r.cstr()?.to_owned());
                    r.u32()?; // type oid
                    r.u32()?; // type modifier
                }
                self.columns.insert(relation, names);
                Ok(None)
            }
            b'I' => {
                let relation = r.u32()?;
                let names = self.columns.get(&relation).with_context(|| {
                    format!("insert into relation {relation} before its Relation message")
                })?;
                ensure!(r.u8()? == b'N', "insert without a new tuple");
                let count = usize::from(r.u16()?);
                ensure!(
                    count == names.len(),
                    "insert has {count} columns, its relation has {}",
                    names.len()
                );
                let mut row = Row::with_capacity(count);
                for name in names {
                    let value = match r.u8()? {
                        b'n' => None,
                        b't' => {
                            let len = r.u32()? as usize;
                            Some(std::str::from_utf8(r.take(len)?)?.to_owned())
                        }
                        kind => bail!("column `{name}`: unsupported value kind {:?}", kind as char),
                    };
                    row.insert(name.clone(), value);
                }
                Ok(Some(row))
            }
            _ => Ok(None),
        }
    }
}

/// Builds the event for an outbox row committed at `commit_lsn`.
pub fn outbox_event(
    mut row: Row,
    commit_lsn: Lsn,
    committed_at: SystemTime,
) -> anyhow::Result<OutboxEvent> {
    let mut take = |column: &str| {
        row.remove(column)
            .flatten()
            .with_context(|| format!("outbox row has no `{column}` value"))
    };
    Ok(OutboxEvent {
        id: take("id")?,
        aggregate_type: take("aggregate_type")?,
        aggregate_id: take("aggregate_id")?,
        event_type: take("event_type")?,
        occurred_at: rfc3339_utc(&take("created_at")?),
        headers: RawValue::from_string(take("headers")?)?,
        payload: RawValue::from_string(take("payload")?)?,
        commit_lsn,
        committed_at,
    })
}

/// Postgres timestamps count microseconds from 2000-01-01 UTC.
pub fn pg_time(micros: i64) -> SystemTime {
    const PG_EPOCH: Duration = Duration::from_secs(946_684_800);
    SystemTime::UNIX_EPOCH + PG_EPOCH + Duration::from_micros(micros.max(0) as u64)
}

/// `2026-09-28 14:03:11.5+00` → `2026-09-28T14:03:11.5Z`. The replication session runs
/// with `TimeZone=UTC`, so the offset is always `+00`. Anything else (`infinity`) passes
/// through unchanged instead of blocking the stream.
fn rfc3339_utc(timestamp: &str) -> String {
    match timestamp.strip_suffix("+00") {
        Some(utc) => format!("{}Z", utc.replacen(' ', "T", 1)),
        None => timestamp.to_owned(),
    }
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> anyhow::Result<&'a [u8]> {
        ensure!(self.0.len() >= n, "pgoutput message is truncated");
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }

    fn u8(&mut self) -> anyhow::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> anyhow::Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into()?))
    }

    fn u32(&mut self) -> anyhow::Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into()?))
    }

    fn cstr(&mut self) -> anyhow::Result<&'a str> {
        let end = self
            .0
            .iter()
            .position(|&b| b == 0)
            .context("unterminated string")?;
        let s = std::str::from_utf8(self.take(end)?)?;
        self.take(1)?; // the terminator
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Captured from Postgres 17: `sql/outbox.sql`'s table, then
    /// `INSERT INTO outbox VALUES ('7c9e6679-…', 'policy', '42', 'policy.approved',
    ///  '{"policy_id": 42}', '{"tenant": "acme"}', '2026-09-28 14:03:11.5+00')`.
    const RELATION: &str = "52000040017075626c6963006f7574626f78006400070169640000000b86ffffffff006167677265676174655f747970650000000019ffffffff006167677265676174655f69640000000019ffffffff006576656e745f747970650000000019ffffffff007061796c6f61640000000edaffffffff00686561646572730000000edaffffffff00637265617465645f617400000004a0ffffffff";
    const INSERT: &str = "49000040014e0007740000002437633965363637392d373432352d343064652d393434622d6530376663316639306165377400000006706f6c69637974000000023432740000000f706f6c6963792e617070726f76656474000000117b22706f6c6963795f6964223a2034327d74000000127b2274656e616e74223a202261636d65227d7400000018323032362d30392d32382031343a30333a31312e352b3030";

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn relation(id: u32, columns: &[&str]) -> Vec<u8> {
        let mut m = vec![b'R'];
        m.extend(id.to_be_bytes());
        m.extend(b"public\0outbox\0d");
        m.extend((columns.len() as u16).to_be_bytes());
        for column in columns {
            m.push(0);
            m.extend(column.as_bytes());
            m.push(0);
            m.extend(25u32.to_be_bytes());
            m.extend((-1i32).to_be_bytes());
        }
        m
    }

    fn insert(id: u32, values: &[Option<&str>]) -> Vec<u8> {
        let mut m = vec![b'I'];
        m.extend(id.to_be_bytes());
        m.push(b'N');
        m.extend((values.len() as u16).to_be_bytes());
        for value in values {
            match value {
                None => m.push(b'n'),
                Some(v) => {
                    m.push(b't');
                    m.extend((v.len() as u32).to_be_bytes());
                    m.extend(v.as_bytes());
                }
            }
        }
        m
    }

    #[test]
    fn decodes_a_real_outbox_insert() {
        let mut decoder = Decoder::default();
        assert!(decoder.decode(&hex(RELATION)).unwrap().is_none());
        let row = decoder.decode(&hex(INSERT)).unwrap().unwrap();
        let event = outbox_event(row, Lsn(7), pg_time(0)).unwrap();

        assert_eq!(event.id, "7c9e6679-7425-40de-944b-e07fc1f90ae7");
        assert_eq!(event.aggregate_type, "policy");
        assert_eq!(event.aggregate_id, "42");
        assert_eq!(event.event_type, "policy.approved");
        assert_eq!(event.occurred_at, "2026-09-28T14:03:11.5Z");
        assert_eq!(event.payload.get(), r#"{"policy_id": 42}"#);
        assert_eq!(event.headers.get(), r#"{"tenant": "acme"}"#);
        assert_eq!(event.commit_lsn, Lsn(7));
    }

    #[test]
    fn maps_columns_by_name_and_ignores_extras() {
        let mut decoder = Decoder::default();
        let columns = [
            "trace",
            "payload",
            "headers",
            "created_at",
            "event_type",
            "aggregate_id",
            "aggregate_type",
            "id",
        ];
        decoder.decode(&relation(9, &columns)).unwrap();
        let values = [
            None,
            Some("{}"),
            Some("{}"),
            Some("2026-01-01 00:00:00+00"),
            Some("x.y"),
            Some("1"),
            Some("x"),
            Some("abc"),
        ];
        let row = decoder.decode(&insert(9, &values)).unwrap().unwrap();
        let event = outbox_event(row, Lsn(1), pg_time(0)).unwrap();

        assert_eq!(event.id, "abc");
        assert_eq!(event.aggregate_type, "x");
        assert_eq!(event.aggregate_id, "1");
        assert_eq!(event.occurred_at, "2026-01-01T00:00:00Z");
    }

    #[test]
    fn a_missing_column_is_an_error_naming_it() {
        let mut decoder = Decoder::default();
        decoder
            .decode(&relation(9, &["id", "aggregate_type", "aggregate_id"]))
            .unwrap();
        let row = decoder
            .decode(&insert(9, &[Some("a"), Some("b"), Some("c")]))
            .unwrap()
            .unwrap();
        let error = outbox_event(row, Lsn(1), pg_time(0)).unwrap_err();
        assert!(error.to_string().contains("event_type"), "{error}");
    }

    #[test]
    fn rejects_an_insert_before_its_relation_and_truncated_messages() {
        let mut decoder = Decoder::default();
        assert!(decoder.decode(&hex(INSERT)).is_err());
        let relation = hex(RELATION);
        assert!(decoder.decode(&relation[..relation.len() - 3]).is_err());
    }

    #[test]
    fn ignores_messages_it_does_not_need() {
        let mut decoder = Decoder::default();
        assert!(decoder.decode(b"Yanything").unwrap().is_none());
        assert!(decoder.decode(b"Oorigin").unwrap().is_none());
    }

    #[test]
    fn converts_times() {
        assert_eq!(
            rfc3339_utc("2026-09-28 14:03:11.128012+00"),
            "2026-09-28T14:03:11.128012Z"
        );
        assert_eq!(rfc3339_utc("infinity"), "infinity");
        assert_eq!(
            pg_time(1_000_000),
            SystemTime::UNIX_EPOCH + Duration::from_secs(946_684_801)
        );
    }
}
