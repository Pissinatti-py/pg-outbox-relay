//! Loads the settings of every layer from a TOML file, then applies environment overrides
//! named `RELAY__<SECTION>__<KEY>` (e.g. `RELAY__SINK__QUEUE_URL`).

use std::collections::HashMap;
use std::net::SocketAddr;

use config::{Environment, File, FileFormat};
use serde::Deserialize;

use crate::adapters::postgres::PgConfig;
use crate::adapters::redis::RedisConfig;
use crate::adapters::sns::SnsConfig;
use crate::adapters::sqs::SqsConfig;
use crate::app::relay::{Batching, Retry};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub source: PgConfig,
    pub sink: SinkConfig,
    #[serde(default)]
    pub batching: Batching,
    #[serde(default)]
    pub retry: Retry,
    #[serde(default)]
    pub server: Server,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum SinkConfig {
    Sqs(SqsConfig),
    Sns(SnsConfig),
    Redis(RedisConfig),
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Server {
    pub listen: SocketAddr,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            listen: ([0, 0, 0, 0], 9090).into(),
        }
    }
}

impl Config {
    /// Reads `path` if it exists (containers can configure through the environment alone),
    /// then applies `RELAY__...` overrides from the process environment.
    pub fn load(path: &str) -> anyhow::Result<Self> {
        Self::from(path, None)
    }

    fn from(path: &str, env: Option<HashMap<String, String>>) -> anyhow::Result<Self> {
        let overrides = Environment::with_prefix("RELAY")
            .prefix_separator("__")
            .separator("__")
            .try_parsing(true)
            .list_separator(",")
            .with_list_parse_key("source.databases")
            .source(env);
        let config = config::Config::builder()
            .add_source(File::new(path, FileFormat::Toml).required(false))
            .add_source(overrides)
            .build()?
            .try_deserialize()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(vars: &[(&str, &str)]) -> Option<HashMap<String, String>> {
        Some(
            vars.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn loads_the_example_file() {
        let config = Config::from("relay.example.toml", env(&[])).unwrap();
        assert_eq!(config.source.slot, "outbox_relay");
        assert_eq!(config.batching.max_events, 10);
        assert_eq!(config.server.listen, "0.0.0.0:9090".parse().unwrap());
        let SinkConfig::Sqs(sqs) = config.sink else {
            panic!("expected the sqs sink");
        };
        assert!(sqs.queue_url.ends_with("events.fifo"));
    }

    #[test]
    fn environment_overrides_the_file() {
        let config = Config::from(
            "relay.example.toml",
            env(&[
                (
                    "RELAY__SINK__QUEUE_URL",
                    "http://localstack:4566/000000000000/other.fifo",
                ),
                ("RELAY__BATCHING__MAX_EVENTS", "5"),
            ]),
        )
        .unwrap();
        assert_eq!(config.batching.max_events, 5);
        let SinkConfig::Sqs(sqs) = config.sink else {
            panic!("expected the sqs sink");
        };
        assert_eq!(
            sqs.queue_url,
            "http://localstack:4566/000000000000/other.fifo"
        );
    }

    #[test]
    fn the_environment_alone_is_enough() {
        let config = Config::from(
            "does-not-exist.toml",
            env(&[
                ("RELAY__SOURCE__DSN", "postgres://relay:relay@db/app"),
                ("RELAY__SOURCE__SLOT", "outbox_relay"),
                ("RELAY__SOURCE__PUBLICATION", "outbox_pub"),
                ("RELAY__SINK__KIND", "sqs"),
                (
                    "RELAY__SINK__QUEUE_URL",
                    "https://sqs.us-east-1.amazonaws.com/1/e.fifo",
                ),
            ]),
        )
        .unwrap();
        assert_eq!(config.retry.max_backoff_ms, 30_000);
    }

    #[test]
    fn selects_the_sink_by_kind() {
        let source = [
            ("RELAY__SOURCE__DSN", "postgres://relay:relay@db/app"),
            ("RELAY__SOURCE__SLOT", "outbox_relay"),
            ("RELAY__SOURCE__PUBLICATION", "outbox_pub"),
        ];
        let sink = |vars: &[(&str, &str)]| {
            Config::from("does-not-exist.toml", env(&[&source[..], vars].concat()))
                .unwrap()
                .sink
        };
        let SinkConfig::Sns(sns) = sink(&[
            ("RELAY__SINK__KIND", "sns"),
            (
                "RELAY__SINK__TOPIC_ARN",
                "arn:aws:sns:us-east-1:1:events.fifo",
            ),
        ]) else {
            panic!("expected the sns sink");
        };
        assert!(sns.topic_arn.ends_with(".fifo"));
        let SinkConfig::Redis(redis) = sink(&[
            ("RELAY__SINK__KIND", "redis"),
            ("RELAY__SINK__URL", "redis://cache:6379"),
        ]) else {
            panic!("expected the redis sink");
        };
        assert_eq!(redis.url, "redis://cache:6379");
        assert_eq!(redis.stream_prefix, "outbox:");
    }

    #[test]
    fn the_environment_lists_databases() {
        let config = Config::from(
            "does-not-exist.toml",
            env(&[
                ("RELAY__SOURCE__DSN", "postgres://relay@db/{database}"),
                ("RELAY__SOURCE__SLOT", "outbox_{database}"),
                ("RELAY__SOURCE__PUBLICATION", "outbox_pub"),
                ("RELAY__SOURCE__DATABASES", "acme,globex"),
                ("RELAY__SINK__KIND", "sqs"),
                (
                    "RELAY__SINK__QUEUE_URL",
                    "https://sqs.us-east-1.amazonaws.com/1/e.fifo",
                ),
            ]),
        )
        .unwrap();
        assert_eq!(config.source.databases, ["acme", "globex"]);
    }

    #[test]
    fn rejects_an_unknown_sink() {
        let result = Config::from("relay.example.toml", env(&[("RELAY__SINK__KIND", "kafka")]));
        assert!(result.is_err());
    }
}
