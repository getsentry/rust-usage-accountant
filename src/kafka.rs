use sentry_arroyo::backends::kafka::config::KafkaConfig as ArroyoKafkaConfig;
use sentry_arroyo::backends::kafka::producer::KafkaProducer as ArroyoKafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::{Producer as _, ProducerError};
use sentry_arroyo::types::{Topic, TopicOrPartition};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;

use crate::Producer;

const DEFAULT_TOPIC_NAME: &str = "shared-resources-usage";

/// This structure wraps the parameters to initialize a producer.
/// This struct is there in order not to expose the rdkafka
/// details outside.
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub topic: String,
    pub config: HashMap<String, String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            topic: DEFAULT_TOPIC_NAME.to_owned(),
            config: HashMap::new(),
        }
    }
}

impl From<&KafkaConfig> for ArroyoKafkaConfig {
    fn from(item: &KafkaConfig) -> Self {
        // bootstrap.servers comes from the config map, which overrides the empty list.
        ArroyoKafkaConfig::new_config(vec![], Some(item.config.clone()))
    }
}

/// Produce status metrics are reported by Arroyo
pub struct KafkaProducer {
    topic: Topic,
    producer: ArroyoKafkaProducer,
}

impl KafkaProducer {
    pub fn new(config: KafkaConfig) -> KafkaProducer {
        let producer = ArroyoKafkaProducer::new((&config).into()).expect("Producer creation error");

        KafkaProducer {
            topic: Topic::new(&config.topic),
            producer,
        }
    }
}

impl Drop for KafkaProducer {
    fn drop(&mut self) {
        let _ = self.producer.flush_for(Duration::from_secs(1));
    }
}

/// Kafka producer errors.
#[derive(Error, Debug)]
pub enum KafkaProducerError {
    /// Failed to send a kafka message.
    #[error("failed to send kafka message")]
    SendFailed(#[source] ProducerError),
}

impl Producer for KafkaProducer {
    type Error = KafkaProducerError;

    fn send(&mut self, payload: Vec<u8>) -> Result<(), Self::Error> {
        self.producer
            .produce(
                &TopicOrPartition::Topic(self.topic),
                KafkaPayload::new(None, None, Some(payload)),
            )
            .map_err(KafkaProducerError::SendFailed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_producer_configuration() {
        let config = KafkaConfig {
            config: HashMap::from([
                (
                    "bootstrap.servers".to_string(),
                    "localhost:9092".to_string(),
                ),
                (
                    "queued.max.messages.kbytes".to_string(),
                    "1000000".to_string(),
                ),
            ]),
            ..Default::default()
        };

        let arroyo_config = ArroyoKafkaConfig::from(&config);
        assert_eq!(
            arroyo_config
                .get_config_value("queued.max.messages.kbytes")
                .map(String::as_str),
            Some("1000000")
        );
        assert_eq!(
            arroyo_config
                .get_config_value("bootstrap.servers")
                .map(String::as_str),
            Some("localhost:9092")
        );
    }
}
