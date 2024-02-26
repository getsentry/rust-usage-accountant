use rdkafka::config::ClientConfig as RdKafkaConfig;
use rdkafka::producer::{BaseRecord, ThreadedProducer};
use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::ClientContext;
use std::collections::HashMap;
use thiserror::Error;
use tracing::{event, Level};

use crate::{Message, Producer};

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

impl From<&KafkaConfig> for RdKafkaConfig {
    fn from(item: &KafkaConfig) -> Self {
        let mut config_obj = RdKafkaConfig::new();
        for (key, val) in item.config.iter() {
            config_obj.set(key, val);
        }
        config_obj
    }
}

struct CaptureErrorContext;

impl ClientContext for CaptureErrorContext {}

impl ProducerContext for CaptureErrorContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult, _delivery_opaque: Self::DeliveryOpaque) {
        match result {
            Ok(_) => {
                event!(Level::DEBUG, "Message produced.")
            }
            Err((kafka_err, _)) => {
                event!(Level::ERROR, "Message production failed. {}", kafka_err)
            }
        }
    }
}

pub struct KafkaProducer {
    topic: String,
    producer: ThreadedProducer<CaptureErrorContext>,
}

impl KafkaProducer {
    pub fn new(config: KafkaConfig) -> KafkaProducer {
        let producer = RdKafkaConfig::from(&config)
            .create_with_context(CaptureErrorContext)
            .expect("Producer creation error");

        KafkaProducer {
            topic: config.topic,
            producer,
        }
    }
}

/// Kafka producer errors.
#[derive(Error, Debug)]
pub enum KafkaProducerError {
    /// Failed to send a kafka message.
    #[error("failed to send kafka message")]
    SendFailed(#[source] rdkafka::error::KafkaError),

    /// Failed to create a kafka producer because of the invalid configuration.
    #[error("failed to create kafka producer: invalid kafka config")]
    InvalidConfig(#[source] rdkafka::error::KafkaError),
}

impl Producer for KafkaProducer {
    type Error = KafkaProducerError;

    fn send(&mut self, message: Message) -> Result<(), Self::Error> {
        // SAFETY: Serializing to JSON cannot fail. The type will always correctly serialize.
        let payload = serde_json::to_vec(&message).unwrap();
        let record: BaseRecord<'_, [u8], [u8]> = BaseRecord::to(&self.topic).payload(&payload);
        self.producer
            .send(record)
            .map_err(|(error, _message)| KafkaProducerError::SendFailed(error))
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

        let rdkafka_config = RdKafkaConfig::from(&config);
        assert_eq!(
            rdkafka_config.get("queued.max.messages.kbytes"),
            Some("1000000")
        );
    }
}
