//! This module provides an abstraction over a Kafka producer in
//! order to allow client code to instantiate the producer
//! implementation they want without depending on the rdkafka
//! ThreadedProducer.
//!
//! It also simplify unit tests.

use serde::Serialize;
use std::fmt;

#[derive(Serialize, Debug)]
pub struct Message {
    pub timestamp: i64,
    pub shared_resource_id: String,
    pub app_feature: String,
    pub usage_unit: UsageUnit,
    pub amount: u64,
}

/// The unit of measures we support when recording usage.
/// more can be added.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageUnit {
    Milliseconds,
    Bytes,
    BytesSec,
}

impl fmt::Display for UsageUnit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UsageUnit::Milliseconds => write!(f, "milliseconds"),
            UsageUnit::Bytes => write!(f, "bytes"),
            UsageUnit::BytesSec => write!(f, "bytes_sec"),
        }
    }
}

/// A Producer trait.
///
/// We do not neet to set headers or key for this data.
pub trait Producer {
    type Error;

    fn send(&mut self, payload: Message) -> Result<(), Self::Error>;
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct DummyProducer {
    pub messages: Vec<Message>,
}

#[cfg(test)]
impl Producer for DummyProducer {
    type Error = std::convert::Infallible;

    fn send(&mut self, message: Message) -> Result<(), Self::Error> {
        self.messages.push(message);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dummy_producer() {
        let mut producer = DummyProducer::default();

        let message = Message {
            timestamp: 1337,
            shared_resource_id: "foo".to_owned(),
            app_feature: "app_feature".to_owned(),
            usage_unit: UsageUnit::Bytes,
            amount: 42,
        };
        producer.send(message).unwrap();

        let Message {
            timestamp,
            shared_resource_id,
            app_feature,
            usage_unit,
            amount,
        } = producer.messages.pop().unwrap();

        assert_eq!(timestamp, 1337);
        assert_eq!(shared_resource_id, "foo");
        assert_eq!(app_feature, "app_feature");
        assert!(matches!(usage_unit, UsageUnit::Bytes));
        assert_eq!(amount, 1337);
    }
}
