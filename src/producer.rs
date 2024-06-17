//! This module provides an abstraction over a Kafka producer in
//! order to allow client code to instantiate the producer
//! implementation they want without depending on the rdkafka
//! ThreadedProducer.
//!
//! It also simplify unit tests.

use crate::Message;

/// A Producer trait.
///
/// We do not neet to set headers or key for this data.
pub trait Producer {
    type Error;

    fn send(&mut self, message: &Message) -> Result<(), Self::Error>;
}

impl<T, P> Producer for T
where
    T: std::ops::DerefMut<Target = P>,
    P: Producer + ?Sized,
{
    type Error = P::Error;

    fn send(&mut self, payload: &Message) -> Result<(), Self::Error> {
        (**self).send(payload)
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct DummyProducer {
    pub messages: Vec<Vec<u8>>,
}

#[cfg(test)]
impl Producer for DummyProducer {
    type Error = std::convert::Infallible;

    fn send(&mut self, payload: &Message) -> Result<(), Self::Error> {
        self.messages.push(payload.serialize());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefMut;

    use crate::UsageUnit;

    use super::*;

    #[test]
    fn test_dummy_producer() {
        let mut producer = DummyProducer::default();

        let message = Message {
            timestamp: 1,
            shared_resource_id: "foo".to_owned(),
            app_feature: "bar".to_owned(),
            usage_unit: UsageUnit::Bytes,
            amount: 42,
        };

        producer.send(&message).unwrap();

        let expected = r#"{"amount":42,"app_feature":"bar","shared_resource_id":"foo","timestamp":1,"usage_unit":"bytes"}"#;
        assert_eq!(
            producer.messages.pop().as_deref(),
            Some(expected.as_bytes())
        );
        assert!(producer.messages.is_empty());
    }

    #[test]
    fn test_smart_pointer_producer_compiles() {
        fn produce<P: Producer>() {}

        produce::<Box<dyn Producer<Error = ()>>>();
        produce::<Box<DummyProducer>>();
        produce::<RefMut<dyn Producer<Error = ()>>>();
        produce::<RefMut<DummyProducer>>();
    }
}
