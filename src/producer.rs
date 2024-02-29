//! This module provides an abstraction over a Kafka producer in
//! order to allow client code to instantiate the producer
//! implementation they want without depending on the rdkafka
//! ThreadedProducer.
//!
//! It also simplify unit tests.

/// A Producer trait.
///
/// We do not neet to set headers or key for this data.
pub trait Producer {
    type Error;

    fn send(&mut self, payload: Vec<u8>) -> Result<(), Self::Error>;
}

impl<T, P> Producer for T
where
    T: std::ops::DerefMut<Target = P>,
    P: Producer + ?Sized,
{
    type Error = P::Error;

    fn send(&mut self, payload: Vec<u8>) -> Result<(), Self::Error> {
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

    fn send(&mut self, payload: Vec<u8>) -> Result<(), Self::Error> {
        self.messages.push(payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefMut;

    use super::*;

    #[test]
    fn test_dummy_producer() {
        let mut producer = DummyProducer::default();

        producer.send("foo".as_bytes().to_vec()).unwrap();

        assert_eq!(producer.messages.pop().as_deref(), Some("foo".as_bytes()));
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
