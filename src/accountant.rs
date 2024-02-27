use crate::{accumulator::UsageAccumulator, Producer};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use std::{fmt, ops::Drop};

/// The unit of measures we support when recording usage.
/// more can be added.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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

/// This is the entry point for the library. It is in most cases
/// everything you need to instrument your application.
///
/// The UsageAccountant needs a `Producer` and a `UsageAccumulator`.
/// Data is stored in the accumulator and, periodically, it is
/// flushed into the Producer via a consumer.
///
/// Accumulating data locally is critical to reduce the performance impact
/// of this library to a minimum and reduce the amount of Kafka messages.
/// This means that this structure should be instantiated rarely.
/// Possibly only once per application (or per thread).
///
/// Avoid creating a UsageAccountant every time some data needs to
/// be recorded.

pub struct UsageAccountant<P: Producer> {
    accumulator: UsageAccumulator,
    producer: P,
}

#[cfg(feature = "kafka")]
impl UsageAccountant<crate::KafkaProducer> {
    /// Instantiates a UsageAccountant from a Kafka config object.
    /// This initialization method lets the `UsageAccountant` create
    /// the producer and own it.
    ///
    /// You should very rarely change topic name or granularity.
    pub fn new_with_kafka(
        producer_config: crate::KafkaConfig,
        granularity: Option<Duration>,
    ) -> UsageAccountant<crate::KafkaProducer> {
        UsageAccountant::new(crate::KafkaProducer::new(producer_config), granularity)
    }
}

impl<P: Producer> UsageAccountant<P> {
    /// Instantiates a UsageAccountant by leaving the responsibility
    /// to provide a producer to the client.
    pub fn new(producer: P, granularity: Option<Duration>) -> Self {
        UsageAccountant {
            accumulator: UsageAccumulator::new(granularity),
            producer,
        }
    }

    /// Records an mount of usage for a resource, and app_feature.
    ///
    /// It flushes the batch if that is ready to be flushed.
    /// The timestamp used is the system timestamp.
    pub fn record(
        &mut self,
        resource_id: &str,
        app_feature: &str,
        amount: u64,
        unit: UsageUnit,
    ) -> Result<(), P::Error> {
        let current_time = Utc::now();
        self.accumulator
            .record(current_time, resource_id, app_feature, amount, unit);
        if self.accumulator.should_flush(current_time) {
            self.flush()?;
        }
        Ok(())
    }

    /// Forces a flush of the existing batch.
    ///
    /// This method is called automatically when the Accountant
    /// goes out of scope.
    pub fn flush(&mut self) -> Result<(), P::Error> {
        let flushed_content = self.accumulator.flush();
        for (key, amount) in flushed_content {
            let message = Message {
                timestamp: key.quantized_timestamp.timestamp(),
                shared_resource_id: key.resource_id,
                app_feature: key.app_feature,
                usage_unit: key.unit,
                amount,
            };

            if let Ok(payload) = serde_json::to_vec(&message) {
                self.producer.send(payload)?;
            }
        }
        Ok(())
    }
}

impl<P: Producer> Drop for UsageAccountant<P> {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    timestamp: i64,
    shared_resource_id: String,
    app_feature: String,
    usage_unit: UsageUnit,
    amount: u64,
}

#[cfg(test)]
mod tests {
    use crate::DummyProducer;

    use super::*;

    #[test]
    fn test_empty_batch() {
        let mut accountant = UsageAccountant::new(DummyProducer::default(), None);

        let res = accountant.flush();
        assert!(res.is_ok());
        assert_eq!(accountant.producer.messages.len(), 0);
    }

    #[test]
    fn test_three_messages() {
        let mut accountant = UsageAccountant::new(DummyProducer::default(), None);

        let res1 = accountant.record("resource_1", "transactions", 100, UsageUnit::Bytes);
        assert!(res1.is_ok());
        let res2 = accountant.record("resource_1", "spans", 200, UsageUnit::Bytes);
        assert!(res2.is_ok());

        let res = accountant.flush();
        assert!(res.is_ok());

        let messages = &accountant.producer.messages;
        assert_eq!(messages.len(), 2);

        let m1: Message = serde_json::from_slice(&messages[0]).unwrap();
        assert_eq!(m1.shared_resource_id, "resource_1");
        assert_eq!(m1.usage_unit, UsageUnit::Bytes);

        let m2: Message = serde_json::from_slice(&messages[1]).unwrap();
        assert_eq!(m2.shared_resource_id, "resource_1");
        assert_eq!(m2.usage_unit, UsageUnit::Bytes);

        // The messages will not necessarily be in order.
        assert_ne!(m1.app_feature, m2.app_feature);
        if m1.app_feature == "transactions" {
            assert_eq!(m2.app_feature, "spans")
        } else {
            assert_eq!(m2.app_feature, "transactions");
            assert_eq!(m1.app_feature, "spans");
        }
        assert_ne!(m1.amount, m2.amount);

        let res = accountant.flush();
        assert!(res.is_ok());
        // Messages are still the same we had before the previous step.
        assert_eq!(accountant.producer.messages.len(), 2);
    }
}
