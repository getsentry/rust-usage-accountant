//! This module contains the structure that accumulates usage data
//! Utcly before flushing to Kafka.
//!
//! The accumulator pre-aggregates usage per timestamp based on
//! the granularity provided at instantiation.
//!

use chrono::{DateTime, Duration, DurationRound, Utc};
use std::collections::HashMap;
use std::mem;

use crate::UsageUnit;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UsageKey {
    pub quantized_timestamp: DateTime<Utc>,
    pub resource_id: String,
    pub app_feature: String,
    pub unit: UsageUnit,
}

pub struct UsageAccumulator {
    usage_batch: HashMap<UsageKey, u64>,
    granularity: Duration,
    first_timestamp: Option<DateTime<Utc>>,
}

impl UsageAccumulator {
    /// Constructs a new Accumulator. Here is where the granularity
    /// is provided.
    pub fn new(granularity: Option<Duration>) -> Self {
        Self {
            usage_batch: HashMap::new(),
            granularity: granularity.unwrap_or(Duration::seconds(60)),
            first_timestamp: None,
        }
    }

    /// Records an amount of usage for a resource, app_feature, timestamp
    /// tuple.
    ///
    /// The timestamp provided is then quantized according to the
    /// granularity this structure is instantiated with so data is
    /// bucketed with fixed bucket sizes.
    /// The system timestamp should be passed in most cases.
    pub fn record(
        &mut self,
        usage_time: DateTime<Utc>,
        resource_id: &str,
        app_feature: &str,
        amount: u64,
        usage_unit: UsageUnit,
    ) {
        // Check for zero here because of chrono bug, which causes a panic:
        // https://github.com/chronotope/chrono/pull/1474
        let quantized_timestamp = if self.granularity.is_zero() {
            usage_time
        } else {
            usage_time.duration_trunc(self.granularity).unwrap()
        };

        if self.first_timestamp.is_none() {
            self.first_timestamp = Some(quantized_timestamp);
        }

        let key = UsageKey {
            quantized_timestamp,
            resource_id: resource_id.to_string(),
            app_feature: app_feature.to_string(),
            unit: usage_unit,
        };

        let value = self.usage_batch.entry(key).or_default();
        *value += amount;
    }

    /// Returns true if the bucket is ready to be flushed.
    ///
    /// Ready to be flushed means that the bucket is not empty
    /// and at least `granularity` seconds have passed since
    /// the first chunk of data was added.
    pub fn should_flush(&self, current_time: DateTime<Utc>) -> bool {
        let Some(first_timestamp) = self.first_timestamp else {
            return false;
        };

        return self.usage_batch.keys().len() > 0
            && current_time - first_timestamp >= self.granularity;
    }

    /// Return the current bucket and clears up the state.
    pub fn flush(&mut self) -> HashMap<UsageKey, u64> {
        self.first_timestamp = None;
        mem::take(&mut self.usage_batch)
    }
}

#[cfg(test)]
mod tests {
    use super::{UsageAccumulator, UsageKey, UsageUnit};
    use chrono::{TimeZone, Utc};
    use std::collections::HashMap;

    #[test]
    fn empty_batch() {
        let mut accumulator = UsageAccumulator::new(None);
        assert!(!accumulator.should_flush(Utc.with_ymd_and_hms(2023, 10, 8, 22, 15, 25).unwrap()));
        assert!(!accumulator.should_flush(Utc.with_ymd_and_hms(2023, 10, 8, 22, 16, 25).unwrap()));

        let message = accumulator.flush();
        assert_eq!(message.keys().len(), 0);
    }

    #[test]
    fn test_multiple_entries() {
        let mut accumulator = UsageAccumulator::new(None);
        accumulator.record(
            Utc.with_ymd_and_hms(2023, 10, 8, 22, 15, 25).unwrap(),
            "genericmetrics_consumer",
            "transactions",
            100,
            UsageUnit::Milliseconds,
        );
        accumulator.record(
            Utc.with_ymd_and_hms(2023, 10, 8, 22, 15, 45).unwrap(),
            "genericmetrics_consumer",
            "spans",
            200,
            UsageUnit::Milliseconds,
        );

        assert!(!accumulator.should_flush(Utc.with_ymd_and_hms(2023, 10, 8, 22, 15, 25).unwrap()));
        assert!(accumulator.should_flush(Utc.with_ymd_and_hms(2023, 10, 8, 22, 16, 25).unwrap()));
        let ret = accumulator.flush();
        let test_val = HashMap::from([
            (
                UsageKey {
                    quantized_timestamp: Utc.with_ymd_and_hms(2023, 10, 8, 22, 15, 0).unwrap(),
                    resource_id: "genericmetrics_consumer".to_string(),
                    app_feature: "transactions".to_string(),
                    unit: UsageUnit::Milliseconds,
                },
                100,
            ),
            (
                UsageKey {
                    quantized_timestamp: Utc.with_ymd_and_hms(2023, 10, 8, 22, 15, 0).unwrap(),
                    resource_id: "genericmetrics_consumer".to_string(),
                    app_feature: "spans".to_string(),
                    unit: UsageUnit::Milliseconds,
                },
                200,
            ),
        ]);
        assert_eq!(ret, test_val);

        let message = accumulator.flush();
        assert_eq!(message.keys().len(), 0);
    }

    #[test]
    fn test_merge_entries() {
        let mut accumulator = UsageAccumulator::new(None);
        accumulator.record(
            Utc.with_ymd_and_hms(2023, 10, 8, 22, 15, 25).unwrap(),
            "genericmetrics_consumer",
            "transactions",
            100,
            UsageUnit::Milliseconds,
        );
        accumulator.record(
            Utc.with_ymd_and_hms(2023, 10, 8, 22, 15, 45).unwrap(),
            "genericmetrics_consumer",
            "transactions",
            100,
            UsageUnit::Milliseconds,
        );
        accumulator.record(
            Utc.with_ymd_and_hms(2023, 10, 8, 22, 16, 45).unwrap(),
            "genericmetrics_consumer",
            "transactions",
            100,
            UsageUnit::Milliseconds,
        );

        let ret = accumulator.flush();
        let test_val = HashMap::from([
            (
                UsageKey {
                    quantized_timestamp: Utc.with_ymd_and_hms(2023, 10, 8, 22, 15, 0).unwrap(),
                    resource_id: "genericmetrics_consumer".to_string(),
                    app_feature: "transactions".to_string(),
                    unit: UsageUnit::Milliseconds,
                },
                200,
            ),
            (
                UsageKey {
                    quantized_timestamp: Utc.with_ymd_and_hms(2023, 10, 8, 22, 16, 0).unwrap(),
                    resource_id: "genericmetrics_consumer".to_string(),
                    app_feature: "transactions".to_string(),
                    unit: UsageUnit::Milliseconds,
                },
                100,
            ),
        ]);
        assert_eq!(ret, test_val);

        let message = accumulator.flush();
        assert_eq!(message.keys().len(), 0);
    }
}
