extern crate sentry_usage_accountant;

use clap::Parser;
use sentry_usage_accountant::{KafkaConfig, KafkaProducer, Producer, UsageUnit};
use std::collections::HashMap;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Kafka broker server in the host:port form
    #[arg(short, long)]
    bootstrap_server: String,

    /// Kafka topic to produce onto
    #[arg(short, long)]
    topic: String,
}

fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let kafka_config = KafkaConfig {
        topic: "test_topic".to_owned(),
        config: HashMap::from([(
            "bootstrap.servers".to_string(),
            args.bootstrap_server.to_string(),
        )]),
    };
    let mut producer = KafkaProducer::new(kafka_config);

    producer
        .send(sentry_usage_accountant::Message {
            timestamp: 1337,
            shared_resource_id: "some_resource".to_owned(),
            app_feature: "feature".to_owned(),
            usage_unit: UsageUnit::Bytes,
            amount: 42,
        })
        .expect("failed to produce message");
}
