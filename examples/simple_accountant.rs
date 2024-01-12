extern crate sentry_usage_accountant;

use clap::Parser;
use sentry_usage_accountant::{KafkaConfig, UsageAccountant, UsageUnit};
use std::collections::HashMap;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Kafka broker server in the host:port form
    #[arg(short, long)]
    bootstrap_server: String,
}

fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let kafka_config = KafkaConfig::new_producer_config(HashMap::from([(
        "bootstrap.servers".to_string(),
        args.bootstrap_server.to_string(),
    )]));
    let mut accountant = UsageAccountant::new_with_kafka(kafka_config, None, None);

    accountant
        .record("my_resource", "my_feature", 100, UsageUnit::Bytes)
        .unwrap();
    accountant
        .record("my_resource", "my_feature", 100, UsageUnit::Bytes)
        .unwrap();
    accountant
        .record("my_resource", "another_feature", 100, UsageUnit::Bytes)
        .unwrap();
    accountant
        .record("my_resource", "yet_another_feature", 100, UsageUnit::Bytes)
        .unwrap();
}
