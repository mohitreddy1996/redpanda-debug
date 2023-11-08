use std::collections::HashMap;
use std::env;
use std::time::Duration;
use rdkafka::{ClientConfig, ClientContext, Statistics, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, CommitMode};
use rdkafka::error::KafkaError;
use rdkafka::consumer::Consumer;
use rdkafka::producer::{BaseProducer, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::Offset;
use bytes::Bytes;
use anyhow::Result;
use fern;
use chrono;
use log;

use rand::{Rng, distributions::Alphanumeric};

const SECURITY_PROTOCOL: &str = "SASL_SSL";
const SASL_SCRAM_SHA512MECHANISM: &str = "SCRAM-SHA-512";


pub fn setup_logger() -> Result<()> {
    use anyhow::Ok;

    fern::Dispatch::new()
        // format each log message to show
        //  1. time
        //  2. module target/name
        //  3. level
        //  4. message
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}:{}] {}",
                chrono::Local::now().format("[%Y-%m-%d %H:%M:%S]"),
                record.level(),
                record.file().unwrap_or(""),
                record.line().unwrap_or(0),
                message
            ))
        })
        // configure the logging level >= Error
        //
        // this will log messages with level >= Error from the dependent crates
        .level(log::LevelFilter::Error)
        // except for current package, for which we set the logging info to be Info
        .level_for(env!("CARGO_PKG_NAME"), log::LevelFilter::Debug)
        // log to stdout
        .chain(std::io::stdout())
        // apply this universally
        .apply()?;
    Ok(())
}


pub fn rand_test_group() -> String {
    let id = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect::<String>();
    format!("__test_{}", id)
}

pub fn rand_test_topic() -> String {
    let id = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect::<String>();
    format!("__test_{}", id)
}

pub fn rand_test_transactional_id() -> String {
    let id = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect::<String>();
    format!("__test_{}", id)
}

pub fn get_bootstrap_server() -> String {
    env::var("KAFKA_HOST").unwrap_or_else(|_| "localhost:9092".to_owned())
}

pub fn get_security_protocol() -> String {
    env::var("KAFKA_PROTOCOL").unwrap_or_else(|_| SECURITY_PROTOCOL.to_owned())
}

pub fn get_sasl_mechanisms() -> String {
    env::var("KAFKA_SASL_MECHANISM").unwrap_or_else(|_| SASL_SCRAM_SHA512MECHANISM.to_owned())
}

pub fn get_username() -> String {
    env::var("KAFKA_USERNAME").unwrap_or_else(|_| "kafka".to_owned())
}

pub fn get_password() -> String {
    env::var("KAFKA_PASSWORD").unwrap_or_else(|_| "kafka".to_owned())
}

fn create_consumer(
    config_overrides: Option<HashMap<&str, &str>>,
) -> Result<BaseConsumer, KafkaError> {
    consumer_config(&rand_test_group(), config_overrides).create()
}

pub fn consumer_config(
    group_id: &str,
    config_overrides: Option<HashMap<&str, &str>>,
) -> ClientConfig {
    let mut config = ClientConfig::new();

    config.set("group.id", group_id);
    config.set("client.id", "rdkafka_integration_test_client");
    config.set("bootstrap.servers", get_bootstrap_server().as_str());
    config.set("security.protocol", get_security_protocol().as_str());
    config.set("enable.ssl.certificate.verification", "false");
    config.set("sasl.mechanisms", get_sasl_mechanisms().as_str());
    config.set("sasl.username", get_username().as_str());
    config.set("sasl.password", get_password().as_str());
    config.set("session.timeout.ms", "6000");
    config.set("enable.auto.commit", "false");
    config.set("statistics.interval.ms", "500");
    config.set("debug", "all");
    config.set("auto.offset.reset", "earliest");

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            config.set(key, value);
        }
    }

    config
}

fn create_producer() -> Result<BaseProducer, KafkaError> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &get_bootstrap_server())
        .set("security.protocol", get_security_protocol().as_str())
        .set("enable.ssl.certificate.verification", "false")
        .set("sasl.mechanisms", get_sasl_mechanisms().as_str())
        .set("sasl.username", get_username().as_str())
        .set("sasl.password", get_password().as_str())
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .set("transactional.id", &rand_test_transactional_id())
        .set("debug", "eos");
    config.set_log_level(RDKafkaLogLevel::Debug);
    config.create()
}

struct ProducerTestContext {
    _some_data: i32
}

impl ClientContext for ProducerTestContext {
    fn stats(&self, _: Statistics) {} // Don't print stats
}

pub async fn populate_topic(
    topic_name: &str,
    count: i32,
    partition: Option<i32>,
    timestamp: Option<i64>,
) -> HashMap<(i32, i64), i32>
{
    let prod_context = ProducerTestContext { _some_data: 1234 };

    // Produce some messages
    let producer = &ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_server().as_str())
        .set("security.protocol", get_security_protocol().as_str())
        .set("sasl.mechanisms", get_sasl_mechanisms().as_str())
        .set("enable.ssl.certificate.verification", "false")
        .set("sasl.username", get_username().as_str())
        .set("sasl.password", get_password().as_str())
        .set("statistics.interval.ms", "500")
        .set("debug", "all")
        .set("message.timeout.ms", "30000")
        .create_with_context::<ProducerTestContext, FutureProducer<_>>(prod_context)
        .expect("Producer creation error");

    let futures = (0..count)
        .map(|id| {
            let future = async move {
                producer
                    .send(
                        FutureRecord {
                            topic: topic_name,
                            payload: Some(&Bytes::from(format!("test {}", id)).to_vec()),
                            key: Some(&Bytes::from(format!("test {}", id)).to_vec()),
                            partition,
                            timestamp,
                            headers: None,
                        },
                        Duration::from_secs(1),
                    )
                    .await
            };
            (id, future)
        })
        .collect::<Vec<_>>();

    let mut message_map = HashMap::new();
    for (id, future) in futures {
        match future.await {
            Ok((partition, offset)) => message_map.insert((partition, offset), id),
            Err((kafka_error, _message)) => panic!("Delivery failed: {}", kafka_error),
        };
    }

    message_map
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");
    setup_logger().unwrap();

    let consume_topic = rand_test_topic();
    let consume_topic2 = rand_test_topic();

    populate_topic(&consume_topic, 30, Some(0), None).await;
    populate_topic(&consume_topic2, 30, Some(0), None).await;

    // Create consumer and subscribe to `consume_topic`.
    let consumer = create_consumer(None)?;
    consumer.subscribe(&[&consume_topic])?;
    consumer.poll(Timeout::Never).unwrap()?;

    let consumer2 = create_consumer(None)?;
    consumer2.subscribe(&[&consume_topic2])?;
    consumer2.poll(Timeout::Never).unwrap()?;

    // Commit the first 10 messages.
    let mut commit_tpl = TopicPartitionList::new();
    commit_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(10))?;
    consumer.commit(&commit_tpl, CommitMode::Sync).unwrap();

    let mut commit_tpl2 = TopicPartitionList::new();
    commit_tpl2.add_partition_offset(&consume_topic2, 0, Offset::Offset(10))?;
    consumer2.commit(&commit_tpl2, CommitMode::Sync).unwrap();

    // Create a producer and start a transaction.
    let producer = create_producer()?;
    producer.init_transactions(Timeout::Never)?;
    producer.begin_transaction()?;

    // Tie the commit of offset 20 to the transaction.
    let cgm = consumer.group_metadata().unwrap();
    let mut txn_tpl = TopicPartitionList::new();
    txn_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(20))?;
    producer.send_offsets_to_transaction(&txn_tpl, &cgm, Timeout::Never)?;

    let cgm2 = consumer2.group_metadata().unwrap();
    let mut txn_tpl2 = TopicPartitionList::new();
    txn_tpl2.add_partition_offset(&consume_topic2, 0, Offset::Offset(20))?;
    producer.send_offsets_to_transaction(&txn_tpl2, &cgm2, Timeout::Never)?;

    // Abort the transaction, but only after producing all messages.
    producer.flush(Timeout::Never)?;
    producer.commit_transaction(Timeout::Never)?;
    Ok(())
}
