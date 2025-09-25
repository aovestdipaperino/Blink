//! Simplified throughput test based on Kitt's approach
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
//! Tests 4 producers and 4 consumers with 10K msg/sec minimum target

mod common;

use kafka_protocol::{messages::TopicName, protocol::StrBytes};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message, TopicPartitionList};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

const NUM_PRODUCERS: usize = 4;
#[allow(dead_code)]
const NUM_CONSUMERS: usize = 4;
const NUM_PARTITIONS: i32 = 8; // 2 partitions per producer/consumer pair
#[allow(dead_code)]
const MEASUREMENT_DURATION_SECS: u64 = 15;
#[allow(dead_code)]
const WARMUP_DURATION_SECS: u64 = 5;
#[allow(dead_code)]
const TARGET_THROUGHPUT: f64 = 10_000.0; // 10K msg/sec minimum
#[allow(dead_code)]
const MESSAGE_SIZE: usize = 1024;
const MAX_BACKLOG: u64 = 1000;

#[derive(Debug)]
struct ThroughputStats {
    messages_produced: AtomicU64,
    messages_consumed: AtomicU64,
    start_time: Option<Instant>,
    #[allow(dead_code)]
    stop_flag: AtomicBool,
}

impl ThroughputStats {
    fn new() -> Self {
        Self {
            messages_produced: AtomicU64::new(0),
            messages_consumed: AtomicU64::new(0),
            start_time: None,
            stop_flag: AtomicBool::new(false),
        }
    }

    #[allow(dead_code)]
    fn start_measurement(&mut self) {
        self.start_time = Some(Instant::now());
        self.messages_produced.store(0, Ordering::Relaxed);
        self.messages_consumed.store(0, Ordering::Relaxed);
    }

    fn get_throughput(&self) -> f64 {
        if let Some(start) = self.start_time {
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                let consumed = self.messages_consumed.load(Ordering::Relaxed) as f64;
                return consumed / elapsed;
            }
        }
        0.0
    }

    fn get_backlog(&self) -> i64 {
        let produced = self.messages_produced.load(Ordering::Relaxed) as i64;
        let consumed = self.messages_consumed.load(Ordering::Relaxed) as i64;
        produced - consumed
    }

    fn should_pause_producer(&self) -> bool {
        self.get_backlog() > MAX_BACKLOG as i64
    }
}

#[allow(dead_code)]
async fn create_producer() -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .set("batch.size", "16384")
        .set("linger.ms", "1")
        .set("compression.type", "none")
        .set("acks", "1")
        .create()
        .expect("Producer creation failed")
}

#[allow(dead_code)]
async fn create_consumer(topic: &str) -> BaseConsumer {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", &format!("throughput-test-{}", Uuid::new_v4()))
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .set("fetch.min.bytes", "1")
        .set("fetch.max.wait.ms", "100")
        .create()
        .expect("Consumer creation failed");

    // Subscribe to all partitions
    let mut tpl = TopicPartitionList::new();
    for partition in 0..NUM_PARTITIONS {
        tpl.add_partition(topic, partition);
    }
    consumer.assign(&tpl).expect("Failed to assign partitions");

    consumer
}

#[allow(dead_code)]
async fn producer_task(
    producer_id: usize,
    topic: String,
    stats: Arc<ThroughputStats>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let producer = create_producer().await;
    let message_payload = "x".repeat(MESSAGE_SIZE);
    let mut message_counter = 0u64;

    // Each producer gets specific partitions (2 partitions per producer)
    let start_partition = (producer_id * 2) as i32;
    let end_partition = start_partition + 2;

    println!(
        "Producer {} handling partitions {}-{}",
        producer_id,
        start_partition,
        end_partition - 1
    );

    while !stats.stop_flag.load(Ordering::Relaxed) {
        // Check if we should pause due to backlog
        if stats.should_pause_producer() {
            sleep(Duration::from_millis(10)).await;
            continue;
        }

        for partition in start_partition..end_partition {
            if stats.stop_flag.load(Ordering::Relaxed) {
                break;
            }

            let key = format!("producer_{}_msg_{}", producer_id, message_counter);
            let record = FutureRecord::to(topic.as_str())
                .key(&key)
                .payload(&message_payload)
                .partition(partition);

            match producer.send(record, Duration::from_secs(1)).await {
                Ok(_) => {
                    stats.messages_produced.fetch_add(1, Ordering::Relaxed);
                    message_counter += 1;
                }
                Err(e) => {
                    eprintln!("Producer {} error: {:?}", producer_id, e);
                    sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
async fn consumer_task(
    consumer_id: usize,
    topic: String,
    stats: Arc<ThroughputStats>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let consumer = create_consumer(&topic).await;

    println!("Consumer {} started", consumer_id);

    while !stats.stop_flag.load(Ordering::Relaxed) {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                stats.messages_consumed.fetch_add(1, Ordering::Relaxed);

                // Validate message
                if let Some(payload) = message.payload() {
                    if payload.len() != MESSAGE_SIZE {
                        eprintln!(
                            "Consumer {} received message with wrong size: {}",
                            consumer_id,
                            payload.len()
                        );
                    }
                }
            }
            Some(Err(e)) => {
                eprintln!("Consumer {} error: {:?}", consumer_id, e);
                sleep(Duration::from_millis(10)).await;
            }
            None => {
                // No message available, continue polling
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
async fn run_throughput_test(
    topic_name: &str,
) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    let stats = Arc::new(ThroughputStats::new());
    let mut handles = Vec::new();

    // Start producers
    for i in 0..NUM_PRODUCERS {
        let stats_clone = stats.clone();
        let topic_clone = topic_name.to_string();
        let handle = tokio::spawn(async move { producer_task(i, topic_clone, stats_clone).await });
        handles.push(handle);
    }

    // Start consumers
    for i in 0..NUM_CONSUMERS {
        let stats_clone = stats.clone();
        let topic_clone = topic_name.to_string();
        let handle = tokio::spawn(async move { consumer_task(i, topic_clone, stats_clone).await });
        handles.push(handle);
    }

    // Warmup period
    println!("Warming up for {} seconds...", WARMUP_DURATION_SECS);
    sleep(Duration::from_secs(WARMUP_DURATION_SECS)).await;

    // Start measurement
    println!(
        "Starting measurement for {} seconds...",
        MEASUREMENT_DURATION_SECS
    );
    // Reset counters for measurement phase
    stats.messages_produced.store(0, Ordering::Relaxed);
    stats.messages_consumed.store(0, Ordering::Relaxed);
    let measurement_start = Instant::now();

    // Measurement period with progress updates
    while measurement_start.elapsed().as_secs() < MEASUREMENT_DURATION_SECS {
        sleep(Duration::from_secs(3)).await;
        let elapsed = measurement_start.elapsed().as_secs_f64();
        let consumed = stats.messages_consumed.load(Ordering::Relaxed) as f64;
        let current_throughput = if elapsed > 0.0 {
            consumed / elapsed
        } else {
            0.0
        };
        let backlog = stats.get_backlog();
        let elapsed_secs = measurement_start.elapsed().as_secs();
        println!(
            "[{}s] Throughput: {:.1} msg/s, Backlog: {}",
            elapsed_secs, current_throughput, backlog
        );
    }

    // Stop all tasks
    stats.stop_flag.store(true, Ordering::Relaxed);

    // Wait for all tasks to complete
    for handle in handles {
        let _ = handle.await;
    }

    let measurement_elapsed = measurement_start.elapsed().as_secs_f64();
    let produced = stats.messages_produced.load(Ordering::Relaxed);
    let consumed = stats.messages_consumed.load(Ordering::Relaxed);
    let final_throughput = if measurement_elapsed > 0.0 {
        consumed as f64 / measurement_elapsed
    } else {
        0.0
    };
    let backlog = stats.get_backlog();

    println!("=== THROUGHPUT TEST RESULTS ===");
    println!("Messages produced: {}", produced);
    println!("Messages consumed: {}", consumed);
    println!("Final backlog: {}", backlog);
    println!("Average throughput: {:.1} msg/s", final_throughput);
    println!("Target throughput: {:.1} msg/s", TARGET_THROUGHPUT);

    if final_throughput >= TARGET_THROUGHPUT {
        println!("✅ THROUGHPUT TARGET ACHIEVED!");
    } else {
        println!("❌ THROUGHPUT TARGET NOT MET");
    }

    Ok(final_throughput)
}

#[cfg(feature = "external-kafka")]
#[tokio::test]
async fn test_throughput_performance() {
    // Start blink server with throughput-optimized settings
    common::start_blink_with_settings("tests/configs/test_throughput_settings.yaml").await;

    // Wait for server to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Create unique topic name
    let topic_name = format!("throughput-test-{}", Uuid::new_v4());

    // Create topic with proper partitions
    let mut socket = common::connect_to_kafka();
    let topic_name_kafka = TopicName(StrBytes::from_string(topic_name.clone()));
    common::create_topic(topic_name_kafka.clone(), &mut socket, NUM_PARTITIONS);

    // Wait for topic to be ready
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Run the throughput test
    let result = run_throughput_test(&topic_name).await;

    // Clean up topic
    common::delete_topic(topic_name_kafka, &mut socket);

    // Assert results
    match result {
        Ok(throughput) => {
            assert!(
                throughput >= TARGET_THROUGHPUT,
                "Throughput {:.1} msg/s is below target {:.1} msg/s",
                throughput,
                TARGET_THROUGHPUT
            );
        }
        Err(e) => {
            panic!("Throughput test failed: {:?}", e);
        }
    }
}

// Basic integration test that can run without external kafka
#[tokio::test]
async fn test_throughput_test_setup() {
    // Just test that we can create the topic and basic setup works
    common::ensure_settings_initialized_with_file("tests/configs/test_throughput_settings.yaml");

    let topic_name = format!("setup-test-{}", Uuid::new_v4());
    let mut socket = common::connect_to_kafka();
    let topic_name_kafka = TopicName(StrBytes::from_string(topic_name));

    // Test topic creation and deletion
    common::create_topic(topic_name_kafka.clone(), &mut socket, NUM_PARTITIONS);
    assert!(common::topic_exists(topic_name_kafka.clone(), &mut socket));

    common::delete_topic(topic_name_kafka, &mut socket);

    println!("✅ Throughput test setup validation passed");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_throughput_stats() {
        let stats = ThroughputStats::new();

        // Test initial state
        assert_eq!(stats.get_throughput(), 0.0);
        assert_eq!(stats.get_backlog(), 0);
        assert!(!stats.should_pause_producer());

        // Test backlog calculation
        stats.messages_produced.store(1500, Ordering::Relaxed);
        stats.messages_consumed.store(400, Ordering::Relaxed);
        assert_eq!(stats.get_backlog(), 1100);
        assert!(stats.should_pause_producer()); // Should pause when backlog > 1000

        // Test balanced state
        stats.messages_consumed.store(1400, Ordering::Relaxed);
        assert_eq!(stats.get_backlog(), 100);
        assert!(!stats.should_pause_producer()); // Should not pause when backlog < 1000
    }

    #[test]
    fn test_partition_assignment() {
        // Test that each producer gets correct partition range
        for producer_id in 0..NUM_PRODUCERS {
            let start_partition = (producer_id * 2) as i32;
            let end_partition = start_partition + 2;

            assert!(start_partition >= 0);
            assert!(end_partition <= NUM_PARTITIONS);
            assert_eq!(end_partition - start_partition, 2); // Each producer gets 2 partitions
        }
    }
}
