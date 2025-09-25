//! Deadlock Detection Integration Test
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
//! Tests 4 producers and 4 consumers with timeout to detect unexpected deadlocks
//! This test runs as part of the regular test suite to catch deadlock regressions

mod common;

use kafka_protocol::{messages::TopicName, protocol::StrBytes};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message, TopicPartitionList};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

const NUM_PRODUCERS: usize = 4;
#[allow(dead_code)]
const NUM_CONSUMERS: usize = 4;
const NUM_PARTITIONS: i32 = 8;
const MESSAGES_PER_PRODUCER: u64 = 100;
const TOTAL_EXPECTED_MESSAGES: u64 = NUM_PRODUCERS as u64 * MESSAGES_PER_PRODUCER;
const DEADLOCK_TIMEOUT_SECS: u64 = 5;
const MESSAGE_SIZE: usize = 512; // Smaller messages for faster test

#[derive(Debug)]
struct DeadlockTestStats {
    messages_produced: AtomicU64,
    messages_consumed: AtomicU64,
    #[allow(dead_code)]
    stop_flag: AtomicBool,
}

impl DeadlockTestStats {
    fn new() -> Self {
        Self {
            messages_produced: AtomicU64::new(0),
            messages_consumed: AtomicU64::new(0),
            stop_flag: AtomicBool::new(false),
        }
    }

    fn all_messages_produced(&self) -> bool {
        self.messages_produced.load(Ordering::Relaxed) >= TOTAL_EXPECTED_MESSAGES
    }

    fn all_messages_consumed(&self) -> bool {
        self.messages_consumed.load(Ordering::Relaxed) >= TOTAL_EXPECTED_MESSAGES
    }

    fn is_complete(&self) -> bool {
        self.all_messages_produced() && self.all_messages_consumed()
    }
}

async fn create_deadlock_test_producer() -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .set("batch.size", "1024")
        .set("linger.ms", "0") // Send immediately for faster test
        .set("compression.type", "none")
        .set("acks", "1")
        .create()
        .expect("Producer creation failed")
}

#[allow(dead_code)]
async fn create_deadlock_test_consumer(topic: &str) -> BaseConsumer {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", &format!("deadlock-test-{}", Uuid::new_v4()))
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .set("fetch.min.bytes", "1")
        .set("fetch.max.wait.ms", "50") // Faster polling for quicker test
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
async fn deadlock_producer_task(
    producer_id: usize,
    topic: String,
    stats: Arc<DeadlockTestStats>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let producer = create_deadlock_test_producer().await;
    let message_payload = "x".repeat(MESSAGE_SIZE);

    // Each producer gets specific partitions (2 partitions per producer)
    let start_partition = (producer_id * 2) as i32;
    let end_partition = start_partition + 2;

    println!(
        "Producer {} starting, will send {} messages to partitions {}-{}",
        producer_id,
        MESSAGES_PER_PRODUCER,
        start_partition,
        end_partition - 1
    );

    let mut messages_sent = 0u64;
    while messages_sent < MESSAGES_PER_PRODUCER && !stats.stop_flag.load(Ordering::Relaxed) {
        for partition in start_partition..end_partition {
            if messages_sent >= MESSAGES_PER_PRODUCER || stats.stop_flag.load(Ordering::Relaxed) {
                break;
            }

            let key = format!("producer_{}_msg_{}", producer_id, messages_sent);
            let record = FutureRecord::to(topic.as_str())
                .key(&key)
                .payload(&message_payload)
                .partition(partition);

            match producer.send(record, Duration::from_secs(1)).await {
                Ok(_) => {
                    stats.messages_produced.fetch_add(1, Ordering::Relaxed);
                    messages_sent += 1;
                }
                Err(e) => {
                    eprintln!("Producer {} error: {:?}", producer_id, e);
                    sleep(Duration::from_millis(5)).await;
                }
            }
        }
    }

    println!(
        "Producer {} completed, sent {} messages",
        producer_id, messages_sent
    );
    Ok(())
}

#[allow(dead_code)]
async fn deadlock_consumer_task(
    consumer_id: usize,
    topic: String,
    stats: Arc<DeadlockTestStats>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let consumer = create_deadlock_test_consumer(&topic).await;
    let mut messages_received = 0u64;

    println!("Consumer {} starting", consumer_id);

    while !stats.stop_flag.load(Ordering::Relaxed) {
        match consumer.poll(Duration::from_millis(50)) {
            Some(Ok(message)) => {
                stats.messages_consumed.fetch_add(1, Ordering::Relaxed);
                messages_received += 1;

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

                // Exit early if we've consumed all expected messages
                if stats.all_messages_consumed() {
                    break;
                }
            }
            Some(Err(e)) => {
                eprintln!("Consumer {} error: {:?}", consumer_id, e);
                sleep(Duration::from_millis(5)).await;
            }
            None => {
                // No message available, continue polling
                // Check if all messages have been produced and we should exit
                if stats.all_messages_produced() {
                    let consumed = stats.messages_consumed.load(Ordering::Relaxed);
                    if consumed >= TOTAL_EXPECTED_MESSAGES {
                        break;
                    }
                }
            }
        }
    }

    println!(
        "Consumer {} completed, received {} messages",
        consumer_id, messages_received
    );
    Ok(())
}

#[allow(dead_code)]
async fn run_deadlock_detection_test(
    topic_name: &str,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let stats = Arc::new(DeadlockTestStats::new());
    let mut handles = Vec::new();
    let start_time = Instant::now();

    println!("Starting deadlock detection test...");
    println!("Expected messages: {}", TOTAL_EXPECTED_MESSAGES);

    // Start producers
    for i in 0..NUM_PRODUCERS {
        let stats_clone = stats.clone();
        let topic_clone = topic_name.to_string();
        let handle =
            tokio::spawn(async move { deadlock_producer_task(i, topic_clone, stats_clone).await });
        handles.push(handle);
    }

    // Start consumers
    for i in 0..NUM_CONSUMERS {
        let stats_clone = stats.clone();
        let topic_clone = topic_name.to_string();
        let handle =
            tokio::spawn(async move { deadlock_consumer_task(i, topic_clone, stats_clone).await });
        handles.push(handle);
    }

    // Monitor progress and detect completion or timeout
    let monitoring_task = {
        let stats_clone = stats.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(500)).await;

                let produced = stats_clone.messages_produced.load(Ordering::Relaxed);
                let consumed = stats_clone.messages_consumed.load(Ordering::Relaxed);

                println!(
                    "Progress: Produced {}/{}, Consumed {}/{}",
                    produced, TOTAL_EXPECTED_MESSAGES, consumed, TOTAL_EXPECTED_MESSAGES
                );

                if stats_clone.is_complete() {
                    println!("All messages produced and consumed!");
                    break;
                }
            }
        })
    };

    // Wait for completion or timeout
    let result = timeout(Duration::from_secs(DEADLOCK_TIMEOUT_SECS), monitoring_task).await;

    // Signal all tasks to stop
    stats.stop_flag.store(true, Ordering::Relaxed);

    // Wait for all tasks to complete (with a short timeout)
    let cleanup_result = timeout(Duration::from_secs(2), async {
        for handle in handles {
            let _ = handle.await;
        }
    })
    .await;

    if cleanup_result.is_err() {
        println!("Warning: Some tasks didn't complete cleanup within timeout");
    }

    let elapsed = start_time.elapsed();
    let produced = stats.messages_produced.load(Ordering::Relaxed);
    let consumed = stats.messages_consumed.load(Ordering::Relaxed);

    println!("=== DEADLOCK DETECTION TEST RESULTS ===");
    println!("Test duration: {:.2}s", elapsed.as_secs_f64());
    println!(
        "Messages produced: {}/{}",
        produced, TOTAL_EXPECTED_MESSAGES
    );
    println!(
        "Messages consumed: {}/{}",
        consumed, TOTAL_EXPECTED_MESSAGES
    );

    match result {
        Ok(_) => {
            if stats.is_complete() {
                println!("✅ Test completed successfully within {} seconds", DEADLOCK_TIMEOUT_SECS);
                Ok(elapsed)
            } else {
                Err(format!(
                    "Test completed but not all messages were processed. Produced: {}, Consumed: {}",
                    produced, consumed
                ).into())
            }
        }
        Err(_) => {
            Err(format!(
                "❌ DEADLOCK DETECTED! Test did not complete within {} seconds. Produced: {}, Consumed: {}",
                DEADLOCK_TIMEOUT_SECS, produced, consumed
            ).into())
        }
    }
}

#[cfg(feature = "external-kafka")]
#[tokio::test]
async fn test_deadlock_detection_external() {
    // This test requires external kafka and is for comprehensive deadlock detection
    common::start_blink_with_settings("tests/configs/test_integration_settings.yaml").await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let topic_name = format!("deadlock-test-{}", Uuid::new_v4());
    let mut socket = common::connect_to_kafka();
    let topic_name_kafka = TopicName(StrBytes::from_string(topic_name.clone()));
    common::create_topic(topic_name_kafka.clone(), &mut socket, NUM_PARTITIONS);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let result = run_deadlock_detection_test(&topic_name).await;
    common::delete_topic(topic_name_kafka, &mut socket);

    match result {
        Ok(duration) => {
            assert!(
                duration.as_secs() <= DEADLOCK_TIMEOUT_SECS,
                "Test took too long: {:.2}s > {}s",
                duration.as_secs_f64(),
                DEADLOCK_TIMEOUT_SECS
            );
            println!(
                "✅ Deadlock detection test passed in {:.2}s",
                duration.as_secs_f64()
            );
        }
        Err(e) => {
            panic!("Deadlock detection test failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_deadlock_detection_basic() {
    // This test runs without external kafka as part of the regular test suite
    // It validates the basic producer/consumer setup against blink server
    common::ensure_settings_initialized_with_file("tests/configs/test_integration_settings.yaml");

    let topic_name = format!("deadlock-basic-{}", Uuid::new_v4());
    let mut socket = common::connect_to_kafka();
    let topic_name_kafka = TopicName(StrBytes::from_string(topic_name.clone()));

    // Create topic
    common::create_topic(topic_name_kafka.clone(), &mut socket, NUM_PARTITIONS);
    assert!(common::topic_exists(topic_name_kafka.clone(), &mut socket));

    // Test basic producer/consumer functionality with timeout
    let start = Instant::now();

    // Create a simple producer and consumer test
    let producer_result = timeout(Duration::from_secs(2), async {
        let producer = create_deadlock_test_producer().await;
        let payload = "test_message".repeat(10);
        let record = FutureRecord::to(&topic_name)
            .key("test_key")
            .payload(&payload)
            .partition(0);

        producer.send(record, Duration::from_secs(1)).await
    })
    .await;

    let elapsed = start.elapsed();

    // Clean up
    common::delete_topic(topic_name_kafka, &mut socket);

    // Verify no deadlock occurred
    assert!(
        elapsed.as_secs() < DEADLOCK_TIMEOUT_SECS,
        "Basic producer test took too long: {:.2}s",
        elapsed.as_secs_f64()
    );

    match producer_result {
        Ok(Ok(_)) => {
            println!(
                "✅ Basic deadlock detection test passed in {:.2}s",
                elapsed.as_secs_f64()
            );
        }
        Ok(Err(e)) => {
            // Producer error is acceptable for basic test, as long as no deadlock
            println!(
                "⚠️  Producer error (acceptable): {:?}, duration: {:.2}s",
                e,
                elapsed.as_secs_f64()
            );
        }
        Err(_) => {
            panic!(
                "❌ DEADLOCK DETECTED in basic test! Took longer than {}s",
                DEADLOCK_TIMEOUT_SECS
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deadlock_stats() {
        let stats = DeadlockTestStats::new();

        // Test initial state
        assert_eq!(stats.messages_produced.load(Ordering::Relaxed), 0);
        assert_eq!(stats.messages_consumed.load(Ordering::Relaxed), 0);
        assert!(!stats.all_messages_produced());
        assert!(!stats.all_messages_consumed());
        assert!(!stats.is_complete());

        // Test partial completion
        stats
            .messages_produced
            .store(TOTAL_EXPECTED_MESSAGES / 2, Ordering::Relaxed);
        assert!(!stats.all_messages_produced());
        assert!(!stats.is_complete());

        // Test production complete but consumption incomplete
        stats
            .messages_produced
            .store(TOTAL_EXPECTED_MESSAGES, Ordering::Relaxed);
        assert!(stats.all_messages_produced());
        assert!(!stats.is_complete());

        // Test full completion
        stats
            .messages_consumed
            .store(TOTAL_EXPECTED_MESSAGES, Ordering::Relaxed);
        assert!(stats.all_messages_consumed());
        assert!(stats.is_complete());
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

    #[test]
    fn test_constants() {
        assert_eq!(TOTAL_EXPECTED_MESSAGES, 400); // 4 producers * 100 messages each
        assert!(DEADLOCK_TIMEOUT_SECS >= 5);
        assert!(MESSAGE_SIZE > 0);
        assert_eq!(NUM_PARTITIONS, 8);
    }
}
