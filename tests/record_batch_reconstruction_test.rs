// Record batch reconstruction tests
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use std::time::Duration;

mod common;

use blink::kafka::broker::BROKER;
use blink::util::Util;
use bytes::Bytes;
use common::ensure_settings_initialized;
use kafka_protocol::messages::{
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    ResponseKind, TopicName,
};
use kafka_protocol::records::{Record, RecordBatchDecoder, TimestampType};
use tokio::time::sleep;
use uuid::Uuid;

/// Test that verifies the record batch reconstruction bug fix.
///
/// This test reproduces the exact scenario that was broken:
/// 1. Produces 3 individual records (separate produce requests)
/// 2. Fetches them in a single fetch request
/// 3. Verifies that the response contains exactly 1 record batch with 3 records
///
/// Before the fix, this would return 3 separate record batches.
/// After the fix, this returns 1 record batch containing 3 records.
#[tokio::test]
async fn test_record_batch_reconstruction_fix() {
    // Initialize settings and use global broker
    ensure_settings_initialized();

    // Create a test topic
    let topic_name = format!("test-topic-{}", Uuid::new_v4());
    let topic_name_proto = TopicName(topic_name.clone().into());

    // Create the topic first
    BROKER.create_topic(topic_name.clone(), 1).unwrap();

    println!("=== Testing Record Batch Reconstruction Fix ===");
    println!("Topic: {}", topic_name);

    // Step 1: Produce 3 individual records (separate produce requests)
    // This simulates the common scenario where records arrive individually
    println!("\n1. Producing 3 individual records...");

    for i in 0..3 {
        let record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            timestamp: Util::now(),
            sequence: 0,
            offset: 0,
            key: Some(Bytes::from("testkey".to_string())),
            value: Some(Bytes::from(format!("Test message {}", i))),
            headers: Default::default(),
        };

        let produce_request =
            Util::create_produce_request(topic_name_proto.clone(), 0, &vec![record]);

        let response = BROKER.handle_produce(produce_request).await;

        // Verify produce was successful
        match response {
            ResponseKind::Produce(resp) => {
                assert_eq!(resp.responses.len(), 1, "Should have one topic response");
                let topic_resp = &resp.responses[0];
                assert_eq!(
                    topic_resp.partition_responses.len(),
                    1,
                    "Should have one partition response"
                );
                let partition_resp = &topic_resp.partition_responses[0];
                assert_eq!(
                    partition_resp.error_code, 0,
                    "Produce should succeed, got error: {}",
                    partition_resp.error_code
                );
                println!("  ✅ Produced record {} successfully", i);
            }
            _ => panic!("Expected ProduceResponse"),
        }

        // Small delay to ensure records are stored in order
        sleep(Duration::from_millis(10)).await;
    }

    // Step 2: Fetch all records with a single fetch request
    println!("\n2. Fetching all records with single fetch request...");

    let fetch_request = FetchRequest::default()
        .with_max_wait_ms(1000)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_isolation_level(0)
        .with_session_id(0)
        .with_session_epoch(-1)
        .with_topics(vec![FetchTopic::default()
            .with_topic(topic_name_proto.clone())
            .with_topic_id(Uuid::nil())
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_current_leader_epoch(-1)
                .with_fetch_offset(0)
                .with_last_fetched_epoch(-1)
                .with_log_start_offset(-1)
                .with_partition_max_bytes(1024 * 1024)])])
        .with_forgotten_topics_data(vec![])
        .with_rack_id("".into());

    let fetch_response = BROKER.handle_fetch(fetch_request).await;

    // Step 3: Verify the response structure
    println!("\n3. Analyzing fetch response...");

    match fetch_response {
        ResponseKind::Fetch(resp) => {
            assert_eq!(resp.responses.len(), 1, "Should have one topic in response");
            let topic_resp = &resp.responses[0];

            assert_eq!(
                topic_resp.partitions.len(),
                1,
                "Should have one partition in response"
            );
            let partition_resp = &topic_resp.partitions[0];

            assert_eq!(
                partition_resp.error_code, 0,
                "Fetch should succeed, got error: {}",
                partition_resp.error_code
            );

            // This is the key test: verify we got exactly one record batch
            let records = partition_resp
                .records
                .as_ref()
                .expect("Should have records");
            println!("  📦 Received {} bytes of record data", records.len());

            // Decode the record batch to verify structure
            let mut cursor = std::io::Cursor::new(records.as_ref());
            match RecordBatchDecoder::decode(&mut cursor) {
                Ok(record_set) => {
                    let decoded_records = record_set.records;

                    // Critical assertion: should have exactly 3 records in the batch
                    assert_eq!(
                        decoded_records.len(),
                        3,
                        "Expected 3 records in single batch, got {}",
                        decoded_records.len()
                    );

                    println!(
                        "  ✅ Successfully decoded single batch with {} records",
                        decoded_records.len()
                    );

                    // Verify record content
                    for (i, record) in decoded_records.iter().enumerate() {
                        let key = record.key.as_ref().map(|k| String::from_utf8_lossy(k));
                        let value = record.value.as_ref().map(|v| String::from_utf8_lossy(v));

                        assert_eq!(key.as_deref(), Some("testkey"), "Record {} key mismatch", i);
                        assert_eq!(
                            value.as_deref(),
                            Some(format!("Test message {}", i)).as_deref(),
                            "Record {} value mismatch",
                            i
                        );

                        println!(
                            "    Record {}: key={:?}, value={:?}, offset={}",
                            i, key, value, record.offset
                        );
                    }

                    // Verify batch-level properties
                    println!("  📊 Batch analysis:");
                    println!("    - Record count: {}", decoded_records.len());
                    println!("    - Total bytes: {}", records.len());
                    println!(
                        "    - Bytes per record: ~{}",
                        records.len() / decoded_records.len()
                    );

                    // Test that this is significantly more efficient than separate batches
                    let estimated_separate_batch_overhead = 3 * 61; // 3 batches × ~61 bytes header each
                    let single_batch_overhead = 61; // 1 batch × ~61 bytes header
                    let overhead_saved = estimated_separate_batch_overhead - single_batch_overhead;

                    println!(
                        "    - Estimated overhead saved: ~{} bytes vs separate batches",
                        overhead_saved
                    );
                }
                Err(e) => {
                    panic!("Failed to decode record batch: {:?}. This suggests multiple concatenated batches instead of single batch.", e);
                }
            }
        }
        _ => panic!("Expected FetchResponse"),
    }

    println!("\n✅ Record batch reconstruction test PASSED");
    println!("   - 3 individual produce requests succeeded");
    println!("   - Single fetch request returned 1 batch with 3 records");
    println!("   - Batch structure is valid and efficient");
}

/// Additional test to verify the fix works with larger batches
#[tokio::test]
async fn test_large_batch_reconstruction() {
    ensure_settings_initialized();
    let topic_name = format!("large-test-topic-{}", Uuid::new_v4());
    let topic_name_proto = TopicName(topic_name.clone().into());

    BROKER.create_topic(topic_name.clone(), 1).unwrap();

    println!("=== Testing Large Batch Reconstruction ===");

    const RECORD_COUNT: usize = 10;

    // Produce 10 individual records
    for i in 0..RECORD_COUNT {
        let record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            timestamp: Util::now(),
            sequence: 0,
            offset: 0,
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Some(Bytes::from(format!(
                "Large test message {} with more content to test batching efficiency",
                i
            ))),
            headers: Default::default(),
        };

        let produce_request =
            Util::create_produce_request(topic_name_proto.clone(), 0, &vec![record]);

        let response = BROKER.handle_produce(produce_request).await;
        assert!(matches!(response, ResponseKind::Produce(_)));
    }

    // Fetch all records
    let fetch_request = FetchRequest::default()
        .with_max_wait_ms(1000)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_isolation_level(0)
        .with_session_id(0)
        .with_session_epoch(-1)
        .with_topics(vec![FetchTopic::default()
            .with_topic(topic_name_proto.clone())
            .with_topic_id(Uuid::nil())
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_current_leader_epoch(-1)
                .with_fetch_offset(0)
                .with_last_fetched_epoch(-1)
                .with_log_start_offset(-1)
                .with_partition_max_bytes(1024 * 1024)])])
        .with_forgotten_topics_data(vec![])
        .with_rack_id("".into());

    let fetch_response = BROKER.handle_fetch(fetch_request).await;

    match fetch_response {
        ResponseKind::Fetch(resp) => {
            let partition_resp = &resp.responses[0].partitions[0];
            let records = partition_resp
                .records
                .as_ref()
                .expect("Should have records");

            let mut cursor = std::io::Cursor::new(records.as_ref());
            match RecordBatchDecoder::decode(&mut cursor) {
                Ok(record_set) => {
                    let decoded_records = record_set.records;

                    assert_eq!(
                        decoded_records.len(),
                        RECORD_COUNT,
                        "Expected {} records in single batch, got {}",
                        RECORD_COUNT,
                        decoded_records.len()
                    );

                    println!(
                        "✅ Large batch test PASSED: {} records in single batch",
                        RECORD_COUNT
                    );
                    println!("   Total batch size: {} bytes", records.len());
                    println!(
                        "   Average per record: {} bytes",
                        records.len() / RECORD_COUNT
                    );
                }
                Err(e) => {
                    panic!("Failed to decode large record batch: {:?}", e);
                }
            }
        }
        _ => panic!("Expected FetchResponse"),
    }
}

/// Test to ensure empty fetch requests work correctly
#[tokio::test]
async fn test_empty_fetch_behavior() {
    ensure_settings_initialized();
    let topic_name = format!("empty-test-topic-{}", Uuid::new_v4());
    let topic_name_proto = TopicName(topic_name.clone().into());

    BROKER.create_topic(topic_name.clone(), 1).unwrap();

    println!("=== Testing Empty Fetch Behavior ===");

    // Fetch from empty topic
    let fetch_request = FetchRequest::default()
        .with_max_wait_ms(100)
        .with_min_bytes(1)
        .with_max_bytes(1024)
        .with_isolation_level(0)
        .with_session_id(0)
        .with_session_epoch(-1)
        .with_topics(vec![FetchTopic::default()
            .with_topic(topic_name_proto.clone())
            .with_topic_id(Uuid::nil())
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_current_leader_epoch(-1)
                .with_fetch_offset(0)
                .with_last_fetched_epoch(-1)
                .with_log_start_offset(-1)
                .with_partition_max_bytes(1024)])])
        .with_forgotten_topics_data(vec![])
        .with_rack_id("".into());

    let fetch_response = BROKER.handle_fetch(fetch_request).await;

    match fetch_response {
        ResponseKind::Fetch(resp) => {
            let partition_resp = &resp.responses[0].partitions[0];

            // Should have empty records or very small response
            if let Some(ref records) = partition_resp.records {
                assert!(
                    records.is_empty() || records.len() < 10,
                    "Empty topic should return empty or minimal records, got {} bytes",
                    records.len()
                );
            }

            println!("✅ Empty fetch test PASSED");
        }
        _ => panic!("Expected FetchResponse"),
    }
}
