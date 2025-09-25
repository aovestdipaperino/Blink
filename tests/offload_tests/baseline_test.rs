// Baseline offload functionality tests
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use crate::common::{connect_to_kafka, create_topic, delete_topic, fetch_records, produce_records};
use blink::kafka::storage::MEMORY;
use blink::settings::SETTINGS;
use blink::util::Util;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::sleep;

/// Simple baseline test to verify basic produce/consume works before testing offload
#[tokio::test]
async fn test_baseline_produce_consume() {
    crate::common::ensure_settings_initialized();

    println!("=== Baseline Test: Basic Produce/Consume ===");

    // Create topic
    let topic_name = TopicName(StrBytes::from_static_str("baseline_test"));
    let mut socket = connect_to_kafka();
    create_topic(topic_name.clone(), &mut socket, 1);

    let initial_memory = MEMORY.load(Ordering::Relaxed);
    println!("Initial memory usage: {} bytes", initial_memory);

    // Create a small record (1KB)
    let data = "x".repeat(1000);
    let small_record = Util::create_new_record("test_key", data.leak());

    // Produce the record
    println!("Producing small record...");
    match produce_records(
        topic_name.clone(),
        0,
        &vec![small_record.clone()],
        &mut socket,
    ) {
        Ok(_) => println!("✅ Record produced successfully"),
        Err(e) => {
            if e.contains("Throttling") {
                println!("⚠️ Hit throttling but continuing test - system under memory pressure");
            } else {
                panic!("Unexpected error: {}", e);
            }
        }
    }

    let after_produce_memory = MEMORY.load(Ordering::Relaxed);
    println!(
        "Memory after produce: {} bytes (increase: {} bytes)",
        after_produce_memory,
        after_produce_memory - initial_memory
    );

    // Wait a bit
    sleep(Duration::from_millis(100)).await;

    // Try to fetch it back
    println!("Fetching record back...");
    fetch_records(topic_name.clone(), 0, 0, &vec![small_record], &mut socket);

    println!("✅ Baseline test passed - basic produce/consume works");

    // Cleanup
    delete_topic(topic_name, &mut socket);
}

/// Test memory tracking with progressively larger records
#[tokio::test]
async fn test_memory_tracking() {
    crate::common::ensure_settings_initialized();

    println!("=== Memory Tracking Test ===");

    let topic_name = TopicName(StrBytes::from_static_str("memory_test"));
    let mut socket = connect_to_kafka();
    create_topic(topic_name.clone(), &mut socket, 1);

    let max_memory = SETTINGS.max_memory;
    println!("Max memory limit: {} bytes", max_memory);

    let initial_memory = MEMORY.load(Ordering::Relaxed);
    println!("Initial memory: {} bytes", initial_memory);

    // Test with increasing record sizes
    let sizes = vec![1024, 5120, 10240, 25600]; // 1KB, 5KB, 10KB, 25KB

    for (i, size) in sizes.iter().enumerate() {
        println!("\n--- Test {}: {}KB record ---", i + 1, size / 1024);

        let before_memory = MEMORY.load(Ordering::Relaxed);
        println!("Memory before produce: {} bytes", before_memory);

        let data = "x".repeat(*size);
        let key = format!("key_{}", i);
        let record = Util::create_new_record(key.leak(), data.leak());

        // Try to produce the record
        match produce_records(topic_name.clone(), 0, &vec![record], &mut socket) {
            Ok(_) => {
                let after_memory = MEMORY.load(Ordering::Relaxed);
                println!(
                    "✅ Produce succeeded. Memory after: {} bytes (increase: {} bytes)",
                    after_memory,
                    after_memory - before_memory
                );
            }
            Err(e) => {
                if e.contains("Throttling") {
                    println!(
                        "⚠️ Hit throttling on record {} - system under memory pressure",
                        i + 1
                    );
                    break; // Stop testing when memory pressure is reached
                } else {
                    panic!("Unexpected error: {}", e);
                }
            }
        }

        // Wait for processing
        sleep(Duration::from_millis(200)).await;

        // Check if we're approaching memory limits
        let current_memory = MEMORY.load(Ordering::Relaxed);
        if current_memory >= max_memory {
            println!(
                "⚠️  Approaching memory threshold: {} / {} bytes",
                current_memory, max_memory
            );
        }
    }

    let final_memory = MEMORY.load(Ordering::Relaxed);
    println!("\nFinal memory usage: {} bytes", final_memory);

    // Cleanup
    delete_topic(topic_name, &mut socket);
}

/// Test to see if offload is triggered at all
#[tokio::test]
async fn test_offload_directory_creation() {
    crate::common::ensure_settings_initialized();

    println!("=== Offload Directory Creation Test ===");

    let topic_name = TopicName(StrBytes::from_static_str("offload_dir_test"));
    let mut socket = connect_to_kafka();
    create_topic(topic_name.clone(), &mut socket, 1);

    println!("Offload enabled: {}", SETTINGS.record_storage_path.is_some());
    if let Some(ref record_storage) = SETTINGS.record_storage_path {
        println!("Record storage directory: {}", record_storage);
    } else {
        println!("Record storage directory: None");
    }
    println!("Max memory: {} bytes", SETTINGS.max_memory);

    // Get offload directory path for later use
    let offload_dir_path = SETTINGS
        .record_storage_path
        .as_ref()
        .map(|s| std::path::PathBuf::from(s));

    // Remove any existing offload directory
    if let Some(ref offload_path) = offload_dir_path {
        if offload_path.exists() {
            std::fs::remove_dir_all(offload_path).ok();
        }
    }

    // Try to produce records that accumulate to exceed memory
    let record_size = 25 * 1024; // 25KB each record - avoid individual throttling
    println!("Creating records of size: {} KB", record_size / 1024);

    for i in 0..50 {
        let before_memory = MEMORY.load(Ordering::Relaxed);
        println!(
            "\nProducing record {} (memory before: {} KB)",
            i,
            before_memory / 1024
        );

        let data = "x".repeat(record_size);
        let key = format!("key_{}", i);
        let record = Util::create_new_record(key.leak(), data.leak());

        // Try to produce the record
        match produce_records(topic_name.clone(), 0, &vec![record], &mut socket) {
            Ok(_) => {
                let after_memory = MEMORY.load(Ordering::Relaxed);
                println!(
                    "✅ Record {} produced (memory after: {} KB)",
                    i,
                    after_memory / 1024
                );
            }
            Err(e) => {
                if e.contains("Throttling") {
                    println!("Hit throttling after {} records - continuing test", i);
                    break;
                }
                panic!("Unexpected error: {}", e);
            }
        }

        // Wait and check for offload directory
        sleep(Duration::from_millis(200)).await;

        if let Some(ref offload_path) = offload_dir_path {
            if offload_path.exists() {
                println!("✅ Offload directory created!");
                if let Ok(entries) = std::fs::read_dir(offload_path) {
                    for entry in entries {
                        if let Ok(entry) = entry {
                            println!("  Found file: {}", entry.path().display());
                        }
                    }
                }
                break;
            } else if i % 10 == 9 {
                println!("Offload directory not created yet after {} records", i + 1);
            }
        }
    }

    // Final check
    let current_memory = MEMORY.load(Ordering::Relaxed);
    println!("\nFinal state:");
    println!("  Memory usage: {} KB", current_memory / 1024);
    println!("  Memory threshold: {} KB", SETTINGS.max_memory / 1024);
    println!(
        "  Above threshold: {}",
        current_memory >= SETTINGS.max_memory
    );
    println!(
        "  Offload directory exists: {}",
        offload_dir_path.as_ref().map_or(false, |p| p.exists())
    );

    // Cleanup
    delete_topic(topic_name, &mut socket);
}
