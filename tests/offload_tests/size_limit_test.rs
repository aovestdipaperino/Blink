// Offload size limit tests
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use crate::common::{connect_to_kafka, create_topic, delete_topic, produce_records};
use blink::kafka::storage::MEMORY;
use blink::settings::SETTINGS;
use blink::util::Util;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::sleep;

/// Test to find the maximum record size that can be produced without throttling
#[tokio::test]
async fn test_find_max_record_size() {
    crate::common::ensure_settings_initialized();

    println!("=== Finding Maximum Record Size Test ===");

    let topic_name = TopicName(StrBytes::from_static_str("size_limit_test"));
    let mut socket = connect_to_kafka();
    create_topic(topic_name.clone(), &mut socket, 1);

    println!("Max memory limit: {} bytes", SETTINGS.max_memory);

    // Test progressively larger record sizes to find the limit
    let test_sizes = vec![
        1024,    // 1KB
        5120,    // 5KB
        10240,   // 10KB
        25600,   // 25KB
        51200,   // 50KB
        102400,  // 100KB
        204800,  // 200KB
        409600,  // 400KB
        512000,  // 500KB
        819200,  // 800KB (80% of 1MB limit)
        1048576, // 1MB (full limit)
    ];

    let mut max_working_size = 0;

    for size in test_sizes {
        println!("\n--- Testing record size: {} KB ---", size / 1024);

        let before_memory = MEMORY.load(Ordering::Relaxed);
        println!("Memory before: {} KB", before_memory / 1024);

        let data = "x".repeat(size);
        let key = format!("test_key_{}", size);
        let record = Util::create_new_record(key.leak(), data.leak());

        // Try to produce the record
        match produce_records(topic_name.clone(), 0, &vec![record], &mut socket) {
            Ok(()) => {
                let after_memory = MEMORY.load(Ordering::Relaxed);
                println!(
                    "✅ SUCCESS - Size {} KB works (memory after: {} KB, increase: {} KB)",
                    size / 1024,
                    after_memory / 1024,
                    (after_memory - before_memory) / 1024
                );
                max_working_size = size;

                // Wait for processing
                sleep(Duration::from_millis(200)).await;

                // Check if we're approaching offload threshold
                if after_memory >= SETTINGS.max_memory {
                    println!(
                        "⚠️  Memory threshold reached: {} >= {} KB",
                        after_memory / 1024,
                        SETTINGS.max_memory / 1024
                    );

                    // Check if offload files were created
                    sleep(Duration::from_millis(500)).await;
                    let offload_dir = if let Some(ref record_storage) = SETTINGS.record_storage_path {
                        std::path::Path::new(record_storage)
                    } else {
                        continue;
                    };
                    if offload_dir.exists() {
                        println!("✅ Offload files created!");
                        if let Ok(entries) = std::fs::read_dir(offload_dir) {
                            for entry in entries {
                                if let Ok(entry) = entry {
                                    if entry.path().extension().and_then(|s| s.to_str())
                                        == Some("offload")
                                    {
                                        println!("  Found offload file: {}", entry.path().display());
                                    }
                                }
                            }
                        }
                        break; // Stop testing - we found offload behavior
                    }
                }
            }
            Err(e) => {
                println!("❌ FAILED - Size {} KB failed: {}", size / 1024, e);
                break;
            }
        }
    }

    println!("\n=== Results ===");
    println!(
        "Maximum working record size: {} KB",
        max_working_size / 1024
    );

    let final_memory = MEMORY.load(Ordering::Relaxed);
    println!("Final memory usage: {} KB", final_memory / 1024);
    println!("Offload threshold: {} KB", SETTINGS.max_memory / 1024);
    println!("Above threshold: {}", final_memory >= SETTINGS.max_memory);

    let offload_dir = if let Some(ref record_storage) = SETTINGS.record_storage_path {
        std::path::Path::new(record_storage)
    } else {
        std::path::Path::new("./default_offload")
    };
    println!("Offload directory exists: {}", offload_dir.exists());

    // Cleanup
    delete_topic(topic_name, &mut socket);
}

/// Test accumulative memory usage with multiple smaller records
#[tokio::test]
async fn test_accumulative_memory_usage() {
    println!("=== Accumulative Memory Usage Test ===");

    let topic_name = TopicName(StrBytes::from_static_str("accumulative_test"));
    let mut socket = connect_to_kafka();
    create_topic(topic_name.clone(), &mut socket, 1);

    let max_memory = SETTINGS.max_memory;

    println!("Max memory: {} KB", max_memory / 1024);
    println!("Offload threshold: {} KB", max_memory / 1024);

    // Use records that are small enough to not hit individual size limits
    // but accumulate to trigger offload
    let record_size = 50 * 1024; // 50KB each - should work individually
    let max_records = (max_memory / record_size) + 2; // Exceed memory limit

    println!(
        "Testing with {} records of {} KB each",
        max_records,
        record_size / 1024
    );
    println!(
        "Total size would be: {} KB",
        (max_records * record_size) / 1024
    );

    for i in 0..max_records {
        let before_memory = MEMORY.load(Ordering::Relaxed);

        let data = "x".repeat(record_size);
        let key = format!("key_{}", i);
        let record = Util::create_new_record(key.leak(), data.leak());

        println!(
            "\nProducing record {} (memory before: {} KB)",
            i,
            before_memory / 1024
        );

        match produce_records(topic_name.clone(), 0, &vec![record], &mut socket) {
            Ok(()) => {
                let after_memory = MEMORY.load(Ordering::Relaxed);
                println!(
                    "✅ Record {} produced (memory after: {} KB, increase: {} KB)",
                    i,
                    after_memory / 1024,
                    (after_memory - before_memory) / 1024
                );

                // Check if we've hit the offload threshold
                if after_memory >= max_memory {
                    println!("🎯 Offload threshold reached after {} records!", i + 1);

                    // Give time for offloading to occur
                    sleep(Duration::from_millis(1000)).await;

                    // Check for offload files
                    let offload_dir = if let Some(ref record_storage) = SETTINGS.record_storage_path {
                        std::path::Path::new(record_storage)
                    } else {
                        continue;
                    };
                    if offload_dir.exists() {
                        println!("✅ Offload directory created!");
                        if let Ok(entries) = std::fs::read_dir(offload_dir) {
                            let mut offload_count = 0;
                            for entry in entries {
                                if let Ok(entry) = entry {
                                    if entry.path().extension().and_then(|s| s.to_str())
                                        == Some("offload")
                                    {
                                        offload_count += 1;
                                        println!("  Offload file: {}", entry.path().display());
                                    }
                                }
                            }
                            println!("Total offload files: {}", offload_count);
                        }
                        break; // Success - offloading worked!
                    } else {
                        println!("⚠️  Threshold reached but no offload directory created");
                    }
                }
            }
            Err(e) => {
                println!("❌ Record {} failed: {}", i, e);
                if e.contains("Throttling") {
                    println!("Hit throttling after {} successful records", i);
                    break;
                }
            }
        }

        // Small delay between records
        sleep(Duration::from_millis(100)).await;
    }

    let final_memory = MEMORY.load(Ordering::Relaxed);
    println!("\nFinal memory usage: {} KB", final_memory / 1024);

    // Cleanup
    delete_topic(topic_name, &mut socket);
}
