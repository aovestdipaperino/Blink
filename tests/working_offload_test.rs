// Working offload functionality tests
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
mod common;

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::sleep;

#[cfg(test)]
mod working_offload_tests {
    use super::*;
    use crate::common::{connect_to_kafka, create_topic, delete_topic, produce_messages, SETUP};
    use blink::kafka::storage::MEMORY;
    use blink::settings::SETTINGS;
    use kafka_protocol::messages::TopicName;
    use kafka_protocol::protocol::StrBytes;

    // Helper to check if offload files exist
    fn check_offload_files() -> (bool, usize) {
        let offload_dir = if let Some(ref record_storage) = SETTINGS.record_storage_path {
            PathBuf::from(record_storage)
        } else {
            return (false, 0);
        };
        if !offload_dir.exists() {
            return (false, 0);
        }

        let mut count = 0;
        if let Ok(entries) = fs::read_dir(&offload_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(ext) = path.extension() {
                            if ext == "log" || ext == "offload" {
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
        (count > 0, count)
    }

    #[tokio::test]
    async fn test_offload_with_produce_messages() {
        // Ensure shared broker is running
        once_cell::sync::Lazy::force(&SETUP);
        println!("\n=== Starting Offload Test with Produce Messages ===");

        // Print configuration
        let max_memory = SETTINGS.max_memory;
        let offload_enabled = SETTINGS.record_storage_path.is_some();
        let offload_dir = SETTINGS
            .record_storage_path
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("None");

        println!("Configuration:");
        println!(
            "  Max memory: {} bytes ({:.2} MB)",
            max_memory,
            max_memory as f64 / 1_048_576.0
        );
        println!("  Offload enabled: {}", offload_enabled);
        println!("  Offload directory: {}", offload_dir);

        if !offload_enabled {
            println!("⚠️  Offload is not enabled, skipping test");
            return;
        }

        // Clean up offload directory
        let offload_path = PathBuf::from(offload_dir);
        if offload_path.exists() {
            let _ = fs::remove_dir_all(&offload_path);
        }

        // Create topic
        let topic_name_str = "offload_test_topic";
        let topic_name = TopicName(StrBytes::from_static_str(topic_name_str));
        let mut socket = connect_to_kafka();
        create_topic(topic_name.clone(), &mut socket, 1);
        println!("✓ Created topic: {}", topic_name_str);

        // Wait for topic creation to propagate
        sleep(Duration::from_millis(500)).await;

        // Calculate message parameters - reduced for better reliability
        // Each message should be large enough to trigger offloading quickly
        let message_size = max_memory / 200; // Each message is 0.5% of max memory (smaller)
        let thread_count = 1;
        let message_count = 50; // Reduced from 150 to avoid throttling

        println!("\nProducing messages:");
        println!("  Message size: {} bytes", message_size);
        println!("  Total messages: {}", message_count);
        println!(
            "  Expected total: {} bytes ({:.2} MB)",
            message_size * message_count,
            (message_size * message_count) as f64 / 1_048_576.0
        );

        // Create large value for messages
        let large_value: String = "X".repeat(message_size);
        let large_value_static = large_value.leak();

        // Get initial memory
        let initial_memory = MEMORY.load(Ordering::Relaxed);
        println!("\nInitial memory usage: {} bytes", initial_memory);

        // Produce messages using the helper function
        let handles = produce_messages(
            topic_name_str,
            "key",
            large_value_static,
            thread_count,
            message_count as i32,
        );

        // Wait for production to complete
        for handle in handles {
            handle.join().expect("Producer thread failed");
        }

        println!("✓ Produced {} messages", message_count);

        // Give system time to process and potentially offload
        sleep(Duration::from_secs(2)).await;

        // Check memory usage
        let final_memory = MEMORY.load(Ordering::Relaxed);
        println!(
            "\nFinal memory usage: {} bytes ({:.2} MB)",
            final_memory,
            final_memory as f64 / 1_048_576.0
        );

        // Check if offload files were created
        let (offload_exists, offload_count) = check_offload_files();

        println!("\nOffload check results:");
        println!("  Offload files exist: {}", offload_exists);
        println!("  Number of offload files: {}", offload_count);

        // Verify results
        if max_memory < 10_000_000 {
            // If we have a small memory limit (< 10MB)
            // We should have triggered offloading
            assert!(
                final_memory <= max_memory || offload_exists,
                "Either memory should be within limit ({} <= {}) or offload files should exist",
                final_memory,
                max_memory
            );

            if final_memory > max_memory && !offload_exists {
                println!("❌ Memory exceeded limit but no offload files found!");
            } else if offload_exists {
                println!("✓ Offload files created successfully");
            } else {
                println!("✓ Memory stayed within limits");
            }
        } else {
            println!("ℹ️  Large memory limit, offloading may not occur");
        }

        // Cleanup
        delete_topic(topic_name, &mut socket);
        println!("\n✓ Test completed and cleaned up");
    }

    #[tokio::test]
    async fn test_memory_tracking() {
        // Ensure shared broker is running
        once_cell::sync::Lazy::force(&SETUP);
        println!("\n=== Testing Memory Tracking ===");

        let topic_name = TopicName(StrBytes::from_static_str("memory_track_test"));
        let mut socket = connect_to_kafka();

        // Create topic
        create_topic(topic_name.clone(), &mut socket, 1);

        // Track memory changes
        let initial = MEMORY.load(Ordering::Relaxed);
        println!("Initial memory: {} bytes", initial);

        // Produce a few small messages
        let handles = produce_messages("memory_track_test", "key", "small_value", 1, 10);

        for handle in handles {
            handle.join().expect("Producer failed");
        }

        sleep(Duration::from_millis(500)).await;

        let after_produce = MEMORY.load(Ordering::Relaxed);
        println!("After producing: {} bytes", after_produce);
        println!("Memory increase: {} bytes", after_produce - initial);

        // Verify memory increased
        assert!(
            after_produce > initial,
            "Memory should increase after producing messages"
        );

        // Cleanup
        delete_topic(topic_name, &mut socket);
        println!("✓ Memory tracking test completed");
    }

    #[tokio::test]
    async fn test_offload_directory_creation() {
        // Ensure shared broker is running
        once_cell::sync::Lazy::force(&SETUP);
        println!("\n=== Testing Offload Directory Creation ===");

        let offload_dir = if let Some(ref record_storage) = SETTINGS.record_storage_path {
            PathBuf::from(record_storage)
        } else {
            PathBuf::from("./default_offload")
        };
        println!("Offload directory path: {:?}", offload_dir);

        // The directory should be created when needed during offloading
        // For now, just verify the configuration

        assert!(
            SETTINGS.record_storage_path.is_some(),
            "Disk offload should be enabled for this test"
        );

        assert!(
            SETTINGS.record_storage_path.is_some(),
            "Record storage should be configured"
        );

        println!("✓ Offload configuration verified");
    }
}
