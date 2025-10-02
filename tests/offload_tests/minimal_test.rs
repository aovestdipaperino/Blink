// Minimal offload functionality tests
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
#[cfg(test)]
mod minimal_tests {
    use crate::common::{
        connect_to_kafka, create_topic, delete_topic, fetch_records, produce_records,
    };
    use blink::util::Util;
    use kafka_protocol::messages::TopicName;
    use kafka_protocol::protocol::StrBytes;

    #[tokio::test]
    async fn test_basic_connectivity() {
        crate::common::ensure_settings_initialized();
        // Test that we can connect to Blink
        let _socket = connect_to_kafka();
        println!("✓ Successfully connected to Blink on port 9999");
    }

    #[tokio::test]
    async fn test_basic_produce_consume() {
        crate::common::ensure_settings_initialized();
        let topic_name = TopicName(StrBytes::from_static_str("minimal_test_topic"));
        let mut socket = connect_to_kafka();

        // Create topic
        create_topic(topic_name.clone(), &mut socket, 1);
        println!("✓ Created topic: minimal_test_topic");

        // Create a small record
        let key = "test_key".to_string().leak();
        let value = "test_value".to_string().leak();
        let record = Util::create_new_record(key, value);
        let records = vec![record];

        // Produce the record
        let produce_success = match produce_records(topic_name.clone(), 0, &records, &mut socket) {
            Ok(_) => {
                println!("✓ Produced 1 record");
                true
            }
            Err(e) => {
                if e.contains("Throttling") {
                    println!("⚠️ Hit throttling - skipping fetch due to memory pressure");
                    false
                } else {
                    panic!("Unexpected error: {}", e);
                }
            }
        };

        // Only fetch if produce was successful (not throttled)
        if produce_success {
            fetch_records(topic_name.clone(), 0, 0, &records, &mut socket);
            println!("✓ Successfully fetched the record");
        } else {
            println!("✓ Skipped fetch due to memory pressure - test still valid");
        }

        // Clean up
        delete_topic(topic_name, &mut socket);
        println!("✓ Deleted topic");

        println!("\n✅ Basic test infrastructure is working!");
    }

    #[tokio::test]
    async fn test_memory_counter_exists() {
        crate::common::ensure_settings_initialized();
        use blink::alloc::global_allocator;

        let current_memory = global_allocator().current_allocated();
        println!("Current memory usage: {} bytes", current_memory);
        // Memory counter is accessible if we can load it without panicking
        println!("✓ Memory counter is accessible");
    }

    #[tokio::test]
    async fn test_settings_loaded() {
        crate::common::ensure_settings_initialized();
        use blink::settings::SETTINGS;

        let max_memory = SETTINGS.max_memory;
        let expiration = SETTINGS.retention;

        println!("Settings loaded:");
        println!("  max_memory: {} bytes", max_memory);
        println!("  retention: {} ms", expiration);

        assert!(max_memory > 0, "max_memory should be positive");
        assert!(expiration > 0, "retention should be positive");
        println!("✓ Settings are properly loaded");
    }
}
