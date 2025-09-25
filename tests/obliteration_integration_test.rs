// Integration test for obliteration functionality
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use blink::settings::Settings;
use tempfile::TempDir;

/// Test basic obliteration event functionality through direct module usage
#[tokio::test]
async fn test_obliteration_event_module() {
    use blink::kafka::counters::ObliterationEvent;
    use uuid::Uuid;

    // Test event creation
    let topic_id = Uuid::new_v4();
    let event = ObliterationEvent::new(topic_id, 0, 5, 20);

    assert_eq!(event.topic_id, topic_id);
    assert_eq!(event.partition, 0);
    assert_eq!(event.obliterated_count, 5);
    assert_eq!(event.total_purged_count, 20);
    assert_eq!(event.obliteration_rate(), 25.0);

    println!("✅ Obliteration event creation test passed");
}

/// Test environment variable configuration
#[tokio::test]
async fn test_obliteration_env_var_config() {
    // Set environment variable
    std::env::set_var("OBLITERATION_WARNING_INTERVAL_MINUTES", "10");

    let temp_dir = TempDir::new().unwrap();
    let settings_path = temp_dir.path().join("test_env_settings.yaml");

    let test_settings_content = r#"
retention: 1000ms
rest_port: 30009
broker_ports: [9097]
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 1
max_memory: 1MB
purge_delay_seconds: 1
"#;

    std::fs::write(&settings_path, test_settings_content).unwrap();

    // Load settings and verify environment variable was applied
    let settings = Settings::load_from_path(settings_path.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(settings.obliteration_warning_interval_minutes, 10);

    println!("✅ Environment variable configuration test passed");

    // Clean up environment variable
    std::env::remove_var("OBLITERATION_WARNING_INTERVAL_MINUTES");
}
