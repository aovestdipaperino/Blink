// Google Cloud Platform logging integration tests
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use blink::settings::Settings;
use std::fs;
use std::sync::Mutex;
use tempfile::TempDir;

// Mutex to prevent parallel execution of environment variable tests
static ENV_TEST_MUTEX: Mutex<()> = Mutex::new(());

#[tokio::test]
async fn test_gcp_logging_disabled_by_default() {
    let _lock = ENV_TEST_MUTEX.lock().unwrap();
    // Ensure GCP_PROJECT_ID environment variable is not set
    std::env::remove_var("GCP_PROJECT_ID");

    let temp_dir = TempDir::new().unwrap();
    let settings_path = temp_dir.path().join("test_settings.yaml");

    let test_settings_content = r#"
retention: 1m
rest_port: 30004
broker_ports: [9092]
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 8
max_memory: 768MB

"#;

    fs::write(&settings_path, test_settings_content).unwrap();

    let settings = Settings::load_from_path(settings_path.to_str().unwrap())
        .await
        .unwrap();

    // GCP logging should be disabled when environment variable is not set
    assert!(!settings.gcp_logging_enabled());
    assert_eq!(settings.gcp_project_id(), None);
}

#[tokio::test]
async fn test_gcp_logging_enabled_with_project_id() {
    let _lock = ENV_TEST_MUTEX.lock().unwrap();
    // Set GCP_PROJECT_ID environment variable
    std::env::set_var("GCP_PROJECT_ID", "my-production-project");

    let temp_dir = TempDir::new().unwrap();
    let settings_path = temp_dir.path().join("test_settings.yaml");

    let test_settings_content = r#"
retention: 1m
rest_port: 30004
broker_ports: [9092]
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 8
max_memory: 768MB

"#;

    fs::write(&settings_path, test_settings_content).unwrap();

    let settings = Settings::load_from_path(settings_path.to_str().unwrap())
        .await
        .unwrap();

    // GCP logging should be enabled when environment variable is set
    assert!(settings.gcp_logging_enabled());
    assert_eq!(
        settings.gcp_project_id(),
        Some("my-production-project".to_string())
    );

    // Clean up
    std::env::remove_var("GCP_PROJECT_ID");
}

#[tokio::test]
async fn test_gcp_logging_behavior_migration_scenario() {
    let _lock = ENV_TEST_MUTEX.lock().unwrap();
    // Set GCP_PROJECT_ID environment variable
    std::env::set_var("GCP_PROJECT_ID", "analytics-production-env");

    let temp_dir = TempDir::new().unwrap();
    let settings_path = temp_dir.path().join("test_settings.yaml");

    let test_settings_content = r#"
retention: 5m
rest_port: 30006
broker_ports: [9094, 9092]
kafka_hostname: "kafka-server"
kafka_cfg_num_partitions: 16
max_memory: 2GB

record_storage_path: "/tmp/kafka-offload"
enable_consumer_groups: true
boot_topic: "events"
purge_delay_seconds: 10
obliteration_warning_interval_minutes: 15
"#;

    fs::write(&settings_path, test_settings_content).unwrap();

    let settings = Settings::load_from_path(settings_path.to_str().unwrap())
        .await
        .unwrap();

    // Verify all settings are loaded correctly
    assert_eq!(settings.retention, 300000); // 5 minutes in ms
    assert_eq!(settings.rest_port, 30006);
    assert_eq!(settings.broker_ports, vec![9094, 9092]);
    assert_eq!(settings.kafka_hostname, "kafka-server");
    assert_eq!(settings.kafka_cfg_num_partitions, 16);

    assert_eq!(
        settings.record_storage_path,
        Some("/tmp/kafka-offload".to_string())
    );
    assert!(settings.enable_consumer_groups);
    assert_eq!(settings.boot_topic, Some("events".to_string()));
    assert_eq!(settings.purge_delay_seconds, 10);
    assert_eq!(settings.obliteration_warning_interval_minutes, 15);

    // Most importantly, GCP logging should be enabled due to environment variable
    assert!(settings.gcp_logging_enabled());
    assert_eq!(
        settings.gcp_project_id(),
        Some("analytics-production-env".to_string())
    );

    // Clean up
    std::env::remove_var("GCP_PROJECT_ID");
}

#[tokio::test]
async fn test_empty_project_id_still_enables_logging() {
    let _lock = ENV_TEST_MUTEX.lock().unwrap();
    // Edge case: empty string project ID should still enable GCP logging
    // (though it would likely fail at runtime)
    std::env::set_var("GCP_PROJECT_ID", "");

    let temp_dir = TempDir::new().unwrap();
    let settings_path = temp_dir.path().join("test_settings.yaml");

    let test_settings_content = r#"
retention: 1m
rest_port: 30004
broker_ports: [9092]
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 8
max_memory: 768MB

"#;

    fs::write(&settings_path, test_settings_content).unwrap();

    let settings = Settings::load_from_path(settings_path.to_str().unwrap())
        .await
        .unwrap();

    // Even empty string should enable GCP logging (presence check, not content check)
    assert!(settings.gcp_logging_enabled());
    assert_eq!(settings.gcp_project_id(), Some("".to_string()));

    // Clean up
    std::env::remove_var("GCP_PROJECT_ID");
}

#[test]
fn test_settings_debug_output_without_enable_gcp_logging() {
    let _lock = ENV_TEST_MUTEX.lock().unwrap();
    // Set environment variable for this test
    std::env::set_var("GCP_PROJECT_ID", "debug-test-project");

    // Verify that the Debug implementation works correctly without enable_gcp_logging field
    let settings = Settings::default();

    let debug_output = format!("{:?}", settings);

    // Should contain gcp_project_id but not enable_gcp_logging
    assert!(debug_output.contains("gcp_project_id"));
    assert!(debug_output.contains("debug-test-project"));
    assert!(!debug_output.contains("enable_gcp_logging"));

    // Clean up
    std::env::remove_var("GCP_PROJECT_ID");
}
