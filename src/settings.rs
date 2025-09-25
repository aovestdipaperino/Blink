// Configuration settings management
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use once_cell::sync::OnceCell;
use serde::de::{self, Deserializer};
use serde::Deserialize;
use std::{
    error::Error,
    fs::File,
    io::{BufReader, Read},
};

const DEFAULT_RETENTION: &str = "5m";
const NUM_PARTITIONS_ENV: &str = "KAFKA_CFG_NUM_PARTITIONS";
const REST_PORT_ENV: &str = "REST_PORT";
const RETENTION_ENV: &str = "RETENTION";
const HOSTNAME_ENV: &str = "KAFKA_HOSTNAME";
const BROKER_PORTS_ENV: &str = "BROKER_PORTS";
const ENABLE_CONSUMER_GROUPS_ENV: &str = "ENABLE_CONSUMER_GROUPS";
const KAFKA_MEM_HEAP_ENV: &str = "KAFKA_MEM_HEAP";
const OBLITERATION_WARNING_INTERVAL_ENV: &str = "OBLITERATION_WARNING_INTERVAL_MINUTES";
const GCP_PROJECT_ID_ENV: &str = "GCP_PROJECT_ID";
const USE_LAST_ACCESSED_OFFSET_ENV: &str = "USE_LAST_ACCESSED_OFFSET";
const CHECK_FOR_SKIPPED_BATCHES_ENV: &str = "CHECK_FOR_SKIPPED_BATCHES";
const HEAP_MEMORY_FACTOR_ENV: &str = "HEAP_MEMORY_FACTOR";
const TRACE_ENV: &str = "TRACE";

/// Macro to parse environment variable and set field on settings struct
macro_rules! parse_env_var {
    ($settings:expr, $env_var:expr, $field:ident, $error_msg:expr) => {
        if let Ok(value) = std::env::var($env_var) {
            $settings.$field = value.parse().expect($error_msg);
        }
    };
    ($settings:expr, $env_var:expr, $field:ident) => {
        parse_env_var!(
            $settings,
            $env_var,
            $field,
            &format!("Unable to parse {}", stringify!($field))
        );
    };
}

#[derive(Deserialize)]
pub struct Settings {
    #[serde(
        deserialize_with = "deserialize_duration",
        default = "default_retention"
    )]
    pub retention: i64,
    #[serde(default = "default_rest_port")]
    pub rest_port: u16,
    #[serde(default = "default_broker_ports")]
    pub broker_ports: Vec<u16>,
    #[serde(default = "default_kafka_hostname")]
    pub kafka_hostname: String,
    #[serde(default = "default_kafka_cfg_num_partitions")]
    pub kafka_cfg_num_partitions: u8,
    #[serde(
        deserialize_with = "deserialize_memory",
        default = "default_max_memory"
    )]
    pub max_memory: usize,
    pub boot_topic: Option<String>,
    pub record_storage_path: Option<String>,
    #[serde(default = "default_false")]
    pub enable_consumer_groups: bool,
    /// Specifies the interval in seconds between background purge operations.
    /// The background purge task runs periodically to clean up expired records
    /// based on the retention setting. Lower values result in more frequent
    /// purging (higher CPU usage but more responsive), while higher values
    /// result in less frequent purging (lower CPU usage but less responsive).
    /// Default: 1 second if not specified.
    #[serde(default = "default_purge_delay")]
    pub purge_delay_seconds: u64,
    /// Minimum interval in minutes between obliteration warning events.
    /// This prevents spam when the queue is consistently congested.
    /// Obliteration events are always recorded, but warnings are throttled.
    /// Default: 5 minutes if not specified.
    #[serde(default = "default_obliteration_warning_interval")]
    pub obliteration_warning_interval_minutes: u64,
    #[serde(default = "default_false")]
    pub use_last_accessed_offset: bool,
    #[serde(default = "default_false")]
    pub check_for_skipped_batches: bool,
    #[serde(default = "default_heap_memory_factor")]
    pub heap_memory_factor: f64,
    #[serde(default = "default_plugin_paths")]
    pub plugin_paths: Vec<String>,
    #[serde(default = "default_trace")]
    pub trace: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiTrace {
    None,
    Request,
    Response,
    Both,
}

impl Settings {
    /// Returns true if GCP logging should be enabled based on the presence of GCP_PROJECT_ID env var
    pub fn gcp_logging_enabled(&self) -> bool {
        std::env::var(GCP_PROJECT_ID_ENV).is_ok()
    }

    /// Returns the GCP project ID from environment variable if set
    pub fn gcp_project_id(&self) -> Option<String> {
        std::env::var(GCP_PROJECT_ID_ENV).ok()
    }

    /// Returns what type of tracing should be performed for the given API.
    ///
    /// Returns:
    /// - ApiTrace::Both if the API name itself is in the trace list (e.g., "PRODUCE")
    /// - ApiTrace::Request if only the "APIRequest" variant is in the trace list (e.g., "PRODUCERequest")
    /// - ApiTrace::Response if only the "APIResponse" variant is in the trace list (e.g., "PRODUCEResponse")
    /// - ApiTrace::Both if both "APIRequest" and "APIResponse" variants are in the trace list
    /// - ApiTrace::None if no tracing is configured for this API
    pub fn should_trace_api(&self, api_name: &str) -> ApiTrace {
        let api_name_str = api_name.to_string();
        let request_variant = format!("{}Request", api_name);
        let response_variant = format!("{}Response", api_name);

        let has_api_name = self.trace.contains(&api_name_str);
        let has_request = self.trace.contains(&request_variant);
        let has_response = self.trace.contains(&response_variant);

        // If the API name itself is configured, trace both request and response
        if has_api_name {
            return ApiTrace::Both;
        }

        // Check specific request/response configurations
        match (has_request, has_response) {
            (true, true) => ApiTrace::Both,
            (true, false) => ApiTrace::Request,
            (false, true) => ApiTrace::Response,
            (false, false) => ApiTrace::None,
        }
    }
}

impl std::fmt::Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settings")
            .field("retention", &format_duration_ms(self.retention))
            .field("rest_port", &self.rest_port)
            .field("broker_ports", &self.broker_ports)
            .field("kafka_hostname", &self.kafka_hostname)
            .field("kafka_cfg_num_partitions", &self.kafka_cfg_num_partitions)
            .field("max_memory", &self.max_memory)
            .field("boot_topic", &self.boot_topic)
            .field("record_storage_path", &self.record_storage_path)
            .field("enable_consumer_groups", &self.enable_consumer_groups)
            .field("purge_delay_seconds", &self.purge_delay_seconds)
            .field(
                "obliteration_warning_interval_minutes",
                &self.obliteration_warning_interval_minutes,
            )
            .field("use_last_accessed_offset", &self.use_last_accessed_offset)
            .field("check_for_skipped_batches", &self.check_for_skipped_batches)
            .field("heap_memory_factor", &self.heap_memory_factor)
            .field("plugin_paths", &self.plugin_paths)
            .field("trace", &self.trace)
            .field("gcp_project_id", &self.gcp_project_id())
            .finish()
    }
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            retention: parse_duration_to_ms(DEFAULT_RETENTION).unwrap(),
            rest_port: 30004,
            broker_ports: vec![9094, 9092],
            kafka_hostname: "localhost".to_string(),
            kafka_cfg_num_partitions: 1,
            max_memory: parse_memory_size_binary("1024GB").unwrap(),
            boot_topic: None,
            record_storage_path: Some("./offload".to_string()),
            enable_consumer_groups: false,
            purge_delay_seconds: 1,
            obliteration_warning_interval_minutes: 5,
            use_last_accessed_offset: false,
            check_for_skipped_batches: false,
            heap_memory_factor: 1.9,
            plugin_paths: default_plugin_paths(),
            trace: vec![],
        }
    }
}

fn default_rest_port() -> u16 {
    30004
}

fn default_broker_ports() -> Vec<u16> {
    vec![9094, 9092]
}

fn default_kafka_hostname() -> String {
    "localhost".to_string()
}

fn default_kafka_cfg_num_partitions() -> u8 {
    1
}

fn default_retention() -> i64 {
    parse_duration_to_ms(DEFAULT_RETENTION).unwrap()
}

fn default_max_memory() -> usize {
    parse_memory_size_binary("1024GB").unwrap()
}

fn default_purge_delay() -> u64 {
    10
}

fn default_obliteration_warning_interval() -> u64 {
    5
}

fn default_false() -> bool {
    false
}

fn default_heap_memory_factor() -> f64 {
    1.9
}

fn default_plugin_paths() -> Vec<String> {
    Vec::new()
}

fn default_trace() -> Vec<String> {
    vec![]
}

fn parse_memory_size_binary(s: &str) -> Result<usize, String> {
    let s = s.trim();
    let units = [
        ("kb", 1 << 10),
        ("mb", 1 << 20),
        ("gb", 1 << 30),
        ("k", 1 << 10),
    ];

    for (unit, factor) in units.iter() {
        if s.to_lowercase().ends_with(unit) {
            let num_str = &s[..s.len() - unit.len()].trim();
            return num_str
                .parse::<f64>()
                .map(|n| (n * *factor as f64) as usize)
                .map_err(|e| format!("Invalid number: {e}"));
        }
    }

    s.parse::<usize>()
        .map_err(|e| format!("Invalid raw byte value: {e}"))
}

fn parse_kafka_memory_heap(s: &str, heap_memory_factor: f64) -> Result<usize, String> {
    let s = s.trim().to_lowercase();

    if s.ends_with('m') {
        let num_str = &s[..s.len() - 1];
        return num_str
            .parse::<f64>()
            .map(|n| (n * 1_048_576.0 * heap_memory_factor) as usize) // 1MB = 1024*1024 bytes
            .map_err(|e| format!("Invalid number: {e}"));
    }

    if s.ends_with('g') {
        let num_str = &s[..s.len() - 1];
        return num_str
            .parse::<f64>()
            .map(|n| (n * 1_073_741_824.0 * heap_memory_factor) as usize) // 1GB = 1024*1024*1024 bytes
            .map_err(|e| format!("Invalid number: {e}"));
    }

    // If no suffix, assume bytes
    s.parse::<usize>()
        .map_err(|e| format!("Invalid memory value: {e}"))
}

fn deserialize_memory<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_memory_size_binary(&s).map_err(de::Error::custom)
}

fn parse_duration_to_ms(s: &str) -> Result<i64, String> {
    let s = s.trim();

    if s.ends_with("ms") {
        let num_str = &s[..s.len() - 2].trim();
        return num_str
            .parse::<i64>()
            .map_err(|e| format!("Invalid number: {e}"));
    }

    if s.ends_with("s") {
        let num_str = &s[..s.len() - 1].trim();
        return num_str
            .parse::<i64>()
            .map(|n| n * 1000)
            .map_err(|e| format!("Invalid number: {e}"));
    }

    if s.ends_with("m") {
        let num_str = &s[..s.len() - 1].trim();
        return num_str
            .parse::<i64>()
            .map(|n| n * 60 * 1000)
            .map_err(|e| format!("Invalid number: {e}"));
    }

    s.parse::<i64>()
        .map_err(|e| format!("Invalid duration value: {e}"))
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_duration_to_ms(&s).map_err(de::Error::custom)
}

pub fn format_duration_ms(ms: i64) -> String {
    if ms % 60000 == 0 && ms >= 60000 {
        format!("{}m", ms / 60000)
    } else if ms % 1000 == 0 && ms >= 1000 {
        format!("{}s", ms / 1000)
    } else {
        format!("{}ms", ms)
    }
}

static SETTINGS_CELL: OnceCell<Settings> = OnceCell::new();

pub struct SettingsRef;

impl std::ops::Deref for SettingsRef {
    type Target = Settings;

    fn deref(&self) -> &Self::Target {
        SETTINGS_CELL
            .get()
            .expect("Settings not initialized. Call Settings::init() first.")
    }
}

pub static SETTINGS: SettingsRef = SettingsRef;

impl Settings {
    /// Initialize settings from the specified file path.
    /// This must be called once before accessing SETTINGS.
    ///
    /// # Environment Variable Override: KAFKA_MEM_HEAP
    ///
    /// If the `KAFKA_MEM_HEAP` environment variable is set, it will override
    /// the following settings:
    /// - `max_memory`: Set to the value specified in KAFKA_MEM_HEAP (supports 'm' for MB, 'g' for GB)
    /// - `record_storage_path`: Set to `/bitnami/kafka`
    ///
    /// # Examples
    ///
    /// ```bash
    /// # Set memory to 512 MB
    /// export KAFKA_MEM_HEAP=512m
    ///
    /// # Set memory to 2 GB
    /// export KAFKA_MEM_HEAP=2g
    /// ```
    pub async fn init(settings_file_path: &str) -> Result<(), Box<dyn Error>> {
        // Check if already initialized
        if SETTINGS_CELL.get().is_some() {
            return Ok(());
        }

        let settings = Self::load_from_path(settings_file_path).await?;
        SETTINGS_CELL
            .set(settings)
            .map_err(|_| "Settings already initialized")?;
        Ok(())
    }

    /// Initialize settings with an already loaded Settings instance
    pub fn init_with_settings(settings: Settings) -> Result<(), Box<dyn std::error::Error>> {
        // Check if already initialized
        if SETTINGS_CELL.get().is_some() {
            return Ok(());
        }

        SETTINGS_CELL
            .set(settings)
            .map_err(|_| "Settings already initialized")?;
        Ok(())
    }

    /// Check if settings have been initialized
    #[allow(dead_code)] // Used in tests
    pub fn is_initialized() -> bool {
        SETTINGS_CELL.get().is_some()
    }

    /// Load settings from a specified file path
    pub async fn load_from_path(settings_file_path: &str) -> Result<Self, Box<dyn Error>> {
        let file = File::open(settings_file_path)?;
        let mut buf_reader = BufReader::new(file);
        let mut contents = String::new();
        buf_reader.read_to_string(&mut contents)?;
        let mut settings: Settings =
            serde_yaml::from_str(&contents).expect("Unable to parse YAML file");

        parse_env_var!(
            settings,
            REST_PORT_ENV,
            rest_port,
            "Unable to parse REST port"
        );

        if let Ok(broker_ports) = std::env::var(BROKER_PORTS_ENV) {
            settings.broker_ports = broker_ports
                .split(',')
                .map(|port| port.trim().parse().expect("Unable to parse broker port"))
                .collect();
        }

        parse_env_var!(
            settings,
            NUM_PARTITIONS_ENV,
            kafka_cfg_num_partitions,
            "Unable to parse partitions"
        );

        if let Ok(retention) = std::env::var(RETENTION_ENV) {
            settings.retention =
                parse_duration_to_ms(&retention).expect("Unable to parse retention duration");
        }

        if let Ok(kafka_hostname) = std::env::var(HOSTNAME_ENV) {
            settings.kafka_hostname = kafka_hostname;
        }

        parse_env_var!(settings, ENABLE_CONSUMER_GROUPS_ENV, enable_consumer_groups);

        parse_env_var!(
            settings,
            HEAP_MEMORY_FACTOR_ENV,
            heap_memory_factor,
            "Unable to parse heap memory factor"
        );

        if let Ok(kafka_mem_heap) = std::env::var(KAFKA_MEM_HEAP_ENV) {
            settings.max_memory =
                parse_kafka_memory_heap(&kafka_mem_heap, settings.heap_memory_factor)
                    .expect("Unable to parse KAFKA_MEM_HEAP");
            settings.record_storage_path = Some("/bitnami/kafka".to_string());
        }

        parse_env_var!(
            settings,
            OBLITERATION_WARNING_INTERVAL_ENV,
            obliteration_warning_interval_minutes,
            "Unable to parse obliteration warning interval"
        );

        parse_env_var!(
            settings,
            USE_LAST_ACCESSED_OFFSET_ENV,
            use_last_accessed_offset
        );

        parse_env_var!(
            settings,
            CHECK_FOR_SKIPPED_BATCHES_ENV,
            check_for_skipped_batches
        );

        if let Ok(trace_apis) = std::env::var(TRACE_ENV) {
            settings.trace = trace_apis
                .split(',')
                .map(|api| api.trim().to_string())
                .filter(|api| !api.is_empty())
                .collect();
        }

        Ok(settings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Mutex;
    use tempfile::TempDir;

    // Mutex to prevent parallel execution of environment variable tests
    static ENV_TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_settings_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let settings_path = temp_dir.path().join("test_settings.yaml");

        let test_settings_content = r#"
retention: 45s
rest_port: 30006
broker_ports: [9094]
kafka_hostname: "test-host"
kafka_cfg_num_partitions: 2
max_memory: 256MB
record_storage_path: "./test_offload"
enable_consumer_groups: true
use_last_accessed_offset: false
check_for_skipped_batches: false
"#;

        fs::write(&settings_path, test_settings_content).unwrap();

        let settings =
            tokio_test::block_on(Settings::load_from_path(settings_path.to_str().unwrap()))
                .unwrap();
        assert_eq!(settings.retention, 45000);
    }

    #[test]
    fn test_kafka_mem_heap_megabytes() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        // Clean up any existing environment variable first
        std::env::remove_var(KAFKA_MEM_HEAP_ENV);

        let temp_dir = TempDir::new().unwrap();
        let settings_path = temp_dir.path().join("test_settings.yaml");

        let test_settings_content = r#"
retention: 1m
rest_port: 30004
broker_ports: [9092]
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 8
max_memory: 768MB
enable_consumer_groups: false
use_last_accessed_offset: false
check_for_skipped_batches: false
"#;

        fs::write(&settings_path, test_settings_content).unwrap();

        // Set KAFKA_MEM_HEAP environment variable
        std::env::set_var(KAFKA_MEM_HEAP_ENV, "512m");

        let settings =
            tokio_test::block_on(Settings::load_from_path(settings_path.to_str().unwrap()))
                .unwrap();

        // Clean up environment variable immediately after loading
        std::env::remove_var(KAFKA_MEM_HEAP_ENV);

        // Verify the settings were overridden correctly
        assert_eq!(
            settings.max_memory,
            (512.0 * 1_048_576.0 * settings.heap_memory_factor) as usize
        ); // 512MB * heap_memory_factor
        assert_eq!(
            settings.record_storage_path,
            Some("/bitnami/kafka".to_string())
        );
    }

    #[test]
    fn test_kafka_mem_heap_gigabytes() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        // Clean up any existing environment variable first
        std::env::remove_var(KAFKA_MEM_HEAP_ENV);

        let temp_dir = TempDir::new().unwrap();
        let settings_path = temp_dir.path().join("test_settings.yaml");

        let test_settings_content = r#"
retention: 1m
rest_port: 30004
broker_ports: [9092]
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 8
max_memory: 768MB
enable_consumer_groups: false
use_last_accessed_offset: false
check_for_skipped_batches: false
"#;

        fs::write(&settings_path, test_settings_content).unwrap();

        // Set KAFKA_MEM_HEAP environment variable
        std::env::set_var(KAFKA_MEM_HEAP_ENV, "2g");

        let settings =
            tokio_test::block_on(Settings::load_from_path(settings_path.to_str().unwrap()))
                .unwrap();

        // Clean up environment variable immediately after loading
        std::env::remove_var(KAFKA_MEM_HEAP_ENV);

        // Verify the settings were overridden correctly
        assert_eq!(
            settings.max_memory,
            (2.0 * 1_073_741_824.0 * settings.heap_memory_factor) as usize
        ); // 2GB * heap_memory_factor
        assert_eq!(
            settings.record_storage_path,
            Some("/bitnami/kafka".to_string())
        );
    }

    #[test]
    fn test_parse_kafka_memory_heap() {
        // Test megabytes (512MB * 1.9 = 1,020,054,732, 100MB * 1.9 = 199,229,440)
        assert_eq!(
            parse_kafka_memory_heap("512m", 1.9).unwrap(),
            (512.0 * 1_048_576.0 * 1.9) as usize
        );
        assert_eq!(
            parse_kafka_memory_heap("100M", 1.9).unwrap(),
            (100.0 * 1_048_576.0 * 1.9) as usize
        );

        // Test gigabytes (1GB * 1.9 = 2,040,109,465, 2GB * 1.9 = 4,080,218,931)
        assert_eq!(
            parse_kafka_memory_heap("1g", 1.9).unwrap(),
            (1.0 * 1_073_741_824.0 * 1.9) as usize
        );
        assert_eq!(
            parse_kafka_memory_heap("2G", 1.9).unwrap(),
            (2.0 * 1_073_741_824.0 * 1.9) as usize
        );

        // Test decimal values (1.5GB * 1.9 = 3,060,164,198, 512.5MB * 1.9 = 1,020,578,816)
        assert_eq!(
            parse_kafka_memory_heap("1.5g", 1.9).unwrap(),
            (1.5 * 1_073_741_824.0 * 1.9) as usize
        );
        assert_eq!(
            parse_kafka_memory_heap("512.5m", 1.9).unwrap(),
            (512.5 * 1_048_576.0 * 1.9) as usize
        );

        // Test raw bytes (no suffix)
        assert_eq!(parse_kafka_memory_heap("1024", 1.9).unwrap(), 1024);

        // Test error cases
        assert!(parse_kafka_memory_heap("invalid", 1.9).is_err());
        assert!(parse_kafka_memory_heap("", 1.9).is_err());
    }

    #[test]
    fn test_kafka_mem_heap_not_set() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        // Clean up any existing environment variable first
        std::env::remove_var(KAFKA_MEM_HEAP_ENV);

        let temp_dir = TempDir::new().unwrap();
        let settings_path = temp_dir.path().join("test_settings.yaml");

        let test_settings_content = r#"
retention: 1m
rest_port: 30004
broker_ports: [9092]
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 8
max_memory: 768MB
enable_consumer_groups: false
use_last_accessed_offset: false
check_for_skipped_batches: false
"#;

        fs::write(&settings_path, test_settings_content).unwrap();

        let settings =
            tokio_test::block_on(Settings::load_from_path(settings_path.to_str().unwrap()))
                .unwrap();

        // Verify the original settings from YAML are preserved when KAFKA_MEM_HEAP is not set
        assert_eq!(settings.max_memory, 805_306_368); // 768 * 1024 * 1024
        assert_eq!(settings.record_storage_path, None);
    }

    #[test]
    fn test_broker_ports_functionality() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        // Clean up any existing environment variable first
        std::env::remove_var(BROKER_PORTS_ENV);

        let temp_dir = TempDir::new().unwrap();
        let settings_path = temp_dir.path().join("test_settings.yaml");

        // Test with broker_ports specified
        let test_settings_content = r#"
retention: 1m
rest_port: 30004
broker_ports: [9092, 9094, 9096]
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 8
max_memory: 768MB
enable_consumer_groups: false
use_last_accessed_offset: false
check_for_skipped_batches: false
"#;

        fs::write(&settings_path, test_settings_content).unwrap();

        let settings =
            tokio_test::block_on(Settings::load_from_path(settings_path.to_str().unwrap()))
                .unwrap();

        // Verify broker_ports are loaded correctly
        assert_eq!(settings.broker_ports, vec![9092, 9094, 9096]);
    }

    #[test]
    fn test_broker_ports_default_values() {
        // Test default settings
        let default_settings = Settings::default();
        assert_eq!(default_settings.broker_ports, vec![9094, 9092]);
    }

    #[test]
    fn test_default_retention_value() {
        // Test that default retention is 5m = 300000ms
        let default_settings = Settings::default();
        assert_eq!(default_settings.retention, 300000);

        // Test that the constant parses correctly
        assert_eq!(parse_duration_to_ms(DEFAULT_RETENTION).unwrap(), 300000);
    }

    #[test]
    fn test_broker_ports_env_override() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        // Clean up any existing environment variable first
        std::env::remove_var(BROKER_PORTS_ENV);

        let temp_dir = TempDir::new().unwrap();
        let settings_path = temp_dir.path().join("test_settings.yaml");

        let test_settings_content = r#"
retention: 1m
rest_port: 30004
broker_ports: [9092]
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 8
max_memory: 768MB
enable_consumer_groups: false
use_last_accessed_offset: false
check_for_skipped_batches: false
"#;

        fs::write(&settings_path, test_settings_content).unwrap();

        // Set BROKER_PORTS environment variable
        std::env::set_var(BROKER_PORTS_ENV, "9093,9095,9097");

        let settings =
            tokio_test::block_on(Settings::load_from_path(settings_path.to_str().unwrap()))
                .unwrap();

        // Clean up environment variable immediately after loading
        std::env::remove_var(BROKER_PORTS_ENV);

        // Verify the settings were overridden correctly
        assert_eq!(settings.broker_ports, vec![9093, 9095, 9097]);
    }

    #[test]
    fn test_gcp_project_id_configuration() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let temp_dir = TempDir::new().unwrap();
        let settings_path = temp_dir.path().join("test_settings.yaml");

        // Test with gcp_project_id specified
        let test_settings_content = r#"
retention: 1m
rest_port: 30004
broker_ports: [9092]
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 8
max_memory: 768MB
enable_consumer_groups: false
use_last_accessed_offset: false
check_for_skipped_batches: false
"#;

        fs::write(&settings_path, test_settings_content).unwrap();

        let settings =
            tokio_test::block_on(Settings::load_from_path(settings_path.to_str().unwrap()))
                .unwrap();

        // Set environment variable for this test
        std::env::set_var(GCP_PROJECT_ID_ENV, "test-project-123");

        // Verify gcp_project_id is read from environment
        assert_eq!(
            settings.gcp_project_id(),
            Some("test-project-123".to_string())
        );

        // Clean up
        std::env::remove_var(GCP_PROJECT_ID_ENV);
    }

    #[test]
    fn test_gcp_project_id_none_by_default() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        // Ensure environment variable is not set before test
        std::env::remove_var(GCP_PROJECT_ID_ENV);

        let temp_dir = TempDir::new().unwrap();
        let settings_path = temp_dir.path().join("test_settings.yaml");

        // Test without gcp_project_id specified
        let test_settings_content = r#"
retention: 1m
rest_port: 30004
broker_ports: [9092]
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 8
max_memory: 768MB
enable_consumer_groups: false
use_last_accessed_offset: false
check_for_skipped_batches: false
"#;

        fs::write(&settings_path, test_settings_content).unwrap();

        let settings =
            tokio_test::block_on(Settings::load_from_path(settings_path.to_str().unwrap()))
                .unwrap();

        // Verify gcp_project_id is None when env var not set
        assert_eq!(settings.gcp_project_id(), None);

        // Test default settings as well
        let default_settings = Settings::default();
        assert_eq!(default_settings.gcp_project_id(), None);
    }

    #[test]
    fn test_gcp_logging_enabled_with_env_var() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        // Clean start
        std::env::remove_var(GCP_PROJECT_ID_ENV);

        // Test with GCP_PROJECT_ID environment variable set
        std::env::set_var(GCP_PROJECT_ID_ENV, "test-project");
        let settings = Settings::default();
        assert!(settings.gcp_logging_enabled());
        assert_eq!(settings.gcp_project_id(), Some("test-project".to_string()));

        // Clean up
        std::env::remove_var(GCP_PROJECT_ID_ENV);
    }

    #[test]
    fn test_gcp_logging_disabled_without_env_var() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        // Ensure clean environment
        std::env::remove_var(GCP_PROJECT_ID_ENV);

        let settings = Settings::default();
        assert!(!settings.gcp_logging_enabled());
        assert_eq!(settings.gcp_project_id(), None);
    }

    #[test]
    fn test_gcp_logging_enabled_with_empty_env_var() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        // Clean start
        std::env::remove_var(GCP_PROJECT_ID_ENV);

        // Test with empty project id (empty string should still enable logging)
        std::env::set_var(GCP_PROJECT_ID_ENV, "");
        let settings = Settings::default();
        assert!(settings.gcp_logging_enabled());
        assert_eq!(settings.gcp_project_id(), Some("".to_string()));

        // Clean up
        std::env::remove_var(GCP_PROJECT_ID_ENV);
    }

    #[test]
    fn test_heap_memory_factor_env_override() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        // Clean up any existing environment variables first
        std::env::remove_var(HEAP_MEMORY_FACTOR_ENV);
        std::env::remove_var(KAFKA_MEM_HEAP_ENV);

        let temp_dir = TempDir::new().unwrap();
        let settings_path = temp_dir.path().join("test_settings.yaml");

        let test_settings_content = r#"
retention: 1m
rest_port: 30004
broker_ports: [9092]
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 1
max_memory: 512MB
enable_consumer_groups: false
use_last_accessed_offset: false
check_for_skipped_batches: false
heap_memory_factor: 1.9
"#;

        fs::write(&settings_path, test_settings_content).unwrap();

        // Test with HEAP_MEMORY_FACTOR environment variable override
        std::env::set_var(HEAP_MEMORY_FACTOR_ENV, "2.5");

        let settings =
            tokio_test::block_on(Settings::load_from_path(settings_path.to_str().unwrap()))
                .unwrap();

        // Clean up environment variable immediately after loading
        std::env::remove_var(HEAP_MEMORY_FACTOR_ENV);

        // Verify the heap_memory_factor was overridden by environment variable
        assert_eq!(settings.heap_memory_factor, 2.5);

        // Also test that HEAP_MEMORY_FACTOR uses the overridden factor
        std::env::set_var(KAFKA_MEM_HEAP_ENV, "1g");
        std::env::set_var(HEAP_MEMORY_FACTOR_ENV, "3.0");

        let settings_with_heap =
            tokio_test::block_on(Settings::load_from_path(settings_path.to_str().unwrap()))
                .unwrap();

        // Clean up environment variables
        std::env::remove_var(KAFKA_MEM_HEAP_ENV);
        std::env::remove_var(HEAP_MEMORY_FACTOR_ENV);

        // Verify memory calculation uses the custom factor (1GB * 3.0)
        assert_eq!(settings_with_heap.heap_memory_factor, 3.0);
        assert_eq!(
            settings_with_heap.max_memory,
            (1.0 * 1_073_741_824.0 * 3.0) as usize
        );
    }

    #[test]
    fn test_default_heap_memory_factor() {
        let settings = Settings::default();
        assert_eq!(settings.heap_memory_factor, 1.9);

        // Also test that the default function works
        assert_eq!(default_heap_memory_factor(), 1.9);
    }

    #[test]
    fn test_should_trace_api() {
        // Test default settings (empty trace list)
        let default_settings = Settings::default();
        assert_eq!(default_settings.trace, Vec::<String>::new());
        assert_eq!(default_settings.should_trace_api("PRODUCE"), ApiTrace::None);
        assert_eq!(default_settings.should_trace_api("FETCH"), ApiTrace::None);
        assert_eq!(
            default_settings.should_trace_api("METADATA"),
            ApiTrace::None
        );

        // Test with specific APIs configured for tracing
        let mut settings_with_trace = Settings::default();
        settings_with_trace.trace = vec!["PRODUCE".to_string(), "FETCH".to_string()];

        assert_eq!(
            settings_with_trace.should_trace_api("PRODUCE"),
            ApiTrace::Both
        );
        assert_eq!(
            settings_with_trace.should_trace_api("FETCH"),
            ApiTrace::Both
        );
        assert_eq!(
            settings_with_trace.should_trace_api("METADATA"),
            ApiTrace::None
        );
        assert_eq!(
            settings_with_trace.should_trace_api("ApiVersions"),
            ApiTrace::None
        );

        // Test case sensitivity
        assert_eq!(
            settings_with_trace.should_trace_api("produce"),
            ApiTrace::None
        ); // lowercase should not match
        assert_eq!(
            settings_with_trace.should_trace_api("Produce"),
            ApiTrace::None
        ); // mixed case should not match

        // Test request/response specific tracing
        let mut request_only_settings = Settings::default();
        request_only_settings.trace = vec!["PRODUCERequest".to_string()];
        assert_eq!(
            request_only_settings.should_trace_api("PRODUCE"),
            ApiTrace::Request
        );

        let mut response_only_settings = Settings::default();
        response_only_settings.trace = vec!["PRODUCEResponse".to_string()];
        assert_eq!(
            response_only_settings.should_trace_api("PRODUCE"),
            ApiTrace::Response
        );

        // Test conceptual merging - ProduceRequest + ProduceResponse should equal Both
        let mut merged_settings = Settings::default();
        merged_settings.trace = vec!["PRODUCERequest".to_string(), "PRODUCEResponse".to_string()];
        assert_eq!(merged_settings.should_trace_api("PRODUCE"), ApiTrace::Both);
    }

    #[test]
    fn test_trace_env_variable() {
        let _guard = ENV_TEST_MUTEX.lock().unwrap();

        let temp_file_content = r#"
record_storage_path: "./test_records"
boot_topic: "test_topic"
kafka_cfg_num_partitions: 1
use_last_accessed_offset: false
trace: []
"#;
        let temp_file = std::env::temp_dir().join("test_trace_env_settings.yaml");
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Test with comma-separated APIs
        std::env::set_var(TRACE_ENV, "PRODUCE,FETCH,METADATA");
        std::fs::write(&temp_file, temp_file_content).unwrap();

        let settings = rt
            .block_on(Settings::load_from_path(temp_file.to_str().unwrap()))
            .unwrap();

        assert_eq!(settings.trace, vec!["PRODUCE", "FETCH", "METADATA"]);
        assert_eq!(settings.should_trace_api("PRODUCE"), ApiTrace::Both);
        assert_eq!(settings.should_trace_api("FETCH"), ApiTrace::Both);
        assert_eq!(settings.should_trace_api("METADATA"), ApiTrace::Both);
        assert_eq!(settings.should_trace_api("ApiVersions"), ApiTrace::None);

        // Test with spaces around commas
        std::env::set_var(TRACE_ENV, " PRODUCE , FETCH , METADATA ");
        std::fs::write(&temp_file, temp_file_content).unwrap();

        let settings = rt
            .block_on(Settings::load_from_path(temp_file.to_str().unwrap()))
            .unwrap();

        assert_eq!(settings.trace, vec!["PRODUCE", "FETCH", "METADATA"]);

        // Test with empty string
        std::env::set_var(TRACE_ENV, "");
        std::fs::write(&temp_file, temp_file_content).unwrap();

        let settings = rt
            .block_on(Settings::load_from_path(temp_file.to_str().unwrap()))
            .unwrap();

        assert_eq!(settings.trace, Vec::<String>::new());

        // Test with single API
        std::env::set_var(TRACE_ENV, "PRODUCE");
        std::fs::write(&temp_file, temp_file_content).unwrap();

        let settings = rt
            .block_on(Settings::load_from_path(temp_file.to_str().unwrap()))
            .unwrap();

        assert_eq!(settings.trace, vec!["PRODUCE"]);
        assert_eq!(settings.should_trace_api("PRODUCE"), ApiTrace::Both);
        assert_eq!(settings.should_trace_api("FETCH"), ApiTrace::None);

        // Test environment variable overrides YAML setting
        std::env::set_var(TRACE_ENV, "FETCH");
        let yaml_with_trace = r#"
record_storage_path: "./test_records"
boot_topic: "test_topic"
kafka_cfg_num_partitions: 1
use_last_accessed_offset: false
trace: ["PRODUCE", "METADATA"]
"#;
        std::fs::write(&temp_file, yaml_with_trace).unwrap();

        let settings = rt
            .block_on(Settings::load_from_path(temp_file.to_str().unwrap()))
            .unwrap();

        // Environment variable should override YAML
        assert_eq!(settings.trace, vec!["FETCH"]);
        assert_eq!(settings.should_trace_api("PRODUCE"), ApiTrace::None);
        assert_eq!(settings.should_trace_api("FETCH"), ApiTrace::Both);
        assert_eq!(settings.should_trace_api("METADATA"), ApiTrace::None);

        // Clean up
        std::fs::remove_file(&temp_file).unwrap();
        std::env::remove_var(TRACE_ENV);
    }
}
