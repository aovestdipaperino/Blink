# Test Configuration Files

This directory contains all test-specific settings files for the Blink Kafka-compatible message broker. These files are used to configure different test scenarios without modifying the main `settings.yaml` file.

## File Organization

### `test_settings.yaml` - Default Test Configuration
**Purpose**: General-purpose test settings optimized for offload testing and memory pressure scenarios.

**Key Settings**:
- **Memory Limit**: 1MB (very low to trigger offload behavior)
- **Message Expiration**: 5 seconds (fast cleanup for tests)
- **Consumer Groups**: Disabled
- **Disk Offload**: Enabled
- **Offload Directory**: `./blink_offload`
- **Partitions**: 1 (minimal setup)

**Used By**:
- Offload tests (`tests/offload_tests/`)
- Working offload tests (`tests/working_offload_test.rs`)
- General integration tests (`tests/tests.rs`)
- Default test initialization

### `test_integration_settings.yaml` - Integration Test Configuration
**Purpose**: Settings optimized for consumer group testing and API integration scenarios.

**Key Settings**:
- **Memory Limit**: 512KB (extremely low for aggressive testing)
- **Message Expiration**: 5 minutes (longer for complex test scenarios)
- **Consumer Groups**: Enabled ⭐
- **Disk Offload**: Disabled (focus on in-memory operations)
- **Offload Directory**: `./test_offload`
- **Partitions**: 1 (simple setup)

**Used By**:
- Consumer group tests (`tests/consumer_group.rs`)
- RdKafka integration tests (`tests/rdkafka_integration_test.rs`)
- API coordination tests

### `test_stress_settings.yaml` - Stress Test Configuration
**Purpose**: High-performance settings for stress testing and throughput scenarios.

**Key Settings**:
- **Memory Limit**: 8MB (higher capacity for stress testing)
- **Message Expiration**: 5 minutes (longer retention)
- **Consumer Groups**: Disabled
- **Disk Offload**: Enabled
- **Offload Directory**: `./stress_offload`
- **Partitions**: 16 (high concurrency)

**Used By**:
- Stress tests (`tests/stress_test.rs`)
- Performance benchmarks
- High-throughput scenarios

### `test_throughput_settings.yaml` - Throughput Test Configuration
**Purpose**: Optimized settings for balanced throughput testing with producer/consumer load balancing.

**Key Settings**:
- **Memory Limit**: 16MB (high capacity for throughput testing)
- **Message Expiration**: 10 minutes (longer retention for sustained testing)
- **Consumer Groups**: Disabled
- **Disk Offload**: Disabled (pure in-memory for maximum performance)
- **Partitions**: 8 (optimized for 4 producers + 4 consumers)
- **Flush Settings**: Optimized for throughput (10K messages or 1s intervals)

**Used By**:
- Throughput tests (`tests/throughput_test.rs`)
- Kitt-style balanced load testing
- 10K msg/sec performance validation

## Usage Patterns

### Command Line Usage
```bash
# Use specific test configuration
blink --settings tests/configs/test_settings.yaml
blink --settings tests/configs/test_integration_settings.yaml
blink --settings tests/configs/test_stress_settings.yaml
blink --settings tests/configs/test_throughput_settings.yaml
```

### In Test Code
```rust
// Use default test settings (test_settings.yaml)
crate::common::ensure_settings_initialized();

// Use specific test settings
crate::common::ensure_settings_initialized_with_file(
    "tests/configs/test_integration_settings.yaml"
);

// Start server with specific settings
crate::common::start_blink_with_settings(
    "tests/configs/test_stress_settings.yaml"
).await;

// Start server for throughput testing
crate::common::start_blink_with_settings(
    "tests/configs/test_throughput_settings.yaml"
).await;
```

## Configuration Guidelines

### When to Use Each File

| Test Type | Configuration File | Reason |
|-----------|-------------------|---------|
| Offload Tests | `test_settings.yaml` | Low memory triggers offload behavior |
| Consumer Group Tests | `test_integration_settings.yaml` | Consumer groups enabled |
| Memory Tests | `test_settings.yaml` | 1MB limit for predictable behavior |
| Performance Tests | `test_stress_settings.yaml` | Higher capacity + more partitions |
| Throughput Tests | `test_throughput_settings.yaml` | Balanced producer/consumer testing |
| API Tests | `test_integration_settings.yaml` | Longer timeouts for complex flows |
| General Tests | `test_settings.yaml` | Default balanced configuration |

### Environment Variable Overrides
All settings files support environment variable overrides:

```bash
# Override specific settings
KAFKA_HOSTNAME=localhost blink --settings tests/configs/test_settings.yaml
REST_PORT=30005 blink --settings tests/configs/test_integration_settings.yaml
RETENTION=10s blink --settings tests/configs/test_stress_settings.yaml
REST_PORT=30006 blink --settings tests/configs/test_throughput_settings.yaml
```

Available environment variables:
- `KAFKA_HOSTNAME` - Override broker hostname
- `REST_PORT` - Override REST API port
- `BROKER_PORT` - Override Kafka protocol port
- `RETENTION` - Override message expiration time
- `KAFKA_CFG_NUM_PARTITIONS` - Override default partition count
- `ENABLE_CONSUMER_GROUPS` - Override consumer group setting

## Best Practices

### 1. File Isolation
- Each test scenario uses its own dedicated settings file
- No test should modify or overwrite these configuration files
- Temporary configurations should use `tempfile` crate in tests

### 2. Predictable Settings
- Settings are designed to be deterministic for consistent test results
- Memory limits are intentionally low to trigger edge cases
- Timeouts are balanced between test speed and reliability

### 3. Directory Separation
- Each configuration uses different offload directories to avoid conflicts
- Cleanup routines should target the appropriate directories

### 4. Documentation
- Always document why specific settings values are chosen
- Include comments in YAML files for complex configurations

## Adding New Test Configurations

When adding new test configuration files:

1. **Create descriptive filename**: `test_[scenario]_settings.yaml`
2. **Document purpose**: Add entry to this README
3. **Use appropriate defaults**: Base on existing configurations
4. **Update test code**: Use `ensure_settings_initialized_with_file()`
5. **Test isolation**: Ensure unique offload directories and ports if needed

### Example New Configuration
```yaml
# test_custom_settings.yaml - Custom scenario configuration
retention: "30s"  # 30 seconds
rest_port: 30004
broker_port: 9092
kafka_hostname: "localhost"
kafka_cfg_num_partitions: 4   # Multiple partitions
max_memory: "2MB"             # Medium memory limit
record_storage_path: "./custom_offload"  # Unique directory
enable_consumer_groups: false
```

## Migration Notes

This directory structure replaces the previous approach where tests would:
- Copy `test_*.yaml` files over `settings.yaml`
- Create backup files (`settings_backup.yaml`)
- Restore original settings after tests

The new approach provides:
- ✅ No file system pollution
- ✅ Parallel test execution safety
- ✅ Clear separation of concerns
- ✅ Easier debugging and maintenance
- ✅ Reproducible test environments

## Troubleshooting

### Common Issues

**Settings not found error**:
```
Error: No such file or directory (os error 2)
```
- Ensure the path starts with `tests/configs/`
- Verify the file exists in this directory

**Settings not initialized error**:
```
Error: Settings not initialized. Call Settings::init() first.
```
- Add `crate::common::ensure_settings_initialized()` to your test
- Or use `ensure_settings_initialized_with_file()` for specific configs

**Consumer group tests skipped**:
- Ensure you're using `test_integration_settings.yaml` which has `enable_consumer_groups: true`
- Check that the settings file path is correct

### Validation

To verify configuration files are valid:
```bash
# Test each configuration loads correctly
cargo run --bin blink -- --settings tests/configs/test_settings.yaml --help
cargo run --bin blink -- --settings tests/configs/test_integration_settings.yaml --help
cargo run --bin blink -- --settings tests/configs/test_stress_settings.yaml --help
cargo run --bin blink -- --settings tests/configs/test_throughput_settings.yaml --help
```
