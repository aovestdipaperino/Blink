# Blink Tests Directory

This directory contains all test-related code, configurations, and scripts for the Blink Kafka-compatible message broker.

## Directory Structure

```
tests/
├── README.md                    # This file - test directory overview
├── common/                      # Shared test utilities and helpers
│   └── mod.rs                  # Common test functions and setup
├── configs/                     # Test configuration files
│   ├── README.md               # Configuration documentation
│   ├── test_settings.yaml      # Default test settings (1MB memory, offload enabled)
│   ├── test_integration_settings.yaml  # Integration test settings (consumer groups enabled)
│   └── test_stress_settings.yaml       # Stress test settings (8MB memory, 16 partitions)
├── scripts/                     # Test execution scripts
│   ├── README.md               # Script documentation
│   ├── run_stress_test.sh      # Automated stress testing
│   └── test_offload_with_blink.sh # End-to-end offload testing with server management
├── offload_tests/                 # WAL offload functionality tests
│   ├── mod.rs                  # Core WAL offload testing functions
│   ├── baseline_test.rs        # Basic WAL offload functionality tests
│   ├── config_tests.rs         # Configuration-specific WAL offload tests
│   ├── minimal_test.rs         # Minimal WAL offload test cases
│   └── size_limit_test.rs      # Size limit and boundary tests
├── consumer_group.rs            # Consumer group API tests
├── rdkafka_integration_test.rs  # RdKafka compatibility tests
├── stress_test.rs               # High-load and performance tests
├── tests.rs                     # General integration tests
├── throughput_test.rs           # Throughput performance tests (Kitt-style balanced testing)
├── deadlock_detection_test.rs   # Deadlock detection tests (4 producers/4 consumers with timeout)
└── working_offload_test.rs        # Working WAL offload functionality tests
```

## Test Categories

### 1. Unit Tests (`src/`)
Located in the main source code with `#[cfg(test)]` modules.
- Test individual functions and modules
- Fast execution, no external dependencies
- Run with: `cargo test --lib`

### 2. Integration Tests (`tests/*.rs`)
Test complete workflows and API interactions.

#### Core Integration Tests (`tests.rs`)
- Basic producer/consumer functionality
- Topic management
- Metadata operations
- Protocol compliance

#### Consumer Group Tests (`consumer_group.rs`)
- Consumer group coordination
- Rebalancing protocols
- Offset management
- Group membership

#### Stress Tests (`stress_test.rs`)
- High-throughput scenarios
- Memory pressure testing
- Concurrent operations
- Performance validation

#### Throughput Tests (`throughput_test.rs`)
- Balanced producer/consumer load testing
- Kitt-style throughput measurement
- 4 producers + 4 consumers configuration
- 10K msg/sec minimum performance validation
- Backlog-based flow control

#### Deadlock Detection Tests (`deadlock_detection_test.rs`)
- 4 producers + 4 consumers deadlock validation
- 5-second timeout to detect unexpected deadlocks
- 400 total messages (100 per producer)
- Integrated with BVT test suite for regular validation
- Basic and external Kafka test variants

#### Offload Tests (`working_offload_test.rs`)
- End-to-end WAL offload functionality
- Memory-to-WAL transitions
- WAL storage management

### 3. Specialized Test Suites (`tests/offload_tests/`)
Focused testing of specific functionality areas.

#### WAL Offload Tests
- **Baseline Tests**: Core WAL offload functionality
- **Configuration Tests**: Settings-driven behavior
- **Minimal Tests**: Simple validation cases
- **Size Limit Tests**: Boundary condition testing

### 4. Compatibility Tests
- **RdKafka Integration**: Compatibility with standard Kafka clients
- **Protocol Compliance**: Kafka wire protocol adherence

## Configuration Management

### Test Settings Files (`tests/configs/`)

All test scenarios use dedicated configuration files instead of modifying the main `settings.yaml`:

| File | Purpose | Key Settings | Used By |
|------|---------|--------------|---------|
| `test_settings.yaml` | Default test config | 1MB memory, WAL offload enabled | General tests, WAL offload tests |
| `test_integration_settings.yaml` | Integration testing | Consumer groups enabled | Consumer group tests, API tests |
| `test_stress_settings.yaml` | Performance testing | 8MB memory, 16 partitions | Stress tests, benchmarks |
| `test_throughput_settings.yaml` | Throughput testing | 16MB memory, 8 partitions, no offload | Throughput tests, balanced load testing |
| `test_integration_settings.yaml` | Integration testing | 512KB memory, consumer groups enabled | Deadlock detection, consumer group tests |

### Usage in Tests
```rust
// Use default test settings
crate::common::ensure_settings_initialized();

// Use specific configuration
crate::common::ensure_settings_initialized_with_file(
    "tests/configs/test_integration_settings.yaml"
);
```

## Running Tests

### Basic Test Execution
```bash
# Run all tests (multi-threaded, may have flaky failures)
cargo test

# Run specific test file
cargo test --test consumer_group
cargo test --test throughput_test
cargo test --test deadlock_detection_test

# Run with single thread (prevents flaky failures due to shared state)
cargo test -- --test-threads=1

# Run specific test function
cargo test test_basic_join_group
```

### Recommended Test Execution (via run_tests.sh)
```bash
# Run core tests (single-threaded by default to prevent flaky failures)
./run_tests.sh

# Run all tests (single-threaded by default)  
./run_tests.sh all

# Run tests in parallel mode (may be flaky)
./run_tests.sh --parallel

# Run specific test group
./run_tests.sh unit
./run_tests.sh bvt
./run_tests.sh offload

# Run throughput test (requires external-kafka feature)
cargo test --test throughput_test --features external-kafka

# Run deadlock detection test (basic version runs without external kafka)
cargo test test_deadlock_detection_basic --test deadlock_detection_test
```

### Test Scripts
```bash
# Automated stress testing (uses tests/configs/test_stress_settings.yaml)
./tests/scripts/run_stress_test.sh -d 5 -p 8 -c 4 -t 2000

# End-to-end WAL offload testing with CLI --settings argument
./tests/scripts/test_offload_with_blink.sh --test-settings

# Run specific WAL offload test with verbose output
./tests/scripts/test_offload_with_blink.sh --test baseline_test --verbose
```

### Environment Variables
```bash
# Override specific settings
KAFKA_HOSTNAME=localhost cargo test
REST_PORT=30005 cargo test consumer_group
RETENTION=10s cargo test offload_tests
```

## Test Development Guidelines

### Writing New Tests

1. **Choose the right location**:
   - Unit tests: In `src/` with `#[cfg(test)]`
   - Integration tests: New `.rs` file in `tests/`
   - Specialized suites: Subdirectory in `tests/`

2. **Use appropriate configuration**:
   ```rust
   #[tokio::test]
   async fn test_example() {
       // Initialize settings first
       crate::common::ensure_settings_initialized();

       // Test code here
   }
   ```

3. **Follow naming conventions**:
   - Test functions: `test_descriptive_name`
   - Test files: `descriptive_test.rs`
   - Modules: `descriptive_tests`

### Test Isolation

- Each test should be independent
- Use unique topic names to avoid conflicts
- Clean up resources in test functions
- Don't modify shared state

### Configuration Best Practices

- Use dedicated test configuration files in `tests/configs/`
- Never modify main `settings.yaml` file in tests or scripts
- Use Blink's `--settings` CLI argument for custom configurations
- Document why specific settings are needed
- Use appropriate memory limits for test scenarios

## Common Test Utilities (`tests/common/`)

### Available Functions

```rust
// Server management (uses CLI --settings argument internally)
start_blink().await;  // Start with tests/configs/test_settings.yaml
start_blink_with_settings("tests/configs/custom.yaml").await;

// Settings initialization (for tests that don't start server)
ensure_settings_initialized();  // Use tests/configs/test_settings.yaml
ensure_settings_initialized_with_file("tests/configs/path.yaml");

// Kafka operations
connect_to_kafka();
create_topic(topic_name, &mut socket, partitions);
delete_topic(topic_name, &mut socket);
produce_records(topic, partition, records, &mut socket);
fetch_records(topic, partition, offset, &mut socket);

// Test data
create_string_of_length(size, char);
```

### TestTopic Helper

Automatic topic lifecycle management:
```rust
let topic = TestTopic::create("my_test_topic");
// Topic is automatically deleted when `topic` goes out of scope

let topic = TestTopic::create_with_partitions("multi_partition", 4);
// Creates topic with specified partition count
```

## Performance Testing

### Stress Tests (`tests/stress_test.rs`)
- Integrated with test suite
- Configurable load scenarios
- Automated via scripts

### Load Testing Scripts
- `run_stress_test.sh`: Parameterized stress testing
- `test_offload_with_blink.sh`: Memory pressure and WAL offload testing

### Throughput Testing
- `throughput_test.rs`: Kitt-style balanced throughput measurement
- Uses 4 producers and 4 consumers with 8 partitions
- Targets minimum 10K messages/second throughput
- Includes backlog-based flow control to prevent consumer lag
- Requires `external-kafka` feature for full integration testing

### Deadlock Detection Testing
- `deadlock_detection_test.rs`: Fast deadlock detection with timeout validation
- Uses 4 producers and 4 consumers with 8 partitions
- Tests 400 total messages (100 per producer) in under 5 seconds
- **Integrated with BVT suite**: Runs automatically with `./run_tests.sh bvt` or `./run_tests.sh core`
- Basic test runs without external Kafka dependencies
- External test variant available with `external-kafka` feature

## Debugging Tests

### Verbose Output
```bash
# Show detailed test output
cargo test -- --nocapture

# Enable debug logging
RUST_LOG=debug cargo test

# Combine with specific test
RUST_LOG=debug cargo test test_basic_join_group -- --nocapture
```

### Test-Specific Debugging
```bash
# Debug WAL offload tests with verbose output
./tests/scripts/test_offload_with_blink.sh --test memory_tracking --verbose

# Debug stress tests with specific parameters
./tests/scripts/run_stress_test.sh -d 1 -t 100 --verbose
```

### Common Issues

1. **Settings not initialized**: Add `ensure_settings_initialized()` to test
2. **Port conflicts**: Use `--test-threads=1` (default in run_tests.sh) or unique ports
3. **Consumer group tests skipped**: Use `test_integration_settings.yaml`
4. **Memory and WAL tests unreliable**: Use consistent memory limits in test config

## Continuous Integration

### CI-Friendly Commands
```bash
# Fast test execution
cargo test --jobs 1

# With timeout for hanging tests
timeout 300 cargo test

# Specific test categories
cargo test --test consumer_group
cargo test offload_tests
cargo test --test throughput_test --features external-kafka
cargo test --test deadlock_detection_test
```

### Test Isolation in CI
- Tests use dedicated configuration files
- No file system pollution between runs
- Parallel execution safe with proper setup

## Contributing

When adding new tests:

1. **Document the test purpose** in comments
2. **Use appropriate configuration** for the test scenario
3. **Follow existing patterns** in similar tests
4. **Add to CI pipeline** if needed
5. **Update this README** if adding new test categories

### Code Review Checklist

- [ ] Test uses proper configuration file
- [ ] Test is properly isolated
- [ ] Test has descriptive name and documentation
- [ ] Test cleans up resources
- [ ] Test works in CI environment
- [ ] Test follows project conventions
