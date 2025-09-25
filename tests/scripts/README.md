# Test Scripts

This directory contains shell scripts for running various test scenarios with the Blink Kafka-compatible message broker.

## Scripts Overview

### `run_stress_test.sh` - Stress Test Runner

A comprehensive stress testing script that runs various load scenarios against Blink.

**Features:**
- Configurable test duration, producer/consumer counts, and throughput
- Automatic test selection based on parameters
- Built-in help and usage examples
- Automatic cleanup and result reporting

**Usage:**
```bash
# Run with defaults (1 minute, conservative load)
./tests/scripts/run_stress_test.sh

# Run high-throughput stress test for 5 minutes
./tests/scripts/run_stress_test.sh -d 5 -p 8 -c 4 -t 2000

# Run memory pressure test
./tests/scripts/run_stress_test.sh --duration 10 --throughput 5000
```

**Options:**
- `-d, --duration MINUTES` - Test duration (default: 1)
- `-p, --producers COUNT` - Number of producer threads (default: 4)
- `-c, --consumers COUNT` - Number of consumer threads (default: 4)  
- `-t, --throughput MSG/SEC` - Target throughput (default: 1000)
- `--topic TOPIC_NAME` - Topic name (default: stress_test_topic)
- `-h, --help` - Show help message

**Test Selection:**
The script automatically selects the appropriate test based on parameters:
- Conservative load: Duration ≤ 1min, Throughput ≤ 200 msg/s
- Basic stress: Default parameters
- High throughput: Throughput ≥ 800 msg/s
- Memory pressure: Throughput ≥ 1500 msg/s

**Configuration:**
Uses `tests/configs/test_stress_settings.yaml` automatically.

### `test_offload_with_blink.sh` - Offload Test Runner

An end-to-end testing script that starts Blink, runs offload tests, and handles cleanup.

**Features:**
- Automatic Blink server startup and shutdown
- Uses Blink's --settings CLI argument (no file manipulation)
- Real-time readiness detection
- Comprehensive cleanup on exit
- Support for specific test execution

**Usage:**
```bash
# Run all offload tests with automatic Blink management
./tests/scripts/test_offload_with_blink.sh

# Use test configuration (1MB memory limit)
./tests/scripts/test_offload_with_blink.sh --test-settings

# Run specific test with verbose output
./tests/scripts/test_offload_with_blink.sh --test baseline_test --verbose

# Build only (don't run tests)
./tests/scripts/test_offload_with_blink.sh --build-only
```

**Options:**
- `--test-settings` - Use `tests/configs/test_settings.yaml` (1MB memory limit)
- `--test <name>` - Run specific offload test
- `--verbose` - Show detailed output and debug logs
- `--build-only` - Only build Blink, don't run tests
- `--help` - Show help message

**Safety Features:**
- No modification of main `settings.yaml` file
- Process cleanup on script exit (via trap)
- Force-kill protection for stuck processes
- Offload directory cleanup
- Port availability checking

### `run_throughput_test.sh` - Throughput Test Runner

A simplified throughput testing script based on Kitt's balanced load approach.

**Features:**
- 4 producers and 4 consumers with 8 partitions
- Backlog-based flow control to prevent consumer lag
- Target minimum throughput of 10K messages/second
- Real-time throughput monitoring
- Automatic topic creation and cleanup

**Usage:**
```bash
# Run with default settings (15 second measurement)
./tests/scripts/run_throughput_test.sh

# Run with verbose output
./tests/scripts/run_throughput_test.sh --verbose

# Show help
./tests/scripts/run_throughput_test.sh --help
```

**Options:**
- `-d, --duration SECONDS` - Measurement duration (default: 15, note: currently hardcoded in test)
- `-v, --verbose` - Enable verbose output and debug logging
- `-h, --help` - Show help message

**Test Configuration:**
- Uses `tests/configs/test_throughput_settings.yaml` automatically
- 16MB memory limit for optimal performance
- 8 partitions (2 per producer/consumer pair)
- 1024 byte message size
- 1000 message maximum backlog

**Requirements:**
- Requires `external-kafka` feature enabled
- rdkafka dependency must be available

## Directory Structure

```
tests/scripts/
├── README.md                    # This file
├── run_stress_test.sh          # Stress test runner
├── run_throughput_test.sh      # Throughput test runner (Kitt-style)
└── test_offload_with_blink.sh    # Offload test runner with Blink management
```

## Configuration Integration

All scripts are designed to work with the new configuration file structure:

- **Stress tests**: Automatically use `tests/configs/test_stress_settings.yaml`
- **Throughput tests**: Automatically use `tests/configs/test_throughput_settings.yaml`
- **Offload tests**: Use `tests/configs/test_settings.yaml` when `--test-settings` is specified
- **CLI Integration**: Use Blink's `--settings` argument instead of file manipulation

## Common Use Cases

### Quick Smoke Test
```bash
# Fast validation that everything works
./tests/scripts/run_stress_test.sh -d 0.5 -t 100
```

### Memory Pressure Testing
```bash
# Test offload behavior under memory pressure (uses CLI --settings argument)
./tests/scripts/test_offload_with_blink.sh --test-settings --verbose
```

### Performance Benchmarking
```bash
# High-throughput performance test
./tests/scripts/run_stress_test.sh -d 10 -p 8 -c 8 -t 3000

# Balanced throughput test (Kitt-style)
./tests/scripts/run_throughput_test.sh --verbose
```

### Debugging Specific Issues
```bash
# Debug specific offload test with full logging
./tests/scripts/test_offload_with_blink.sh --test memory_tracking --verbose

# Debug throughput performance issues
./tests/scripts/run_throughput_test.sh --verbose
```

## Requirements

### System Dependencies
- **Bash**: Version 4.0 or higher
- **netcat (nc)**: For port availability checking
- **Rust/Cargo**: For building and running Blink

### Permissions
- Scripts must be executable: `chmod +x tests/scripts/*.sh`
- Write access to project directory for temporary files
- Network access to bind to test ports (9092, 30004)

## Error Handling

All scripts include comprehensive error handling:

### Common Exit Codes
- `0` - Success
- `1` - General error (build failure, test failure, etc.)
- `2` - Invalid command line arguments
- `130` - Interrupted by user (Ctrl+C)

### Automatic Cleanup
- Process termination on script exit
- Temporary file cleanup
- Offload directory cleanup
- Port release

## Best Practices

### Running in CI/CD
```bash
# Use timeout to prevent hanging builds
timeout 300 ./tests/scripts/run_stress_test.sh -d 2

# Capture output for build logs
./tests/scripts/test_offload_with_blink.sh --test-settings 2>&1 | tee test_output.log

# Run throughput validation in CI
./tests/scripts/run_throughput_test.sh
```

### Local Development
```bash
# Quick iteration during development (no settings file changes)
./tests/scripts/test_offload_with_blink.sh --build-only
./tests/scripts/test_offload_with_blink.sh --test <specific_test>
```

### Performance Testing
```bash
# Gradual load increase for capacity planning
for load in 500 1000 2000 3000; do
    echo "Testing throughput: $load msg/s"
    ./tests/scripts/run_stress_test.sh -t $load -d 2
done

# Balanced throughput validation (10K msg/sec target)
./tests/scripts/run_throughput_test.sh

# Combined performance testing
./tests/scripts/run_stress_test.sh -d 5 -t 2000
./tests/scripts/run_throughput_test.sh --verbose
```

## Troubleshooting

### Port Already in Use
If you see "port already in use" errors:
```bash
# Check what's using the port
lsof -i :9092
netstat -tulpn | grep :9092

# Kill any existing Blink processes
pkill -f "blink"
```

### Permission Denied
```bash
# Make scripts executable
chmod +x tests/scripts/*.sh

# If still failing, check directory permissions
ls -la tests/scripts/
```

### Settings File Not Found
```bash
# Verify test configuration files exist
ls -la tests/configs/
# Should show test_settings.yaml, test_integration_settings.yaml, test_stress_settings.yaml
```

### Build Failures
```bash
# Clean and rebuild
cargo clean
cargo build --release

# Check for missing dependencies
cargo check
```

### Test Failures
```bash
# Run with verbose output for debugging
./tests/scripts/test_offload_with_blink.sh --verbose

# Check which settings file is being used
./tests/scripts/test_offload_with_blink.sh --test-settings --build-only

# Verify configuration files
cat tests/configs/test_settings.yaml
```

## Contributing

When modifying these scripts:

1. **Test thoroughly** on different environments
2. **Update documentation** if behavior changes
3. **Maintain backward compatibility** where possible
4. **Add error handling** for new failure modes
5. **Update help text** for new options

### Script Conventions
- Use `set -e` for fail-fast behavior
- Include colored output for readability
- Provide comprehensive help text
- Handle cleanup with trap handlers
- Validate inputs and dependencies
- Use CLI arguments instead of file manipulation
- Preserve main settings.yaml file integrity