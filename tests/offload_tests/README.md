# Offload File Integration Tests

This directory contains comprehensive integration tests for Blink's offload file functionality, which allows the system to gracefully handle memory pressure by offloading older batches to disk.

## Overview

The offload file feature enables Blink to:
- Continue operating when data exceeds available RAM
- Maintain low latency for hot data while gracefully degrading for older data
- Automatically clean up offload files when no longer needed
- Provide transparent access to both in-memory and offloaded data

## Test Coverage

### Core Functionality Tests (`mod.rs`)

1. **test_offload_triggering_when_ram_limit_reached**
   - Verifies that offloading occurs when memory usage exceeds `max_memory`
   - Checks that offload files are created in the `blink_offload` directory
   - Ensures memory usage stays within limits after offloading

2. **test_reading_offloaded_batches**
   - Tests reading data that has been offloaded to disk
   - Verifies transparent loading of offloaded batches during fetch operations
   - Ensures data integrity is maintained through the offload/load cycle

3. **test_mixed_memory_disk_batch_retrieval**
   - Tests fetching data that spans both in-memory and offloaded batches
   - Verifies seamless access across storage boundaries
   - Tests the system's ability to handle hybrid storage scenarios

4. **test_offload_file_cleanup**
   - Verifies automatic deletion of offload files when all offloaded batches are purged
   - Tests the offloaded batch counter mechanism
   - Ensures no orphaned offload files remain after cleanup

5. **test_multi_partition_offloading**
   - Tests offloading across multiple partitions
   - Verifies each partition maintains its own offload file
   - Ensures partition isolation is maintained during offloadover

6. **test_offload_recovery_after_fetch**
   - Tests the cycle of offload → fetch → re-offload
   - Verifies that loaded batches can be re-offloaded if needed
   - Tests memory management under dynamic load patterns

### Configuration Tests (`config_tests.rs`)

1. **test_small_memory_limit_triggers_aggressive_offloading**
   - Tests behavior with very restrictive memory limits
   - Verifies aggressive offloading under tight constraints

2. **test_custom_offload_directory**
   - Placeholder for testing custom offload directory configuration
   - Would verify offload files are created in configured location

3. **test_memory_limit_boundary_conditions**
   - Tests edge cases around the memory limit threshold
   - Verifies correct behavior at exact memory boundaries

4. **test_no_offloading_under_memory_limit**
   - Ensures no unnecessary offloading occurs when under limits
   - Verifies efficient memory usage when space is available

5. **test_offload_directory_permissions**
   - Tests automatic creation of offload directory
   - Verifies proper handling of directory permissions

6. **test_memory_tracking_accuracy**
   - Validates accuracy of memory usage tracking
   - Ensures MEMORY counter reflects actual usage

## Prerequisites

Before running the tests, ensure:

1. **Blink is running on port 9999**:
   ```bash
   # Start Blink with test configuration
   cargo run --release -- --port 9999
   ```

2. **Settings file is configured**:
   - Copy `settings.yaml` or `test_settings.yaml` to the project root
   - Ensure `broker_port` is set to 9092 in the settings file

3. **Clean state**:
   - Remove any existing offload directory: `rm -rf blink_offload/`

## Running the Tests

### Run all offload tests:
```bash
cargo test --test tests offload_tests::
```

### Run specific test:
```bash
cargo test --test tests offload_tests::test_offload_triggering_when_ram_limit_reached
```

### Run with test settings (small memory limit):
```bash
# First, ensure Blink is running with the test settings
cp test_settings.yaml settings.yaml
cargo run --release -- --port 9999 &
BLINK_PID=$!

# Wait for Blink to start
sleep 2

# Run the tests
cargo test --test tests offload_tests::

# Stop Blink after tests
kill $BLINK_PID
```

## Test Configuration

The tests use settings from `settings.yaml`. For testing offloadover behavior, use `test_settings.yaml` which sets:
- `max_memory: "1MB"` - Very low limit to easily trigger offloading
- `retention: '5s'` - Short expiration for cleanup tests

## Implementation Details

### Offload File Format
- Binary format: `[4-byte length][data][4-byte length][data]...`
- Files named: `{topic_name}_{partition}.data`
- Located in: `blink_offload/` directory

### Memory Management
- Global memory tracked via `MEMORY` atomic counter
- Offloading triggered when `MEMORY > SETTINGS.max_memory`
- Per-partition `offloaded_batch_count` tracks offloaded batches

### Cleanup Strategy
- Offload files deleted when `offloaded_batch_count` reaches 0
- Happens automatically during purge operations
- No manual intervention required

## Common Issues and Debugging

1. **Tests fail with "Connection refused"**
   - Ensure Blink is running on port 9999
   - Check that no other process is using that port
   - Verify Blink started successfully without errors

2. **Tests fail with "Offload files should exist"**
   - Check `max_memory` setting is low enough
   - Verify records are large enough to trigger offloading
   - Add debug logging to see actual memory usage

2. **Cleanup tests fail**
   - Check `retention` is set appropriately
   - Ensure enough time is given for expiration
   - Verify purge is being triggered

3. **Performance issues**
   - Offload files on slow storage can impact test speed
   - Consider using tmpfs for `blink_offload` during tests
   - Monitor disk I/O during test runs

## Future Enhancements

- Test offload file compression
- Test concurrent offloading across many partitions
- Benchmark offload/load performance
- Test recovery after crashes with existing offload files
- Test offload file corruption handling
