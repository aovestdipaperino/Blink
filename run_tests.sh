#!/bin/bash

# Blink Test Runner
#
# (c) 2025 Cleafy S.p.A.
# Author: Enzo Lombardi
#
# Comprehensive test runner for the Blink Kafka-compatible message broker
# Supports running different test groups with various configuration options
#
# TIMEOUT IMPLEMENTATION:
# - All test groups are configured with a 2-minute (120 seconds) timeout
# - Uses the system 'timeout' command to enforce hard limits
# - Timeout applies to each individual test execution within a group
# - Tests that exceed 2 minutes will be terminated and marked as failed

# Handle errors manually to collect all failures

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAILURES_FILE="docs/FAILURES.md"
VERBOSE=false
QUIET=false
PARALLEL=false
RELEASE=false
NO_CAPTURE=false
FEATURE_FLAGS=""
TIMEOUT=""
TEST_GROUP="core"
SPECIFIC_TEST=""
LIST_ONLY=false
CLEANUP=true
FAILED_TESTS=()
PASSED_TESTS=()
TOTAL_INDIVIDUAL_TESTS=0
TOTAL_INDIVIDUAL_PASSED=0
TOTAL_INDIVIDUAL_FAILED=0

# Function to print colored output
print_header() {
    echo -e "${BOLD}${BLUE}$1${NC}"
    echo -e "${BLUE}$(echo "$1" | sed 's/./=/g')${NC}"
}

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "${CYAN}[STEP]${NC} $1"
}

print_success() {
    echo -e "${BOLD}${GREEN}✅ $1${NC}"
}

print_failure() {
    echo -e "${BOLD}${RED}❌ $1${NC}"
}

# Usage function
show_usage() {
    cat << EOF
${BOLD}Blink Test Runner${NC}

${BOLD}USAGE:${NC}
    $0 [TEST_GROUP] [OPTIONS]

${BOLD}TEST GROUPS:${NC}
    core         Run core tests: unit + bvt + consumer (default)
    all          Run all tests
    unit         Run unit tests only
    integration  Run integration tests (includes GCP logging, obliteration, and offload tests)
    bvt          Run basic validation tests (BVT) - core functionality & deadlock detection

    stress       Run stress tests
    offload        Run offload file tests
    consumer     Run consumer group tests
    rdkafka      Run rdkafka integration tests

${BOLD}OPTIONS:${NC}
    -v, --verbose         Enable verbose output
    -q, --quiet           Suppress most output (opposite of verbose)
    -p, --parallel        Run tests in parallel (default: single-threaded)
    -r, --release         Build and run tests in release mode
    --no-capture          Don't capture test output (shows println! output)
    --features FEATURES   Enable specific cargo features (comma-separated)
    --timeout SECONDS     Set test timeout (default: no timeout)
    --test TEST_NAME      Run a specific test by name
    --list               List available tests without running them
    --no-cleanup         Don't clean up temporary files after tests
    -h, --help           Show this help message

${BOLD}EXAMPLES:${NC}
    $0                                    # Run core tests (unit + bvt + consumer)
    $0 all                                # Run all tests
    $0 bvt --verbose                      # Run BVT tests with verbose output
    $0 perf --release                     # Run performance tests in release mode
    $0 unit --parallel --no-capture       # Run unit tests in parallel with output
    $0 --test offload_tests::test_memory    # Run specific offload test
    $0 --list                             # List all available tests
    $0 stress --features stress-test      # Run stress tests with stress-test feature

${BOLD}FEATURES:${NC}
    external-kafka    Enable external Kafka integration tests
    stress-test       Enable stress testing capabilities

${BOLD}NOTES:${NC}
    - Failed tests are logged to ${FAILURES_FILE}
    - Use --verbose to see detailed test output
    - Performance tests require --release for accurate results
    - Some tests may require specific features to be enabled
    - Offload tests automatically manage Blink server lifecycle

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        core|all|unit|integration|bvt|perf|stress|offload|consumer|rdkafka)
            TEST_GROUP="$1"
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -q|--quiet)
            QUIET=true
            shift
            ;;
        -p|--parallel)
            PARALLEL=true
            shift
            ;;
        -r|--release)
            RELEASE=true
            shift
            ;;
        --no-capture)
            NO_CAPTURE=true
            shift
            ;;
        --features)
            FEATURE_FLAGS="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --test)
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        --list)
            LIST_ONLY=true
            shift
            ;;
        --no-cleanup)
            CLEANUP=false
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Validate exclusive options
if [[ "$VERBOSE" == true && "$QUIET" == true ]]; then
    print_error "Cannot use both --verbose and --quiet options"
    exit 1
fi

# Build cargo command base
build_cargo_cmd() {
    local cmd="cargo test"

    if [[ "$RELEASE" == true ]]; then
        cmd="$cmd --release"
    fi

    if [[ -n "$FEATURE_FLAGS" ]]; then
        cmd="$cmd --features $FEATURE_FLAGS"
    fi

    echo "$cmd"
}

# Build test arguments
build_test_args() {
    local args=""

    # Force single-threaded execution by default to prevent flaky failures
    # due to environment variable contamination and shared state issues
    if [[ "$PARALLEL" != true ]]; then
        args="$args --test-threads=1"
    fi

    if [[ "$NO_CAPTURE" == true ]]; then
        args="$args --nocapture"
    fi

    if [[ "$QUIET" == true ]]; then
        args="$args --quiet"
    elif [[ "$VERBOSE" == true ]]; then
        args="$args --show-output"
    fi

    if [[ -n "$TIMEOUT" ]]; then
        args="$args --timeout $TIMEOUT"
    fi

    echo "$args"
}

# Parse test results from cargo output
parse_test_counts() {
    local output="$1"
    local passed_count=0
    local failed_count=0
    local total_count=0

    # Parse lines like "test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s"
    while IFS= read -r line; do
        if [[ "$line" =~ test\ result:.*\ ([0-9]+)\ passed.*\ ([0-9]+)\ failed ]]; then
            local line_passed="${BASH_REMATCH[1]}"
            local line_failed="${BASH_REMATCH[2]}"
            passed_count=$((passed_count + line_passed))
            failed_count=$((failed_count + line_failed))
            total_count=$((total_count + line_passed + line_failed))
        fi
    done <<< "$output"

    echo "$total_count $passed_count $failed_count"
}

# Execute test command and track results
execute_test() {
    local test_name="$1"
    local cmd="$2"
    local description="$3"

    if [[ "$QUIET" != true ]]; then
        print_step "Running $description (with 2-minute timeout)"
        if [[ "$VERBOSE" == true ]]; then
            print_info "Command: timeout 120 $cmd"
        fi
    fi

    # Capture both exit code and output with 2-minute timeout
    local temp_output=$(mktemp)
    if timeout 120 bash -c "$cmd" > "$temp_output" 2>&1; then
        local exit_code=0
    else
        local exit_code=$?
        # Check if timeout occurred (exit code 124)
        if [[ $exit_code -eq 124 ]]; then
            echo "TEST TIMEOUT: $description exceeded 2-minute limit" >> "$temp_output"
            print_error "$test_name timed out after 2 minutes"
        fi
    fi

    # Show output
    cat "$temp_output"

    # Parse test counts from output
    local test_counts=($(parse_test_counts "$(cat "$temp_output")"))
    local individual_total=${test_counts[0]:-0}
    local individual_passed=${test_counts[1]:-0}
    local individual_failed=${test_counts[2]:-0}

    # Update global counters
    TOTAL_INDIVIDUAL_TESTS=$((TOTAL_INDIVIDUAL_TESTS + individual_total))
    TOTAL_INDIVIDUAL_PASSED=$((TOTAL_INDIVIDUAL_PASSED + individual_passed))
    TOTAL_INDIVIDUAL_FAILED=$((TOTAL_INDIVIDUAL_FAILED + individual_failed))

    # Clean up temp file
    rm -f "$temp_output"

    if [[ $exit_code -eq 0 ]]; then
        if [[ "$QUIET" != true ]]; then
            print_success "$test_name passed"
        fi
        PASSED_TESTS+=("$test_name")
        return 0
    else
        print_failure "$test_name failed"
        FAILED_TESTS+=("$test_name|$cmd")
        return 1
    fi
}

# List available tests
list_tests() {
    print_header "Available Tests"

    local base_cmd=$(build_cargo_cmd)
    echo "Listing all available tests..."
    echo

    $base_cmd -- --list
}

# Run unit tests
# Storage tests are now in a separate test file (tests/storage_test.rs) covering:
# - retrieve_record_batch and store_record_batch functionality
# - Race condition testing with concurrent operations
# - Memory management and offloading behavior
# - Data integrity and consistency verification
# - Error handling and edge cases
# - Binary search optimization testing
# - Batch location transitions (memory to file)
# - Watermark consistency and ingestion time tracking
# - Property-based testing with proptest (37 total tests)
run_unit_tests() {
    local base_cmd=$(build_cargo_cmd)
    local test_args=$(build_test_args)

    # Run lib tests
    # Single-threaded by default to prevent async initialization conflicts
    local cmd="$base_cmd --lib -- $test_args"
    execute_test "unit-lib" "$cmd" "unit tests (lib)"

    # Run main tests
    cmd="$base_cmd --bin blink -- $test_args"
    execute_test "unit-main" "$cmd" "unit tests (main)"

    # Run storage unit tests (includes 37 comprehensive storage tests)
    # Covers retrieve_record_batch, store_record_batch functionality, race conditions,
    # memory management, offloading behavior, data integrity, and property-based testing
    cmd="$base_cmd --test storage_test -- $test_args"
    execute_test "unit-storage" "$cmd" "storage unit tests"
}

# Run integration tests
run_integration_tests() {
    local base_cmd=$(build_cargo_cmd)
    local test_args=$(build_test_args)

    # Run only safe integration tests - exclude problematic high-load and offload tests
    print_info "Running safe integration tests (excluding problematic tests)"
    local safe_tests="basic_test delete_topic_test test_metadata test_delete_topics_protocol_versions test_topic_deletion_cleanup"
    local cmd="$base_cmd --test tests -- $test_args $safe_tests"
    execute_test "integration-safe" "$cmd" "safe integration tests"

    # Run GCP logging integration tests
    print_info "Running GCP logging integration tests"
    cmd="$base_cmd --test gcp_logging_integration_test -- $test_args"
    execute_test "integration-gcp-logging" "$cmd" "GCP logging integration tests"

    # Run obliteration integration tests
    print_info "Running obliteration integration tests"
    cmd="$base_cmd --test obliteration_integration_test -- $test_args"
    execute_test "integration-obliteration" "$cmd" "obliteration integration tests"

    # Run working offload test
    print_info "Running working offload integration test"
    cmd="$base_cmd --test working_offload_test -- $test_args"
    execute_test "integration-working-offload" "$cmd" "working offload test"

    # Run offload tests via dedicated script (they work this way)
    print_info "Running offload tests via dedicated script"
    local offload_script="./tests/scripts/test_offload_with_blink.sh"
    if [[ -f "$offload_script" ]]; then
        local offload_args=""
        if [[ "$VERBOSE" == true ]]; then
            offload_args="--verbose"
        fi
        # Note: offload script doesn't support --quiet, so we run without args in quiet mode
        execute_test "integration-offload" "$offload_script $offload_args" "offload tests (managed)"
    else
        print_warn "Offload test script not found, skipping offload tests"
    fi
}

# Run BVT (Basic Validation Tests)
run_bvt_tests() {
    local base_cmd=$(build_cargo_cmd)
    local test_args=$(build_test_args)

    print_info "Running Basic Validation Tests (core functionality)"

    # Run all BVT tests in one command to share the same process and avoid settings conflicts
    local cmd="$base_cmd --test tests -- $test_args basic_test delete_topic_test test_metadata test_multiple_messages"
    execute_test "bvt-all" "$cmd" "all BVT tests"

    # Deadlock detection test (validates no deadlocks in producer/consumer workflows)
    cmd="$base_cmd --test deadlock_detection_test -- $test_args test_deadlock_detection_basic"
    execute_test "bvt-deadlock" "$cmd" "deadlock detection validation"

    # Memory tracking unit test (now in separate storage test file)
    cmd="$base_cmd --test storage_test test_record_batch_memory_tracking -- $test_args"
    execute_test "bvt-memory" "$cmd" "memory tracking validation"
}



# Run stress tests
run_stress_tests() {
    local base_cmd=$(build_cargo_cmd)
    local test_args=$(build_test_args)

    # Add stress-test feature if not already specified
    if [[ "$FEATURE_FLAGS" != *"stress-test"* ]]; then
        if [[ -n "$FEATURE_FLAGS" ]]; then
            FEATURE_FLAGS="$FEATURE_FLAGS,stress-test"
        else
            FEATURE_FLAGS="stress-test"
        fi
        base_cmd="cargo test"
        if [[ "$RELEASE" == true ]]; then
            base_cmd="$base_cmd --release"
        fi
        base_cmd="$base_cmd --features $FEATURE_FLAGS"
    fi

    local cmd="$base_cmd --test stress_test -- $test_args"
    execute_test "stress" "$cmd" "stress tests"
}

# Run offload tests with Blink server management
run_offload_tests() {
    print_step "Running offload tests (with automatic Blink server management)"

    local script_args=""
    if [[ "$VERBOSE" == true ]]; then
        script_args="$script_args --verbose"
    fi

    # Use the existing offload test script
    local cmd="./tests/scripts/test_offload_with_blink.sh $script_args"

    if [[ "$VERBOSE" == true ]]; then
        print_info "Command: $cmd"
    fi

    # Capture exit code but continue execution with 2-minute timeout
    timeout 120 bash -c "$cmd"
    local exit_code=$?

    # Check if timeout occurred (exit code 124)
    if [[ $exit_code -eq 124 ]]; then
        print_error "Offload tests timed out after 2 minutes"
    fi

    if [[ $exit_code -eq 0 ]]; then
        print_success "Offload tests passed"
    else
        print_failure "Offload tests failed"
        FAILED_TESTS+=("offload-managed|$cmd")
    fi
}

# Run consumer group tests
run_consumer_tests() {
    local base_cmd=$(build_cargo_cmd)
    local test_args=$(build_test_args)

    # Run all consumer group tests (single-threaded by default via build_test_args)
    local cmd="$base_cmd --test consumer_group -- $test_args"
    execute_test "consumer-group" "$cmd" "consumer group tests"
}

# Run rdkafka integration tests
run_rdkafka_tests() {
    local base_cmd=$(build_cargo_cmd)
    local test_args=$(build_test_args)

    # Add external-kafka feature if not already specified
    if [[ "$FEATURE_FLAGS" != *"external-kafka"* ]]; then
        if [[ -n "$FEATURE_FLAGS" ]]; then
            FEATURE_FLAGS="$FEATURE_FLAGS,external-kafka"
        else
            FEATURE_FLAGS="external-kafka"
        fi
        base_cmd="cargo test"
        if [[ "$RELEASE" == true ]]; then
            base_cmd="$base_cmd --release"
        fi
        base_cmd="$base_cmd --features $FEATURE_FLAGS"
    fi

    local cmd="$base_cmd --test rdkafka_integration_test -- $test_args"
    execute_test "rdkafka-integration" "$cmd" "rdkafka integration tests"
}

# Run specific test
run_specific_test() {
    local base_cmd=$(build_cargo_cmd)
    local test_args=$(build_test_args)

    local cmd="$base_cmd $SPECIFIC_TEST -- $test_args"
    execute_test "specific-$SPECIFIC_TEST" "$cmd" "specific test: $SPECIFIC_TEST"
}

# Generate failures documentation
generate_failures_doc() {
    if [[ ${#FAILED_TESTS[@]} -eq 0 ]]; then
        return 0
    fi

    print_step "Generating failures documentation"

    # Ensure docs directory exists
    mkdir -p "$(dirname "$FAILURES_FILE")"

    cat > "$FAILURES_FILE" << 'EOF'
# Test Failures

This document lists failed tests and how to run them individually for debugging.

Generated automatically by `run_tests.sh`.

## Failed Tests

EOF

    for failure in "${FAILED_TESTS[@]}"; do
        local test_name="${failure%%|*}"
        local command="${failure#*|}"

        cat >> "$FAILURES_FILE" << EOF
### $test_name

**Command to run individually:**
\`\`\`bash
$command
\`\`\`

EOF
    done

    cat >> "$FAILURES_FILE" << 'EOF'
## Debugging Tips

1. **Run with verbose output:**
   ```bash
   ./run_tests.sh --verbose --test <test_name>
   ```

2. **Run specific test groups:**
   ```bash
   ./run_tests.sh <group> --verbose --no-capture
   ```

3. **Run with debug logging:**
   ```bash
   RUST_LOG=debug ./run_tests.sh --test <test_name> --verbose
   ```

4. **For offload tests, use the dedicated script:**
   ```bash
   ./tests/scripts/test_offload_with_blink.sh --verbose --test <test_name>
   ```

5. **For stress tests, ensure features are enabled:**
   ```bash
   ./run_tests.sh stress --features stress-test --release
   ```

EOF

    print_info "Failures documented in $FAILURES_FILE"
}

# Cleanup function
cleanup() {
    if [[ "$CLEANUP" == true ]]; then
        print_step "Cleaning up temporary files"
        # Clean up offload directories
        rm -rf blink_offload/ 2>/dev/null || true
        # Clean up any test artifacts
        find . -name "*.tmp" -delete 2>/dev/null || true
    fi
}

# Main execution
main() {
    print_header "🚀 Blink Test Runner"

    # Verify we're in the right directory
    if [[ ! -f "Cargo.toml" ]] || [[ ! -d "src" ]]; then
        print_error "Must be run from the Blink project root directory"
        exit 1
    fi



    # List tests if requested
    if [[ "$LIST_ONLY" == true ]]; then
        list_tests
        exit 0
    fi

    # Setup cleanup trap
    trap cleanup EXIT

    # Run specific test if requested
    if [[ -n "$SPECIFIC_TEST" ]]; then
        run_specific_test
    else
        # Run test groups
        case "$TEST_GROUP" in
            core)
                print_info "Running core test groups (unit + bvt + consumer)"
                run_unit_tests
                # Add delay between test groups to prevent resource conflicts
                sleep 1
                run_bvt_tests
                sleep 1
                run_consumer_tests
                ;;
            all)
                print_info "Running all test groups"
                run_unit_tests
                # Add delay between test groups to prevent resource conflicts
                sleep 2
                run_bvt_tests
                sleep 2
                run_integration_tests
                sleep 2
                run_consumer_tests
                sleep 2
                run_offload_tests
                ;;
            unit)
                run_unit_tests
                ;;
            integration)
                run_integration_tests
                ;;
            bvt)
                run_bvt_tests
                ;;
            stress)
                run_stress_tests
                ;;
            offload)
                run_offload_tests
                ;;
            consumer)
                run_consumer_tests
                ;;
            rdkafka)
                run_rdkafka_tests
                ;;
            *)
                print_error "Unknown test group: $TEST_GROUP"
                show_usage
                exit 1
                ;;
        esac
    fi

    # Generate documentation for failures
    if [[ ${#FAILED_TESTS[@]} -gt 0 ]]; then
        generate_failures_doc
    fi

    # Summary
    echo
    print_header "📊 Test Summary"

    local passed_groups=${#PASSED_TESTS[@]}
    local failed_groups=${#FAILED_TESTS[@]}
    local total_groups=$((passed_groups + failed_groups))

    print_info "Test Groups: $total_groups total - $passed_groups passed, $failed_groups failed"
    print_info "Individual Tests: $TOTAL_INDIVIDUAL_TESTS total - $TOTAL_INDIVIDUAL_PASSED passed, $TOTAL_INDIVIDUAL_FAILED failed"

    if [[ $failed_groups -eq 0 ]]; then
        print_success "All tests passed! 🎉"
        exit 0
    else
        print_failure "$failed_groups test group(s) failed"
        print_info "Check $FAILURES_FILE for details on running individual tests"
        exit 1
    fi
}

# Run main function
main "$@"
