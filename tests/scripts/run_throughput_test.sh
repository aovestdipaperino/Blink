#!/bin/bash

# Throughput Test Runner Script
# Runs the simplified throughput test based on Kitt's approach
# Tests 4 producers and 4 consumers with 10K msg/sec minimum target
#
# (c) 2025 Cleafy S.p.A.
# Author: Enzo Lombardi
#

set -e

# Script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default values
DURATION=15
VERBOSE=false
HELP=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
Throughput Test Runner - Blink Kafka Implementation

USAGE:
    $0 [OPTIONS]

DESCRIPTION:
    Runs a simplified throughput test based on Kitt's approach with:
    - 4 producers and 4 consumers
    - 8 partitions (2 per producer/consumer pair)
    - Target minimum throughput of 10K messages/second
    - Balanced producer/consumer load with backlog control

OPTIONS:
    -d, --duration SECONDS    Measurement duration in seconds (default: 15)
    -v, --verbose            Enable verbose output
    -h, --help              Show this help message

EXAMPLES:
    # Run with default settings (15 second measurement)
    $0

    # Run with longer measurement period
    $0 --duration 30

    # Run with verbose output
    $0 --verbose

REQUIREMENTS:
    - Rust and Cargo installed
    - external-kafka feature enabled
    - rdkafka dependency available

FEATURES:
    - Automatic topic creation and cleanup
    - Real-time throughput monitoring
    - Backlog-based flow control
    - Pass/fail validation against 10K msg/sec target
    - Kitt-style balanced load testing

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            HELP=true
            shift
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Show help if requested
if [ "$HELP" = true ]; then
    show_help
    exit 0
fi

# Validate duration
if ! [[ "$DURATION" =~ ^[0-9]+$ ]] || [ "$DURATION" -lt 5 ]; then
    print_error "Duration must be a number >= 5 seconds"
    exit 1
fi

print_status "Starting Blink Throughput Test Runner"
print_status "========================================="
print_status "Duration: ${DURATION} seconds"
print_status "Target: 10,000 messages/second minimum"
print_status "Configuration: 4 producers + 4 consumers, 8 partitions"
print_status ""

# Change to project root
cd "$PROJECT_ROOT"

# Check if Cargo.toml exists
if [ ! -f "Cargo.toml" ]; then
    print_error "Cargo.toml not found. Please run this script from the blink project directory."
    exit 1
fi

# Check if external-kafka feature is available
if ! grep -q "external-kafka" Cargo.toml; then
    print_error "external-kafka feature not found in Cargo.toml"
    exit 1
fi

# Set environment variables for the test
export RUST_LOG=${RUST_LOG:-info}
if [ "$VERBOSE" = true ]; then
    export RUST_LOG=debug
fi

# Build the project first
print_status "Building project with external-kafka feature..."
if [ "$VERBOSE" = true ]; then
    cargo build --features external-kafka
else
    cargo build --features external-kafka --quiet
fi

if [ $? -ne 0 ]; then
    print_error "Failed to build project"
    exit 1
fi

print_success "Build completed successfully"

# Run the throughput test
print_status "Running throughput test..."
print_status "Test will measure for $DURATION seconds after a 5-second warmup"
print_status ""

# Set test duration if different from default
if [ "$DURATION" != "15" ]; then
    print_warning "Custom duration specified. Note: test code uses hardcoded 15s measurement period."
    print_warning "For custom durations, modify MEASUREMENT_DURATION_SECS in throughput_test.rs"
fi

# Run the test
if [ "$VERBOSE" = true ]; then
    cargo test --test throughput_test --features external-kafka -- --nocapture
    TEST_RESULT=$?
else
    cargo test --test throughput_test --features external-kafka --quiet
    TEST_RESULT=$?
fi

print_status ""
print_status "========================================"

# Check test results
if [ $TEST_RESULT -eq 0 ]; then
    print_success "THROUGHPUT TEST PASSED! ✅"
    print_success "Achieved target minimum throughput of 10K msg/sec"
else
    print_error "THROUGHPUT TEST FAILED! ❌"
    print_error "Did not achieve target minimum throughput of 10K msg/sec"
fi

print_status ""
print_status "Test Configuration Details:"
print_status "- Producers: 4"
print_status "- Consumers: 4"
print_status "- Partitions: 8 (2 per producer/consumer pair)"
print_status "- Message Size: 1024 bytes"
print_status "- Max Backlog: 1000 messages"
print_status "- Warmup Period: 5 seconds"
print_status "- Measurement Period: 15 seconds"
print_status ""

if [ $TEST_RESULT -eq 0 ]; then
    print_status "For more detailed performance analysis, consider running:"
    print_status "  ./tests/scripts/run_stress_test.sh"
else
    print_status "To debug performance issues, try:"
    print_status "  $0 --verbose"
    print_status "  cargo test --test throughput_test --features external-kafka -- --nocapture"
    print_status "  ./tests/scripts/run_stress_test.sh --verbose"
fi

exit $TEST_RESULT
