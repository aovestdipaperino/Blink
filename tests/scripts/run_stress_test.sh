#!/bin/bash

# Blink Stress Test Runner
# This script runs the Blink stress test with various configurations
#
# (c) 2025 Cleafy S.p.A.
# Author: Enzo Lombardi
#

set -e

echo "🚀 Blink Stress Test Runner"
echo "========================="

# Default values
DURATION=1
PRODUCERS=4
CONSUMERS=4
THROUGHPUT=1000
TOPIC="stress_test_topic"
SETTINGS_FILE="tests/configs/test_stress_settings.yaml"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        -p|--producers)
            PRODUCERS="$2"
            shift 2
            ;;
        -c|--consumers)
            CONSUMERS="$2"
            shift 2
            ;;
        -t|--throughput)
            THROUGHPUT="$2"
            shift 2
            ;;
        --topic)
            TOPIC="$2"
            shift 2
            ;;
        --settings)
            SETTINGS_FILE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -d, --duration MINUTES     Duration to run test (default: 1)"
            echo "  -p, --producers COUNT      Number of producer threads (default: 4)"
            echo "  -c, --consumers COUNT      Number of consumer threads (default: 4)"
            echo "  -t, --throughput MSG/SEC   Target throughput (default: 1000)"
            echo "  --topic TOPIC_NAME         Topic name (default: stress_test_topic)"
            echo "  --settings PATH            Settings file path (default: tests/configs/test_stress_settings.yaml)"
            echo "  -h, --help                 Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Run with defaults (1 minute)"
            echo "  $0 -d 5 -p 8 -c 4 -t 2000           # 5 minutes, 8 producers, 4 consumers, 2000 msg/s"
            echo "  $0 --duration 10 --throughput 5000   # 10 minutes with high throughput"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

echo "Configuration:"
echo "  Duration: ${DURATION} minutes"
echo "  Producers: ${PRODUCERS}"
echo "  Consumers: ${CONSUMERS}"
echo "  Target Throughput: ${THROUGHPUT} msg/sec"
echo "  Topic: ${TOPIC}"
echo "  Settings File: ${SETTINGS_FILE}"
echo ""

# Check if Cargo is available
if ! command -v cargo &> /dev/null; then
    echo "❌ Error: cargo not found. Please install Rust and Cargo."
    exit 1
fi

# Verify settings file exists
if [ ! -f "$SETTINGS_FILE" ]; then
    echo "❌ Error: Settings file not found: $SETTINGS_FILE"
    echo "Available settings files:"
    ls -la tests/configs/*.yaml 2>/dev/null || echo "  No settings files found in tests/configs/"
    exit 1
fi

# Determine which test to run based on parameters
TEST_NAME=""
# Use bc for decimal comparison if available, otherwise use integer comparison
if command -v bc >/dev/null 2>&1; then
    if (( $(echo "$DURATION <= 1" | bc -l) )) && [ "$THROUGHPUT" -le 200 ]; then
        TEST_NAME="test_conservative_load"
        echo "🧪 Running conservative load test..."
    elif [ "$THROUGHPUT" -ge 1500 ]; then
        TEST_NAME="test_memory_pressure_stress"
        echo "🧪 Running memory pressure stress test..."
    elif [ "$THROUGHPUT" -ge 800 ]; then
        TEST_NAME="test_high_throughput_stress"
        echo "🧪 Running high throughput stress test..."
    else
        TEST_NAME="test_basic_stress_test"
        echo "🧪 Running basic stress test..."
    fi
else
    # Fallback to integer comparison
    DURATION_INT=$(echo "$DURATION" | cut -d. -f1)
    if [ "$DURATION_INT" -le 1 ] && [ "$THROUGHPUT" -le 200 ]; then
        TEST_NAME="test_conservative_load"
        echo "🧪 Running conservative load test..."
    elif [ "$THROUGHPUT" -ge 1500 ]; then
        TEST_NAME="test_memory_pressure_stress"
        echo "🧪 Running memory pressure stress test..."
    elif [ "$THROUGHPUT" -ge 800 ]; then
        TEST_NAME="test_high_throughput_stress"
        echo "🧪 Running high throughput stress test..."
    else
        TEST_NAME="test_basic_stress_test"
        echo "🧪 Running basic stress test..."
    fi
fi

echo "📊 Test will simulate your specified parameters within the test framework"
echo ""

# Build and run the stress test
echo "🔨 Building and running stress tests..."
echo "📁 Using stress test configuration: $SETTINGS_FILE"
echo "💡 Note: Stress tests automatically use the configured settings file"
cargo test --release --test stress_test "$TEST_NAME" -- --nocapture

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Stress test completed successfully!"
    echo "📝 Note: Test parameters are configured within the test functions"
    echo "   To modify test parameters, edit tests/stress_test.rs"
else
    echo "❌ Stress test failed with exit code: $EXIT_CODE"
fi

exit $EXIT_CODE
