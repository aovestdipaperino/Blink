#!/bin/bash

# Script to run Blink offload tests with automatic Blink startup/shutdown
# This ensures tests have a running Blink instance
#
# (c) 2025 Cleafy S.p.A.
# Author: Enzo Lombardi
#

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BLINK_PORT=9092
BLINK_PID=""
TEST_FAILED=0
SETTINGS_FILE=""

# Function to print colored output
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
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Cleanup function
cleanup() {
    print_step "Cleaning up..."

    # Kill Blink if it's running
    if [ -n "$BLINK_PID" ] && kill -0 $BLINK_PID 2>/dev/null; then
        print_info "Stopping Blink (PID: $BLINK_PID)"
        kill $BLINK_PID 2>/dev/null || true

        # Wait for process to terminate
        for i in {1..10}; do
            if ! kill -0 $BLINK_PID 2>/dev/null; then
                break
            fi
            sleep 0.5
        done

        # Force kill if still running
        if kill -0 $BLINK_PID 2>/dev/null; then
            print_warn "Force killing Blink"
            kill -9 $BLINK_PID 2>/dev/null || true
        fi
    fi

    # Clean up offload directory
    if [ -d "blink_offload" ]; then
        print_info "Removing offload directory"
        rm -rf blink_offload/
    fi
}

# Trap cleanup on exit
trap cleanup EXIT

# Function to wait for Blink to be ready
wait_for_blink() {
    local max_attempts=30
    local attempt=0

    print_info "Waiting for Blink to be ready on port $BLINK_PORT..."

    while [ $attempt -lt $max_attempts ]; do
        if nc -z localhost $BLINK_PORT 2>/dev/null; then
            print_info "Blink is ready!"
            return 0
        fi

        attempt=$((attempt + 1))
        sleep 0.5

        # Check if process is still running
        if [ -n "$BLINK_PID" ] && ! kill -0 $BLINK_PID 2>/dev/null; then
            print_error "Blink process died unexpectedly"
            return 1
        fi
    done

    print_error "Blink failed to start within 15 seconds"
    return 1
}

# Parse command line arguments
USE_TEST_SETTINGS=false
SPECIFIC_TEST=""
VERBOSE=false
BUILD_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --test-settings)
            USE_TEST_SETTINGS=true
            shift
            ;;
        --test)
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --build-only)
            BUILD_ONLY=true
            shift
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --test-settings    Use tests/configs/test_settings.yaml with small memory limit"
            echo "  --test <name>      Run specific test"
            echo "  --verbose          Show detailed output"
            echo "  --build-only       Only build, don't run tests"
            echo "  --help             Show this help message"
            echo ""
            echo "Note: Uses Blink's --settings CLI argument instead of modifying settings.yaml"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Main execution
echo "🚀 Blink Offload File Test Runner"
echo "================================"
echo

# Step 1: Determine settings file
print_step "Determining configuration..."

if [ "$USE_TEST_SETTINGS" = true ]; then
    SETTINGS_FILE="tests/configs/test_settings.yaml"
    if [ ! -f "$SETTINGS_FILE" ]; then
        print_error "tests/configs/test_settings.yaml not found!"
        exit 1
    fi
    print_info "Will use test settings: $SETTINGS_FILE (max_memory: 1MB)"
else
    SETTINGS_FILE="settings.yaml"
    if [ ! -f "$SETTINGS_FILE" ]; then
        print_error "settings.yaml not found!"
        echo "Please create a settings.yaml file or use --test-settings"
        exit 1
    fi
    print_info "Will use existing settings: $SETTINGS_FILE"
fi

# Step 2: Build Blink
print_step "Building Blink..."

if [ "$VERBOSE" = true ]; then
    cargo build --release
else
    cargo build --release >/dev/null 2>&1 || {
        print_error "Failed to build Blink"
        exit 1
    }
fi

if [ "$BUILD_ONLY" = true ]; then
    print_info "Build complete (--build-only specified)"
    exit 0
fi

# Step 3: Start Blink
print_step "Starting Blink on port $BLINK_PORT with settings: $SETTINGS_FILE"

# Clean up any existing offload directory
rm -rf blink_offload/

# Start Blink in the background with specified settings file
if [ "$VERBOSE" = true ]; then
    RUST_LOG=debug cargo run --release --bin blink -- --settings "$SETTINGS_FILE" &
else
    cargo run --release --bin blink -- --settings "$SETTINGS_FILE" >/dev/null 2>&1 &
fi
BLINK_PID=$!

print_info "Blink started with PID: $BLINK_PID using settings: $SETTINGS_FILE"

# Wait for Blink to be ready
if ! wait_for_blink; then
    print_error "Failed to start Blink"
    exit 1
fi

# Step 4: Run tests
print_step "Running offload file tests..."
echo

if [ -n "$SPECIFIC_TEST" ]; then
    # Run specific test
    print_info "Running specific test: $SPECIFIC_TEST"

    if [ "$VERBOSE" = true ]; then
        RUST_LOG=debug cargo test --test tests "offload_tests::$SPECIFIC_TEST" -- --nocapture || TEST_FAILED=1
    else
        cargo test --test tests "offload_tests::$SPECIFIC_TEST" || TEST_FAILED=1
    fi
else
    # Run all offload tests
    print_info "Running all offload tests"

    if [ "$VERBOSE" = true ]; then
        RUST_LOG=debug cargo test --test tests offload_tests:: -- --nocapture || TEST_FAILED=1
    else
        cargo test --test tests offload_tests:: || TEST_FAILED=1
    fi
fi

# Step 5: Check for leftover offload files
echo
print_step "Checking for leftover offload files..."

if [ -d "blink_offload" ]; then
    OFFLOAD_COUNT=$(find blink_offload -name "*.offload" 2>/dev/null | wc -l | tr -d ' ')
    if [ "$OFFLOAD_COUNT" -gt 0 ]; then
        print_warn "Found $OFFLOAD_COUNT leftover offload files"
        if [ "$VERBOSE" = true ]; then
            ls -la blink_offload/*.offload 2>/dev/null || true
        fi
    else
        print_info "No leftover offload files found"
    fi
else
    print_info "Offload directory was cleaned up"
fi

# Report results
echo
echo "================================"
if [ $TEST_FAILED -eq 0 ]; then
    print_info "✅ All tests passed!"
    exit 0
else
    print_error "❌ Some tests failed!"
    exit 1
fi
