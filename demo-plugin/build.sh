#!/bin/bash

# Build script for demo plugin
#
# (c) 2025 Cleafy S.p.A.
# Author: Enzo Lombardi
#

set -e

echo "Building demo plugin..."

# Build the WASM module
cargo build --target wasm32-wasip1 --release

# Check if wasm-tools is installed
if ! command -v wasm-tools &> /dev/null; then
    echo "wasm-tools not found. Installing..."
    cargo install wasm-tools
fi

# Convert the WASM module to a component
echo "Converting to WASM component..."
wasm-tools component new target/wasm32-wasip1/release/demo_plugin.wasm \
    -o demo-plugin.wasm

echo "Demo plugin built successfully: demo-plugin.wasm"
echo ""
echo "To use this plugin, add the following to your settings.yaml:"
echo "plugin_paths:"
echo "  - \"demo-plugin/demo-plugin.wasm\""
