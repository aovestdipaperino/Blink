.PHONY: build clean install

# Default target
all: build

# Build the WASM plugin component
build:
	@echo "Building demo plugin..."
	cargo build --target wasm32-wasip1 --release
	@echo "Converting to WASM component..."
	wasm-tools component new target/wasm32-wasip1/release/demo_plugin.wasm \
		-o demo-plugin.wasm
	@echo "Demo plugin built successfully: demo-plugin.wasm"

# Clean build artifacts
clean:
	cargo clean
	rm -f demo-plugin.wasm

# Install wasm-tools if not present
install-tools:
	@if ! command -v wasm-tools >/dev/null 2>&1; then \
		echo "Installing wasm-tools..."; \
		cargo install wasm-tools; \
	else \
		echo "wasm-tools already installed"; \
	fi

# Setup target for first-time users
setup: install-tools
	@echo "Adding wasm32-wasip1 target..."
	rustup target add wasm32-wasip1

# Help target
help:
	@echo "Available targets:"
	@echo "  build       - Build the demo plugin (default)"
	@echo "  clean       - Clean build artifacts"
	@echo "  setup       - Install required tools and targets"
	@echo "  help        - Show this help message"
	@echo ""
	@echo "To use the plugin, add this to your settings.yaml:"
	@echo "plugin_paths:"
	@echo "  - \"demo-plugin/demo-plugin.wasm\""
