# Blink WASM Plugin System

This document describes the production-ready WebAssembly (WASM) plugin system for Blink, which allows you to extend Blink's functionality with custom plugins written in Rust and compiled to WebAssembly components.

## Overview

The Blink plugin system uses WebAssembly Interface Types (WIT) and the Component Model to provide a type-safe, sandboxed environment for extending Blink's message processing pipeline. Plugins are loaded dynamically at runtime and can hook into both initialization and record processing phases.

**Key Features:**
- **Host Logging**: Plugins can log messages back to the host using `log(level, message)`
- **Type Safety**: WIT interface ensures correct plugin implementation
- **Memory Isolation**: WASM sandbox prevents plugin errors from crashing the host
- **Dynamic Loading**: Plugins are loaded at runtime from configuration
- **Production Ready**: Built on Wasmtime Component Model

## Architecture

- **Runtime**: Wasmtime Component Model
- **Interface**: WebAssembly Interface Types (WIT)
- **Target**: `wasm32-wasip1` 
- **Host Functions**: Plugin-to-host logging system
- **Isolation**: Memory-safe WASM sandbox
- **Configuration**: YAML-based plugin loading

## Plugin Interface

### WIT Definition

The complete WIT interface (`src/resources/blink-plugin.wit`):

```wit
package blink:plugin@0.1.0;

interface host {
    // Log levels for the host logging function
    enum log-level {
        trace,
        debug,
        info,
        warn,
        error,
    }

    // Host function: allows plugin to log messages to the host
    log: func(level: log-level, message: string);
}

interface plugin {
    // Initialize the plugin, returns an optional port number or None
    init: func() -> option<u16>;

    // Handle a record being produced
    // Returns true to continue processing, false to reject the record
    on-record: func(topic: string, partition: u32, key: option<string>, value: string) -> bool;
}

world blink-plugin {
    import host;
    export plugin;
}
```

### Plugin Methods

- **`init()`**: Called once during Blink startup
  - Return `None` for no special requirements
  - Return `Some(port)` if your plugin needs a specific port
  - Use `log()` to send initialization messages to the host

- **`on-record(topic, partition, key, value)`**: Called for each produced record
  - `topic`: The Kafka topic name
  - `partition`: Partition number (u32)
  - `key`: Optional record key 
  - `value`: Record value as string
  - Return `true` to allow record processing, `false` to reject

### Host Functions

- **`log(level, message)`**: Send log messages to the host
  - `level`: One of `trace`, `debug`, `info`, `warn`, `error`
  - `message`: Log message as `&str`
  - Messages appear in host logs with `🔌 Plugin:` prefix

## Configuration

Add plugins to your `settings.yaml`:

```yaml
# Plugin Configuration
plugin_paths:
  - "demo-plugin/demo-plugin.wasm"
  - "path/to/your/plugin.wasm"
```

## Demo Plugin

### Building the Demo Plugin

The demo plugin is located in `demo-plugin/` and demonstrates all plugin capabilities:

```bash
# Prerequisites (first time only)
cd demo-plugin
rustup target add wasm32-wasip1
cargo install wasm-tools

# Download WASI adapter
curl -L -o wasi_snapshot_preview1.reactor.wasm \
  https://github.com/bytecodealliance/wasmtime/releases/download/v25.0.0/wasi_snapshot_preview1.reactor.wasm

# Build the plugin
cargo build --target wasm32-wasip1 --release

# Convert to component
wasm-tools component new target/wasm32-wasip1/release/demo_plugin.wasm \
  -o demo-plugin.wasm \
  --adapt wasi_snapshot_preview1=wasi_snapshot_preview1.reactor.wasm
```

### Demo Plugin Code

```rust
wit_bindgen::generate!({
    world: "blink-plugin",
    path: "wit"
});

use blink::plugin::host::{log, LogLevel};
use exports::blink::plugin::plugin::Guest;

struct DemoPlugin;

impl Guest for DemoPlugin {
    fn init() -> Option<u16> {
        // Use host logging instead of println!
        log(LogLevel::Info, "Hello world from Rust WASM plugin!");
        log(LogLevel::Debug, "Demo plugin initialization complete");
        None
    }

    fn on_record(topic: String, partition: u32, key: Option<String>, value: String) -> bool {
        // Log dots for each record
        log(LogLevel::Trace, ".");
        
        // Detailed processing info
        let key_str = key.as_deref().unwrap_or("<none>");
        let message = format!(
            "Processing record: topic={}, partition={}, key={}, value_len={}",
            topic, partition, key_str, value.len()
        );
        log(LogLevel::Debug, &message);
        
        true // Always allow the record
    }
}

export!(DemoPlugin);
```

## Creating Your Own Plugin

### 1. Project Setup

```bash
cargo new --lib my-plugin
cd my-plugin
```

### 2. Configure Cargo.toml

```toml
[package]
name = "my-plugin"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.33.0"

[profile.release]
lto = true
opt-level = "s"
strip = true
```

### 3. Copy WIT Interface

```bash
cp -r path/to/blink/wit ./
```

### 4. Implement Your Plugin

```rust
wit_bindgen::generate!({
    world: "blink-plugin",
    path: "wit"
});

use blink::plugin::host::{log, LogLevel};
use exports::blink::plugin::plugin::Guest;

struct MyPlugin;

impl Guest for MyPlugin {
    fn init() -> Option<u16> {
        log(LogLevel::Info, "MyPlugin initialized");
        None
    }

    fn on_record(topic: String, partition: u32, key: Option<String>, value: String) -> bool {
        // Your record processing logic here
        if value.is_empty() {
            log(LogLevel::Warn, "Rejecting empty record");
            return false;
        }
        
        log(LogLevel::Debug, &format!("Processing {} bytes on topic {}", 
                                     value.len(), topic));
        true
    }
}

export!(MyPlugin);
```

### 5. Build Your Plugin

```bash
# Download WASI adapter (if not already present)
curl -L -o wasi_snapshot_preview1.reactor.wasm \
  https://github.com/bytecodealliance/wasmtime/releases/download/v25.0.0/wasi_snapshot_preview1.reactor.wasm

# Build WASM module
cargo build --target wasm32-wasip1 --release

# Convert to component
wasm-tools component new target/wasm32-wasip1/release/my_plugin.wasm \
  -o my-plugin.wasm \
  --adapt wasi_snapshot_preview1=wasi_snapshot_preview1.reactor.wasm

# Verify
wasm-tools validate my-plugin.wasm
```

### 6. Configure Blink

Add to `settings.yaml`:

```yaml
plugin_paths:
  - "path/to/my-plugin.wasm"
```

## Running with Plugins

### Start Blink with Plugins

```bash
# Normal operation
./target/release/blink -s settings.yaml

# Debug logging to see plugin messages
LOG_LEVEL=DEBUG ./target/release/blink -s settings.yaml
```

### Expected Output

```
INFO blink::server: Initializing plugin system with 1 plugins
INFO blink::plugins: Using real WASM plugins
INFO blink::plugins: Loading 1 WASM plugins
INFO blink::plugins: Registered plugin: demo-plugin.wasm at demo-plugin/demo-plugin.wasm
INFO blink::plugins: Plugin 'demo-plugin.wasm' initialized successfully
INFO blink::plugins: 🔌 Plugin: Hello world from Rust WASM plugin!
DEBUG blink::plugins: 🔌 Plugin: Demo plugin initialization complete
INFO blink::server: Plugin system initialized successfully
```

## Testing Your Plugin

### Send Test Messages

```bash
# Create topic and send message
curl -X POST "http://localhost:30005/produce" \
  -H "Content-Type: application/json" \
  -d '{"topic": "test-topic", "partition": 0, "key": "test-key", "value": "Hello Plugin!"}'
```

### Monitor Plugin Activity

```bash
# Watch plugin logs
LOG_LEVEL=DEBUG ./target/release/blink -s settings.yaml | grep "🔌 Plugin:"
```

## Troubleshooting

### Build Issues

**Problem**: `failed to resolve import wasi_snapshot_preview1::fd_write`
```bash
# Solution: Use WASI adapter
wasm-tools component new target/wasm32-wasip1/release/plugin.wasm \
  -o plugin.wasm \
  --adapt wasi_snapshot_preview1=wasi_snapshot_preview1.reactor.wasm
```

**Problem**: `mismatched types expected &str, found String`
```rust
// Correct usage
log(LogLevel::Info, "message");           // ✓
log(LogLevel::Info, &format!("count: {}", n)); // ✓

// Wrong usage  
log(LogLevel::Info, "message".to_string()); // ✗
```

### Runtime Issues

**Problem**: Plugin not loading
- Check file path in `settings.yaml`
- Verify component: `wasm-tools validate plugin.wasm`
- Check Blink logs for errors

**Problem**: No plugin output
- Ensure `LOG_LEVEL=DEBUG` is set
- Verify plugin `on_record` is called by checking topic creation
- Check plugin returns `true` from methods

### Debug Commands

```bash
# Validate WASM component
wasm-tools validate plugin.wasm

# Inspect component
wasm-tools component wit plugin.wasm

# Check logs
LOG_LEVEL=DEBUG ./target/release/blink -s settings.yaml | head -20
```

## Plugin Development Tips

### Best Practices

1. **Use host logging**: Always use `log()` instead of `println!`
2. **Handle errors gracefully**: Return appropriate boolean values
3. **Keep init() fast**: Initialization blocks server startup
4. **Optimize on_record()**: Called for every message
5. **Use appropriate log levels**: 
   - `Error`: Critical failures
   - `Warn`: Important warnings  
   - `Info`: General information
   - `Debug`: Detailed debugging
   - `Trace`: Very verbose output

### Example Patterns

```rust
// Content filtering
fn on_record(topic: String, partition: u32, key: Option<String>, value: String) -> bool {
    // Reject empty values
    if value.is_empty() {
        log(LogLevel::Warn, "Rejecting empty record");
        return false;
    }
    
    // Topic-based filtering
    if topic.starts_with("sensitive-") && key.is_none() {
        log(LogLevel::Warn, "Sensitive topic requires key");
        return false;
    }
    
    log(LogLevel::Debug, &format!("Processed {}", topic));
    true
}
```

## Supported Languages

While this guide focuses on Rust, any language that compiles to WASM and supports WIT can be used:
- **Rust** (recommended, best tooling)
- **C/C++** (via wasi-sdk)
- **Go** (via TinyGo)
- **JavaScript/TypeScript** (via ComponentizeJS)

## Security & Performance

- **Memory Isolation**: Plugins run in WASM sandbox
- **Type Safety**: WIT prevents type confusion
- **Resource Limits**: WASM runtime enforces limits  
- **No System Access**: Only host functions available
- **Performance**: Near-native speed via Wasmtime JIT

## Contributing

To contribute to the plugin system:
1. Follow existing WIT interface definitions
2. Add comprehensive tests
3. Update documentation
4. Provide example plugins

---

**The Blink WASM plugin system provides a production-ready way to extend Blink with custom functionality while maintaining security, performance, and type safety.**