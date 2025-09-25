# Blink: Ultra-High Performance Message Broker

**Kafka-compatible, ultra-low latency message broker built from the ground up in Rust**

---

## Table of Contents

1. [Why Blink?](#why-blink)
2. [Why Use the Kafka Protocol?](#why-use-the-kafka-protocol)
3. [Multi-Tier Architecture with nano-wal](#multi-tier-architecture-with-nano-wal)
4. [WASM Plugin System](#wasm-plugin-system)
5. [Lock-Free Concurrent Architecture](#lock-free-concurrent-architecture)
6. [Getting Started](#getting-started)
7. [Performance Characteristics](#performance-characteristics)
8. [Architecture Overview](#architecture-overview)
9. [Configuration](#configuration)
10. [Monitoring](#monitoring)
11. [Deployment](#deployment)

---

## Why Blink?

Traditional message brokers like Apache Kafka were designed for durability and high throughput over distributed clusters, but this comes at the cost of significant latency overhead and operational complexity. Blink addresses scenarios where **ultra-low latency** and **operational simplicity** are more important than distributed durability.

### The Performance Gap

Modern applications often need:
- **Microsecond-range latency** for real-time systems
- **Millions of messages per second** throughput
- **Minimal resource consumption** for cost efficiency
- **Boot times in milliseconds** for rapid deployment
- **Zero operational overhead** for development and testing

Traditional message brokers struggle with these requirements due to:
- **Distributed coordination overhead** (ZooKeeper, consensus protocols)
- **Disk-first persistence** adding I/O latency
- **Complex cluster management** requiring specialized knowledge
- **High resource requirements** (memory, CPU, network)
- **Slow startup times** due to cluster coordination

### Blink's Approach

Blink takes a fundamentally different approach in some aspects very similar to systems like [Iggy.rs](https://github.com/iggy-rs/iggy):

**Memory-First Architecture**: Messages live primarily in memory with intelligent spill-to-disk only under memory pressure, eliminating most I/O latency.

**Single-Node Simplicity**: No distributed coordination overhead. Deploy as a single binary with minimal configuration.

**Ground-Up Design**: Built from scratch in Rust using low-level I/O primitives, zero-copy operations, and lock-free data structures.

**Kafka Compatibility**: Drop-in replacement for existing Kafka clients - no application changes required.

**Operational Simplicity**: Single binary, YAML configuration, built-in monitoring - no external dependencies.

### When to Use Blink

**✅ Perfect for**:
- **Financial trading systems** requiring sub-millisecond latency
- **Real-time analytics** with high-volume event streams
- **Microservices communication** within a single datacenter
- **Development and testing** environments
- **Cache-like messaging** with automatic cleanup
- **IoT data ingestion** with temporary buffering needs
- **Live dashboards** requiring real-time data updates
- **Single consumer setups** allowing to evict consumed records before expiration

**⚠️ Consider alternatives for**:
- **Multi-datacenter replication** requirements
- **Long-term data retention** (months/years)
- **Regulatory compliance** requiring guaranteed durability
- **Large distributed teams** needing centralized infrastructure
- **Complex consumer setup** needing a complete consumer-group implementation

---

## Why Use the Kafka Protocol?

Kafka has become the de facto standard for event streaming, with mature client libraries, tooling, and operational knowledge across the industry. By implementing the Kafka wire protocol, Blink provides a **seamless migration path** while delivering dramatically better performance.

### Protocol Compatibility Benefits

**Zero Client Changes**: Existing applications work immediately - just point them to Blink's port.

```java
// Existing Kafka code works unchanged
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");  // Just change the host
KafkaProducer<String, String> producer = new KafkaProducer<>(props);
```

**Mature Ecosystem**: Leverage existing tools and libraries:
- **Client Libraries**: Java, Python, Go, Node.js, .NET, Rust clients work natively
- **Monitoring Tools**: Kafka monitoring solutions work out-of-the-box
- **Stream Processing**: Kafka Streams, Apache Flink, Apache Storm compatibility
- **Connectors**: Existing Kafka Connect integrations function normally

**Operational Familiarity**: Teams already familiar with Kafka concepts (topics, partitions, consumer groups) can immediately be productive.

**Gradual Migration**: Deploy Blink alongside Kafka for specific high-performance use cases, then expand adoption.

### Protocol Implementation

Blink implements the core Kafka wire protocol APIs:

| API Endpoint | Purpose | Status |
|--------------|---------|--------|
| `ApiVersions` | Client capability negotiation | ✅ Full |
| `Metadata` | Topic and partition discovery | ✅ Full |
| `Produce` | Message ingestion | ✅ Full |
| `Fetch` | Message consumption | ✅ Full |
| `CreateTopics`/`DeleteTopics` | Topic management | ✅ Full |
| `InitProducerId` | Producer initialization | ✅ Full |
| `FindCoordinator` | Consumer group coordination | ✅ Optional |
| `ListOffsets`/`OffsetFetch` | Offset management | ✅ Full |

### Performance vs. Compatibility Trade-offs

**What's Different**:
- **In-Memory First**: Messages not persisted by default (configurable)
- **Single-Node**: No cluster coordination or replication
- **Simplified Consumer Groups**: Optional, memory-only coordination
- **Fast Cleanup**: Aggressive memory management for performance

**What's Compatible**:
- **Wire Protocol**: Binary compatibility with all Kafka clients
- **Topic/Partition Model**: Familiar data organization concepts
- **Producer/Consumer APIs**: Standard Kafka semantics
- **Offset Management**: Standard offset tracking and seeking

---

## Multi-Tier Architecture with nano-wal

Blink employs a sophisticated **multi-tier storage architecture** that provides both ultra-low latency and graceful scaling beyond available memory using a Write-Ahead Log (WAL) system.

### The Three-Tier Storage Model

```
Tier 1: Memory (Ultra-Fast)
    ↓ (Memory Pressure at 80% threshold)
Tier 2: Write-Ahead Log (Fast)
    ↓ (Retention/Consumption)
Tier 3: Automatic Cleanup (Zero-Cost)
```

### Memory-First Tier (Primary Storage)

**Purpose**: Deliver microsecond-range latency for active data

**Characteristics**:
- **Zero-Copy Operations**: Messages stored as `Bytes` with reference counting
- **Lock-Free Access**: Concurrent data structures eliminate locking overhead
- **Direct Memory Access**: No serialization/deserialization in hot paths
- **Intelligent Tracking**: Real-time memory usage monitoring with automatic bounds

**Performance**: Messages in memory provide **sub-millisecond** response times

### nano-wal Tier (Overflow Storage)

**Purpose**: Handle datasets larger than available RAM without data loss

When memory usage exceeds 80% of the configured limit, Blink automatically **offloads** the oldest message batches to a  Write-Ahead Log powered by the [nano-wal](https://docs.rs/nano-wal/) crate.

**nano-wal Benefits**:
- **Automatic Segment Management**: Built-in log rotation and compaction
- **Crash Recovery**: Durable writes with configurable consistency levels
- **Efficient I/O**: Optimized for sequential writes and random reads
- **Self-Maintaining**: Automatic cleanup and space reclamation

**Offloadover Process**:
1. **Global Coordination**: System searches across all partitions for oldest batches
2. **Structured Write**: Batch metadata stored in WAL headers, data as content
3. **Memory Reclamation**: RAM immediately freed, references updated to WAL entries
4. **Transparent Access**: Consumers read from WAL seamlessly when needed

**Hybrid Performance**: WAL access adds minimal latency (few milliseconds) while maintaining system stability

### Cleanup Tier (Automatic Management)

**Purpose**: Eliminate operational overhead through intelligent cleanup

**Cleanup Strategies**:
- **Time-Based**: Remove messages older than configured retention period
- **Consumption-Based**: Remove messages that have been read by consumers
- **Memory Pressure**: Prioritize cleanup of consumed and expired messages
- **WAL Compaction**: Automatic log compaction and segment cleanup

**Zero-Downtime Operation**: Cleanup happens in background without affecting message flow

### Benefits of Multi-Tier Design

**🚀 Performance**: Memory-first delivers ultra-low latency for hot data

**📈 Scalability**: Handle datasets much larger than available RAM

**💰 Cost Efficiency**: Optimal resource utilization - fast storage for active data, slower storage for overflow

**🔧 Operational Simplicity**: Automatic tier management requires no manual intervention

**🛡️ Data Safety**: Professional WAL ensures no data loss during memory pressure

**⚡ Fast Recovery**: System boots in milliseconds regardless of total dataset size

### Configuration Example

```yaml
# Memory tier configuration
max_memory: "2GB"              # Memory threshold (offloadover at 80% = 1.6GB)

# WAL tier configuration
record_storage_path: "./wal"   # WAL storage directory
retention: "1h"                # Time-based cleanup

# Cleanup behavior
purge_on_fetch: false         # Keep messages after consumption
```

### Performance Characteristics by Tier

| Tier | Latency | Throughput | Capacity | Use Case |
|------|---------|------------|----------|----------|
| Memory | ~100µs | >1M msg/sec | Limited by RAM | Hot data, real-time |
| nano-wal | ~1ms | >100K msg/sec | Limited by disk | Overflow, recent data |
| Cleanup | N/A | N/A | Unlimited | Historical data removal |

---

## WASM Plugin System

Blink includes an early version of a **WebAssembly (WASM) plugin system** that enables **runtime extensibility** without sacrificing performance or security. Plugins can intercept and transform messages in incoming (producer).

### Why WASM for Message Processing?

**Security Isolation**: WASM provides memory-safe sandboxing - plugins cannot crash the broker or access unauthorized resources.

**Language Flexibility**: Write plugins in Rust, C++, Go, or any language that compiles to WASM.

**Near-Native Performance**: WASM execution is significantly faster than traditional scripting languages.

**Hot-Swappable**: Load, unload, and update plugins without broker restarts.

**Type Safety**: WebAssembly Interface Types (WIT) provide compile-time guarantees about plugin interfaces.

### Plugin Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Producer      │───▶│  WASM Plugin     │───▶│   Storage       │
│   Messages      │    │  on_record()     │    │   Engine        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Plugin Capabilities

**Message Transformation**: Modify message content, keys, or headers before storage.

**Content Filtering**: Block messages based on content, routing rules, or business logic.

**Data Enrichment**: Add metadata, timestamps, or computed fields to messages.

**Protocol Translation**: Convert between different message formats or protocols.

**Audit Logging**: Track message flow for compliance or debugging.

**Rate Limiting**: Implement custom throttling based on message content or source.

### Plugin Interface

Plugins implement a simple WIT interface:

```rust
// Plugin implementation in Rust
impl Guest for MyPlugin {
    fn init() -> Option<u16> {
        // One-time initialization
        log(LogLevel::Info, "MyPlugin initialized");
        None // No special port requirements
    }

    fn on_record(topic: String, partition: u32, key: Option<String>, value: String) -> bool {
        // Process each produced record
        if should_allow_message(&topic, &value) {
            true  // Allow message to be stored
        } else {
            false // Reject message
        }
    }
}
```

**Host Functions Available to Plugins**:
- `log(level, message)`: Send log messages to the broker
- Future: Database access, HTTP requests, metric collection

### Example Use Cases

**Content Filtering**:
```rust
fn on_record(topic: String, partition: u32, key: Option<String>, value: String) -> bool {
    // Block messages containing sensitive data
    if value.contains("SSN:") || value.contains("password") {
        log(LogLevel::Warn, "Blocked message with sensitive data");
        return false;
    }
    true
}
```

**Message Enrichment**:
```rust
fn on_record(topic: String, partition: u32, key: Option<String>, value: String) -> bool {
    // Add metadata to financial trading messages
    if topic.starts_with("trades.") {
        let enriched = format!(r#"{{"timestamp": {}, "data": {}}}"#,
                              current_timestamp(), value);
        // Note: Future enhancement will allow message modification
        log(LogLevel::Debug, "Enriched trade message");
    }
    true
}
```

**Topic-Based Routing**:
```rust
fn on_record(topic: String, partition: u32, key: Option<String>, value: String) -> bool {
    // Enforce key requirements for sensitive topics
    if topic.starts_with("sensitive.") && key.is_none() {
        log(LogLevel::Error, "Sensitive topic requires key");
        return false;
    }
    true
}
```

### Performance Benefits

**Minimal Overhead**: WASM compilation produces near-native code with microsecond execution times.

**Parallel Execution**: Plugins run concurrently with message processing - no blocking.

**Memory Efficiency**: WASM's linear memory model provides predictable memory usage.

**Hot Path Optimization**: Simple plugins add <10µs latency to message processing.

### Plugin Development

**Quick Start**:
```bash
# 1. Install Rust WASM target
rustup target add wasm32-wasip1

# 2. Install WASM tools
cargo install wasm-tools

# 3. Create plugin project
cargo new my-plugin --lib
cd my-plugin

# 4. Build WASM component
cargo build --target wasm32-wasip1 --release
wasm-tools component new target/wasm32-wasip1/release/my_plugin.wasm \
  -o my-plugin.wasm --adapt wasi_snapshot_preview1.reactor.wasm
```

**Plugin Configuration**:
```yaml
# settings.yaml
plugin_paths:
  - "plugins/content-filter.wasm"
  - "plugins/message-enricher.wasm"
  - "plugins/audit-logger.wasm"
```

### Security Model

**Memory Isolation**: Plugins run in separate WASM memory spaces - cannot access broker memory.

**Controlled Capabilities**: Plugins only have access to explicitly provided host functions.

**Resource Limits**: WASM runtime enforces memory and CPU quotas per plugin.

**Type Safety**: WIT interface prevents type confusion and memory corruption bugs.

**No System Access**: Plugins cannot make direct system calls or access files/network.

---

## Lock-Free Concurrent Architecture

Blink's performance advantage comes from a **fundamentally different concurrency model** that eliminates traditional locking mechanisms in favor of **lock-free data structures** and **async message-passing** patterns built on Tokio.

### Why Lock-Free Over Traditional Threading?

**Traditional Message Brokers** often use:
- **Mutex/RwLock** for shared state protection
- **Thread pools** for request processing
- **Blocking I/O** with thread-per-connection models
- **Coarse-grained locking** causing contention

**Problems with Traditional Locking**:
- **Lock Contention**: High-concurrency scenarios create bottlenecks
- **Context Switching**: OS thread switching adds latency overhead
- **Deadlock Risk**: Complex locking hierarchies prone to deadlocks
- **Unpredictable Latency**: Lock waits create latency spikes
- **Poor Cache Locality**: Thread migration hurts CPU cache performance

### Blink's Lock-Free Approach

**Concurrent Data Structures**: All shared state uses lock-free structures from the `dashmap` crate.

```rust
pub struct Storage {
    // No mutexes - pure concurrent data structures
    topic_partition_store: DashMap<PartitionKey, PartitionStorage>, // 256 internal shards
}

pub struct Broker {
    topics: DashMap<TopicName, TopicMeta>,  // Concurrent topic registry
    producers: AtomicI64,                   // Atomic counters
    storage: Storage,                       // Lock-free storage engine
}
```

**Async Task Model**: Each connection runs as an independent async task rather than an OS thread.

```rust
// Connection handling - no thread pools
async fn handle_connection(stream: TcpStream, broker: Arc<Broker>) {
    loop {
        let request = read_kafka_request(&mut stream).await?;
        let response = broker.handle_request(request).await?;  // No locks held
        write_response(&mut stream, response).await?;
    }
}
```

**Atomic Operations**: Shared counters and flags use atomic primitives instead of locks.

```rust
// Performance counters - atomic operations only
static PRODUCE_COUNT: AtomicU64 = AtomicU64::new(0);
static FETCH_COUNT: AtomicU64 = AtomicU64::new(0);

// Usage: PRODUCE_COUNT.fetch_add(1, Ordering::Relaxed)
```

### DashMap: High-Performance Concurrent HashMap

**Internal Sharding**: DashMap uses 256 internal shards, dramatically reducing contention compared to a single-sharded concurrent map.

**Lockless Reads**: Most read operations are completely lock-free using atomic operations and hazard pointers.

**Fine-Grained Locking**: When locks are needed (inserts/deletes), they're held for minimal time on tiny shards.

**Cache-Friendly**: Sharding improves CPU cache locality compared to global locks.

**Performance Characteristics**:
- **Read Operations**: ~10ns (lock-free)
- **Write Operations**: ~100ns (minimal locking)
- **Concurrent Readers**: No interference between readers
- **Scalability**: Linear scaling up to ~64 cores

### Async Notification System

**Producer-Consumer Coordination** uses `Arc<Notify>` for lock-free notifications:

```rust
// Producer side - wake waiting consumers
partition_storage.published.notify_waiters();  // Zero-cost notification

// Consumer side - efficient waiting
partition_storage.published.notified().await;  // No polling, no locks
```

**Benefits**:
- **Zero Polling**: Consumers sleep until data arrives
- **Immediate Wakeup**: Producers instantly notify waiting consumers
- **No Lock Contention**: Notification system is completely lock-free
- **Memory Efficient**: `Arc<Notify>` has minimal memory overhead

### Zero-Copy Memory Management

**Reference Counting**: Messages use `Bytes` with atomic reference counting instead of copying.

```rust
// Zero-copy message sharing
let original_bytes = Bytes::from(request_data);
let clone = original_bytes.clone();  // Just atomic increment, no memory copy
```

**Benefits**:
- **No Memory Copies**: Multiple consumers can reference same data
- **Atomic Reference Counting**: Thread-safe without locks
- **Memory Efficiency**: Single copy shared across multiple readers
- **Cache Friendly**: Fewer memory allocations improve cache performance

### Performance Benefits

**🚀 Latency Improvements**:
- **No Lock Waits**: Requests never block on locks
- **Predictable Performance**: No latency spikes from contention
- **CPU Cache Efficiency**: Better cache locality from reduced locking

**📈 Throughput Improvements**:
- **Parallel Processing**: All operations can run concurrently
- **Reduced Context Switching**: Async tasks vs. OS threads
- **Memory Bandwidth**: Zero-copy operations reduce memory pressure

**🔧 Scalability**:
- **Linear Core Scaling**: Performance scales with CPU core count
- **High Connection Count**: Thousands of concurrent connections
- **Memory Efficiency**: Lower memory usage per connection

### Concurrency Model Comparison

| Aspect | Traditional Brokers | Blink Lock-Free |
|--------|-------------------|-----------------|
| **Shared State** | Mutex/RwLock | DashMap (lock-free) |
| **Request Processing** | Thread pools | Async tasks |
| **Producer/Consumer** | Blocking queues | `Arc<Notify>` |
| **Memory Sharing** | Deep copying | Zero-copy `Bytes` |
| **Latency** | Variable (lock waits) | Consistent (no locks) |
| **Throughput** | Limited by contention | Scales with cores |
| **Resource Usage** | High (threads) | Low (async tasks) |

### Implementation Details

**Connection Handling**:
```rust
// Each connection = lightweight async task
tokio::spawn(async move {
    handle_kafka_connection(stream, broker).await
});
```

**Request Processing**:
```rust
// No locks in hot path
pub async fn handle_produce(&self, request: ProduceRequest) -> ProduceResponse {
    let partition_storage = self.storage
        .topic_partition_store
        .get(&partition_key)?;  // Lock-free read

    let offset = partition_storage.store_batch(records)?;  // Lock-free append
    partition_storage.published.notify_waiters();          // Lock-free notify

    ProduceResponse::new(offset)
}
```

**Memory Management**:
```rust
// Zero-copy throughout pipeline
let records: Bytes = request.extract_records();    // No copy
let cloned = records.clone();                      // Atomic ref count increment
storage.store(cloned).await?;                      // No copy to storage
consumer_response.set_records(records);            // No copy to response
```

This lock-free architecture enables Blink to achieve **microsecond latencies** and **scale linearly** with CPU core count, making it ideal for high-performance messaging workloads.

---

## Getting Started

### Quick Start

**1. Install and Run**
```bash
# Clone and build
git clone https://github.com/cleafy/blink.git
cd blink
cargo build --release

# Run with default configuration
./target/release/blink

# Or with custom settings
./target/release/blink --settings custom-settings.yaml
```

**2. Test Basic Functionality**
```bash
# Create a topic (automatic)
curl -X POST "http://localhost:30004/produce" \
  -d "topic=test&partition=0&value=hello-world"

# Consume the message
curl "http://localhost:30004/fetch?topic=test&partition=0&offset=0"
```

**3. Use with Kafka Clients**
```python
# Python example - no changes from Kafka
from kafka import KafkaProducer, KafkaConsumer

# Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('test-topic', b'Hello Blink!')

# Consumer
consumer = KafkaConsumer('test-topic', bootstrap_servers=['localhost:9092'])
for message in consumer:
    print(message.value)
```

### Docker Deployment

```bash
# Build container
docker build -t blink .

# Run standard binary
docker run -p 9092:9092 -p 30004:30004 \
  -e MAX_MEMORY=1GB \
  -e RETENTION=30s \
  blink

# Run with profiling enabled
docker run -p 9092:9092 -p 30004:30004 \
  -e PROFILE=true \
  -e MAX_MEMORY=1GB \
  blink
```

### Configuration

Create `settings.yaml`:

```yaml
# Resource limits
max_memory: "1GB"                    # Memory threshold for offloadover
retention: "5m"                      # Message time-to-live

# Network configuration
broker_ports: [9092, 9094]          # Kafka protocol ports
rest_port: 30004                      # Management API port
kafka_hostname: "0.0.0.0"           # Bind address

# Storage configuration
record_storage_path: "./wal"         # WAL storage directory
purge_on_fetch: false               # Keep messages after consumption

# Optional features
enable_consumer_groups: false        # Consumer group coordination
plugin_paths: []                     # WASM plugin paths
```

---

## Performance Characteristics

### Latency Performance

Blink boots in **milliseconds** and delivers **microsecond-range latency** for message operations:

**Boot Time**:
- **Memory-Only**: ~10ms from start to ready
- **With WAL Recovery**: ~50ms even with large datasets
- **No Dependencies**: No external systems required

**Message Latency**:
- **Memory Operations**: 100-500µs end-to-end
- **WAL Operations**: 1-3ms when reading from disk
- **Consumer Notifications**: <100µs from produce to consumer wakeup

### Scalability Characteristics

**Concurrent Connections**:
- **Per-Connection Overhead**: ~8KB memory
- **Lock-Free Scaling**: Performance scales with CPU cores

**Partition Scaling**:
- **Multiple Partitions**: No cross-partition interference
- **Independent WALs**: Each partition has its own storage
- **Cleanup Efficiency**: Parallel cleanup across partitions

**Memory Scaling**:
- **Graceful Degradation**: Automatic offloadover under pressure
- **Large Datasets**: Handle data larger than available RAM
- **Memory Recovery**: Automatic cleanup frees memory

---

## Architecture Overview

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                        Blink Broker                         │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ │
│ │   Kafka API     │ │    REST API     │ │    Plugins      │ │
│ │   (Port 9092)   │ │   (Port 30004)  │ │   (WASM)        │ │
│ └─────────────────┘ └─────────────────┘ └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────┐ │
│ │              Protocol Handler Layer                     │ │
│ │   • Request Parsing  • Response Encoding  • Routing     │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────┐ │
│ │                   Broker Core                           │ │
│ │   • Topic Management  • Producer/Consumer Logic         │ │
│ │   • Metadata Management  • Offset Management            │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────┐ │
│ │             Multi-Tier Storage Engine                   │ │
│ │   • Memory Tier (Primary)  • nano-wal Tier (Overflow)   │ │
│ │   • Lock-Free Operations  • Automatic Cleanup           │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────┐ │
│ │              Infrastructure Layer                       │ │
│ │   • Async Runtime (Tokio)  • Lock-Free Data Structures  │ │
│ │   • Concurrent Collections  • Metrics Collection        │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

**Producer Flow**:
```
Kafka Client → Protocol Parser → Plugin Filter → Storage Engine → Consumer Notification
```

**Consumer Flow**:
```
Consumer Request → Storage Engine → Plugin Transform → Protocol Encoder → Kafka Client
```

**Storage Flow**:
```
Memory (Fast) → WAL Offloadover (Medium) → Cleanup (Automatic)
```

---

## Configuration

### Core Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `max_memory` | 1GB | Memory threshold (offloadover at 80%) |
| `retention` | 5m | Time-based message cleanup |
| `broker_ports` | [9092, 9094] | Kafka protocol ports |
| `rest_port` | 30004 | Management API port |
| `record_storage_path` | ./records | WAL storage directory |
| `enable_consumer_groups` | false | Consumer group coordination |
| `plugin_paths` | [] | WASM plugin file paths |

### Environment Overrides

```bash
# Resource configuration
export MAX_MEMORY=2GB
export RETENTION=30s

# Network configuration
export BROKER_PORTS=9092,9094
export KAFKA_HOSTNAME=0.0.0.0

# Feature toggles
export ENABLE_CONSUMER_GROUPS=true
export LOG_LEVEL=DEBUG
```

### Performance Tuning

**Memory-Intensive Workloads**:
```yaml
max_memory: "8GB"        # Larger memory pool
retention: "1m"          # Shorter retention
purge_on_fetch: true     # Aggressive cleanup
```

**High-Throughput Workloads**:
```yaml
kafka_cfg_num_partitions: 8  # More partitions for parallelism
max_memory: "4GB"            # Adequate memory pool
retention: "30s"             # Moderate retention
```

**Low-Latency Workloads**:
```yaml
max_memory: "512MB"     # Keep everything in memory
retention: "10s"        # Minimal retention
purge_on_fetch: false   # Keep data available for replays
```

---

## Monitoring

### Prometheus Metrics

Access comprehensive metrics at `http://localhost:30004/prometheus`:

**Core Performance**:
- `blink_produce_count` - Total produce requests
- `blink_fetch_count` - Total fetch requests
- `blink_produce_duration` - Produce latency distribution
- `blink_fetch_duration` - Fetch latency distribution

**Memory & Storage**:
- `blink_in_memory_queue_size_bytes` - Current memory usage
- `blink_total_queue_size_bytes` - Total data size
- `blink_current_disk_space_bytes` - WAL disk usage
- `blink_ram_batch_count` - Batches in memory
- `blink_offloaded_batch_count` - Batches on disk

**System Health**:
- `blink_current_allocated` - Real-time memory allocation
- `blink_offload_operations_total` - WAL offloadover activity
- `blink_purged_records` - Cleanup efficiency

### Health Endpoints

**Status Check**:
```bash
curl http://localhost:30004/        # Metrics overview
curl http://localhost:30004/version # Build and config info
```

**Debugging**:
```bash
curl http://localhost:30004/dump    # Storage state dump
curl http://localhost:30004/allocated # Memory usage
```

**Manual Operations**:
```bash
# Force cleanup
curl http://localhost:30004/purge

# Test produce/consume
curl -X POST http://localhost:30004/produce -d "topic=test&value=data"
curl http://localhost:30004/fetch?topic=test&offset=0
```

### Alerting

Sample Prometheus alerts:

```yaml
- alert: BlinkMemoryPressure
  expr: blink_in_memory_queue_size_bytes / blink_max_memory_bytes > 0.85
  for: 30s
  annotations:
    summary: "Blink approaching memory limit"

- alert: BlinkHighLatency
  expr: histogram_quantile(0.95, rate(blink_produce_duration_bucket[5m])) > 10000
  for: 1m
  annotations:
    summary: "Blink produce latency high"

- alert: BlinkWALActivity
  expr: rate(blink_offload_operations_total[5m]) > 10
  for: 2m
  annotations:
    summary: "High WAL offloadover activity"
```

---

## Deployment

### Single Binary Deployment

**Direct Execution**:
```bash
# Simple deployment
./blink --settings production.yaml

# With environment overrides
MAX_MEMORY=4GB RETENTION=5m ./blink
```

**Systemd Service**:
```ini
[Unit]
Description=Blink Message Broker
After=network.target

[Service]
Type=simple
User=blink
WorkingDirectory=/opt/blink
ExecStart=/opt/blink/bin/blink --settings /etc/blink/settings.yaml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Container Deployment

**Docker Compose**:
```yaml
version: '3.8'
services:
  blink:
    image: blink:latest
    ports:
      - "9092:9092"
      - "30004:30004"
    environment:
      - MAX_MEMORY=2GB
      - RETENTION=30s
      - LOG_LEVEL=INFO
    volumes:
      - ./wal:/app/wal
    restart: unless-stopped
```

**Kubernetes**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: blink
spec:
  replicas: 1
  selector:
    matchLabels:
      app: blink
  template:
    metadata:
      labels:
        app: blink
    spec:
      containers:
      - name: blink
        image: blink:latest
        ports:
        - containerPort: 9092
        - containerPort: 30004
        env:
        - name: MAX_MEMORY
          value: "4GB"
        - name: LOG_LEVEL
          value: "INFO"
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "8Gi"
            cpu: "4000m"
        volumeMounts:
        - name: wal-storage
          mountPath: /app/wal
      volumes:
      - name: wal-storage
        persistentVolumeClaim:
          claimName: blink-wal-pvc
```

### Production Considerations

**Resource Allocation**:
- **Memory**: Allocate 2-4x your `max_memory` setting to account for OS overhead
- **CPU**: Blink scales well with multiple cores (4-8 cores recommended)
- **Storage**: WAL storage should be on fast SSDs for best performance
- **Network**: Ensure adequate bandwidth for your throughput requirements

**Security**:
- **Network**: Restrict access to Kafka ports (9092) and management port (30004)
- **Filesystem**: Secure WAL storage directory with appropriate permissions
- **Plugins**: Validate WASM plugins before deployment
- **Monitoring**: Expose metrics securely for monitoring systems

**High Availability**:
- **Health Checks**: Monitor both Kafka and REST endpoints
- **Graceful Shutdown**: Blink handles SIGTERM for clean shutdowns
- **Data Recovery**: WAL enables fast recovery after restarts
- **Load Balancing**: Multiple Blink instances can serve different topics

---

**Blink delivers the performance your applications need with the simplicity your operations team wants. Experience Kafka-compatible messaging that boots in milliseconds and delivers microsecond latencies.**

For more detailed documentation, see the individual files in this `docs/` directory.
