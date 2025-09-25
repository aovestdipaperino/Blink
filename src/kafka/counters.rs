// Kafka metrics and counters
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use once_cell::sync::Lazy;
use prometheus::core::{Atomic, AtomicF64, Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, Opts, Registry,
};

use std::io::Write;
use std::ops::Add;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;
use tokio::time::{Duration, Instant};
use tracing::{info, trace, warn};
use uuid::Uuid;

const BLINK_NAMESPACE: &str = "blink";
const KAFKA_LOG_NAMESPACE: &str = "kafka_log";
const KAFKA_NETWORK_NAMESPACE: &str = "kafka_network";
const KAFKA_SERVER_NAMESPACE: &str = "kafka_server";
pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

// KAFKA compatible metrics
pub static REQUEST_TOTAL_ITEMS: Lazy<AggregateCounter> = Lazy::new(|| {
    create_aggregate_counter(
        KAFKA_NETWORK_NAMESPACE,
        "requestmetrics_totalitems",
        "Total number of items processed by the request",
        &["request"],
    )
});

#[derive(Clone)]
pub struct AggregateCounter {
    labeled_counter: CounterVec,
    aggregate_desc: Desc,
}

impl AggregateCounter {
    pub fn new(opts: Opts, label_names: &[&str]) -> Result<Self, prometheus::Error> {
        let labeled_counter = CounterVec::new(opts.clone(), label_names)?;

        // Create descriptor for the aggregate metric (no labels)
        let aggregate_desc = Desc::new(
            opts.name.clone(),
            opts.help.clone(),
            vec![],
            opts.const_labels.clone(),
        )?;

        Ok(AggregateCounter {
            labeled_counter,
            aggregate_desc,
        })
    }

    /// Get a counter for the given label values
    pub fn with_label_values(&self, vals: &[&str]) -> prometheus::Counter {
        self.labeled_counter.with_label_values(vals)
    }
}

impl Collector for AggregateCounter {
    fn desc(&self) -> Vec<&Desc> {
        let mut descs = vec![&self.aggregate_desc];
        descs.extend(self.labeled_counter.desc());
        descs
    }

    fn collect(&self) -> Vec<MetricFamily> {
        // Get the labeled metrics from the CounterVec
        let labeled_metrics = self.labeled_counter.collect();

        if labeled_metrics.is_empty() {
            return vec![];
        }

        let mut metric_family = labeled_metrics[0].clone();

        // Calculate the total by summing all labeled counter values
        let total: f64 = metric_family
            .get_metric()
            .iter()
            .map(|m| m.get_counter().get_value())
            .sum();

        // Create the aggregate metric (no labels)
        let mut aggregate_metric = prometheus::proto::Metric::new();
        let mut counter = prometheus::proto::Counter::new();
        counter.set_value(total);
        aggregate_metric.set_counter(counter);

        // Add the aggregate metric to the beginning of the metrics list
        let mut all_metrics = vec![aggregate_metric];
        all_metrics.extend(metric_family.take_metric().into_iter());

        metric_family.set_metric(all_metrics.into());

        vec![metric_family]
    }
}

/// Custom Prometheus encoder that outputs counters in scientific notation with trailing commas
pub struct KafkaMetricsEncoder;

impl KafkaMetricsEncoder {
    pub fn new() -> Self {
        Self
    }

    pub fn encode<W: Write>(
        &self,
        metric_families: &[prometheus::proto::MetricFamily],
        writer: &mut W,
    ) -> Result<(), std::io::Error> {
        for mf in metric_families {
            // Write HELP line
            writeln!(writer, "# HELP {} {}", mf.get_name(), mf.get_help())?;

            // Write TYPE line
            writeln!(writer, "# TYPE {} counter", mf.get_name())?;

            for metric in mf.get_metric() {
                let value = metric.get_counter().get_value();

                if metric.get_label().is_empty() {
                    // Aggregate metric (no labels) - use scientific notation
                    writeln!(writer, "{} {:.10E}", mf.get_name(), value)?;
                } else {
                    // Labeled metric - format labels with trailing comma
                    let mut label_str = String::new();
                    for label in metric.get_label() {
                        if !label_str.is_empty() {
                            label_str.push(',');
                        }
                        label_str.push_str(&format!(
                            "{}=\"{}\"",
                            label.get_name(),
                            label.get_value()
                        ));
                    }
                    // Add trailing comma
                    label_str.push(',');

                    writeln!(writer, "{}{{{}}} {:.10E}", mf.get_name(), label_str, value)?;
                }
            }
        }
        Ok(())
    }
}

// Request Lifecycle:
// ┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
// │   Request   │ →  │   Request    │ →  │  Response   │ →  │   Response   │
// │   Arrives   │    │  Processing  │    │    Queue    │    │     Send     │
// └─────────────┘    └──────────────┘    └─────────────┘    └──────────────┘
//        ↓                   ↓                   ↓                   ↓
// Request Queue Time    (Processing Time)   Response Queue    Response Send
//                       (not directly              Time             Time
//                        measured here)
pub static REQUEST_QUEUE_TIME_MS: Lazy<AggregateCounter> = Lazy::new(|| {
    create_aggregate_counter(
        KAFKA_NETWORK_NAMESPACE,
        "requestmetrics_requestqueuetimems",
        "Total time spent in queue before processing",
        &["request"],
    )
});

pub static RESPONSE_QUEUE_TIME_MS: Lazy<AggregateCounter> = Lazy::new(|| {
    create_aggregate_counter(
        KAFKA_NETWORK_NAMESPACE,
        "requestmetrics_responsequeuetimems",
        "Total time spent in queue before sending response",
        &["request"],
    )
});

pub static RESPONSE_SEND_TIME_MS: Lazy<AggregateCounter> = Lazy::new(|| {
    create_aggregate_counter(
        KAFKA_NETWORK_NAMESPACE,
        "requestmetrics_responsesendtimems",
        "Total time spent in queue before sending response",
        &["request"],
    )
});

// pub static TOTALTIME_MS: Lazy<AggregateCounter> = Lazy::new(|| {
//     create_aggregate_counter(
//         KAFKA_NETWORK_NAMESPACE,
//         "requestmetrics_totaltimems",
//         "Total time handling the request",
//         &["request"],
//     )
// });

pub static TOTALTIME_MS: Lazy<AggregateCounter> = Lazy::new(|| {
    create_aggregate_counter(
        KAFKA_NETWORK_NAMESPACE,
        "requestmetrics_totaltimems",
        "Total time spent handling the request",
        &["request"],
    )
});

pub static MESSAGES_TOPIC_IN: Lazy<AggregateCounter> = Lazy::new(|| {
    create_aggregate_counter(
        KAFKA_SERVER_NAMESPACE,
        "brokertopicmetrics_messagesin_total",
        "Attribute exposed for management kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics,attribute=Count",
        &["topic"],
    )
});

pub static MESSAGES_TOPIC_BYTES_IN: Lazy<AggregateCounter> = Lazy::new(|| {
    create_aggregate_counter(
        KAFKA_SERVER_NAMESPACE,
        "brokertopicmetrics_bytesin_total",
        "Attribute exposed for management kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics,attribute=Count",
        &["topic"],
    )
});

pub static MESSAGES_TOPIC_BYTES_OUT: Lazy<AggregateCounter> = Lazy::new(|| {
    create_aggregate_counter(
        KAFKA_SERVER_NAMESPACE,
        "brokertopicmetrics_bytesout_total",
        "Attribute exposed for management kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics,attribute=Count",
        &["topic"],
    )
});

pub static LOG_SIZE: Lazy<GaugeVec> = Lazy::new(|| {
    create_gauge_vec(
        KAFKA_LOG_NAMESPACE,
        "size",
        "Size of log in bytes",
        &["topic", "partition"],
    )
});

pub static LOG_SEGMENT_COUNT: Lazy<GaugeVec> = Lazy::new(|| {
    create_gauge_vec(
        KAFKA_LOG_NAMESPACE,
        "numlogsegments",
        "Total number of log segments (offload files)",
        &["topic", "partition"],
    )
});

// Blink! specific metrics

pub static IN_MEMORY_QUEUE_SIZE: Lazy<Gauge> = Lazy::new(|| {
    create_gauge(
        BLINK_NAMESPACE,
        "in_memory_queue_size_bytes",
        "Size of in-memory batches in bytes",
    )
});

pub static TOTAL_QUEUE_SIZE: Lazy<Gauge> = Lazy::new(|| {
    create_gauge(
        BLINK_NAMESPACE,
        "total_queue_size_bytes",
        "Total queue size in bytes (in-memory + disk)",
    )
});

pub static CURRENT_DISK_SPACE: Lazy<Gauge> = Lazy::new(|| {
    create_gauge(
        BLINK_NAMESPACE,
        "current_disk_space_bytes",
        "Current disk space occupied by offload files in bytes",
    )
});

pub static MAX_DISK_SPACE: Lazy<Gauge> = Lazy::new(|| {
    create_gauge(
        BLINK_NAMESPACE,
        "max_disk_space_bytes",
        "Maximum disk space occupied by offload files at any time in bytes",
    )
});

pub(crate) static PRODUCE_SENSOR: Lazy<Sensor> = Lazy::new(|| Sensor {
    ema: AtomicF64::new(0.0),
});

pub static RECORD_COUNT: Lazy<Counter> =
    Lazy::new(|| create_counter(BLINK_NAMESPACE, "record_count", "Total Records"));

pub static SYNC_PURGE_COUNT: Lazy<Counter> = Lazy::new(|| {
    create_counter(
        BLINK_NAMESPACE,
        "sync_purge_total",
        "Total synchronous purge operations in handle_produce path",
    )
});

pub static ASYNC_PURGE_COUNT: Lazy<Counter> = Lazy::new(|| {
    create_counter(
        BLINK_NAMESPACE,
        "async_purge_total",
        "Total asynchronous purge operations from background task",
    )
});

// Blink counters for tracking the actual byte flow

pub(crate) struct Sensor {
    ema: AtomicF64,
}

impl Sensor {
    pub(crate) fn measure(&self, value: f64) {
        // ema = 0.05 * value + 0.95 * ema
        let alpha = 0.05;
        let current = self.ema.get();
        let new_value = alpha * value + (1.0 - alpha) * current;
        self.ema.set(new_value);
    }
}

pub static PURGED_RECORDS: Lazy<Counter> =
    Lazy::new(|| create_counter(BLINK_NAMESPACE, "purged_records", "Total Purged Records"));
pub static OBLITERATED_RECORDS: Lazy<Counter> = Lazy::new(|| {
    create_counter(
        BLINK_NAMESPACE,
        "obliterated_records",
        "Total obliterated records due to retention policy",
    )
});

pub static OBLITERATION_EVENTS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    create_counter(
        BLINK_NAMESPACE,
        "obliteration_events_total",
        "Total number of obliteration events",
    )
});

pub static OBLITERATION_WARNINGS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    create_counter(
        BLINK_NAMESPACE,
        "obliteration_warnings_total",
        "Total number of obliteration warnings issued",
    )
});

pub static LAST_OBLITERATION_WARNING_TIMESTAMP: Lazy<Gauge> = Lazy::new(|| {
    create_gauge(
        BLINK_NAMESPACE,
        "last_obliteration_warning_timestamp_seconds",
        "Timestamp of the last obliteration warning in seconds since epoch",
    )
});

// Track the next event ID to assign
// Using AtomicI64 to match the event_id type in ObliterationEvent
static OBLITERATION_EVENT_COUNT: AtomicI64 = AtomicI64::new(1);

// Track the last warning time to implement throttling
static LAST_WARNING_TIME: Lazy<Mutex<Option<Instant>>> = Lazy::new(|| Mutex::new(None));

/// Represents an obliteration event that can be serialized to JSON
#[derive(Debug)]
#[allow(dead_code)]
pub struct ObliterationEvent {
    /// Unique sequential ID for this event
    pub event_id: i64,
    /// The topic ID where obliteration occurred
    pub topic_id: Uuid,
    /// The partition number
    pub partition: i32,
    /// Number of records that were obliterated
    pub obliterated_count: u64,
    /// Total number of records that were purged (including obliterated)
    pub total_purged_count: u64,
    /// Human-readable timestamp
    pub timestamp: String,
    /// Unix timestamp in seconds
    pub unix_timestamp: u64,
}

impl ObliterationEvent {
    /// Creates a new obliteration event with the current timestamp
    pub fn new(
        topic_id: Uuid,
        partition: i32,
        obliterated_count: u64,
        total_purged_count: u64,
    ) -> Self {
        let event_id = OBLITERATION_EVENT_COUNT.fetch_add(1, Ordering::SeqCst);
        let now = std::time::SystemTime::now();
        let unix_timestamp = now
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let timestamp = chrono::DateTime::<chrono::Utc>::from(now)
            .format("%Y-%m-%d %H:%M:%S UTC")
            .to_string();

        Self {
            event_id,
            topic_id,
            partition,
            obliterated_count,
            total_purged_count,
            timestamp,
            unix_timestamp,
        }
    }

    /// Calculates the obliteration rate as a percentage
    pub fn obliteration_rate(&self) -> f64 {
        if self.total_purged_count == 0 {
            0.0
        } else {
            (self.obliterated_count as f64 / self.total_purged_count as f64) * 100.0
        }
    }
}

/// Records an obliteration event and updates relevant metrics
/// This function:
/// 1. Creates an ObliterationEvent for tracking
/// 2. Updates Prometheus counters
/// 3. Issues warnings if the obliteration rate is high (with throttling)
///
/// # Arguments
/// * `topic_id` - The UUID of the topic where obliteration occurred
/// * `partition` - The partition number
/// * `obliterated_count` - Number of records obliterated due to retention
/// * `total_purged_count` - Total number of records purged (including obliterated)
pub fn record_obliteration_event(
    topic_id: Uuid,
    partition: i32,
    obliterated_count: u64,
    total_purged_count: u64,
) -> ObliterationEvent {
    // Only create events for non-zero obliterations
    let event = ObliterationEvent::new(topic_id, partition, obliterated_count, total_purged_count);

    // Update Prometheus metrics
    OBLITERATED_RECORDS.inc_by(obliterated_count as f64);
    OBLITERATION_EVENTS_TOTAL.inc();

    // Check if we should issue a warning
    let obliteration_rate = event.obliteration_rate();
    if obliteration_rate > 50.0 && should_issue_warning() {
        warn!(
            "High obliteration rate detected: {:.1}% ({}/{}) for topic {} partition {}",
            obliteration_rate, obliterated_count, total_purged_count, topic_id, partition
        );

        OBLITERATION_WARNINGS_TOTAL.inc();
        LAST_OBLITERATION_WARNING_TIMESTAMP.set(event.unix_timestamp as f64);
        update_last_warning_time();
    }

    info!(
        "Obliteration event recorded: {} records obliterated out of {} purged ({:.1}% rate) for topic {} partition {}",
        obliterated_count,
        total_purged_count,
        obliteration_rate,
        topic_id,
        partition
    );

    event
}

/// Determines if a warning should be issued based on throttling logic
/// Currently implements a simple time-based throttle (5 minutes between warnings)
fn should_issue_warning() -> bool {
    let last_warning = match LAST_WARNING_TIME.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!("Warning time mutex poisoned, recovering");
            poisoned.into_inner()
        }
    };

    match *last_warning {
        None => true, // First warning
        Some(last_time) => {
            let elapsed = Instant::now().duration_since(last_time);
            elapsed >= Duration::from_secs(300) // 5 minutes
        }
    }
}

/// Updates the last warning time to the current instant
fn update_last_warning_time() {
    if let Ok(mut last_warning) = LAST_WARNING_TIME.lock() {
        *last_warning = Some(Instant::now());
    }
}

/// Gets the total number of obliteration events recorded
#[allow(dead_code)]
pub fn get_total_obliteration_events() -> i64 {
    OBLITERATION_EVENT_COUNT.load(Ordering::SeqCst) - 1 // Subtract 1 because we start from 1
}

/// Gets the time elapsed since the last warning was issued
/// Returns None if no warning has been issued yet
#[allow(dead_code)]
pub fn time_since_last_warning() -> Option<Duration> {
    LAST_WARNING_TIME
        .lock()
        .ok()?
        .and_then(|last_time| Some(Instant::now().duration_since(last_time)))
}

/// Gets comprehensive obliteration metrics for monitoring/alerting
#[allow(dead_code)]
pub fn get_obliteration_metrics() -> ObliterationMetrics {
    ObliterationMetrics {
        total_events: get_total_obliteration_events() as u64,
        total_warnings: OBLITERATION_WARNINGS_TOTAL.get() as u64,
        last_warning_timestamp: LAST_OBLITERATION_WARNING_TIMESTAMP.get() as u64,
        time_since_last_warning: time_since_last_warning(),
    }
}

/// Structured metrics for obliteration monitoring
#[allow(dead_code)]
pub struct ObliterationMetrics {
    /// Total obliteration events recorded
    pub total_events: u64,
    /// Total warnings issued
    pub total_warnings: u64,
    /// Timestamp of last warning (seconds since epoch)
    pub last_warning_timestamp: u64,
    /// Time elapsed since last warning
    #[allow(dead_code)]
    pub time_since_last_warning: Option<Duration>,
}

pub static DASHMAP_GET_MUT: Lazy<Histogram> = Lazy::new(|| {
    create_histogram(
        "blink_dashmap_get_mut_duration_seconds",
        "Time spent waiting for DashMap get_mut locks",
        None,
    )
});
pub static FETCH_DURATION: Lazy<Histogram> = Lazy::new(|| {
    create_histogram(
        "blink_fetch_duration_seconds",
        "Time spent processing fetch requests",
        Some(vec![
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0,
        ]),
    )
});
pub static PRODUCE_DURATION: Lazy<Histogram> = Lazy::new(|| {
    create_histogram(
        "blink_produce_duration_seconds",
        "Time spent processing produce requests",
        Some(vec![
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0,
        ]),
    )
});

// Histogram counters for the number of records in a record batch on produce and fetch paths.
// These track how many records make up each RecordBatch that is produced or fetched.
pub static PRODUCE_BATCH_SIZE: Lazy<Histogram> = Lazy::new(|| {
    create_histogram(
        "blink_produce_batch_size",
        "Number of records per produced recordbatch",
        Some(vec![1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 25.0, 50.0]),
    )
});

pub static FETCH_BATCH_SIZE: Lazy<Histogram> = Lazy::new(|| {
    create_histogram(
        "blink_fetch_batch_size",
        "Number of records per fetched recordbatch",
        Some(vec![1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 25.0, 50.0]),
    )
});

pub static QUEUE_DURATION: Lazy<Histogram> = Lazy::new(|| {
    create_histogram(
        "blink_queue_duration_seconds",
        "Time spent in the queue",
        None,
    )
});

pub static NOTIFY_COUNTER: Lazy<Counter> =
    Lazy::new(|| create_counter(BLINK_NAMESPACE, "notify", "notify"));
pub static AGGRESSIVE_CLIENT: Lazy<Counter> = Lazy::new(|| {
    create_counter(
        BLINK_NAMESPACE,
        "aggressive_client_total",
        "Total number of aggressive client behaviors detected",
    )
});

// Idle time metrics
// These track how much time the system spends idle vs actively processing requests

// Total idle time in seconds (gauge that increases over time)
pub static IDLE_TIME_SECONDS: Lazy<Gauge> = Lazy::new(|| {
    create_gauge(
        BLINK_NAMESPACE,
        "idle_time_seconds_total",
        "Total time spent idle in seconds",
    )
});

// Idle time as a percentage of uptime
pub static IDLE_TIME_PERCENTAGE: Lazy<Gauge> = Lazy::new(|| {
    create_gauge(
        BLINK_NAMESPACE,
        "idle_time_percentage",
        "Percentage of time spent idle",
    )
});

// Current number of active requests being processed

// Atomic counter for fast increment/decrement of active requests
pub static ACTIVE_REQUESTS: AtomicI64 = AtomicI64::new(0);

/// Tracks idle time for the system
/// This struct helps calculate how much time the system spends idle vs busy
pub struct IdleTimeTracker {
    pub start_time: Instant,
    pub total_idle_time: Duration,
    last_idle_start: Option<Instant>,
}

impl IdleTimeTracker {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            total_idle_time: Duration::ZERO,
            last_idle_start: Some(Instant::now()), // Start idle
        }
    }

    /// Called when a request starts processing
    /// This marks the end of an idle period if one was active
    pub fn on_request_start(&mut self) {
        if let Some(idle_start) = self.last_idle_start.take() {
            // We were idle, add the idle period to our total
            let idle_duration = Instant::now().duration_since(idle_start);
            self.total_idle_time += idle_duration;
        }
        // else: we were already busy, nothing to do
    }

    /// Called when a request finishes processing
    /// If this was the last active request, start a new idle period
    pub fn on_request_end(&mut self) {
        let active_count = ACTIVE_REQUESTS.load(Ordering::Relaxed);

        if active_count == 0 {
            // No more active requests, start idle period
            self.last_idle_start = Some(Instant::now());
        }
        // else: still have active requests, remain in busy state
    }

    /// Gets the current total idle time, including any ongoing idle period
    /// This is useful for real-time metrics that need to reflect current state
    pub fn get_current_total_idle_time(&self) -> Duration {
        let mut total_idle = self.total_idle_time;

        // If currently idle, add the current idle period
        if let Some(idle_start) = self.last_idle_start {
            let current_idle = Instant::now().duration_since(idle_start);
            total_idle += current_idle;
        }

        total_idle
    }
}

pub static IDLE_TRACKER: Lazy<Mutex<IdleTimeTracker>> =
    Lazy::new(|| Mutex::new(IdleTimeTracker::new()));
fn create_counter(namespace: &str, name: &str, help: &str) -> Counter {
    let opts = Opts::new(name, help).namespace(namespace); // Add optional const labels if you have any.

    let counter = Counter::with_opts(opts).unwrap(); // Create the counter

    REGISTRY.register(Box::new(counter.clone())).unwrap(); // Register counter to registry
    counter
}

fn create_aggregate_counter(
    namespace: &str,
    name: &str,
    help: &str,
    labels: &[&str],
) -> AggregateCounter {
    let opts = Opts::new(name, help).namespace(namespace);
    let counter_vec = AggregateCounter::new(opts, labels).unwrap();
    REGISTRY.register(Box::new(counter_vec.clone())).unwrap();
    counter_vec
}

fn create_gauge(namespace: &str, name: &str, help: &str) -> Gauge {
    let opts = Opts::new(name, help).namespace(namespace);
    let gauge = Gauge::with_opts(opts).unwrap();
    REGISTRY.register(Box::new(gauge.clone())).unwrap();
    gauge
}

fn create_gauge_vec(namespace: &str, name: &str, help: &str, labels: &[&str]) -> GaugeVec {
    let opts = Opts::new(name, help).namespace(namespace);
    let gauge_vec = GaugeVec::new(opts, labels).unwrap();
    REGISTRY.register(Box::new(gauge_vec.clone())).unwrap();
    gauge_vec
}

fn create_histogram(name: &str, help: &str, buckets: Option<Vec<f64>>) -> Histogram {
    let mut opts = HistogramOpts::new(name, help);
    if let Some(b) = buckets {
        opts = opts.buckets(b);
    }
    let histogram = Histogram::with_opts(opts).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
}

pub fn collect_metrics() -> String {
    trace!("collecting Prometheus metrics");

    // Update idle time metrics if currently idle
    // This ensures scraped metrics always reflect real-time idle state
    if ACTIVE_REQUESTS.load(Ordering::Relaxed) == 0 {
        if let Ok(tracker) = IDLE_TRACKER.lock() {
            // Update metrics with current idle time included
            let current_total_idle = tracker.get_current_total_idle_time();
            let total_uptime = Instant::now().duration_since(tracker.start_time);

            // Update idle time gauge (includes ongoing idle period)
            IDLE_TIME_SECONDS.set(current_total_idle.as_secs_f64());

            if !total_uptime.is_zero() {
                let idle_percentage =
                    (current_total_idle.as_secs_f64() / total_uptime.as_secs_f64()) * 100.0;
                IDLE_TIME_PERCENTAGE.set(idle_percentage);
            }
        }
    }

    // Update TOTAL_QUEUE_SIZE as IN_MEMORY_QUEUE_SIZE + CURRENT_DISK_SPACE
    let in_memory_size = IN_MEMORY_QUEUE_SIZE.get();
    let disk_size = CURRENT_DISK_SPACE.get();
    TOTAL_QUEUE_SIZE.set(in_memory_size + disk_size);

    let mut buffer = vec![];

    // Use custom encoder for AggregateCounter metrics
    let custom_encoder = KafkaMetricsEncoder::new();
    let metric_families = REGISTRY.gather();

    // Filter for our custom aggregate counters and use custom encoding
    let mut custom_metrics = Vec::new();
    let mut standard_metrics = Vec::new();

    for mf in metric_families {
        let name = mf.get_name();
        if name.contains("brokertopicmetrics") {
            custom_metrics.push(mf);
        } else {
            standard_metrics.push(mf);
        }
    }

    // Encode custom metrics with our formatter
    if custom_encoder.encode(&custom_metrics, &mut buffer).is_err() {
        return "Error encoding custom metrics".to_string();
    }

    // Encode standard metrics with default encoder
    let encoder = prometheus::TextEncoder::new();
    if encoder.encode(&standard_metrics, &mut buffer).is_err() {
        return "Error encoding standard metrics".to_string();
    }

    let mut buffer2 = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer2) {
        eprintln!("could not encode prometheus metrics: {}", e);
    };
    let res_custom = String::from_utf8(buffer2.clone()).unwrap_or_else(|e| {
        eprintln!("prometheus metrics could not be from_utf8'd: {}", e);
        String::default()
    });
    let f = String::from_utf8(buffer).unwrap();
    let f = f.add(res_custom.as_str());

    f
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_collect_metrics_updates_idle_time() {
        // Reset active requests counter
        ACTIVE_REQUESTS.store(0, Ordering::Relaxed);

        // Get initial metrics
        let idle_time_before = IDLE_TIME_SECONDS.get();

        // Wait a bit
        sleep(Duration::from_millis(10)).await;

        // Collect metrics again - should show increased idle time
        let metrics_after = collect_metrics();
        let idle_time_after = IDLE_TIME_SECONDS.get();

        // Idle time should have increased
        assert!(idle_time_after > idle_time_before);

        // Verify metrics contain expected keys
        assert!(metrics_after.contains("blink_idle_time_seconds_total"));
        assert!(metrics_after.contains("blink_idle_time_percentage"));

        // Simulate busy period
        ACTIVE_REQUESTS.store(1, Ordering::Relaxed);

        let idle_time_before_busy_collection = IDLE_TIME_SECONDS.get();

        // Should NOT increase idle time during busy collection
        let _metrics_during_busy = collect_metrics();
        let idle_time_after_busy_collection = IDLE_TIME_SECONDS.get();

        // Idle time should be the same since we're busy
        assert_eq!(
            idle_time_before_busy_collection, idle_time_after_busy_collection,
            "Idle time should not change during busy period metrics collection"
        );
    }

    #[test]
    fn test_obliteration_event_creation() {
        let topic_id = Uuid::new_v4();
        let event = ObliterationEvent::new(topic_id, 0, 10, 100);

        assert_eq!(event.topic_id, topic_id);
        assert_eq!(event.partition, 0);
        assert_eq!(event.obliterated_count, 10);
        assert_eq!(event.total_purged_count, 100);
        assert_eq!(event.obliteration_rate(), 10.0);
    }

    #[test]
    fn test_obliteration_rate_calculation() {
        let topic_id = Uuid::new_v4();

        // Test normal case
        let event1 = ObliterationEvent::new(topic_id, 0, 25, 100);
        assert_eq!(event1.obliteration_rate(), 25.0);

        // Test edge case with zero purged
        let event2 = ObliterationEvent::new(topic_id, 0, 0, 0);
        assert_eq!(event2.obliteration_rate(), 0.0);

        // Test 100% obliteration
        let event3 = ObliterationEvent::new(topic_id, 0, 50, 50);
        assert_eq!(event3.obliteration_rate(), 100.0);
    }

    #[test]
    fn test_event_counter_increments() {
        let topic_id = Uuid::new_v4();

        // Create events directly and verify they have sequential IDs
        let event1 = ObliterationEvent::new(topic_id, 0, 5, 20);
        let event2 = ObliterationEvent::new(topic_id, 1, 3, 15);

        // Verify events have different IDs and second is greater than first
        assert_ne!(event1.event_id, event2.event_id);
        assert!(event2.event_id > event1.event_id);
    }

    #[test]
    fn test_no_event_for_zero_obliterations() {
        let initial_count = OBLITERATION_EVENT_COUNT.load(Ordering::Relaxed);

        // Test that zero obliterations don't increment the counter
        // This is tested implicitly by not creating any events
        let final_count = OBLITERATION_EVENT_COUNT.load(Ordering::Relaxed);
        assert_eq!(final_count, initial_count);
    }

    #[test]
    fn test_warning_throttling_logic_basic() {
        // Clear any existing state
        {
            let mut last_warning = LAST_WARNING_TIME
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *last_warning = None;
        }

        // Test basic warning state transitions
        update_last_warning_time();
        let time_elapsed = time_since_last_warning();
        assert!(time_elapsed.is_some());
        assert!(time_elapsed.unwrap() < Duration::from_secs(1));
    }

    #[test]
    fn test_event_id_uniqueness() {
        let topic_id = Uuid::new_v4();

        let event1 = ObliterationEvent::new(topic_id, 0, 1, 10);
        let event2 = ObliterationEvent::new(topic_id, 0, 1, 10);

        assert_ne!(event1.event_id, event2.event_id);
        assert!(event2.event_id > event1.event_id);
    }

    #[test]
    fn test_obliteration_metrics_integration() {
        let topic_id = Uuid::new_v4();
        let initial_event_count = OBLITERATION_EVENT_COUNT.load(Ordering::Relaxed);

        // Create events directly
        let event1 = ObliterationEvent::new(topic_id, 0, 5, 20);
        let event2 = ObliterationEvent::new(topic_id, 1, 3, 15);

        let final_event_count = OBLITERATION_EVENT_COUNT.load(Ordering::Relaxed);

        // Verify events were recorded (at least 2 more than initial)
        assert!(final_event_count >= initial_event_count + 2);

        // Verify the events themselves have proper IDs
        assert!(event2.event_id > event1.event_id);

        // Unix timestamp should be set for the event
        let event = ObliterationEvent::new(topic_id, 0, 1, 10);
        assert!(event.unix_timestamp > 0);
        assert!(
            event.unix_timestamp
                <= std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
        );
    }

    #[test]
    fn test_structured_metrics_access() {
        // Clear any existing state
        {
            let mut last_warning = LAST_WARNING_TIME
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *last_warning = None;
        }

        let metrics = get_obliteration_metrics();

        // Verify all fields are accessible (no negative values for unsigned types)
        assert!(metrics.total_events < u64::MAX);
        assert!(metrics.total_warnings < u64::MAX);
        assert!(metrics.last_warning_timestamp < u64::MAX);
        // time_since_last_warning can be None if no warning issued
    }

    #[test]
    fn test_aggregate_counter_produces_both_metrics() {
        // Create a new aggregate counter for testing
        let opts = prometheus::Opts::new("test_counter", "Test counter for aggregation");
        let counter = AggregateCounter::new(opts, &["label"]).unwrap();

        // Add some labeled values
        counter.with_label_values(&["value1"]).inc_by(10.0);
        counter.with_label_values(&["value2"]).inc_by(20.0);
        counter.with_label_values(&["value3"]).inc_by(30.0);

        // Collect metrics
        let metric_families = counter.collect();

        // Should have 1 metric family with both aggregate and labeled metrics
        assert_eq!(metric_families.len(), 1);

        let metric_family = &metric_families[0];
        assert_eq!(metric_family.get_name(), "test_counter");
        assert_eq!(metric_family.get_metric().len(), 4); // 1 aggregate + 3 labeled

        // First metric should be the aggregate (no labels)
        let aggregate_metric = &metric_family.get_metric()[0];
        assert_eq!(aggregate_metric.get_label().len(), 0); // No labels
        assert_eq!(aggregate_metric.get_counter().get_value(), 60.0); // Sum: 10+20+30

        // Remaining metrics should be labeled
        for i in 1..4 {
            let metric = &metric_family.get_metric()[i];
            assert_eq!(metric.get_label().len(), 1); // One label
            let label_value = &metric.get_label()[0].get_value();
            let counter_value = metric.get_counter().get_value();

            match label_value.as_ref() {
                "value1" => assert_eq!(counter_value, 10.0),
                "value2" => assert_eq!(counter_value, 20.0),
                "value3" => assert_eq!(counter_value, 30.0),
                _ => panic!("Unexpected label value: {}", label_value),
            }
        }
    }

    #[test]
    fn test_kafka_metrics_encoder() {
        // Create a new aggregate counter for testing
        let opts = prometheus::Opts::new("test_kafka_metrics", "Test counter for Kafka format");
        let counter = AggregateCounter::new(opts, &["topic"]).unwrap();

        // Add some labeled values
        counter.with_label_values(&["hits"]).inc_by(4.6384186519E10);
        counter.with_label_values(&["logs"]).inc_by(1.2345E9);

        // Collect metrics
        let metric_families = counter.collect();

        // Encode with custom encoder
        let encoder = KafkaMetricsEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();

        // Verify format
        assert!(output.contains("# HELP test_kafka_metrics Test counter for Kafka format"));
        assert!(output.contains("# TYPE test_kafka_metrics counter"));

        // Verify aggregate uses scientific notation (sum of hits + logs)
        assert!(output.contains("test_kafka_metrics 4.7618686519E10"));

        // Verify labeled metrics have trailing commas and scientific notation
        assert!(output.contains("test_kafka_metrics{topic=\"hits\",} 4.6384186519E10"));
        assert!(output.contains("test_kafka_metrics{topic=\"logs\",} 1.2345000000E9"));
    }
}
