// Kafka broker implementation and request handling
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//

use crate::kafka::counters::{ASYNC_PURGE_COUNT, REQUEST_QUEUE_TIME_MS, REQUEST_TOTAL_ITEMS};
use crate::kafka::metadata::TopicMeta;
use crate::kafka::storage::Storage;
use crate::metric_with_fetch_consumer;
use crate::settings::{ApiTrace, SETTINGS};
use crate::util::pretty_print_debug;
use dashmap::DashMap;

use kafka_protocol::messages::{RequestKind, ResponseKind, TopicName};
use kafka_protocol::ResponseError;
use once_cell::sync::Lazy;

use std::sync::atomic::AtomicI64;
use std::time::Duration;
use tokio::time::{interval, Instant};
use tracing::debug;

use crate::kafka::groups::ConsumerGroupManager;

/// Macro to handle consumer group requests that require the consumer groups feature to be enabled
macro_rules! consumer_group_request {
    ($self:expr, $handler:expr) => {
        if SETTINGS.enable_consumer_groups {
            $handler
        } else {
            return Err(ResponseError::InvalidRequest);
        }
    };
}

pub static BROKER: Lazy<Broker> = Lazy::new(|| Broker::new());

#[derive(Debug)]
pub struct Broker {
    pub host: String,
    pub port: i32,
    pub(crate) topics: DashMap<TopicName, TopicMeta>,
    pub(crate) producers: AtomicI64,
    pub(crate) storage: Storage,
    pub(crate) consumer_groups: Option<ConsumerGroupManager>,
}

impl Broker {
    pub fn new() -> Broker {
        let primary_port = SETTINGS.broker_ports.first().copied().unwrap_or(9092);
        Broker {
            host: SETTINGS.kafka_hostname.to_string(),
            port: primary_port as i32,
            topics: DashMap::new(),
            producers: AtomicI64::new(1),
            storage: Storage::new(),
            consumer_groups: if SETTINGS.enable_consumer_groups {
                Some(ConsumerGroupManager::new())
            } else {
                None
            },
        }
    }

    pub async fn reply(
        &self,
        request_time: &mut Instant,
        request_name: &str,
        request_kind: RequestKind,
    ) -> Result<ResponseKind, ResponseError> {
        // Log request if API is configured for tracing
        let trace_config = SETTINGS.should_trace_api(request_name);
        if matches!(trace_config, ApiTrace::Request | ApiTrace::Both) {
            println!(
                "➡️ {} Request: ___________________________\n{}",
                request_name,
                pretty_print_debug(&format!("{:?}", request_kind))
            );
        }

        REQUEST_TOTAL_ITEMS.with_label_values(&[request_name]).inc();
        let elapsed = request_time.elapsed().as_millis() as f64;
        metric_with_fetch_consumer!(REQUEST_QUEUE_TIME_MS, request_name, elapsed);
        *request_time = Instant::now();

        let result = match request_kind {
            RequestKind::ApiVersions(request) => self.handle_api_versions(request),
            RequestKind::CreateTopics(request) => self.handle_create_topics(request),
            RequestKind::FindCoordinator(request) => self.handle_find_coordinator(request),
            RequestKind::InitProducerId(request) => self.handle_init_producer_id(request),
            RequestKind::ListOffsets(request) => self.handle_list_offsets(request),
            RequestKind::Fetch(request) => self.handle_fetch(request).await,
            RequestKind::Metadata(request) => self.handle_metadata(request),
            RequestKind::Produce(request) => self.handle_produce(request).await,
            RequestKind::OffsetFetch(request) => self.handle_offset_fetch(request),
            RequestKind::DeleteTopics(request) => self.handle_delete_topics(request),
            RequestKind::JoinGroup(request) => {
                consumer_group_request!(self, self.handle_join_group(request).await)
            }
            RequestKind::SyncGroup(request) => {
                consumer_group_request!(self, self.handle_sync_group(request).await)
            }
            RequestKind::Heartbeat(request) => {
                consumer_group_request!(self, self.handle_heartbeat(request).await)
            }
            RequestKind::LeaveGroup(request) => {
                consumer_group_request!(self, self.handle_leave_group(request).await)
            }
            RequestKind::OffsetCommit(request) => {
                consumer_group_request!(self, self.handle_offset_commit(request))
            }
            _ => return Err(ResponseError::InvalidRequest),
        };

        // Log response if API is configured for tracing
        if matches!(trace_config, ApiTrace::Response | ApiTrace::Both) {
            println!(
                "⬅️ {} Response: ___________________________\n{}",
                request_name,
                pretty_print_debug(&format!("{:?}", result))
            );
        }

        Ok(result)
    }

    pub fn start_background_purge_task() {
        tokio::spawn(async {
            let mut interval = interval(Duration::from_secs(SETTINGS.purge_delay_seconds));
            loop {
                interval.tick().await;
                debug!("Background purge task running");

                // Only increment counter if some purging happened
                if BROKER.purge_now() > 0 && !BROKER.topics.is_empty() {
                    ASYNC_PURGE_COUNT.inc();
                    debug!("Background purge completed successfully");
                }
            }
        });
    }

    pub fn start_consumer_group_tasks() {
        if SETTINGS.enable_consumer_groups {
            if let Some(ref consumer_groups) = BROKER.consumer_groups {
                // Start the rebalance watchdog task
                consumer_groups.start_rebalance_watchdog();
            }
        }
    }

    pub fn start_idle_time_tracking_task() {
        tokio::spawn(async {
            let mut interval = interval(Duration::from_secs(5));
            loop {
                interval.tick().await;

                // Update idle time percentage periodically
                if let Ok(tracker) = crate::kafka::counters::IDLE_TRACKER.lock() {
                    // Force an update of the idle percentage calculation
                    let current_total_idle = tracker.get_current_total_idle_time();
                    let total_uptime =
                        tokio::time::Instant::now().duration_since(tracker.start_time);

                    // Update idle time gauge
                    crate::kafka::counters::IDLE_TIME_SECONDS.set(current_total_idle.as_secs_f64());

                    // Update idle percentage
                    if !total_uptime.is_zero() {
                        let idle_percentage =
                            (current_total_idle.as_secs_f64() / total_uptime.as_secs_f64()) * 100.0;
                        crate::kafka::counters::IDLE_TIME_PERCENTAGE.set(idle_percentage);
                    }
                }
            }
        });
    }
}
