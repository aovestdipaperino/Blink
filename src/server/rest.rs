// REST API server implementation
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use crate::alloc::GLOBAL_ALLOCATOR;
use crate::kafka::broker::BROKER;
use crate::kafka::counters;

use crate::server::VERSION;
use crate::settings::SETTINGS;
use crate::util::toggles::Toggles;
use crate::util::Util;
use hyper::{body::Body, Request as HyperRequest, Response};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;

pub(crate) struct RestApi;

impl RestApi {
    pub async fn handle_request(req: HyperRequest<Body>) -> Result<Response<Body>, hyper::Error> {
        let response = match (req.method(), req.uri().path()) {
            // Debug functionality
            (_get, "/") => Self::response_from_string(counters::collect_metrics()),
            (_get, "/dump") => Self::response_from_string(BROKER.storage.dump()),
            (_get, "/version") => {
                Self::response_from_string(format!("{}\n{:#?}", *VERSION, *SETTINGS))
            }
            (_get, "/metrics") => Self::response_from_string(counters::collect_metrics()),
            (_get, "/crash") => {
                std::process::exit(1);
            }
            (_post, "/produce") => {
                let (topic, partition, key, value) =
                    Self::read_produce_form_fields(req).await.unwrap();
                Self::produce_request(topic, partition, key.clone().leak(), value.clone().leak())
                    .await
            }
            (_post, "/fetch") => {
                let (topic, partition, offset) = Self::read_fetch_form_fields(req).await.unwrap();
                Self::fetch_request(topic, partition, offset).await
            }
            (_get, "/purge") => {
                let now = Util::now();
                BROKER.purge_now();
                Self::response_from_string(format!("Purge done in {} msecs", Util::now() - now))
            }
            (_get, "/allocated") => {
                let allocated = GLOBAL_ALLOCATOR.current_allocated();
                Self::response_from_string(format!("Currently allocated: {}", allocated))
            }
            (_get, "/purge_enabled") => {
                let enable_purge = Toggles::get_enable_purge();
                Self::response_from_string(format!("Enable purge: {}", enable_purge))
            }
            (_get, "/disable_purge") => {
                Toggles::set_enable_purge(false);
                Self::response_from_string("Purge disabled".to_string())
            }
            (_get, "/enable_purge") => {
                Toggles::set_enable_purge(true);
                Self::response_from_string("Purge enabled".to_string())
            }
            #[cfg(feature = "profiled")]
            (_get, "/profiling-report") => Self::response_from_string(Self::get_profile_counts()),

            _ => Response::builder()
                .status(404)
                .body(Body::from("Not found".to_string()))
                .unwrap(),
        };

        Ok(response)
    }

    async fn read_produce_form_fields(
        req: HyperRequest<Body>,
    ) -> Result<(String, i32, String, String), hyper::Error> {
        let body_bytes = hyper::body::to_bytes(req.into_body()).await?;
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

        let mut topic = String::new();
        let mut partition = 0;
        let mut key = String::new();
        let mut value = String::new();

        for (field, value_str) in form_urlencoded::parse(body_str.as_bytes()) {
            match field.as_ref() {
                "topic" => topic = value_str.into_owned(),
                "partition" => partition = value_str.parse().unwrap_or(0),
                "key" => key = value_str.into_owned(),
                "value" => value = value_str.into_owned(),
                _ => {}
            }
        }

        Ok((topic, partition, key, value))
    }

    async fn read_fetch_form_fields(
        req: HyperRequest<Body>,
    ) -> Result<(String, i32, i64), hyper::Error> {
        let body_bytes = hyper::body::to_bytes(req.into_body()).await?;
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

        let mut topic = String::new();
        let mut partition = 0;
        let mut offset = 0i64;

        for (field, value_str) in form_urlencoded::parse(body_str.as_bytes()) {
            match field.as_ref() {
                "topic" => topic = value_str.into_owned(),
                "partition" => partition = value_str.parse().unwrap_or(0),
                "offset" => offset = value_str.parse().unwrap_or(0),
                _ => {}
            }
        }

        Ok((topic, partition, offset))
    }
    async fn fetch_request(topic: String, partition: i32, offset: i64) -> Response<Body> {
        let request = FetchRequest::default()
            .with_max_bytes(1024 * 1024)
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_string(topic)))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(partition)
                    .with_partition_max_bytes(1024 * 1024)
                    .with_fetch_offset(offset)])]);
        let response = BROKER.handle_fetch(request).await;
        Self::response_from_string(format!("{:?}", response))
    }
    async fn produce_request(
        topic: String,
        partition: i32,
        key: &'static str,
        value: &'static str,
    ) -> Response<Body> {
        let record = Util::create_new_record(key, value);

        let produce_request = Util::create_produce_request(
            TopicName(StrBytes::from_string(topic)),
            partition,
            &vec![record],
        );

        let response = BROKER.handle_produce(produce_request).await;
        Self::response_from_string(format!("{:?}", response))
    }

    fn response_from_string(s: String) -> Response<Body> {
        Response::builder()
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            .header("Access-Control-Allow-Headers", "Content-Type")
            .header("Content-Type", "text/plain")
            .body(Body::from(format!("{}\n", s)))
            .unwrap()
    }

    #[cfg(feature = "profiled")]
    fn get_profile_counts() -> String {
        use quantum_pulse::{DefaultCategory, ReportBuilder, TimeFormat};

        let mut output = String::new();
        output.push_str("Blink Kafka Broker - Profile Counts\n");
        output.push_str("===================================\n\n");

        if !quantum_pulse::ProfileCollector::has_data() {
            output.push_str("No profiling data available yet.\n");
            output.push_str("Run some produce/fetch operations to generate profile data.\n\n");
        } else {
            // Use quantum-pulse's built-in comprehensive reporting
            let report = ReportBuilder::<DefaultCategory>::new()
                .include_percentiles(true)
                .time_format(TimeFormat::Auto)
                .group_by_category(false)
                .include_summary(true)
                .sort_by_time(true)
                .build();

            // Get the formatted report
            output.push_str(&report.to_console_string());
        }

        output.push_str("\n");
        output.push_str("STATUS: Profiling is ENABLED\n");
        output.push_str("- Real-time performance metrics collected by quantum-pulse\n");
        output.push_str("- Comprehensive percentile statistics (P50, P95, P99, P99.9)\n");
        output.push_str("- Visit /profile-counts to refresh data\n");
        output.push_str("- To disable profiling, rebuild without --features profiled\n");

        output
    }
}
