// Utility modules
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ProduceRequest, TopicName};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

pub mod iterators;
pub mod protocol;
pub mod toggles;

/// Macro to handle the repetitive pattern of adding FetchConsumer metrics
/// when the request_name is "Fetch".
///
/// Usage:
/// metric_with_fetch_consumer!(METRIC_NAME, request_name, elapsed)
#[macro_export]
macro_rules! metric_with_fetch_consumer {
    ($metric:expr, $request_name:expr, $elapsed:expr) => {
        $metric.with_label_values(&[$request_name]).inc_by($elapsed);
        if $request_name == "Fetch" {
            $metric
                .with_label_values(&["FetchConsumer"])
                .inc_by($elapsed);
        }
    };
}

pub struct Util;

impl Util {
    pub fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    pub fn create_produce_request(
        topic_name: TopicName,
        partition: i32,
        records: &Vec<Record>,
    ) -> ProduceRequest {
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();
        let bytes = encoded.freeze();

        ProduceRequest::default()
            .with_acks(1)
            .with_timeout_ms(5000)
            .with_topic_data(
                [TopicProduceData::default()
                    .with_name(topic_name)
                    .with_partition_data(vec![PartitionProduceData::default()
                        .with_index(partition)
                        .with_records(Some(bytes))])]
                .into(),
            )
    }

    pub fn create_new_record(key: &'static str, value: &'static str) -> Record {
        let offset = 0;
        let v2 = true;
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: if v2 { 0 } else { -1 },
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            timestamp: offset,
            sequence: if v2 { offset as _ } else { -1 },
            offset,
            key: Some(Bytes::from(key.as_bytes())),
            value: Some(Bytes::from(value.as_bytes())),
            headers: Default::default(),
        }
    }
}

/// Pretty print a debug string with proper indentation
pub fn pretty_print_debug(debug_str: &str) -> String {
    let mut result = String::new();
    let mut indent_level = 0;
    let mut chars = debug_str.chars().peekable();
    let mut in_string = false;
    let mut after_newline = false;

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                in_string = !in_string;
                result.push(ch);
            }
            '{' | '[' if !in_string => {
                result.push(ch);
                result.push('\n');
                indent_level += 1;
                result.push_str(&"  ".repeat(indent_level));
                after_newline = true;
            }
            '}' | ']' if !in_string => {
                // Remove trailing spaces from the current line
                while result.ends_with(' ') {
                    result.pop();
                }
                if !result.ends_with('\n') {
                    result.push('\n');
                }
                indent_level = indent_level.saturating_sub(1);
                result.push_str(&"  ".repeat(indent_level));
                result.push(ch);

                // Handle comma after closing brace/bracket
                if chars.peek() == Some(&',') && !in_string {
                    chars.next(); // consume the comma
                    result.push(',');
                    result.push('\n');
                    result.push_str(&"  ".repeat(indent_level));
                } else if indent_level == 0 {
                    // End of top-level structure
                    result.push('\n');
                }
            }
            ',' if !in_string => {
                result.push(ch);
                result.push('\n');
                result.push_str(&"  ".repeat(indent_level));
                after_newline = true;
            }
            ':' if !in_string => {
                result.push(ch);
                result.push(' ');
                // Skip any additional spaces after the colon
                while chars.peek() == Some(&' ') {
                    chars.next();
                }
            }
            ' ' if after_newline && !in_string => {
                // Skip spaces at the beginning of new lines (we handle indentation ourselves)
            }
            _ => {
                result.push(ch);
                after_newline = false;
            }
        }
    }

    // Clean up any trailing whitespace
    result.trim_end().to_string()
}
