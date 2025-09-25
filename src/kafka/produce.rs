// Kafka produce request handling
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use crate::kafka::broker::{Broker, BROKER};
use crate::kafka::counters::PRODUCE_SENSOR;
use crate::kafka::groups::GroupState;
use crate::plugins::call_plugins_on_record;
use crate::profiling::profile;
#[cfg(feature = "profiled")]
use crate::profiling::BlinkProfileOp;

use crate::alloc::GLOBAL_ALLOCATOR;
use crate::kafka::counters::{
    MESSAGES_TOPIC_BYTES_IN, MESSAGES_TOPIC_IN, PRODUCE_DURATION, SYNC_PURGE_COUNT,
};
use crate::settings::SETTINGS;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    InitProducerIdRequest, InitProducerIdResponse, ProduceRequest, ProduceResponse, ProducerId,
    ResponseKind,
};
use kafka_protocol::ResponseError;

use std::sync::atomic::Ordering;
use tokio::time::Instant;
use tracing::{debug, error};

pub fn compute_byte_size(request: &ProduceRequest) -> usize {
    request.topic_data.iter().fold(0, |total_size, topic| {
        total_size
            + topic
                .partition_data
                .iter()
                .fold(0, |partition_size, partition| {
                    partition_size
                        + partition
                            .records
                            .as_ref()
                            .map(|records| records.len())
                            .unwrap_or(0)
                })
    })
}

impl Broker {
    pub(crate) fn handle_init_producer_id(&self, _request: InitProducerIdRequest) -> ResponseKind {
        ResponseKind::InitProducerId(
            InitProducerIdResponse::default()
                .with_producer_id(ProducerId::from(
                    self.producers.fetch_add(1, Ordering::SeqCst),
                ))
                .with_producer_epoch(0),
        )
    }
    pub(crate) fn purge_now(&self) -> i32 {
        debug!("Purging data");
        self.topics
            .iter()
            .flat_map(|topic| {
                let topic_id = topic.topic_id;
                let partitions = topic.partitions;
                (0..partitions).map(move |partition| (topic_id, partition))
            })
            .map(|(topic_id, partition)| {
                self.storage
                    .purge_partition(&topic_id, partition)
                    .expect("ERROR PURGING")
            })
            .sum()
    }

    pub async fn handle_produce(&self, mut request: ProduceRequest) -> ResponseKind {
        profile!(BlinkProfileOp::HandleProduce, {
            if SETTINGS.enable_consumer_groups {
                // Check for consumer group rebalancing in transactional scenarios
                if let Some(transactional_id) = &request.transactional_id {
                    if !transactional_id.is_empty() {
                        // For transactional producers, check if any related consumer groups are rebalancing
                        // In practice, this would involve checking groups that coordinate with this transaction
                        // For now, we'll check if there are any active rebalancing groups that might affect this producer
                        let has_rebalancing_groups =
                            if let Some(ref consumer_groups) = self.consumer_groups {
                                consumer_groups.groups.iter().any(|entry| {
                                    if let Ok(group) = entry.value().try_read() {
                                        matches!(
                                            group.state,
                                            GroupState::PreparingRebalance
                                                | GroupState::CompletingRebalance
                                        )
                                    } else {
                                        false
                                    }
                                })
                            } else {
                                false
                            };

                        if has_rebalancing_groups {
                            // Return appropriate error for transactional producer during rebalance
                            let mut topic_responses = vec![];
                            for topic_data in request.topic_data.iter() {
                                let mut topic_response = TopicProduceResponse::default()
                                    .with_name(topic_data.name.clone());
                                for partition_data in topic_data.partition_data.iter() {
                                    topic_response.partition_responses.push(
                                        PartitionProduceResponse::default()
                                            .with_index(partition_data.index)
                                            .with_error_code(
                                                ResponseError::RebalanceInProgress.code(),
                                            ),
                                    );
                                }
                                topic_responses.push(topic_response);
                            }
                            return ResponseKind::Produce(
                                ProduceResponse::default().with_responses(topic_responses),
                            );
                        }
                    }
                }
            }

            let now = Instant::now();
            PRODUCE_SENSOR.measure(1.0);

            // Track bytes received for memory limit check
            let request_size = compute_byte_size(&request);

            let allocated = GLOBAL_ALLOCATOR.current_allocated();
            if allocated + request_size > SETTINGS.max_memory {
                debug!("Memory limit exceeded: {} bytes", allocated);
                BROKER.purge_now();
                SYNC_PURGE_COUNT.inc();
                let mut topic_responses = vec![];
                for topic_data in request.topic_data.iter() {
                    let mut topic_response = TopicProduceResponse::default();
                    for partition_data in topic_data.partition_data.iter() {
                        topic_response.partition_responses.push(
                            PartitionProduceResponse::default()
                                .with_index(partition_data.index)
                                .with_error_code(ResponseError::ThrottlingQuotaExceeded.code()),
                        );
                    }
                    topic_responses.push(topic_response);
                } // sleep(Duration.millis(100)).await;
                return ResponseKind::Produce(
                    ProduceResponse::default()
                        .with_throttle_time_ms(100)
                        .with_responses(topic_responses),
                );
            }
            let mut produce_response = ProduceResponse::default();
            //sleep(Duration::from_micros(500)).await;

            for topic_data in request.topic_data.iter_mut() {
                let mut topic_response =
                    TopicProduceResponse::default().with_name(topic_data.name.clone());
                let topic_name = &topic_data.name;

                // Track metrics per topic before validation to ensure they're always updated
                for partition_data in topic_data.partition_data.iter() {
                    if let Some(ref records) = partition_data.records {
                        let record_bytes = records.len();
                        let record_count = self.storage.read_record_batch_count(records);

                        MESSAGES_TOPIC_IN
                            .with_label_values(&[&topic_name.0])
                            .inc_by(record_count as f64);
                        MESSAGES_TOPIC_BYTES_IN
                            .with_label_values(&[&topic_name.0])
                            .inc_by(record_bytes as f64);

                        // Observe the number of records in this produced RecordBatch so we can
                        // aggregate produce batch-size distributions.
                        crate::kafka::counters::PRODUCE_BATCH_SIZE.observe(record_count as f64);
                    }
                }

                let topic_id = self.get_topic_id(&topic_name);
                let topic_id = match topic_id {
                    Some(topic_id) => topic_id,
                    None => {
                        error!("Unknown topic: {:?}", topic_name);
                        // This should not happen as a well-behaved client should have fetched
                        // metadata before producing.
                        for partition_data in topic_data.partition_data.iter() {
                            topic_response.partition_responses.push(
                                PartitionProduceResponse::default()
                                    .with_index(partition_data.index)
                                    .with_error_code(ResponseError::UnknownTopicOrPartition.code()),
                            );
                        }
                        produce_response.responses.push(topic_response);
                        continue;
                    }
                };

                for partition_data in topic_data.partition_data.iter_mut() {
                    let records = partition_data.records.take().unwrap();

                    // Call plugins for each record batch
                    call_plugins_on_record(
                        &topic_name.0,
                        partition_data.index as u32,
                        None,         // For now, we don't extract individual keys from the batch
                        "batch_data", // Simplified - in a real implementation, we'd extract individual records
                    )
                    .await;

                    // Use the new zero-copy method that accepts Bytes directly
                    let result = self.storage.store_record_batch(
                        &topic_id,
                        partition_data.index,
                        records, // Pass Bytes directly without conversion
                    );
                    if let Err(e) = result {
                        error!("Error storing record batch: {:?}", e);
                        topic_response.partition_responses.push(
                            PartitionProduceResponse::default()
                                .with_index(partition_data.index)
                                .with_error_code(ResponseError::UnknownServerError.code()),
                        );
                        continue;
                    }
                    topic_response.partition_responses.push(
                        PartitionProduceResponse::default()
                            .with_index(partition_data.index)
                            .with_error_code(0),
                    );
                }
                produce_response.responses.push(topic_response);
            }

            let response = ResponseKind::Produce(produce_response.with_throttle_time_ms(0));
            PRODUCE_DURATION.observe(now.elapsed().as_micros() as f64);
            response
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::broker::BROKER;
    use crate::util::Util;
    use kafka_protocol::messages::{ResponseKind, TopicName};
    use kafka_protocol::protocol::StrBytes;

    #[tokio::test]
    async fn test_produce_counters_updated() {
        // Initialize settings if not already done
        use std::sync::Once;
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            if !crate::settings::Settings::is_initialized() {
                let handle = std::thread::spawn(move || -> Result<(), String> {
                    let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
                    rt.block_on(crate::settings::Settings::init("settings.yaml"))
                        .map_err(|e| e.to_string())
                });
                match handle.join() {
                    Ok(Ok(())) => {
                        // Settings initialized successfully
                    }
                    Ok(Err(e)) => {
                        // Settings initialization failed, but that's ok if already initialized
                        if !e.contains("already initialized") {
                            panic!("Failed to initialize settings: {}", e);
                        }
                    }
                    Err(_) => {
                        panic!("Settings initialization thread panicked");
                    }
                }
            }
        });

        // Get initial counter values
        let initial_message_count = MESSAGES_TOPIC_IN.with_label_values(&["test-topic"]).get();
        let initial_bytes_count = MESSAGES_TOPIC_BYTES_IN
            .with_label_values(&["test-topic"])
            .get();

        // Create a test produce request
        let records = vec![
            Util::create_new_record("key1", "value1"),
            Util::create_new_record("key2", "value2"),
        ];

        let produce_request = Util::create_produce_request(
            TopicName(StrBytes::from_string("test-topic".to_string())),
            0,
            &records,
        );

        // Handle the produce request
        let response = BROKER.handle_produce(produce_request).await;

        // Verify the response structure (error code might be non-zero for unknown topic)
        match response {
            ResponseKind::Produce(resp) => {
                assert_eq!(resp.responses.len(), 1);
                assert_eq!(resp.responses[0].partition_responses.len(), 1);
                // Topic might not exist in test, so we don't assert on success
            }
            _ => panic!("Expected Produce response"),
        }

        // Check that counters were updated
        let final_message_count = MESSAGES_TOPIC_IN.with_label_values(&["test-topic"]).get();
        let final_bytes_count = MESSAGES_TOPIC_BYTES_IN
            .with_label_values(&["test-topic"])
            .get();

        // Message count should have increased by 2 (we sent 2 records)
        let message_increase = final_message_count - initial_message_count;
        assert_eq!(message_increase, 2.0, "Expected 2 messages to be tracked");

        // Bytes count should have increased (we sent some data)
        let bytes_increase = final_bytes_count - initial_bytes_count;
        assert!(bytes_increase > 0.0, "Expected some bytes to be tracked");

        // Verify the actual increases are reasonable
        assert!(
            bytes_increase > 50.0,
            "Expected at least 50 bytes for 2 records with keys and values"
        );
    }
}
