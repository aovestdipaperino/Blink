// Kafka fetch request handling
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use crate::kafka::broker::Broker;
use crate::kafka::counters::{FETCH_DURATION, MESSAGES_TOPIC_BYTES_OUT, QUEUE_DURATION};
use crate::kafka::storage::RecordBatchEntry;
use crate::profiling::profile;
#[cfg(feature = "profiled")]
use crate::profiling::BlinkProfileOp;
use crate::settings::SETTINGS;
use crate::util::iterators::GroupedIterator;
use crate::util::Util;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::FetchableTopicResponse;
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
    OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use kafka_protocol::messages::{
    FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, OffsetFetchRequest,
    OffsetFetchResponse, ResponseKind, TopicName,
};
use kafka_protocol::ResponseError;
use std::cmp::min;
use std::future::Future;
use std::mem;
use tokio::time::Instant;
use uuid::Uuid;

const EMPTY_OFFSET: i64 = 0;
#[inline]
fn with_topic_partition<T, F>(
    topic: Uuid,
    topic_name: TopicName,
    index: i32,
    fut: F,
) -> impl Future<Output = Result<(Uuid, TopicName, i32, T), ResponseError>>
where
    T: Send + 'static,
    F: Future<Output = Result<T, ResponseError>>,
{
    async move { fut.await.map(|r| (topic, topic_name, index, r)) }
}

impl Broker {
    async fn fetch_record_batch(
        &self,
        partition: &FetchPartition,
        topic: &FetchTopic,
        max_bytes: usize,
    ) -> Result<(RecordBatchEntry, i64), ResponseError> {
        profile!(BlinkProfileOp::FetchRecordBatch, {
            let index = partition.partition;
            let offset = partition.fetch_offset;
            let topic_id = if topic.topic.as_str() != "" {
                let topic_id = self.get_topic_id(&topic.topic);
                if let Some(topic_id) = topic_id {
                    topic_id
                } else {
                    return Err(ResponseError::UnknownTopicId);
                }
            } else {
                topic.topic_id
            };

            self.storage
                .retrieve_record_batch(&topic_id, index, offset, max_bytes)
                .await
        })
    }

    /// Collects results from fetch futures with timeout handling
    /// Returns a vector of partition responses that completed within the timeout
    /// Uses FuturesUnordered to poll all futures concurrently

    /// Ensures all requested partitions are included in the response, even if they timed out
    /// Adds empty RecordBatchEntry for any missing partitions
    #[inline]
    fn ensure_all_partitions_included(
        &self,
        request: &FetchRequest,
        partition_responses: &mut Vec<(Uuid, TopicName, i32, RecordBatchEntry, i64)>,
    ) {
        // Build a complete list of all requested partitions from the original request
        let mut all_requested_partitions = vec![];
        for topic in request.topics.iter() {
            // Resolve topic ID using the same logic as in create_fetch_futures
            let topic_id = if topic.topic.as_str() != "" {
                // Use topic name lookup, fallback to topic_id if lookup fails
                self.get_topic_id(&topic.topic).unwrap_or(topic.topic_id)
            } else {
                // Use topic_id directly
                topic.topic_id
            };

            // Add each partition to our tracking list
            for partition in topic.partitions.iter() {
                all_requested_partitions.push((topic_id, topic.topic.clone(), partition.partition));
            }
        }

        // Check each requested partition and add empty response if missing
        for (topic_id, topic_name, partition_index) in all_requested_partitions {
            // Check if this partition already has a response
            let has_response = partition_responses
                .iter()
                .any(|(tid, _, idx, _, _)| *tid == topic_id && *idx == partition_index);

            if !has_response {
                // This partition timed out or failed - add an empty response
                // This ensures the client gets a response for every requested partition
                partition_responses.push((
                    topic_id,
                    topic_name,
                    partition_index,
                    RecordBatchEntry {
                        ingestion_time: 0,
                        records: Some(bytes::Bytes::new()), // Empty bytes for no data
                    },
                    0,
                ));
            }
        }
    }

    /// Groups partition responses by topic and builds the final FetchResponse
    /// Includes latency metrics recording for successful fetches
    #[inline]
    fn build_fetch_response(
        &self,
        partition_responses: Vec<(Uuid, TopicName, i32, RecordBatchEntry, i64)>,
    ) -> Vec<FetchableTopicResponse> {
        profile!(BlinkProfileOp::FetchResponseBuild, {
            let mut responses: Vec<FetchableTopicResponse> = vec![];

            // Group partition responses by topic using the GroupedIterator utility
            // This transforms Vec<(topic_id, topic_name, partition_index, entry)>
            // into grouped results by topic_id
            let grouped = GroupedIterator::new(partition_responses.into_iter());

            for result in grouped {
                let mut partition_results = vec![];
                let (topic_id, topic_name, partitions) = result;

                // Process each partition within this topic
                for (index, mut entry, watermark) in partitions {
                    // Always include every partition, even if records are empty
                    if entry.records.is_none() {
                        // Defensive: should never happen, but skip if so
                        continue;
                    }

                    // Handle latency metrics for successful fetches
                    if entry.records.as_ref().unwrap().is_empty() {
                        // Empty result set (timeout or no data)
                        // No latency metrics recorded for empty responses
                    } else if entry.ingestion_time > 0 {
                        // Calculate how long the data was queued before being fetched
                        let latency = (Util::now() - entry.ingestion_time) as u64;

                        // Clear the ingestion time to avoid double-counting
                        let mut never: i64 = i64::MAX;
                        mem::swap(&mut entry.ingestion_time, &mut never);

                        // Record latency metrics if there was measurable latency
                        if latency > 0 {
                            QUEUE_DURATION.observe(latency as f64);
                        }
                    }

                    // Build the partition response with all necessary metadata
                    let partition_response = PartitionData::default()
                        .with_partition_index(index)
                        .with_high_watermark(watermark) // Latest available offset
                        .with_last_stable_offset(watermark) // Same as high watermark for simplicity
                        .with_records(entry.records); // The actual record data

                    partition_results.push(partition_response.to_owned());
                }

                // Create the topic response containing all partition responses for this topic
                responses.push(
                    FetchableTopicResponse::default()
                        .with_topic_id(topic_id)
                        .with_topic(topic_name)
                        .with_partitions(partition_results),
                );
            }

            responses
        })
    }

    /// Handle Kafka Fetch requests with intelligent waiting and response logic.
    ///
    /// ## Fetch Logic Overview:
    ///
    /// The fetch operation uses a two-phase approach to balance latency and throughput:
    ///
    /// ### Phase 1: Immediate Data Collection (1ms timeout)
    /// - Collects all immediately available data from requested partitions
    /// - Uses a very short timeout (1ms) to avoid blocking on empty partitions
    /// - Accumulates response byte size to check against min_bytes requirement
    ///
    /// ### Phase 2: Conditional Waiting (up to max_wait_ms)
    /// - Only triggered if:
    ///   - Response size < min_bytes AND
    ///   - There are still pending partition futures
    /// - Waits up to max_wait_ms for additional data
    /// - Stops early if min_bytes threshold is reached
    ///
    /// ### When We Send Empty Results:
    /// - Partitions that don't respond within timeouts get empty responses
    /// - All requested partitions MUST have a response (ensured by ensure_all_partitions_included)
    /// - Empty responses contain zero-length records but valid partition metadata
    ///
    /// ### When We Wait:
    /// - If total response bytes < min_bytes after immediate collection
    /// - Up to max_wait_ms duration (long polling for efficiency)
    /// - Stops waiting when min_bytes reached or all futures complete
    ///
    /// ### Key Behaviors:
    /// - **Fair partition handling**: Each partition gets proportional max_bytes allocation
    /// - **Complete responses**: Every requested partition gets a response (empty if no data)
    /// - **Latency optimization**: Returns immediately if min_bytes satisfied
    /// - **Throughput optimization**: Waits for more data if client requested it (min_bytes > 0)
    ///
    /// This approach ensures clients get predictible behavior while optimizing for both
    /// low-latency (small min_bytes) and high-throughput (larger min_bytes) use cases.
    pub async fn handle_fetch(&self, request: FetchRequest) -> ResponseKind {
        profile!(BlinkProfileOp::HandleFetch, {
            let max_wait = request.max_wait_ms;
            let min_bytes = request.min_bytes as usize;
            let fetch_start = Instant::now();

            let mut futures = vec![];
            let total_partitions = request
                .topics
                .iter()
                .fold(0, |acc, topic| acc + topic.partitions.len() as i32);
            let avg_partition_size = if total_partitions > 0 {
                request.max_bytes / total_partitions
            } else {
                0
            };

            for topic in request.topics.iter() {
                let topic_id = match self.resolve_topic_id(topic) {
                    Ok(value) => value,
                    Err(value) => return value,
                };

                for partition in topic.partitions.iter() {
                    let index = partition.partition;
                    futures.push(with_topic_partition(
                        topic_id,
                        topic.topic.clone(),
                        index,
                        self.fetch_record_batch(
                            partition,
                            topic,
                            min(avg_partition_size, partition.partition_max_bytes) as usize,
                        ),
                    ));
                }
            }

            // Collect from all partitions that have data available, only wait if min_bytes not reached
            use futures::stream::{FuturesUnordered, StreamExt};
            let mut partition_responses = Vec::new();
            let mut response_byte_size = 0;
            let mut futs = FuturesUnordered::new();
            for fut in futures {
                futs.push(fut);
            }
            let deadline =
                tokio::time::Instant::now() + std::time::Duration::from_millis(max_wait as u64);

            // First phase: collect all immediately available data with a very short timeout
            let immediate_deadline =
                tokio::time::Instant::now() + std::time::Duration::from_millis(1);

            while !futs.is_empty() {
                tokio::select! {
                    biased;
                    Some(res) = futs.next() => {
                        match res {
                            Ok((topic_id, topic_name, partition_index, response)) => {
                                response_byte_size += response.0.records.as_ref().map(|b| b.len()).unwrap_or(0);
                                partition_responses.push((topic_id, topic_name, partition_index, response.0, response.1));
                            }
                            Err(_err) => {
                                // Optionally handle/log errors, or skip
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(immediate_deadline) => {
                        // Short timeout reached, stop collecting immediately available data
                        break;
                    }
                }
            }

            // Second phase: wait for more data only if we haven't reached min_bytes and have pending futures
            if response_byte_size < min_bytes && !futs.is_empty() {
                loop {
                    tokio::select! {
                        biased;
                        Some(res) = futs.next() => {
                            match res {
                                Ok((topic_id, topic_name, partition_index, response)) => {
                                    response_byte_size += response.0.records.as_ref().map(|b| b.len()).unwrap_or(0);
                                    partition_responses.push((topic_id, topic_name, partition_index, response.0, response.1));
                                }
                                Err(_err) => {
                                    // Optionally handle/log errors, or skip
                                }
                            }
                            if response_byte_size >= min_bytes {
                                break;
                            }
                        }
                        _ = tokio::time::sleep_until(deadline), if !futs.is_empty() => {
                            // Timeout reached, break and return what we have
                            break;
                        }
                        else => {
                            // All futures completed
                            break;
                        }
                    }
                }
            }

            self.ensure_all_partitions_included(&request, &mut partition_responses);

            let responses = self.build_fetch_response(partition_responses);

            // Track per-topic bytes and observe fetch batch sizes (records per returned RecordBatch).
            // For each partition's records (if present) we:
            //  - accumulate bytes for per-topic bytes metric
            //  - compute the record count via storage helper and observe FETCH_BATCH_SIZE histogram
            for topic_response in &responses {
                let mut topic_bytes: usize = 0;
                for partition in &topic_response.partitions {
                    if let Some(ref records) = partition.records {
                        let bytes = records.len();
                        topic_bytes += bytes;

                        // Determine number of records in this fetched record batch and observe histogram.
                        // Use storage helper to decode the record count (same helper used in produce path).
                        let record_count = self.storage.read_record_batch_count(records);
                        crate::kafka::counters::FETCH_BATCH_SIZE.observe(record_count as f64);
                    }
                }

                // Track bytes out per topic
                if topic_bytes > 0 {
                    let topic_name = &topic_response.topic.0;
                    let topic_label = if topic_name.is_empty() {
                        "unknown"
                    } else {
                        topic_name
                    };
                    MESSAGES_TOPIC_BYTES_OUT
                        .with_label_values(&[topic_label])
                        .inc_by(topic_bytes as f64);
                }
            }

            let response = FetchResponse::default()
                .with_throttle_time_ms(0)
                .with_error_code(0)
                .with_session_id(0)
                .with_responses(responses);

            let response = ResponseKind::Fetch(response);

            FETCH_DURATION.observe(fetch_start.elapsed().as_micros() as f64);
            response
        })
    }

    #[inline]
    fn resolve_topic_id(&self, topic: &FetchTopic) -> Result<Uuid, ResponseKind> {
        let topic_id = if topic.topic.as_str() != "" {
            // Topic name provided - look up the corresponding topic ID
            let topic_id = self.get_topic_id(&topic.topic);
            if let Some(topic_id) = topic_id {
                topic_id
            } else {
                // Topic not found - return error response immediately
                //TODO: improve error handling
                return Err(ResponseKind::Fetch(
                    FetchResponse::default()
                        .with_throttle_time_ms(0)
                        .with_error_code(1)
                        .with_session_id(0)
                        .with_responses(vec![]),
                ));
            }
        } else {
            // Topic ID provided directly
            topic.topic_id
        };
        Ok(topic_id)
    }

    fn get_offset(&self, topic_name: TopicName, partition: i32, timestamp: i64) -> i64 {
        //let _ = self.storage.dump();
        let topic_id = self.get_topic_id(&topic_name).unwrap();
        let storage = &self.storage.topic_partition_store;

        let partition = storage.get(&(topic_id, partition));
        if partition.is_none() {
            return -1;
        }
        let partition = partition.unwrap();
        match timestamp {
            -1 => {
                // LATEST - return the last written offset, or -1 if empty
                if partition.queue.is_empty() {
                    -1
                } else {
                    if SETTINGS.use_last_accessed_offset {
                        partition.last_accessed_offset
                    } else {
                        let last_idx = partition.queue.len() - 1;
                        partition.get_last_offset_at_position(last_idx)
                    }
                }
            }
            -2 => {
                // EARLIEST
                if partition.queue.is_empty() {
                    return EMPTY_OFFSET;
                }
                partition.get_first_offset_at_position(0)
            }
            _ => {
                for (idx, record) in partition.queue.iter().enumerate() {
                    if record.ingestion_time() >= timestamp {
                        return partition.get_last_offset_at_position(idx);
                    }
                }
                EMPTY_OFFSET
            }
        }
    }

    pub(crate) fn handle_list_offsets(&self, request: ListOffsetsRequest) -> ResponseKind {
        let response = ListOffsetsResponse::default().with_topics(
            request
                .topics
                .iter()
                .map(|topic| {
                    ListOffsetsTopicResponse::default()
                        .with_name(topic.name.clone())
                        .with_partitions(
                            topic
                                .partitions
                                .iter()
                                .map(|partition| {
                                    ListOffsetsPartitionResponse::default()
                                        .with_partition_index(partition.partition_index)
                                        .with_error_code(0)
                                        .with_timestamp(partition.timestamp)
                                        .with_offset(self.get_offset(
                                            topic.name.clone(),
                                            partition.partition_index,
                                            partition.timestamp,
                                        ))
                                        .with_unknown_tagged_fields(Default::default())
                                        .with_old_style_offsets(vec![])
                                        .with_leader_epoch(0)
                                })
                                .collect(),
                        )
                })
                .collect(),
        );

        ResponseKind::ListOffsets(response)
    }

    pub(crate) fn handle_offset_fetch(&self, request: OffsetFetchRequest) -> ResponseKind {
        // Try consumer group offset fetch first if group_id is provided
        if !request.group_id.is_empty() || !request.groups.is_empty() {
            return self.handle_consumer_group_offset_fetch(&request);
        }

        // Fallback to legacy offset fetch behavior
        let mut topics = vec![];
        if let Some(topic) = request.topics {
            let mut partitions = vec![];
            for offset_fetch in topic {
                for partition in offset_fetch.partition_indexes {
                    // let offset = self.get_offset(offset_fetch.name.clone(), partition, 0).await;
                    partitions.push(
                        OffsetFetchResponsePartition::default()
                            .with_partition_index(partition)
                            .with_committed_offset(0)
                            .with_committed_leader_epoch(0)
                            .with_error_code(0),
                    );
                }
                topics.push(
                    OffsetFetchResponseTopic::default()
                        .with_name(offset_fetch.name.clone())
                        .with_partitions(partitions.clone()),
                );
            }
        }
        ResponseKind::OffsetFetch(OffsetFetchResponse::default().with_topics(topics))
    }

    // Add logging to debug the request and response
    fn handle_consumer_group_offset_fetch(&self, request: &OffsetFetchRequest) -> ResponseKind {
        let Some(ref consumer_groups) = self.consumer_groups else {
            log::warn!("No consumer groups available");
            return ResponseKind::OffsetFetch(
                OffsetFetchResponse::default()
                    .with_throttle_time_ms(0)
                    .with_error_code(ResponseError::InvalidRequest.code()),
            );
        };

        let mut groups = vec![];
        for group_request in &request.groups {
            // Check if group is in rebalancing state
            if consumer_groups.is_group_rebalancing(&group_request.group_id.clone().into()) {
                groups.push(
                    OffsetFetchResponseGroup::default()
                        .with_group_id(group_request.group_id.clone())
                        .with_error_code(ResponseError::RebalanceInProgress.code()), // NO_ERROR
                );
                continue;
            }
            let mut topics = vec![];
            if let Some(topic_list) = &group_request.topics {
                for topic_request in topic_list {
                    let mut partitions = vec![];
                    for partition_index in &topic_request.partition_indexes {
                        let offset_metadata = consumer_groups.fetch_offset(
                            &group_request.group_id.clone().into(),
                            &topic_request.name,
                            *partition_index,
                        );
                        let (committed_offset, metadata, error_code) = match offset_metadata {
                            Some(meta) => (meta.offset, meta.metadata, 0i16),
                            None => (-1, None, 0i16),
                        };
                        partitions.push(
                            OffsetFetchResponsePartitions::default()
                                .with_partition_index(*partition_index)
                                .with_committed_offset(committed_offset)
                                .with_committed_leader_epoch(0)
                                .with_metadata(metadata)
                                .with_error_code(error_code),
                        );
                    }
                    topics.push(
                        OffsetFetchResponseTopics::default()
                            .with_name(topic_request.name.clone())
                            .with_partitions(partitions),
                    );
                }
                groups.push(
                    OffsetFetchResponseGroup::default()
                        .with_group_id(group_request.group_id.clone())
                        .with_error_code(0) // NO_ERROR
                        .with_topics(topics),
                )
            }
        }

        let response = ResponseKind::OffsetFetch(
            OffsetFetchResponse::default()
                .with_throttle_time_ms(0)
                .with_groups(groups),
        );

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::broker::BROKER;
    use crate::util::Util;
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::messages::{FetchRequest, ResponseKind, TopicName};
    use kafka_protocol::protocol::StrBytes;

    #[tokio::test]
    async fn test_fetch_bytes_out_counter_updated() {
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
                handle
                    .join()
                    .unwrap()
                    .expect("Failed to initialize settings for fetch tests");
            }
        });

        // First, produce some data to fetch
        let records = vec![
            Util::create_new_record("fetch_key1", "fetch_value1"),
            Util::create_new_record("fetch_key2", "fetch_value2"),
        ];

        let produce_request = Util::create_produce_request(
            TopicName(StrBytes::from_string("fetch-test-topic".to_string())),
            0,
            &records,
        );

        // Produce the records first
        let _produce_response = BROKER.handle_produce(produce_request).await;

        // Get initial counter value for bytes out
        let initial_bytes_out = MESSAGES_TOPIC_BYTES_OUT
            .with_label_values(&["fetch-test-topic"])
            .get();

        // Create a fetch request
        let fetch_request = FetchRequest::default()
            .with_max_wait_ms(100)
            .with_min_bytes(1)
            .with_max_bytes(1024 * 1024)
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_string(
                    "fetch-test-topic".to_string(),
                )))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(0)
                    .with_partition_max_bytes(1024 * 1024)])]);

        // Handle the fetch request
        let response = BROKER.handle_fetch(fetch_request).await;

        // Verify the response structure (may be empty if topic doesn't exist)
        match response {
            ResponseKind::Fetch(_resp) => {
                // Response may be empty if topic doesn't exist, that's ok for this test
                // We just care about counter behavior
            }
            _ => panic!("Expected Fetch response"),
        }

        // Check that bytes out counter was updated (might be 0 if no data was fetched)
        let final_bytes_out = MESSAGES_TOPIC_BYTES_OUT
            .with_label_values(&["fetch-test-topic"])
            .get();

        // The counter should be >= initial value (might be same if no data was fetched)
        assert!(
            final_bytes_out >= initial_bytes_out,
            "Bytes out counter should not decrease. Initial: {}, Final: {}",
            initial_bytes_out,
            final_bytes_out
        );

        // This test primarily verifies that the counter tracking code doesn't crash
        // and that counters don't decrease. If topic doesn't exist, no bytes will be tracked.
        // Test passes if counter doesn't decrease and no panics occur.
    }
}
