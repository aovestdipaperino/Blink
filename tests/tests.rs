// Main test suite
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
mod common;
mod offload_tests;

use crate::common::{
    connect_to_kafka, consume_messages, create_string_of_length, create_topic, delete_topic,
    fetch_records, fetch_records_async, list_offset, produce_messages, produce_records_or_panic,
    receive_response, send_metadata_request_and_dump, send_request, topic_exists, SETUP,
};
use blink::util::Util;
use kafka_protocol::messages::delete_topics_request::DeleteTopicState;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{
    ApiKey, DeleteTopicsRequest, DeleteTopicsResponse, FetchRequest, FetchResponse, RequestHeader,
};
use kafka_protocol::{messages::TopicName, protocol::StrBytes};
use std::sync::Arc;
use std::sync::Once;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, Semaphore};
use tokio::time::timeout;

// Ensure test broker is started once
static INIT_TEST_BROKER: Once = Once::new();

fn init_test_broker() {
    INIT_TEST_BROKER.call_once(|| {
        // Force initialization of the shared broker setup
        once_cell::sync::Lazy::force(&SETUP);
    });
}

struct TestTopic {
    topic_name: TopicName,
}

impl TestTopic {
    fn create(name: &'static str) -> Self {
        Self::create_with_partitions(name, 1)
    }

    fn create_with_partitions(name: &'static str, partitions: i32) -> Self {
        // Ensure broker is running before creating topics
        init_test_broker();

        let topic_name = TopicName(StrBytes::from_static_str(name));
        let mut socket = connect_to_kafka();
        create_topic(topic_name.clone(), &mut socket, partitions);
        TestTopic { topic_name }
    }

    fn topic_name(&self) -> TopicName {
        self.topic_name.clone()
    }
}

impl Drop for TestTopic {
    fn drop(&mut self) {
        // Try to cleanup topic, but don't panic if it fails
        let result = std::panic::catch_unwind(|| {
            let mut socket = connect_to_kafka();
            delete_topic(self.topic_name.clone(), &mut socket);
        });
        // Ignore any errors during cleanup
        let _ = result;
    }
}

#[tokio::test]
async fn test_metadata() {
    init_test_broker();
    // delete_topic(
    //     TopicName(StrBytes::from_static_str("test_metadata")),
    //     &mut connect_to_kafka(),
    // );
    // delete_topic(
    //     TopicName(StrBytes::from_static_str("test_metadata")),
    //     &mut connect_to_kafka(),
    // );
    let _topic = TestTopic::create("test_metadata");
    let mut socket = connect_to_kafka();
    send_metadata_request_and_dump(&mut socket);
}

#[tokio::test]
async fn basic_test() {
    init_test_broker();
    let topic = TestTopic::create("basic_test");

    let mut socket = connect_to_kafka();
    assert!(topic_exists(topic.topic_name(), &mut socket));

    let records = vec![Util::create_new_record("key", "value")];

    assert_eq!(list_offset(topic.topic_name(), 0, &mut socket), -1);
    produce_records_or_panic(topic.topic_name(), 0, &records, &mut socket);
    produce_records_or_panic(topic.topic_name(), 0, &records, &mut socket);
    assert_eq!(list_offset(topic.topic_name(), 0, &mut socket), 1);
    let offset = 0; // it's a newly created topic, start from the beginning.
    fetch_records(topic.topic_name(), 0, offset, &records, &mut socket);
    fetch_records(topic.topic_name(), 0, offset + 1, &records, &mut socket);
}

#[tokio::test]
async fn delete_topic_test() {
    init_test_broker();
    let topic = TestTopic::create("delete_topic_test");
    let topic_name = topic.topic_name();
    let mut socket = connect_to_kafka();
    drop(topic);

    assert!(!topic_exists(topic_name.clone(), &mut socket));
}

#[tokio::test]
async fn test_topic_deletion_cleanup() {
    init_test_broker();
    // Test that topic deletion properly cleans up all partition storage
    let topic = TestTopic::create_with_partitions("storage_cleanup_test", 3);
    let topic_name = topic.topic_name();
    let mut socket = connect_to_kafka();

    // Produce some records to create storage data
    let records = vec![Util::create_new_record("key1", "value1")];

    // Produce to multiple partitions
    produce_records_or_panic(topic_name.clone(), 0, &records, &mut socket);
    produce_records_or_panic(topic_name.clone(), 1, &records, &mut socket);
    produce_records_or_panic(topic_name.clone(), 2, &records, &mut socket);

    // Verify data exists
    assert_eq!(list_offset(topic_name.clone(), 0, &mut socket), 0);
    assert_eq!(list_offset(topic_name.clone(), 1, &mut socket), 0);
    assert_eq!(list_offset(topic_name.clone(), 2, &mut socket), 0);

    // Delete the topic (this triggers comprehensive cleanup)
    drop(topic);

    // Verify topic no longer exists
    assert!(!topic_exists(topic_name.clone(), &mut socket));

    // Note: In a real storage system, we might also verify that:
    // - Partition storage is removed from memory
    // - Consumer group offsets are cleaned up
    // - Any cached data is cleared
    // For now, we verify the topic is properly removed from the broker
}

#[tokio::test]
async fn test_delete_topics_protocol_versions() {
    init_test_broker();
    // Test that deletion works with both topic_names (v1-5) and topics (v6+) field structures

    // Create topic for version 5 test (using topic_names field)
    let topic_v5 = TestTopic::create("delete_topics_v5_test");
    let topic_name_v5 = topic_v5.topic_name();
    let mut socket = connect_to_kafka();

    // Test version 5 deletion (topic_names field)
    let version_5 = 5;
    let header_v5 = RequestHeader::default()
        .with_request_api_key(ApiKey::DeleteTopics as i16)
        .with_request_api_version(version_5);

    let request_v5 = DeleteTopicsRequest::default()
        .with_topic_names(vec![topic_name_v5.clone()])
        .with_timeout_ms(10000);

    send_request(&mut socket, header_v5, request_v5);
    let result_v5: DeleteTopicsResponse = receive_response(&mut socket, version_5).1;

    assert_eq!(result_v5.throttle_time_ms, 0, "v5 response throttle time");
    assert_eq!(result_v5.responses.len(), 1, "v5 should have one response");
    assert_eq!(
        result_v5.responses[0].error_code, 0,
        "v5 deletion should succeed"
    );

    // Verify topic was deleted
    assert!(!topic_exists(topic_name_v5.clone(), &mut socket));

    // Create topic for version 6 test (using topics field)
    let topic_v6 = TestTopic::create("delete_topics_v6_test");
    let topic_name_v6 = topic_v6.topic_name();

    // Test version 6 deletion (topics field) - this is the current implementation
    let version_6 = 6;
    let header_v6 = RequestHeader::default()
        .with_request_api_key(ApiKey::DeleteTopics as i16)
        .with_request_api_version(version_6);

    let request_v6 = DeleteTopicsRequest::default()
        .with_topics(vec![
            DeleteTopicState::default().with_name(Some(topic_name_v6.clone()))
        ])
        .with_timeout_ms(10000);

    send_request(&mut socket, header_v6, request_v6);
    let result_v6: DeleteTopicsResponse = receive_response(&mut socket, version_6).1;

    assert_eq!(result_v6.throttle_time_ms, 0, "v6 response throttle time");
    assert_eq!(result_v6.responses.len(), 1, "v6 should have one response");
    assert_eq!(
        result_v6.responses[0].error_code, 0,
        "v6 deletion should succeed"
    );

    // Verify topic was deleted
    assert!(!topic_exists(topic_name_v6.clone(), &mut socket));
}

#[tokio::test]
async fn long_message_test() {
    init_test_broker();
    let topic = TestTopic::create("long_message_test");

    let mut socket = connect_to_kafka();
    let records = vec![Util::create_new_record(
        "key",
        create_string_of_length(1_000_000, 'A').leak(),
    )];
    let offset = list_offset(topic.topic_name(), 0, &mut socket);

    produce_records_or_panic(topic.topic_name(), 0, &records, &mut socket);
    timeout(Duration::from_secs(3), async {
        fetch_records_async(topic.topic_name(), 0, offset, &records, &mut socket).await;
        println!("Test passed, stopping watch dog.");
    })
    .await
    .expect("Test timed out.");

    // stop_flag.store(true, Ordering::Relaxed);
    // watch_dog.join().unwrap();
}

fn create_fetch_topic(topic_name: TopicName, partition: i32, fetch_offset: i64) -> FetchTopic {
    FetchTopic::default()
        .with_topic(topic_name)
        .with_partitions(vec![FetchPartition::default()
            .with_partition(partition)
            .with_fetch_offset(fetch_offset)])
}
#[tokio::test]
async fn test_multiple_topics() {
    init_test_broker();
    let success_signal = Arc::new(Notify::new());
    let signal_clone = success_signal.clone();

    let topic1 = TestTopic::create("topic1");
    let topic2 = TestTopic::create("topic2");
    let mut socket = connect_to_kafka();
    produce_records_or_panic(
        topic1.topic_name(),
        0,
        &vec![Util::create_new_record("key", "value")],
        &mut socket,
    );
    produce_records_or_panic(
        topic2.topic_name(),
        0,
        &vec![Util::create_new_record("key", "value")],
        &mut socket,
    );

    let topic_name1 = topic1.topic_name();
    let topic_name2 = topic2.topic_name();

    let _ = thread::spawn(move || {
        // In this thread we simulate a long poll.
        // We create a socket and send a fetch request with a long wait time (5000 milliseconds).
        let mut socket = connect_to_kafka();
        let version = 12;
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Fetch as i16)
            .with_request_api_version(version);

        // Legit request with 2 topics.
        let request = FetchRequest::default()
            .with_max_wait_ms(100)
            .with_topics(vec![
                create_fetch_topic(topic_name1.clone(), 0, 0),
                create_fetch_topic(topic_name2.clone(), 0, 0),
            ]);
        send_request(&mut socket, header.clone(), request);
        let results: FetchResponse = receive_response(&mut socket, version).1;
        // Verify that the message is there before killing the thread.
        assert_eq!(2, results.responses.len());

        // TOPIC 2 will timeout failing the whole request.
        let request = FetchRequest::default()
            .with_max_wait_ms(100)
            .with_topics(vec![
                create_fetch_topic(topic_name1.clone(), 0, 0),
                create_fetch_topic(topic_name2.clone(), 0, 1),
            ]);
        send_request(&mut socket, header.clone(), request);
        let results: FetchResponse = receive_response(&mut socket, version).1;
        // Verify that the message is there before killing the thread.
        assert_eq!(0, results.responses.len());

        // Timeout should be valid now.
        let request = FetchRequest::default()
            .with_max_wait_ms(1000)
            .with_topics(vec![
                create_fetch_topic(topic_name1.clone(), 0, 0),
                create_fetch_topic(topic_name2.clone(), 0, 1),
            ]);
        send_request(&mut socket, header.clone(), request);
        let results: FetchResponse = receive_response(&mut socket, version).1;
        // Verify that the message is there before killing the thread.
        assert_eq!(2, results.responses.len());

        // TOPIC 2 will fail due to invalid partition.
        let request = FetchRequest::default()
            .with_max_wait_ms(100)
            .with_topics(vec![
                create_fetch_topic(topic_name1, 0, 0),
                create_fetch_topic(topic_name2, 1, 0),
            ]);
        send_request(&mut socket, header.clone(), request);
        let results: FetchResponse = receive_response(&mut socket, version).1;
        // Verify that the message is there before killing the thread.
        assert_eq!(0, results.responses.len());
        signal_clone.notify_waiters();
    });

    thread::sleep(Duration::from_millis(300));
    produce_records_or_panic(
        topic2.topic_name(),
        0,
        &vec![Util::create_new_record("key", "value")],
        &mut socket,
    );
    let result = timeout(Duration::from_secs(1), success_signal.notified()).await;

    assert!(result.is_ok(), "Timed out waiting for receiver.");
}

#[tokio::test]
async fn test_long_poll() {
    init_test_broker();
    let topic = TestTopic::create("long_poll_test");
    let message_received = Arc::new(Notify::new());
    let sender = message_received.clone();
    let mut socket = connect_to_kafka();
    let topic_name = topic.topic_name(); // to be moved in the closure.

    thread::spawn(move || {
        // In this thread we simulate a long poll.
        // We create a socket and send a fetch request with a long wait time (5000 milliseconds).
        let mut socket = connect_to_kafka();
        let version = 12;
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Fetch as i16)
            .with_request_api_version(version);
        let request = FetchRequest::default()
            .with_max_wait_ms(5000)
            .with_topics(vec![FetchTopic::default()
                .with_topic(topic_name.clone())
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(0)])]);
        send_request(&mut socket, header, request);
        let results: FetchResponse = receive_response(&mut socket, version).1;
        // Signal the main thread that we received the message.
        sender.notify_waiters();
        // Verify that the message is there before killing the thread.
        assert_eq!(1, results.responses.len());
    });

    // The receiving thread is waiting for us to push a message.
    tokio::time::sleep(Duration::from_millis(50)).await;
    produce_records_or_panic(
        topic.topic_name(),
        0,
        &vec![Util::create_new_record("key", "value")],
        &mut socket,
    );
    let result = timeout(Duration::from_secs(2), message_received.notified()).await;
    assert!(result.is_ok(), "Timed out waiting for message");
}

#[tokio::test]
async fn test_dynamic_rate_limiter() {
    init_test_broker();
    let rate_limit = 1; // 200 calls per second
    let permits_per_second = rate_limit;
    let semaphore = Arc::new(Semaphore::new(permits_per_second));
    let semaphore_clone = semaphore.clone();

    // Background task to replenish permits
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let available = semaphore_clone.available_permits();
            semaphore_clone.add_permits(permits_per_second - available);
        }
    });

    let topic = TestTopic::create("dynamic_rate_limiter_test");
    let _socket = connect_to_kafka();
    let topic_name = topic.topic_name();

    let mut threads = vec![];
    for _ in 0..3 {
        let semaphore = semaphore.clone();
        let topic_name = topic_name.clone();
        let mut socket = connect_to_kafka();
        threads.push(tokio::spawn(async move {
            loop {
                let permit = semaphore.acquire().await.unwrap();
                drop(permit); // Release the permit after use
                produce_records_or_panic(
                    topic_name.clone(),
                    0,
                    &vec![Util::create_new_record("key", "BlueBlueBlue")],
                    &mut socket,
                );
            }
        }));
    }

    for thread in threads {
        thread.await.unwrap();
    }
}

#[tokio::test]
async fn test_multiple_messages() {
    init_test_broker();
    let thread_count = 4; // Reduced from 8 to reduce load
    let topic_name_str = "multi-message";
    let message_number = 1000; // Reduced from 10000 to reduce load
    let topic = TestTopic::create_with_partitions(topic_name_str, thread_count);
    let mut socket = connect_to_kafka();
    let base_offset: Vec<i64> = (0..thread_count)
        .map(|i| list_offset(topic.topic_name(), i, &mut socket))
        .collect();
    let k = "Key";
    let v = "Value";
    let mut all_threads = vec![];
    let start = Instant::now();
    all_threads.extend(produce_messages(
        topic_name_str,
        k,
        v,
        thread_count,
        message_number,
    ));
    all_threads.extend(
        consume_messages(
            base_offset,
            topic_name_str,
            k,
            v,
            thread_count,
            message_number,
        )
        .await,
    );
    all_threads.into_iter().for_each(|t| {
        t.join().unwrap();
    });
    let time = start.elapsed();
    println!("Time taken: {:?}", time);
    println!(
        "Messages per second: {}",
        (thread_count * message_number * 2) as f64 / time.as_secs_f64()
    );
}
