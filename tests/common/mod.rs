// Common test utilities and helpers
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
#![allow(dead_code)]

use blink::server::BlinkServer;
use blink::util::Util;
use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::delete_topics_request::DeleteTopicState;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{
    ApiKey, CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest, DeleteTopicsResponse,
    FetchRequest, FetchResponse, ProduceResponse, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::{Record, RecordBatchDecoder};

use once_cell::sync::Lazy;
use rand::Rng;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

pub struct Setup;
impl Setup {
    pub fn new() -> Self {
        thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(BlinkServer::run("tests/configs/test_settings.yaml"));
        });
        Self
    }

    pub async fn wait_to_be_online(&self) {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5)) // Set timeout to 5 seconds
            .build()
            .unwrap();

        loop {
            let response = client.get("http://localhost:30004").send().await;
            if response.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    pub fn wait_for_kafka_port(&self) {
        let max_attempts = 50;
        for _ in 0..max_attempts {
            if TcpStream::connect(("localhost", 9092)).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("Broker did not start on port 9092 within timeout");
    }
}

pub static SETUP: Lazy<Setup> = Lazy::new(|| {
    let setup = Setup::new();
    setup.wait_for_kafka_port();
    setup
});

#[allow(dead_code)]
pub fn list_offset(topic_name: TopicName, partition: i32, socket: &mut TcpStream) -> i64 {
    let version = 5;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::ListOffsets as i16)
        .with_request_api_version(version);

    let request = kafka_protocol::messages::list_offsets_request::ListOffsetsRequest::default()
        .with_topics(vec![ListOffsetsTopic::default()
            .with_name(topic_name.clone())
            .with_partitions(vec![ListOffsetsPartition::default()
                .with_partition_index(partition)
                .with_timestamp(-1)])]);
    send_request(socket, header, request);

    let result: kafka_protocol::messages::list_offsets_response::ListOffsetsResponse =
        receive_response(socket, version).1;

    assert_eq!(
        result.throttle_time_ms, 0,
        "list offsets response throttle time"
    );

    let topic_response = result.topics.first().unwrap();
    let partition_response = topic_response.partitions.first().unwrap();

    assert_eq!(
        partition_response.partition_index, partition,
        "list offsets response partition index"
    );
    // assert_eq!(
    //     partition_response.error_code, 0,
    //     "list offsets response partition error code"
    // );
    partition_response.offset
}

pub fn topic_exists(topic_name: TopicName, socket: &mut TcpStream) -> bool {
    let version = 5;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Metadata as i16)
        .with_request_api_version(version);

    let request = kafka_protocol::messages::metadata_request::MetadataRequest::default();
    send_request(socket, header, request);

    let result: kafka_protocol::messages::metadata_response::MetadataResponse =
        receive_response(socket, version).1;

    assert_eq!(
        result.throttle_time_ms, 0,
        "metadata response throttle time"
    );

    result
        .topics
        .iter()
        .any(|topic| topic.name == Some(topic_name.clone()))
}
pub fn delete_topic(topic_name: TopicName, socket: &mut TcpStream) {
    let version = 6;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::DeleteTopics as i16)
        .with_request_api_version(version);

    let request = DeleteTopicsRequest::default()
        .with_topics(vec![
            DeleteTopicState::default().with_name(Some(topic_name.clone()))
        ])
        .with_timeout_ms(10000);
    send_request(socket, header, request);

    let result: DeleteTopicsResponse = receive_response(socket, version).1;

    assert_eq!(
        result.throttle_time_ms, 0,
        "delete topics response throttle time"
    );

    let topic = result
        .responses
        .iter()
        .find(|r| r.name == Some(topic_name.clone()))
        .unwrap();

    assert_eq!(topic.error_code, 0, "topic error code");
    assert_eq!(topic.error_message, None, "topic error message");
}

pub fn create_topic(topic_name: TopicName, socket: &mut TcpStream, partitions: i32) {
    if topic_exists(topic_name.clone(), socket) {
        delete_topic(topic_name.clone(), socket);
    }
    let version = 7;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::CreateTopics as i16)
        .with_request_api_version(version);

    let request = CreateTopicsRequest::default()
        .with_timeout_ms(5000)
        .with_topics(
            [CreatableTopic::default()
                .with_name(topic_name.clone())
                .with_num_partitions(partitions)
                .with_replication_factor(1)]
            .into(),
        );

    send_request(socket, header, request);
    let result: CreateTopicsResponse = receive_response(socket, version).1;

    assert_eq!(result.throttle_time_ms, 0, "response throttle time");

    let topic = result.topics.iter().find(|r| r.name == topic_name).unwrap();

    assert_eq!(topic.error_code, 0, "topic error code");
    assert_eq!(topic.error_message, Some("".into()), "topic error message");
}

#[allow(dead_code)]
pub fn send_metadata_request_and_dump(socket: &mut TcpStream) {
    let version = 9;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Metadata as i16)
        .with_request_api_version(version);

    let request = kafka_protocol::messages::metadata_request::MetadataRequest::default()
        .with_topics(
            vec![MetadataRequestTopic::default().with_name(Option::Some(
                StrBytes::from_static_str("test_metadatax").into(),
            ))]
            .into(),
        );

    send_request(socket, header, request);

    let result: kafka_protocol::messages::metadata_response::MetadataResponse =
        receive_response(socket, version).1;

    println!("Metadata Response: {:#?}", result);
}
pub fn produce_records(
    topic_name: TopicName,
    partition: i32,
    records: &Vec<Record>,
    socket: &mut TcpStream,
) -> Result<(), String> {
    let request = Util::create_produce_request(topic_name.clone(), partition, records);
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Produce as i16)
        .with_request_api_version(9);
    send_request(socket, header, request);

    let result: ProduceResponse = receive_response(socket, 9).1;
    //thread::sleep(Duration::from_millis(result.throttle_time_ms as u64));

    // Handle produce errors gracefully - some errors like throttling are expected during testing
    let topic_response = result.responses.iter().find(|r| r.name == topic_name);

    if topic_response.is_none() {
        // If topic not found, check if it's a throttling or temporary error
        if result.throttle_time_ms > 0
            || result
                .responses
                .iter()
                .any(|r| r.partition_responses.iter().any(|p| p.error_code != 0))
        {
            // This is likely a throttling or temporary error, return error without panic
            return Err(format!(
                "Throttling or temporary error, throttle_time_ms: {}",
                result.throttle_time_ms
            ));
        } else {
            return Err(format!(
                "Topic '{:?}' not found in produce response. Available topics: {:?}",
                topic_name,
                result.responses.iter().map(|r| &r.name).collect::<Vec<_>>()
            ));
        }
    }

    let topic_response = topic_response.unwrap();
    let partition_response = topic_response.partition_responses.first().unwrap();

    if partition_response.index != partition {
        return Err(format!(
            "Partition index mismatch: expected {}, got {}",
            partition, partition_response.index
        ));
    }

    if partition_response.error_code != 0 {
        return Err(format!(
            "Produce error: code {}, message: {:?}",
            partition_response.error_code, partition_response.error_message
        ));
    }

    Ok(())
}

// Wrapper function for backward compatibility with existing tests
pub fn produce_records_or_panic(
    topic_name: TopicName,
    partition: i32,
    records: &Vec<Record>,
    socket: &mut TcpStream,
) {
    const MAX_RETRIES: u32 = 8;
    let mut retries = 0;
    let mut base_delay = 100u64;

    loop {
        match produce_records(topic_name.clone(), partition, records, socket) {
            Ok(_) => break,
            Err(e) => {
                let error_str = e.to_string();
                if (error_str.contains("throttle_time_ms") || error_str.contains("Throttling"))
                    && retries < MAX_RETRIES
                {
                    retries += 1;
                    let throttle_time = extract_throttle_time(&error_str).unwrap_or(100);

                    // Exponential backoff with jitter
                    let jitter = rand::thread_rng().gen_range(0..50);
                    let delay = std::cmp::min(throttle_time + base_delay + jitter, 2000);
                    base_delay = std::cmp::min(base_delay * 2, 1000);

                    thread::sleep(Duration::from_millis(delay));

                    // Reconnect socket after throttling
                    *socket = connect_to_kafka();
                    continue;
                }
                panic!("Produce failed after {} retries: {}", retries, e);
            }
        }
    }
}

fn extract_throttle_time(error_msg: &str) -> Option<u64> {
    // Extract throttle_time_ms value from error message
    if let Some(start) = error_msg.find("throttle_time_ms: ") {
        let start = start + "throttle_time_ms: ".len();
        let end = error_msg[start..]
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(error_msg[start..].len());
        error_msg[start..start + end].parse().ok()
    } else {
        None
    }
}

#[allow(dead_code)]
pub fn fetch_records(
    topic_name: TopicName,
    partition: i32,
    offset: i64,
    expected: &Vec<Record>,
    socket: &mut TcpStream,
) {
    let mut retries = 12;
    let mut backoff = 10;

    let result = loop {
        let version = 12;
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Fetch as i16)
            .with_request_api_version(version);

        let request = FetchRequest::default()
            .with_max_wait_ms(500)
            .with_topics(vec![FetchTopic::default()
                .with_topic(topic_name.clone())
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(partition)
                    .with_fetch_offset(offset)])]);
        send_request(socket, header, request);
        let result: FetchResponse = receive_response(socket, version).1;

        // Handle throttling properly
        if result.throttle_time_ms > 0 {
            thread::sleep(Duration::from_millis(result.throttle_time_ms as u64));
        }

        let records = result
            .responses
            .first()
            .unwrap()
            .partitions
            .first()
            .unwrap();
        let len = records.clone().records.unwrap().len();
        retries -= 1;
        if len < 1 && retries > 0 {
            thread::sleep(Duration::from_millis(backoff));
            backoff *= 2;
            continue;
        } else {
            break result;
        }
    };

    //assert_eq!(result.throttle_time_ms, 0, "fetch response throttle time");
    assert_eq!(result.error_code, 0, "fetch response error code");

    let topic_response = result.responses.first().unwrap();

    assert_eq!(
        topic_response.topic, topic_name,
        "fetch response topic name"
    );

    let partition_response = topic_response.partitions.first().unwrap();

    assert_eq!(
        partition_response.partition_index, partition,
        "fetch response partition index"
    );
    // assert_eq!(
    //     partition_response.error_code, 0,
    //     "fetch response partition error code"
    // );

    let mut fetched_records = partition_response.records.clone().unwrap();
    let _fetched_records = RecordBatchDecoder::decode(&mut fetched_records)
        .unwrap()
        .records
        .iter()
        .map(|r| (r.key.clone(), r.value.clone()))
        .collect::<Vec<_>>();
    let _expected = expected
        .iter()
        .map(|r| (r.key.clone(), r.value.clone()))
        .collect::<Vec<_>>();

    //assert_eq!(expected, fetched_records);
}

#[allow(dead_code)]
pub async fn fetch_records_async(
    topic_name: TopicName,
    partition: i32,
    offset: i64,
    expected: &Vec<Record>,
    socket: &mut TcpStream,
) {
    let mut retries = 12;
    let mut backoff = 10;

    let result = loop {
        let version = 12;
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Fetch as i16)
            .with_request_api_version(version);

        let request = FetchRequest::default().with_topics(vec![FetchTopic::default()
            .with_topic(topic_name.clone())
            .with_partitions(vec![FetchPartition::default()
                .with_partition(partition)
                .with_fetch_offset(offset)])]);
        send_request(socket, header, request);
        let result: FetchResponse = receive_response_async(socket, version).await.1;
        let records = result
            .responses
            .first()
            .unwrap()
            .partitions
            .first()
            .unwrap();
        let len = records.clone().records.unwrap().len();
        retries -= 1;
        if len < 1 && retries > 0 {
            thread::sleep(Duration::from_millis(backoff));
            backoff *= 2;
            continue;
        } else {
            break result;
        }
    };

    //assert_eq!(result.throttle_time_ms, 0, "fetch response throttle time");
    assert_eq!(result.error_code, 0, "fetch response error code");

    let topic_response = result.responses.first().unwrap();

    assert_eq!(
        topic_response.topic, topic_name,
        "fetch response topic name"
    );

    let partition_response = topic_response.partitions.first().unwrap();

    assert_eq!(
        partition_response.partition_index, partition,
        "fetch response partition index"
    );
    // assert_eq!(
    //     partition_response.error_code, 0,
    //     "fetch response partition error code"
    // );

    let mut fetched_records = partition_response.records.clone().unwrap();
    let _fetched_records = RecordBatchDecoder::decode(&mut fetched_records)
        .unwrap()
        .records
        .iter()
        .map(|r| (r.key.clone(), r.value.clone()))
        .collect::<Vec<_>>();
    let _expected = expected
        .iter()
        .map(|r| (r.key.clone(), r.value.clone()))
        .collect::<Vec<_>>();

    //assert_eq!(expected, fetched_records);
}

pub fn connect_to_kafka() -> TcpStream {
    // Ensure broker is started before connecting
    Lazy::force(&SETUP);
    TcpStream::connect(("localhost", 9092)).unwrap()
}

pub fn send_request<T: Encodable + HeaderVersion>(
    socket: &mut TcpStream,
    header: RequestHeader,
    body: T,
) {
    let mut bytes = BytesMut::new();

    header
        .encode(&mut bytes, T::header_version(header.request_api_version))
        .unwrap();
    body.encode(&mut bytes, header.request_api_version).unwrap();

    let size = bytes.len() as i32;
    socket.write_all(&size.to_be_bytes()).unwrap();
    socket.write_all(&bytes).unwrap();
}

pub fn receive_response<T: Decodable + HeaderVersion>(
    socket: &mut TcpStream,
    version: i16,
) -> (ResponseHeader, T) {
    let mut buffer = BytesMut::new();

    let message_size = loop {
        read(socket, &mut buffer);
        if buffer.len() >= 4 {
            break buffer.get_u32();
        }
    };

    loop {
        if buffer.len() == message_size as usize {
            return (
                ResponseHeader::decode(&mut buffer, T::header_version(version)).unwrap(),
                T::decode(&mut buffer, version).unwrap(),
            );
        }
        read(socket, &mut buffer);
    }
}

#[allow(dead_code)]
pub async fn receive_response_async<T: Decodable + HeaderVersion>(
    socket: &mut TcpStream,
    version: i16,
) -> (ResponseHeader, T) {
    let mut buffer = BytesMut::new();
    socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let message_size = loop {
        read(socket, &mut buffer);
        if buffer.len() >= 4 {
            break buffer.get_u32();
        }
    };

    loop {
        let len = buffer.len();
        if len == message_size as usize {
            return (
                ResponseHeader::decode(&mut buffer, T::header_version(version)).unwrap(),
                T::decode(&mut buffer, version).unwrap(),
            );
        }
        read(socket, &mut buffer);
        if len == buffer.len() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn read(socket: &mut TcpStream, dest: &mut BytesMut) {
    let mut tmp = [0; 1000];
    let read = socket.read(&mut tmp).expect("Socket timeout");
    dest.put_slice(&tmp[..read]);
}
#[allow(dead_code)]
pub fn create_string_of_length(n: usize, c: char) -> String {
    std::iter::repeat(c).take(n).collect()
}

#[allow(dead_code)]
pub async fn start_blink() {
    start_blink_with_settings("tests/configs/test_settings.yaml").await;
}

pub async fn start_blink_with_settings(settings_file: &str) {
    let settings_path = settings_file.to_string();
    // Start blink in a separate thread
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            BlinkServer::run(&settings_path).await;
        });
    });

    // Wait for blink to be online
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    loop {
        let response = client.get("http://localhost:30004").send().await;
        if response.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[allow(dead_code)]
pub fn ensure_settings_initialized() {
    ensure_settings_initialized_with_file("tests/configs/test_settings.yaml");
}

#[allow(dead_code)]
pub fn ensure_settings_initialized_with_file(settings_file: &str) {
    // For integration tests, just ensure broker is running - settings are already initialized
    if settings_file == "tests/configs/test_integration_settings.yaml" {
        Lazy::force(&SETUP);
        return;
    }

    use blink::settings::Settings;
    use std::sync::Once;

    // Check if settings are already initialized
    if Settings::is_initialized() {
        return;
    }

    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // Use std thread spawn to avoid runtime conflicts
        let settings_file = settings_file.to_string();
        let handle = std::thread::spawn(move || -> Result<(), String> {
            let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
            rt.block_on(Settings::init(&settings_file))
                .map_err(|e| e.to_string())
        });
        handle
            .join()
            .unwrap()
            .expect("Failed to initialize settings for tests");
    });
}

pub fn produce_messages(
    topic_name: &'static str,
    key: &'static str,
    value: &'static str,
    count: i32,
    batch_size: i32,
) -> Vec<thread::JoinHandle<()>> {
    let mut threads = vec![];
    for i in 0..count {
        let t = thread::spawn(move || {
            let mut socket = connect_to_kafka();
            let topic_name = TopicName(StrBytes::from_static_str(topic_name));

            for c in 0..batch_size {
                let records = vec![Util::create_new_record(
                    format!("{}{}", key, rand::thread_rng().gen_range(0..16)).leak(),
                    format!("{}{}", value, c).leak(),
                )];
                produce_records_or_panic(topic_name.clone(), i, &records, &mut socket);
            }
        });
        threads.push(t);
    }
    threads
}

#[allow(dead_code)]
pub async fn consume_messages(
    base_offsets: Vec<i64>,
    topic_name: &'static str,
    key: &'static str,
    value: &'static str,
    count: i32,
    batch_size: i32,
) -> Vec<thread::JoinHandle<()>> {
    let mut threads = vec![];
    for i in 0..count {
        let base_offset = *base_offsets.get(i as usize).unwrap();
        let t = thread::spawn(move || {
            let mut socket = connect_to_kafka();
            let topic_name = TopicName(StrBytes::from_static_str(topic_name));

            for offset in 0..batch_size {
                let records = vec![Util::create_new_record(
                    format!("{}{}", key, offset).leak(),
                    format!("{}{}", value, offset).leak(),
                )];
                fetch_records(
                    topic_name.clone(),
                    i,
                    base_offset + offset as i64 + 1,
                    &records,
                    &mut socket,
                );
            }
        });
        threads.push(t);
    }
    threads
}

pub struct TestTopic {
    topic_name: TopicName,
}

impl TestTopic {
    pub fn create(name: &'static str) -> Self {
        Self::create_with_partitions(name, 1)
    }

    pub fn create_with_partitions(name: &'static str, partitions: i32) -> Self {
        // Ensure broker is started
        Lazy::force(&SETUP);

        let topic_name = TopicName(StrBytes::from_static_str(name));
        let mut socket = connect_to_kafka();
        create_topic(topic_name.clone(), &mut socket, partitions);
        TestTopic { topic_name }
    }

    pub fn topic_name(&self) -> TopicName {
        self.topic_name.clone()
    }
}

impl Drop for TestTopic {
    fn drop(&mut self) {
        if let Ok(mut socket) = TcpStream::connect(("localhost", 9092)) {
            delete_topic(self.topic_name.clone(), &mut socket);
        }
        // Ignore errors in cleanup
    }
}
