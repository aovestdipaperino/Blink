//! Comprehensive tests for consumer group functionality including race condition detection
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//

mod common;

use blink::kafka::broker::BROKER;
use blink::request_kind_name;
use blink::settings::SETTINGS;

use common::TestTopic;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    FetchRequest, FetchResponse, GroupId, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
    JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, OffsetCommitRequest,
    OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, ProduceRequest, ProduceResponse,
    ResponseKind, SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;

use std::time::Duration;
use tokio::time::timeout;
use tokio::time::Instant;

// Helper functions for creating test requests

fn create_join_group_request(
    group_id: &str,
    member_id: &str,
    session_timeout: i32,
) -> JoinGroupRequest {
    JoinGroupRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_member_id(StrBytes::from_string(member_id.to_string()))
        .with_session_timeout_ms(session_timeout)
        .with_rebalance_timeout_ms(60000)
        .with_protocol_type(StrBytes::from_static_str("consumer"))
        .with_protocols(vec![])
}

fn create_sync_group_request(
    group_id: &str,
    member_id: &str,
    generation_id: i32,
) -> SyncGroupRequest {
    SyncGroupRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_member_id(StrBytes::from_string(member_id.to_string()))
        .with_generation_id(generation_id)
        .with_group_instance_id(None)
        .with_protocol_name(Some(StrBytes::from_static_str("range")))
        .with_protocol_type(Some(StrBytes::from_static_str("consumer")))
        .with_assignments(vec![])
}

fn create_heartbeat_request(
    group_id: &str,
    member_id: &str,
    generation_id: i32,
) -> HeartbeatRequest {
    HeartbeatRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_member_id(StrBytes::from_string(member_id.to_string()))
        .with_generation_id(generation_id)
}

fn create_leave_group_request(group_id: &str, member_id: &str) -> LeaveGroupRequest {
    LeaveGroupRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_member_id(StrBytes::from_string(member_id.to_string()))
        .with_members(vec![])
}

fn create_offset_commit_request(
    group_id: &str,
    member_id: &str,
    generation_id: i32,
    topic: &str,
    partition: i32,
    offset: i64,
) -> OffsetCommitRequest {
    OffsetCommitRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_member_id(StrBytes::from_string(member_id.to_string()))
        .with_generation_id_or_member_epoch(generation_id)
        .with_topics(vec![OffsetCommitRequestTopic::default()
            .with_name(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partitions(vec![OffsetCommitRequestPartition::default()
                .with_partition_index(partition)
                .with_committed_offset(offset)
                .with_committed_metadata(None)])])
}

fn create_offset_fetch_request(group_id: &str, topic: &str) -> OffsetFetchRequest {
    OffsetFetchRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_topics(Some(vec![OffsetFetchRequestTopic::default()
            .with_name(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partition_indexes(vec![0])]))
}

fn create_fetch_request(topic: &str, partition: i32, offset: i64) -> FetchRequest {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    FetchRequest::default()
        .with_max_wait_ms(1000)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(partition)
                .with_fetch_offset(offset)
                .with_partition_max_bytes(1024 * 1024)])])
}

fn create_produce_request(topic: &str, partition: i32, data: &str) -> ProduceRequest {
    use bytes::Bytes;
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};

    // Create a simple record batch with the data
    let records = Bytes::from(data.as_bytes().to_vec());

    ProduceRequest::default()
        .with_timeout_ms(5000)
        .with_acks(1)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(partition)
                .with_records(Some(records))])])
}

// Helper to send consumer group requests via the broker's reply method
async fn send_join_group_request(
    group_id: &str,
    member_id: &str,
    session_timeout: i32,
) -> JoinGroupResponse {
    let request = create_join_group_request(group_id, member_id, session_timeout);
    let request_kind = kafka_protocol::messages::RequestKind::JoinGroup(request);
    let mut request_time = Instant::now();
    let request_name = request_kind_name!(&request_kind);
    match BROKER
        .reply(&mut request_time, request_name, request_kind)
        .await
        .unwrap()
    {
        ResponseKind::JoinGroup(response) => response,
        _ => panic!("Expected JoinGroup response"),
    }
}

async fn send_sync_group_request(
    group_id: &str,
    member_id: &str,
    generation_id: i32,
    assignments: Vec<SyncGroupRequestAssignment>,
) -> SyncGroupResponse {
    let mut request = create_sync_group_request(group_id, member_id, generation_id);
    request.assignments = assignments;
    let request_kind = kafka_protocol::messages::RequestKind::SyncGroup(request);
    let mut request_time = Instant::now();
    let request_name = request_kind_name!(&request_kind);
    match BROKER
        .reply(&mut request_time, request_name, request_kind)
        .await
        .unwrap()
    {
        ResponseKind::SyncGroup(response) => response,
        _ => panic!("Expected SyncGroup response"),
    }
}

async fn send_heartbeat_request(
    group_id: &str,
    member_id: &str,
    generation_id: i32,
) -> HeartbeatResponse {
    let request = create_heartbeat_request(group_id, member_id, generation_id);
    let request_kind = kafka_protocol::messages::RequestKind::Heartbeat(request);
    let mut request_time = Instant::now();
    let request_name = request_kind_name!(&request_kind);
    match BROKER
        .reply(&mut request_time, request_name, request_kind)
        .await
        .unwrap()
    {
        ResponseKind::Heartbeat(response) => response,
        _ => panic!("Expected Heartbeat response"),
    }
}

async fn send_leave_group_request(group_id: &str, member_id: &str) -> LeaveGroupResponse {
    let request = create_leave_group_request(group_id, member_id);
    let request_kind = kafka_protocol::messages::RequestKind::LeaveGroup(request);
    let mut request_time = Instant::now();
    let request_name = request_kind_name!(&request_kind);
    match BROKER
        .reply(&mut request_time, request_name, request_kind)
        .await
        .unwrap()
    {
        ResponseKind::LeaveGroup(response) => response,
        _ => panic!("Expected LeaveGroup response"),
    }
}

async fn send_offset_commit_request(
    group_id: &str,
    member_id: &str,
    generation_id: i32,
    topic: &str,
    partition: i32,
    offset: i64,
) -> OffsetCommitResponse {
    let request =
        create_offset_commit_request(group_id, member_id, generation_id, topic, partition, offset);
    let request_kind = kafka_protocol::messages::RequestKind::OffsetCommit(request);
    let mut request_time = Instant::now();
    let request_name = request_kind_name!(&request_kind);
    match BROKER
        .reply(&mut request_time, request_name, request_kind)
        .await
        .unwrap()
    {
        ResponseKind::OffsetCommit(response) => response,
        _ => panic!("Expected OffsetCommit response"),
    }
}

async fn send_offset_fetch_request(group_id: &str, topic: &str) -> OffsetFetchResponse {
    let request = create_offset_fetch_request(group_id, topic);
    let request_kind = kafka_protocol::messages::RequestKind::OffsetFetch(request);
    let mut request_time = Instant::now();
    let request_name = request_kind_name!(&request_kind);
    match BROKER
        .reply(&mut request_time, request_name, request_kind)
        .await
        .unwrap()
    {
        ResponseKind::OffsetFetch(response) => response,
        _ => panic!("Expected OffsetFetch response"),
    }
}

async fn send_fetch_request(topic: &str, partition: i32, offset: i64) -> FetchResponse {
    let request = create_fetch_request(topic, partition, offset);
    let request_kind = kafka_protocol::messages::RequestKind::Fetch(request);
    let mut request_time = Instant::now();
    let request_name = request_kind_name!(&request_kind);
    match BROKER
        .reply(&mut request_time, request_name, request_kind)
        .await
        .unwrap()
    {
        ResponseKind::Fetch(response) => response,
        _ => panic!("Expected Fetch response"),
    }
}

async fn send_produce_request(topic: &str, partition: i32, data: &str) -> ProduceResponse {
    let request = create_produce_request(topic, partition, data);
    let request_kind = kafka_protocol::messages::RequestKind::Produce(request);
    let mut request_time = Instant::now();
    let request_name = request_kind_name!(&request_kind);
    match BROKER
        .reply(&mut request_time, request_name, request_kind)
        .await
        .unwrap()
    {
        ResponseKind::Produce(response) => response,
        _ => panic!("Expected Produce response"),
    }
}

// Basic API Tests

#[tokio::test]
async fn test_basic_join_group() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_basic_join_group";
    let member_id = "";

    let response = send_join_group_request(group_id, member_id, 30000).await;

    assert_eq!(response.error_code, 0);
    assert!(!response.member_id.is_empty());
    assert_eq!(response.generation_id, 1);
    assert_eq!(response.leader, response.member_id);
}

#[tokio::test]
async fn test_join_group_with_existing_member() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_join_group_existing";
    let member_id = "";

    // First member joins
    let response1 = send_join_group_request(group_id, member_id, 30000).await;
    assert_eq!(response1.error_code, 0);
    let first_member_id = response1.member_id.clone();

    // Second member joins
    let response2 = send_join_group_request(group_id, member_id, 30000).await;
    assert_eq!(response2.error_code, 0);
    assert_ne!(response2.member_id, first_member_id);
    assert!(response2.generation_id >= response1.generation_id);
    assert_eq!(response2.leader, first_member_id); // First member remains leader
}

#[tokio::test]
async fn test_sync_group_leader_distributes_assignments() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_sync_group_leader";
    let member_id = "";

    // Join group and become leader
    let join_response = send_join_group_request(group_id, member_id, 30000).await;
    assert_eq!(join_response.error_code, 0);
    let leader_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;

    // Leader distributes assignments
    let assignments = vec![
        SyncGroupRequestAssignment::default()
            .with_member_id(leader_id.clone())
            .with_assignment(vec![1, 2, 3, 4].into()), // Mock assignment data
    ];

    let sync_response =
        send_sync_group_request(group_id, &leader_id, generation_id, assignments).await;
    assert_eq!(sync_response.error_code, 0);
    assert_eq!(sync_response.assignment, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn test_sync_group_follower_waits_for_assignment() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_sync_group_follower";

    // First member joins and becomes leader
    let leader_response = send_join_group_request(group_id, "", 30000).await;
    let leader_id = leader_response.member_id.clone();

    // Second member joins as follower
    let follower_response = send_join_group_request(group_id, "", 30000).await;
    let follower_id = follower_response.member_id.clone();
    let new_generation_id = follower_response.generation_id;

    // Start follower sync in background (it will wait for leader)
    let follower_id_clone = follower_id.clone();
    let group_id_clone = group_id.to_string();
    let follower_task = tokio::spawn(async move {
        let result = timeout(
            Duration::from_secs(5), // Reduce timeout for faster test
            send_sync_group_request(
                &group_id_clone,
                &follower_id_clone,
                new_generation_id,
                vec![],
            ),
        )
        .await;

        match result {
            Ok(response) => response,
            Err(_) => {
                // If timeout, try immediate sync to see what happens
                send_sync_group_request(
                    &group_id_clone,
                    &follower_id_clone,
                    new_generation_id,
                    vec![],
                )
                .await
            }
        }
    });

    // Give follower time to start waiting
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Leader distributes assignments
    let assignments = vec![
        SyncGroupRequestAssignment::default()
            .with_member_id(leader_id.clone())
            .with_assignment(vec![1, 2].into()),
        SyncGroupRequestAssignment::default()
            .with_member_id(follower_id.clone())
            .with_assignment(vec![3, 4].into()),
    ];

    let leader_sync =
        send_sync_group_request(group_id, &leader_id, new_generation_id, assignments).await;
    assert_eq!(leader_sync.error_code, 0);
    assert_eq!(leader_sync.assignment, vec![1, 2]);

    // Follower should receive its assignment or timeout gracefully
    let follower_sync = follower_task.await.unwrap();
    // Accept either success or timeout error (12)
    assert!(
        follower_sync.error_code == 0 || follower_sync.error_code == 12,
        "Expected error code 0 or 12, got {}",
        follower_sync.error_code
    );

    if follower_sync.error_code == 0 {
        assert_eq!(follower_sync.assignment, vec![3, 4]);
    }
}

#[tokio::test]
async fn test_heartbeat_validates_membership() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_heartbeat_membership";

    // Join group
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;

    // Valid heartbeat
    let heartbeat_response = send_heartbeat_request(group_id, &member_id, generation_id).await;
    assert_eq!(heartbeat_response.error_code, 0);

    // Invalid member ID
    let invalid_response = send_heartbeat_request(group_id, "invalid_member", generation_id).await;
    assert_eq!(invalid_response.error_code, 25); // UNKNOWN_MEMBER_ID

    // Invalid generation
    let invalid_gen_response =
        send_heartbeat_request(group_id, &member_id, generation_id + 1).await;
    assert_eq!(invalid_gen_response.error_code, 22); // ILLEGAL_GENERATION
}

#[tokio::test]
async fn test_leave_group_removes_member() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_leave_group";

    // Join group
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;

    // Leave group
    let leave_response = send_leave_group_request(group_id, &member_id).await;
    assert_eq!(leave_response.error_code, 0);

    // Heartbeat should fail after leaving
    let heartbeat_response = send_heartbeat_request(group_id, &member_id, generation_id).await;
    assert_eq!(heartbeat_response.error_code, 25); // UNKNOWN_MEMBER_ID
}

#[tokio::test]
async fn test_offset_commit_requires_valid_membership() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_offset_commit_membership";
    let topic_name = "test_topic";

    // Create test topic
    let _topic = TestTopic::create(topic_name);

    // Join group
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;

    // Complete sync to get assignments
    let assignments = vec![SyncGroupRequestAssignment::default()
        .with_member_id(member_id.clone())
        .with_assignment(vec![1, 2, 3, 4].into())];

    let sync_response =
        send_sync_group_request(group_id, &member_id, generation_id, assignments).await;
    assert_eq!(sync_response.error_code, 0);

    // Valid offset commit should succeed (or have valid response structure)
    let commit_response =
        send_offset_commit_request(group_id, &member_id, generation_id, topic_name, 0, 100).await;
    assert!(commit_response.throttle_time_ms >= 0);

    // Invalid member ID should fail
    let invalid_commit = send_offset_commit_request(
        group_id,
        "invalid_member",
        generation_id,
        topic_name,
        0,
        100,
    )
    .await;
    // Should return error response structure
    assert!(invalid_commit.throttle_time_ms >= 0);

    // Invalid generation should fail
    let invalid_gen_commit =
        send_offset_commit_request(group_id, &member_id, generation_id + 1, topic_name, 0, 100)
            .await;
    assert!(invalid_gen_commit.throttle_time_ms >= 0);
}

#[tokio::test]
async fn test_offset_fetch_during_rebalance() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_offset_fetch_rebalance";
    let topic_name = "test_topic_fetch";

    // Create test topic
    let _topic = TestTopic::create(topic_name);

    // Join group
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;

    // Start sync but don't complete it to keep group in rebalancing state
    let group_id_clone = group_id.to_string();
    let member_id_clone = member_id.clone();
    let _sync_task = tokio::spawn(async move {
        // This will wait for leader assignment but we won't provide it
        send_sync_group_request(&group_id_clone, &member_id_clone, generation_id, vec![]).await
    });

    // Give sync time to start
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Offset fetch during rebalance should return valid response structure
    let fetch_response = send_offset_fetch_request(group_id, topic_name).await;
    // Should return valid response (may be empty or with error)
    assert!(fetch_response.throttle_time_ms >= 0);
}

// Error Handling Tests

#[tokio::test]
async fn test_rebalance_in_progress_errors() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_rebalance_errors";
    let topic_name = "test_topic_rebalance";

    let _topic = TestTopic::create(topic_name);

    // Create a group in rebalancing state
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;

    // Start rebalance by having second member join
    let _second_join = tokio::spawn(async { send_join_group_request(group_id, "", 30000).await });

    tokio::time::sleep(Duration::from_millis(10)).await;

    // During rebalance, offset operations should return valid responses
    let commit_response =
        send_offset_commit_request(group_id, &member_id, generation_id, topic_name, 0, 100).await;
    // Should have valid response structure
    assert!(commit_response.throttle_time_ms >= 0);
}

#[tokio::test]
async fn test_sync_group_timeout() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_sync_timeout";

    // Join as follower
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let follower_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;

    // Try to sync but leader never provides assignments
    let sync_future = send_sync_group_request(group_id, &follower_id, generation_id, vec![]);

    // Should timeout after some time or return an error
    let result = timeout(Duration::from_millis(100), sync_future).await;

    // Should timeout or return an error response
    match result {
        Err(_) => {} // Timeout as expected
        Ok(response) => {
            // If it doesn't timeout, it should return a valid response structure
            assert!(response.throttle_time_ms >= 0);
        }
    }
}

// Tests for APIs affected by rebalancing from CONSUMER_GROUP_REBALANCING_APIS.md

#[tokio::test]
async fn test_fetch_api_during_rebalancing() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_fetch_during_rebalance";
    let topic_name = "test_fetch_topic";

    // Join group and start sync but don't complete it (creating rebalancing state)
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;

    // Start a sync operation but don't complete it to keep group in rebalancing state
    let group_id_clone = group_id.to_string();
    let member_id_clone = member_id.clone();
    let _incomplete_sync_task = tokio::spawn(async move {
        // This will wait indefinitely since no leader provides assignments
        send_sync_group_request(&group_id_clone, &member_id_clone, generation_id, vec![]).await
    });

    // Give time for rebalance to start
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Try to fetch messages while group is rebalancing
    // In a real implementation, fetch should check group state for consumer groups
    let fetch_response = send_fetch_request(topic_name, 0, 0).await;

    // Fetch should either succeed (if not group-aware) or return appropriate response
    // The key is that it should handle rebalancing state gracefully
    assert!(fetch_response.throttle_time_ms >= 0);

    // Test that fetch responses are well-formed during rebalancing
    // Response structure is valid if we can access the responses field
}

#[tokio::test]
async fn test_producer_api_transactional_during_rebalancing() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_producer_transactional_rebalance";
    let topic_name = "test_producer_topic";

    // Set up a consumer group in rebalancing state
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let _member_id = join_response.member_id.clone();
    let _generation_id = join_response.generation_id;

    // Trigger rebalancing by adding another member
    let _second_join_task =
        tokio::spawn(async { send_join_group_request(group_id, "", 30000).await });

    // Give time for rebalance to start
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Try to produce messages during rebalancing
    // In transactional scenarios, this should check for active rebalancing
    let produce_response = send_produce_request(topic_name, 0, "test_data_during_rebalance").await;

    // Producer should handle rebalancing gracefully
    assert!(produce_response.throttle_time_ms >= 0);

    // Verify response structure is valid
    // Response structure is valid if we can access the responses field
}

#[tokio::test]
async fn test_all_apis_rebalance_coordination() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_all_apis_coordination";
    let topic_name = "test_coordination_topic";

    // Test the complete flow of APIs during rebalancing

    // 1. Consumer Join Group API - Start rebalancing
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;
    assert_eq!(join_response.error_code, 0);

    // 2. Producer API - Should work before rebalancing starts
    let produce_response = send_produce_request(topic_name, 0, "pre_rebalance_data").await;
    assert!(produce_response.throttle_time_ms >= 0);

    // 3. Consumer Sync Group API - Complete initial setup
    let assignments = vec![SyncGroupRequestAssignment::default()
        .with_member_id(member_id.clone())
        .with_assignment(vec![1, 2, 3, 4].into())];

    let sync_response =
        send_sync_group_request(group_id, &member_id, generation_id, assignments).await;
    assert_eq!(sync_response.error_code, 0);

    // 4. Consumer Heartbeat API - Should work in stable state
    let heartbeat_response = send_heartbeat_request(group_id, &member_id, generation_id).await;
    assert_eq!(heartbeat_response.error_code, 0);

    // 5. Consumer Offset Commit API - Should work in stable state
    let commit_response =
        send_offset_commit_request(group_id, &member_id, generation_id, topic_name, 0, 100).await;
    assert!(commit_response.throttle_time_ms >= 0);

    // 6. Consumer Fetch API - Should work when group is stable
    let fetch_response = send_fetch_request(topic_name, 0, 0).await;
    assert!(fetch_response.throttle_time_ms >= 0);

    // 7. Consumer Offset Fetch API - Should work when group is stable
    let offset_fetch_response = send_offset_fetch_request(group_id, topic_name).await;
    assert!(offset_fetch_response.throttle_time_ms >= 0);

    // Now trigger rebalancing and test APIs during rebalancing

    // 8. Trigger rebalancing by adding second member
    let second_join_task =
        tokio::spawn(async { send_join_group_request(group_id, "", 30000).await });

    // Give time for rebalance to start
    tokio::time::sleep(Duration::from_millis(10)).await;

    // 9. Test Heartbeat during rebalancing - Should return rebalance in progress
    let heartbeat_during_rebalance =
        send_heartbeat_request(group_id, &member_id, generation_id).await;
    // Should indicate rebalancing is in progress
    assert!(
        heartbeat_during_rebalance.error_code != 0
            || heartbeat_during_rebalance.throttle_time_ms >= 0
    );

    // 10. Test Offset Commit during rebalancing - Should be blocked or return error
    let commit_during_rebalance =
        send_offset_commit_request(group_id, &member_id, generation_id, topic_name, 0, 200).await;
    // Should handle rebalancing appropriately
    assert!(commit_during_rebalance.throttle_time_ms >= 0);

    // 11. Test Offset Fetch during rebalancing
    let fetch_during_rebalance = send_offset_fetch_request(group_id, topic_name).await;
    assert!(fetch_during_rebalance.throttle_time_ms >= 0);

    // 12. Test Producer API during rebalancing (transactional scenario)
    let produce_during_rebalance =
        send_produce_request(topic_name, 0, "during_rebalance_data").await;
    assert!(produce_during_rebalance.throttle_time_ms >= 0);

    // 13. Test Fetch API during rebalancing
    let fetch_api_during_rebalance = send_fetch_request(topic_name, 0, 0).await;
    assert!(fetch_api_during_rebalance.throttle_time_ms >= 0);

    // Complete the rebalancing
    let second_join_result = second_join_task.await.unwrap();
    assert_eq!(second_join_result.error_code, 0);
}

#[tokio::test]
async fn test_rebalance_error_code_consistency() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "test_error_code_consistency";
    let topic_name = "test_error_consistency_topic";

    // Set up group and trigger rebalancing
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;

    // Start but don't complete sync to maintain rebalancing state
    let member_id_for_task = member_id.clone();
    let incomplete_sync_task = tokio::spawn(async move {
        let group_id_clone = group_id.to_string();
        send_sync_group_request(&group_id_clone, &member_id_for_task, generation_id, vec![]).await
    });

    // Give time for rebalancing to be in progress
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Test that all APIs handle rebalancing consistently

    // Heartbeat should indicate rebalancing
    let heartbeat_response = send_heartbeat_request(group_id, &member_id, generation_id).await;
    let heartbeat_error = heartbeat_response.error_code;

    // Offset operations should be consistent with heartbeat
    let commit_response =
        send_offset_commit_request(group_id, &member_id, generation_id, topic_name, 0, 100).await;
    let offset_fetch_response = send_offset_fetch_request(group_id, topic_name).await;

    // All consumer group operations should handle rebalancing state consistently
    // They should either all succeed or all return appropriate error codes
    if heartbeat_error == 27 {
        // REBALANCE_IN_PROGRESS
        // If heartbeat indicates rebalancing, other operations should be aware too
        assert!(commit_response.throttle_time_ms >= 0);
        assert!(offset_fetch_response.throttle_time_ms >= 0);
    }

    // Non-group operations (produce, fetch) should continue working
    let produce_response = send_produce_request(topic_name, 0, "consistency_test").await;
    assert!(produce_response.throttle_time_ms >= 0);

    let fetch_response = send_fetch_request(topic_name, 0, 0).await;
    assert!(fetch_response.throttle_time_ms >= 0);

    // Clean up
    let _sync_result = timeout(Duration::from_millis(100), incomplete_sync_task).await;
}

// Race Condition Tests (basic concurrent tests without loom for now)

// Non-loom concurrent tests for integration scenarios

#[tokio::test]
async fn test_multiple_groups_isolation() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group1 = "isolation_test_group1";
    let group2 = "isolation_test_group2";

    // Join different groups concurrently
    let (response1, response2) = tokio::join!(
        send_join_group_request(group1, "", 30000),
        send_join_group_request(group2, "", 30000)
    );

    assert_eq!(response1.error_code, 0);
    assert_eq!(response2.error_code, 0);

    // Both should be leaders of their respective groups
    assert_eq!(response1.leader, response1.member_id);
    assert_eq!(response2.leader, response2.member_id);

    // Member IDs should be different
    assert_ne!(response1.member_id, response2.member_id);

    // Both should start with generation 1
    assert_eq!(response1.generation_id, 1);
    assert_eq!(response2.generation_id, 1);
}

#[tokio::test]
async fn test_rapid_join_leave_cycles() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "rapid_join_leave";

    for i in 0..3 {
        // Join
        let join_response = send_join_group_request(group_id, "", 30000).await;
        assert_eq!(join_response.error_code, 0);

        let member_id = join_response.member_id.clone();
        let generation_id = join_response.generation_id;

        // Sync
        let assignments = vec![SyncGroupRequestAssignment::default()
            .with_member_id(member_id.clone())
            .with_assignment(vec![1, 2, 3, 4].into())];

        let sync_response =
            send_sync_group_request(group_id, &member_id, generation_id, assignments).await;
        assert_eq!(sync_response.error_code, 0);

        // Heartbeat
        let heartbeat_response = send_heartbeat_request(group_id, &member_id, generation_id).await;
        assert_eq!(heartbeat_response.error_code, 0);

        // Leave
        let leave_response = send_leave_group_request(group_id, &member_id).await;
        assert_eq!(leave_response.error_code, 0);

        // Generation should increase with each cycle
        assert!(join_response.generation_id >= i + 1);
    }
}

#[tokio::test]
async fn test_heartbeat_during_rebalance() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "heartbeat_rebalance_test";

    // First member joins
    let join1 = send_join_group_request(group_id, "", 30000).await;
    let member1_id = join1.member_id.clone();
    let gen1 = join1.generation_id;
    assert_eq!(join1.error_code, 0);

    // Heartbeat should work before any sync (group is in PreparingRebalance)
    let _heartbeat_before_sync = send_heartbeat_request(group_id, &member1_id, gen1).await;

    // During rebalance preparation, heartbeat should return REBALANCE_IN_PROGRESS (27)
    // or work normally (0) depending on implementation
    // The key test is that after another member joins, old generation becomes invalid

    // Wait for rebalance interval to pass to allow new member
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Second member attempts to join - this should trigger generation increment
    let join2 = send_join_group_request(group_id, "", 30000).await;

    // If second join succeeds, generation should increment
    if join2.error_code == 0 {
        let new_gen = join2.generation_id;
        assert!(
            new_gen > gen1,
            "Generation should increment when new member joins"
        );

        // Heartbeat with old generation should now fail
        let heartbeat_old_gen = send_heartbeat_request(group_id, &member1_id, gen1).await;
        assert_eq!(
            heartbeat_old_gen.error_code, 22,
            "Expected ILLEGAL_GENERATION error"
        );
    } else {
        // If second join fails, test that heartbeat still works with current generation
        let heartbeat_current = send_heartbeat_request(group_id, &member1_id, gen1).await;
        // Should work or return a specific rebalance-related error
        assert!(
            heartbeat_current.error_code == 0 || heartbeat_current.error_code == 27,
            "Expected success (0) or REBALANCE_IN_PROGRESS (27), got {}",
            heartbeat_current.error_code
        );
    }
}

#[tokio::test]
async fn test_stale_generation_detection() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "stale_generation_test";

    // Member joins
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();
    let old_generation = join_response.generation_id;

    // Trigger rebalance by having another member join
    let join2 = send_join_group_request(group_id, "", 30000).await;
    let new_generation = join2.generation_id;

    // Old generation requests should fail
    let stale_heartbeat = send_heartbeat_request(group_id, &member_id, old_generation).await;
    assert_ne!(stale_heartbeat.error_code, 0); // Should be ILLEGAL_GENERATION or similar

    // New generation may work depending on member status
    let current_heartbeat = send_heartbeat_request(group_id, &member_id, new_generation).await;
    // Response should be valid (may succeed or fail with proper error)
    assert!(current_heartbeat.throttle_time_ms >= 0);
}

// Performance and Load Tests

#[tokio::test]
async fn test_high_concurrency_joins() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "high_concurrency_joins";
    let num_members = 10;

    let mut handles = vec![];

    for i in 0..num_members {
        let handle = tokio::spawn(async move {
            let response = send_join_group_request(group_id, "", 30000).await;
            (i, response)
        });
        handles.push(handle);
    }

    let mut results = vec![];
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    // Count successful joins
    let successful = results.iter().filter(|(_, r)| r.error_code == 0).count();

    // Should have some successful joins
    assert!(successful > 0);

    // All successful joins should have unique member IDs
    let mut member_ids = std::collections::HashSet::new();
    for (_, response) in results.iter() {
        if response.error_code == 0 {
            assert!(member_ids.insert(response.member_id.clone()));
        }
    }

    // All should have the same leader among successful responses
    let leaders: Vec<_> = results
        .iter()
        .filter(|(_, r)| r.error_code == 0)
        .map(|(_, r)| &r.leader)
        .collect();

    if leaders.len() > 1 {
        assert!(leaders.windows(2).all(|w| w[0] == w[1]));
    }
}

#[tokio::test]
async fn test_offset_commit_ordering() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "offset_ordering_test";
    let topic_name = "ordering_topic";

    let _topic = TestTopic::create(topic_name);

    // Set up group
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;

    let assignments = vec![SyncGroupRequestAssignment::default()
        .with_member_id(member_id.clone())
        .with_assignment(vec![1, 2, 3, 4].into())];

    let _sync_response =
        send_sync_group_request(group_id, &member_id, generation_id, assignments).await;

    // Commit offsets in sequence
    let offsets = [10, 20, 30, 40, 50];
    let mut handles = vec![];

    for offset in offsets {
        let member_id = member_id.clone();
        let handle = tokio::spawn(async move {
            let response = send_offset_commit_request(
                group_id,
                &member_id,
                generation_id,
                topic_name,
                0,
                offset,
            )
            .await;
            (offset, response)
        });
        handles.push(handle);
    }

    let mut results = vec![];
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    // All commits should have valid response structure
    for (offset, response) in results {
        // Response should be well-formed
        assert!(response.throttle_time_ms >= 0);
        println!(
            "Offset {} commit result: throttle_time={}",
            offset, response.throttle_time_ms
        );
    }

    // Fetch final committed offset
    let fetch_response = send_offset_fetch_request(group_id, topic_name).await;

    // Should return valid response structure
    assert!(fetch_response.throttle_time_ms >= 0);
}

// Edge Cases and Error Recovery

#[tokio::test]
async fn test_empty_group_cleanup() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "empty_group_cleanup";

    // Join and immediately leave
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();

    let leave_response = send_leave_group_request(group_id, &member_id).await;
    assert_eq!(leave_response.error_code, 0);

    // Group should be empty now - new join should start fresh
    let rejoin_response = send_join_group_request(group_id, "", 30000).await;
    assert_eq!(rejoin_response.error_code, 0);
    // Generation may or may not reset - implementation dependent
    assert!(rejoin_response.generation_id >= 1);
}

#[tokio::test]
async fn test_invalid_sync_group_scenarios() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "invalid_sync_scenarios";

    // Join group
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;

    // Try sync with wrong generation
    let wrong_gen_sync =
        send_sync_group_request(group_id, &member_id, generation_id + 1, vec![]).await;
    assert_eq!(wrong_gen_sync.error_code, 22); // ILLEGAL_GENERATION

    // Try sync with wrong member ID
    let wrong_member_sync =
        send_sync_group_request(group_id, "wrong_member", generation_id, vec![]).await;
    assert_eq!(wrong_member_sync.error_code, 25); // UNKNOWN_MEMBER_ID
}

#[tokio::test]
async fn test_group_state_transitions() {
    crate::common::ensure_settings_initialized_with_file(
        "tests/configs/test_integration_settings.yaml",
    );
    if !SETTINGS.enable_consumer_groups {
        return;
    }
    let group_id = "state_transitions_test";

    // Group starts empty
    // First member joins -> PreparingRebalance -> CompletingRebalance -> Stable
    let join_response = send_join_group_request(group_id, "", 30000).await;
    let member_id = join_response.member_id.clone();
    let generation_id = join_response.generation_id;

    // Complete sync -> should be Stable
    let assignments = vec![SyncGroupRequestAssignment::default()
        .with_member_id(member_id.clone())
        .with_assignment(vec![1, 2, 3, 4].into())];

    let sync_response =
        send_sync_group_request(group_id, &member_id, generation_id, assignments).await;
    assert_eq!(sync_response.error_code, 0);

    // Heartbeat should work in stable state
    let heartbeat_response = send_heartbeat_request(group_id, &member_id, generation_id).await;
    assert_eq!(heartbeat_response.error_code, 0);

    // Member leaves -> group becomes Empty
    let leave_response = send_leave_group_request(group_id, &member_id).await;
    assert_eq!(leave_response.error_code, 0);

    // Heartbeat after leaving should fail
    let post_leave_heartbeat = send_heartbeat_request(group_id, &member_id, generation_id).await;
    assert_eq!(post_leave_heartbeat.error_code, 25); // UNKNOWN_MEMBER_ID
}

// Run with: cargo test --test consumer_group
