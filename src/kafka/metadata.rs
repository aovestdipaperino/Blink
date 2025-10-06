// Kafka metadata management
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use crate::kafka::broker::Broker;
use crate::settings::SETTINGS;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::ApiKey::{
    ApiVersions, CreateTopics, DeleteTopics, Fetch, FindCoordinator, Heartbeat, InitProducerId,
    JoinGroup, ListOffsets, Metadata, OffsetCommit, OffsetFetch, Produce, SyncGroup,
};
use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, BrokerId, CreateTopicsRequest, DeleteTopicsRequest,
    FetchRequest, FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest,
    InitProducerIdRequest, JoinGroupRequest, ListOffsetsRequest, MetadataRequest, MetadataResponse,
    OffsetCommitRequest, OffsetFetchRequest, ProduceRequest, ResponseKind, SyncGroupRequest,
    TopicName,
};
use kafka_protocol::protocol::Message;
use once_cell::sync::Lazy;
use paste::paste;
use rand::Rng;

#[derive(Debug, Clone, Default)]
pub(crate) struct TopicMeta {
    pub(crate) topic_id: uuid::Uuid,
    pub(crate) topic_name: TopicName,
    pub(crate) partitions: i32,
}

impl TopicMeta {
    pub fn with_topic_id(mut self, topic_id: uuid::Uuid) -> Self {
        self.topic_id = topic_id;
        self
    }

    pub fn with_topic_name(mut self, topic_name: TopicName) -> Self {
        self.topic_name = topic_name;
        self
    }

    pub fn with_partitions(mut self, partitions: i32) -> Self {
        self.partitions = partitions;
        self
    }
}

const NODE_ID: i32 = 1;
const CONTROLLER_ID: i32 = 1;

static CLUSTER_ID: Lazy<String> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let random_suffix: String = (0..5)
        .map(|_| {
            let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            chars[rng.gen_range(0..chars.len())] as char
        })
        .collect();
    format!("RW56b0w={}", random_suffix)
});

#[macro_export]
macro_rules! api_version_min {
    ($api_name:ident, $min_version:expr) => {
        paste! {
            ApiVersion::default()
                .with_api_key($api_name as i16)
                .with_min_version($min_version)
                .with_max_version([<$api_name Request>]::VERSIONS.max)
        }
    };
}

macro_rules! api_version_range {
    ($api_name:ident, $min_version:expr, $max_version:expr) => {
        paste! {
            ApiVersion::default()
                .with_api_key($api_name as i16)
                .with_min_version($min_version)
                .with_max_version($max_version)
        }
    };
}

macro_rules! api_version {
    ($api_name:ident) => {
        api_version_min!($api_name, 0)
    };
}

static SUPPORTED_APIS: Lazy<Vec<ApiVersion>> = Lazy::new(|| {
    let mut apis = vec![
        api_version!(Produce),
        api_version!(Metadata),
        api_version!(ApiVersions),
        api_version!(InitProducerId),
        api_version!(ListOffsets),
        api_version!(Fetch),
        api_version!(CreateTopics),
        api_version!(DeleteTopics),
    ];

    // Only include consumer group-related APIs if enabled in settings
    if SETTINGS.enable_consumer_groups {
        apis.extend(vec![
            api_version_range!(FindCoordinator, 0, 4),
            api_version!(Heartbeat),
            api_version!(OffsetCommit),
            api_version_min!(OffsetFetch, 8),
            api_version!(JoinGroup),
            api_version!(SyncGroup),
        ]);
    }

    apis.sort_by_key(|api| api.api_key);
    apis
});

impl Broker {
    pub(crate) fn handle_find_coordinator(&self, request: FindCoordinatorRequest) -> ResponseKind {
        // Handle both version 0 (single key) and newer versions (coordinator_keys array)
        let mut keys_to_process = Vec::new();

        // For version 0+, check if there's a single key field
        if !request.key.is_empty() {
            keys_to_process.push(request.key.clone());
        }

        // For version 2+, also process coordinator_keys array
        if !request.coordinator_keys.is_empty() {
            keys_to_process.extend(request.coordinator_keys.iter().cloned());
        }

        let coordinators: Vec<Coordinator> = keys_to_process
            .iter()
            .map(|key| {
                let coordinator = Coordinator::default()
                    .with_key(key.clone())
                    .with_node_id(BrokerId::from(NODE_ID))
                    .with_host(self.host.clone().into())
                    .with_port(self.port)
                    .with_error_code(0)
                    .with_unknown_tagged_fields(Default::default());

                coordinator
            })
            .collect();

        // For version 0, populate both the legacy single fields AND the coordinators array
        let mut response = FindCoordinatorResponse::default()
            .with_throttle_time_ms(0)
            .with_coordinators(coordinators.clone())
            .with_unknown_tagged_fields(Default::default());

        // For version 0 compatibility, also set the single coordinator fields
        if let Some(first_coordinator) = coordinators.first() {
            response = response
                .with_node_id(first_coordinator.node_id)
                .with_host(first_coordinator.host.clone())
                .with_port(first_coordinator.port)
                .with_error_code(first_coordinator.error_code);
        }

        ResponseKind::FindCoordinator(response)
    }
    pub fn get_topic_id(&self, topic_name: &TopicName) -> Option<uuid::Uuid> {
        self.topics.get(topic_name).map(|topic| topic.topic_id)
    }

    pub fn get_topic_name_by_id(&self, topic_id: &uuid::Uuid) -> Option<TopicName> {
        self.topics
            .iter()
            .find(|entry| entry.topic_id == *topic_id)
            .map(|entry| entry.topic_name.clone())
    }

    pub(crate) fn handle_metadata(&self, _request: MetadataRequest) -> ResponseKind {
        let topics: Vec<MetadataResponseTopic> = self
            .topics
            .iter()
            .map(|topic| {
                MetadataResponseTopic::default()
                    .with_topic_id(topic.topic_id)
                    .with_name(Some(topic.topic_name.clone()))
                    .with_partitions(
                        (0..topic.partitions)
                            .map(|idx| {
                                MetadataResponsePartition::default()
                                    .with_partition_index(idx)
                                    .with_leader_id(BrokerId::from(NODE_ID))
                                    .with_leader_epoch(0)
                                    .with_isr_nodes(vec![BrokerId::from(NODE_ID)])
                                    .with_replica_nodes(vec![BrokerId::from(NODE_ID)])
                            })
                            .collect(),
                    )
            })
            .collect();

        let response = ResponseKind::Metadata(
            MetadataResponse::default()
                .with_throttle_time_ms(0)
                .with_brokers(vec![MetadataResponseBroker::default()
                    .with_node_id(BrokerId::from(CONTROLLER_ID))
                    .with_host(self.host.clone().into())
                    .with_port(self.port)
                    .with_unknown_tagged_fields(Default::default())])
                .with_controller_id(BrokerId::from(CONTROLLER_ID))
                .with_cluster_id(Some(CLUSTER_ID.clone().into()))
                .with_topics(topics)
                //.with_cluster_authorized_operations(0)
                .with_unknown_tagged_fields(Default::default()),
        );

        response
    }

    pub(crate) fn handle_api_versions(&self, _request: ApiVersionsRequest) -> ResponseKind {
        let response = ResponseKind::ApiVersions(
            ApiVersionsResponse::default()
                .with_throttle_time_ms(0)
                .with_error_code(0)
                .with_unknown_tagged_fields(Default::default())
                .with_api_keys(SUPPORTED_APIS.clone()),
        );

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_id_format() {
        let cluster_id = &*CLUSTER_ID;

        // Should start with "RW56b0w="
        assert!(cluster_id.starts_with("RW56b0w="));

        // Should have exactly 5 additional characters after the base
        assert_eq!(cluster_id.len(), "RW56b0w=".len() + 5);

        // The suffix should be alphanumeric
        let suffix = &cluster_id["RW56b0w=".len()..];
        assert_eq!(suffix.len(), 5);
        assert!(suffix.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn test_cluster_id_uniqueness() {
        // Create multiple instances to verify they would be different
        // (though the Lazy ensures only one is created per process)
        let id1 = &*CLUSTER_ID;
        let id2 = &*CLUSTER_ID;

        // They should be the same since it's a Lazy static
        assert_eq!(id1, id2);

        // But verify the format is correct
        assert!(id1.starts_with("RW56b0w="));
        assert_eq!(id1.len(), 13); // "RW56b0w=" (8) + 5 random chars
    }

    #[test]
    fn test_cluster_id_generation() {
        let cluster_id = &*CLUSTER_ID;
        println!("Generated CLUSTER_ID: {}", cluster_id);

        // Verify it starts with the expected prefix
        assert!(cluster_id.starts_with("RW56b0w="));

        // Verify total length
        assert_eq!(cluster_id.len(), 13);

        // Verify the suffix is exactly 5 characters
        let suffix = &cluster_id[8..];
        assert_eq!(suffix.len(), 5);

        // Verify all suffix characters are alphanumeric
        assert!(suffix.chars().all(|c| c.is_ascii_alphanumeric()));
    }
}
