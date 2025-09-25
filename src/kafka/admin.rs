// Kafka admin operations implementation
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use crate::kafka::broker::Broker;
use crate::kafka::metadata::TopicMeta;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::delete_topics_request::DeleteTopicState;
use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use kafka_protocol::messages::{
    CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest, DeleteTopicsResponse,
    ResponseKind, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::ResponseError;
use tracing::debug;
use uuid::Uuid;

impl Broker {
    pub(crate) fn handle_create_topics(&self, request: CreateTopicsRequest) -> ResponseKind {
        let mut topics = vec![];

        for topic in request.topics.iter() {
            let num_partitions = topic.num_partitions;
            let topic_id = self.create_topic(topic.name.clone().to_string(), num_partitions);
            debug!("Created topic ID: {:?}", topic_id);
            let topic_result = match topic_id {
                Ok(_) => CreatableTopicResult::default()
                    .with_topic_id(topic_id.unwrap())
                    .with_name(topic.name.clone())
                    .with_num_partitions(num_partitions),
                Err(e) => CreatableTopicResult::default().with_error_code(e.code()),
            };

            topics.push(topic_result);
        }

        ResponseKind::CreateTopics(CreateTopicsResponse::default().with_topics(topics))
    }

    fn get_topic_name(&self, topic: &DeleteTopicState) -> Option<TopicName> {
        match topic.name {
            Some(ref name) => Some(name.clone()),
            None => self
                .topics
                .iter()
                .find(|entry| entry.topic_id == topic.topic_id)
                .map(|entry| entry.value().topic_name.clone()),
        }
    }

    pub(crate) fn handle_delete_topics(&self, request: DeleteTopicsRequest) -> ResponseKind {
        let mut topics = vec![];

        // Handle different protocol versions:
        // v1-v5: Use topic_names field (Vec<String>)
        // v6+: Use topics field (Vec<DeleteTopicState>)

        // Check if topic_names field has content (versions 1-5), otherwise use topics field (version 6+)
        if !request.topic_names.is_empty() {
            // Protocol versions 1-5: topic_names field
            debug!("Deleting topics (v1-5): {}", request.topic_names.len());

            for topic_name in request.topic_names.iter() {
                let topic_result = match self.topics.remove(topic_name) {
                    Some((_, v)) => {
                        debug!(
                            "Deleting topic: name={}, id={}, partitions={}",
                            topic_name.0, v.topic_id, v.partitions
                        );

                        // Delete all partition storage and perform comprehensive cleanup
                        self.storage
                            .cleanup_topic_completely(v.topic_id, v.partitions);

                        // TODO: When consumer groups are implemented, also clean up:
                        // - Consumer group offsets for this topic
                        // - Any pending assignments for this topic's partitions
                        // Example: self.consumer_groups.cleanup_topic_offsets(v.topic_id);

                        debug!("Successfully deleted topic: {}", topic_name.0);

                        DeletableTopicResult::default()
                            .with_name(Some(topic_name.clone()))
                            .with_topic_id(v.topic_id)
                            .with_error_code(0)
                    }
                    None => {
                        debug!("Attempted to delete non-existent topic: {}", topic_name.0);
                        DeletableTopicResult::default()
                            .with_name(Some(topic_name.clone()))
                            .with_error_code(1)
                    }
                };
                topics.push(topic_result);
            }
        } else if !request.topics.is_empty() {
            // Protocol version 6+: topics field
            debug!("Deleting topics (v6+): {}", request.topics.len());

            for topic in request.topics.iter() {
                let topic_result = match self.get_topic_name(topic) {
                    Some(name) => match self.topics.remove(&name) {
                        // The topic name exists in the map.
                        Some((_, v)) => {
                            debug!(
                                "Deleting topic: name={}, id={}, partitions={}",
                                name.0, v.topic_id, v.partitions
                            );

                            // Delete all partition storage and perform comprehensive cleanup
                            self.storage
                                .cleanup_topic_completely(v.topic_id, v.partitions);

                            // TODO: When consumer groups are implemented, also clean up:
                            // - Consumer group offsets for this topic
                            // - Any pending assignments for this topic's partitions
                            // Example: self.consumer_groups.cleanup_topic_offsets(v.topic_id);

                            debug!("Successfully deleted topic: {}", name.0);

                            DeletableTopicResult::default()
                                .with_name(Some(name.clone()))
                                .with_topic_id(topic.topic_id)
                                .with_error_code(0)
                        }
                        // The topic name cannot be removed from the map.
                        None => {
                            debug!("Attempted to delete non-existent topic: {}", name.0);
                            DeletableTopicResult::default()
                                .with_name(Some(name.clone()))
                                .with_topic_id(topic.topic_id)
                                .with_error_code(1)
                        }
                    },
                    // No topic name was passed in the request and
                    // no matching name found matching the topic_id passed.
                    None => {
                        debug!(
                            "Cannot delete topic: no name provided and no topic found for id={:?}",
                            topic.topic_id
                        );
                        DeletableTopicResult::default()
                            .with_topic_id(topic.topic_id)
                            .with_error_code(1)
                    }
                };
                topics.push(topic_result);
            }
        }

        ResponseKind::DeleteTopics(DeleteTopicsResponse::default().with_responses(topics))
    }

    pub fn create_topic(
        &self,
        topic_name: String,
        num_partitions: i32,
    ) -> Result<Uuid, ResponseError> {
        let topic_name = TopicName(StrBytes::from(topic_name));
        let topic_id = Uuid::new_v4();
        {
            if self.topics.contains_key(&topic_name) {
                return Err(ResponseError::TopicAlreadyExists);
            }
            self.topics.insert(
                topic_name.clone(),
                TopicMeta::default()
                    .with_topic_id(topic_id)
                    .with_topic_name(topic_name)
                    .with_partitions(num_partitions),
            );
        }

        self.storage.create_topic_storage(topic_id, num_partitions);

        Ok(topic_id)
    }
}
