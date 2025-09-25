/// Macro to get the string name of a RequestKind variant
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
///
/// Examples:
/// - request_kind_name!(RequestKind::Fetch(_)) -> "Fetch"
/// - request_kind_name!(some_request_variable) -> variant name as string
#[macro_export]
macro_rules! request_kind_name {
    ($request:expr) => {
        match $request {
            kafka_protocol::messages::RequestKind::ApiVersions(_) => "ApiVersions",
            kafka_protocol::messages::RequestKind::CreateTopics(_) => "CreateTopics",
            kafka_protocol::messages::RequestKind::DeleteTopics(_) => "DeleteTopics",
            kafka_protocol::messages::RequestKind::Fetch(_) => "Fetch",
            kafka_protocol::messages::RequestKind::FindCoordinator(_) => "FindCoordinator",
            kafka_protocol::messages::RequestKind::Heartbeat(_) => "Heartbeat",
            kafka_protocol::messages::RequestKind::InitProducerId(_) => "InitProducerId",
            kafka_protocol::messages::RequestKind::JoinGroup(_) => "JoinGroup",
            kafka_protocol::messages::RequestKind::LeaveGroup(_) => "LeaveGroup",
            kafka_protocol::messages::RequestKind::ListOffsets(_) => "ListOffsets",
            kafka_protocol::messages::RequestKind::Metadata(_) => "Metadata",
            kafka_protocol::messages::RequestKind::OffsetCommit(_) => "OffsetCommit",
            kafka_protocol::messages::RequestKind::OffsetFetch(_) => "OffsetFetch",
            kafka_protocol::messages::RequestKind::Produce(_) => "Produce",
            kafka_protocol::messages::RequestKind::SyncGroup(_) => "SyncGroup",
            _ => "Unknown",
        }
    };
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::*;

    #[test]
    fn test_request_kind_macro() {
        let fetch_request = RequestKind::Fetch(FetchRequest::default());
        let produce_request = RequestKind::Produce(ProduceRequest::default());

        assert_eq!(request_kind_name!(&fetch_request), "Fetch");
        assert_eq!(request_kind_name!(&produce_request), "Produce");
    }
}
