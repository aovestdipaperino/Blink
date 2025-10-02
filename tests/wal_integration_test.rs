//! Integration tests for WAL-based offload functionality
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
//!
//! These tests verify that the nano-wal integration works correctly for
//! offloading batches to disk and reading them back.

use blink::kafka::storage::{PartitionStorage, QueueRecord, RecordBatch};
use bytes::Bytes;
use std::sync::Arc;

/// Helper to create test data
fn create_test_data(prefix: &str, size: usize) -> Bytes {
    let mut data = Vec::with_capacity(size);
    let prefix_bytes = prefix.as_bytes();
    let mut i = 0;
    while data.len() < size {
        data.push(prefix_bytes[i % prefix_bytes.len()]);
        i += 1;
    }
    Bytes::from(data)
}

/// Helper to create a memory batch
fn create_memory_batch(data: Bytes, assigned_offset: i64, _ingestion_time: i64) -> RecordBatch {
    RecordBatch::new(data.clone(), assigned_offset)
}

/// Test that debug functionality works with mixed batches
#[tokio::test]
async fn test_wal_debug_functionality() {
    let mut partition_storage = PartitionStorage::new();

    // Set base_offset to match expected values
    partition_storage.base_offset = 100;

    // Add some memory batches
    let data1 = create_test_data("debug1", 50);
    let data2 = create_test_data("debug2", 75);

    let batch1 = create_memory_batch(data1, 100, 1111);
    let batch2 = create_memory_batch(data2, 101, 2222);

    let batch1_arc = Arc::new(batch1);
    let batch2_arc = Arc::new(batch2);

    let record1 = kafka_protocol::records::Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: kafka_protocol::records::TimestampType::Creation,
        timestamp: 1111,
        sequence: 0,
        offset: 100,
        key: None,
        value: Some(batch1_arc.records.clone()),
        headers: Default::default(),
    };
    partition_storage.queue.push_back(QueueRecord::Memory {
        batch: Arc::clone(&batch1_arc),
        record: record1,
        ingestion_time: 1111,
        fetched: false,
    });
    let record2 = kafka_protocol::records::Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: kafka_protocol::records::TimestampType::Creation,
        timestamp: 2222,
        sequence: 0,
        offset: 101,
        key: None,
        value: Some(batch2_arc.records.clone()),
        headers: Default::default(),
    };
    partition_storage.queue.push_back(QueueRecord::Memory {
        batch: Arc::clone(&batch2_arc),
        record: record2,
        ingestion_time: 2222,
        fetched: false,
    });

    // Test debug output
    let debug_result = partition_storage.debug_wal();
    assert!(debug_result.is_ok());

    let debug_data = debug_result.unwrap();
    assert_eq!(debug_data.len(), 2);
    assert_eq!(debug_data[0], (100, 1)); // (first_offset, record_count)
    assert_eq!(debug_data[1], (101, 1)); // sequential offset based on queue position
}

/// Test basic structure and access patterns
#[tokio::test]
async fn test_wal_record_structure() {
    // Test that we can create WAL records and access their properties
    let entry_ref = unsafe { std::mem::zeroed() }; // Mock EntryRef for testing
    let placeholder_record = kafka_protocol::records::Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: kafka_protocol::records::TimestampType::Creation,
        timestamp: 123456789,
        sequence: 0,
        offset: 0,
        key: None,
        value: Some(Bytes::from_static(b"test_wal_record")),
        headers: Default::default(),
    };

    let wal_record = QueueRecord::Wal {
        entry_ref,
        record: placeholder_record,
        byte_size: 100,
        ingestion_time: 123456789,
        fetched: false,
    };

    // Test accessor methods
    assert_eq!(wal_record.ingestion_time(), 123456789);
    assert_eq!(wal_record.byte_size(), 100);
    assert_eq!(wal_record.record_count(), 1);
    assert_eq!(wal_record.is_fetched(), false);

    // Test that we can modify fetch status
    let mut wal_record_mut = wal_record;
    wal_record_mut.set_fetched();
    assert_eq!(wal_record_mut.is_fetched(), true);
}

/// Test memory tracking behavior
#[tokio::test]
async fn test_memory_tracking() {
    let mut partition_storage = PartitionStorage::new();

    // Verify initial state
    assert_eq!(partition_storage.ram_batch_count, 0);
    assert_eq!(partition_storage.offloaded_batch_count, 0);
    assert_eq!(partition_storage.queue.len(), 0);
    assert!(partition_storage.wal.is_none());

    // Add a memory batch
    let test_data = create_test_data("memory_test", 200);
    let batch = create_memory_batch(test_data, 1, 1000);

    let batch_arc = Arc::new(batch);

    let record = kafka_protocol::records::Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: kafka_protocol::records::TimestampType::Creation,
        timestamp: 33333,
        sequence: 0,
        offset: 0,
        key: None,
        value: Some(batch_arc.records.clone()),
        headers: Default::default(),
    };
    partition_storage.queue.push_back(QueueRecord::Memory {
        batch: Arc::clone(&batch_arc),
        record: record,
        ingestion_time: 33333,
        fetched: false,
    });
    partition_storage.ram_batch_count = 1;

    assert_eq!(partition_storage.ram_batch_count, 1);
    assert_eq!(partition_storage.offloaded_batch_count, 0);
    assert_eq!(partition_storage.queue.len(), 1);

    // Verify memory counter is tracked
    let current_memory = blink::alloc::global_allocator().current_allocated();
    assert!(current_memory > 0, "Memory tracking should be active");
}
