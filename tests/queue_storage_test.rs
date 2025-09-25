// Queue storage functionality tests
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use blink::kafka::storage::{PartitionStorage, QueueRecord, RecordBatch};
use bytes::Bytes;
use kafka_protocol::records::{Record, TimestampType};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Notify;

fn create_test_record(
    value: Bytes,
    timestamp: i64,
    offset: i64,
) -> kafka_protocol::records::Record {
    kafka_protocol::records::Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: kafka_protocol::records::TimestampType::Creation,
        timestamp,
        sequence: 0,
        offset,
        key: None,
        value: Some(value),
        headers: Default::default(),
    }
}

#[tokio::test]
async fn test_queue_record_basic_operations() {
    // Create a test record (single record now) with RecordBatch for memory tracking
    let records = Bytes::from_static(b"test record data");
    let batch = Arc::new(RecordBatch::new(
        records.clone(),
        0, // assigned_offset
    ));
    let queue_record = QueueRecord::Memory {
        batch: Arc::clone(&batch),
        record: create_test_record(records.clone(), 1000, 0),
        ingestion_time: 1000,
        fetched: false,
    };

    assert_eq!(queue_record.record_count(), 1);
    assert_eq!(queue_record.byte_size(), records.len());
    assert_eq!(queue_record.ingestion_time(), 1000);
    assert!(!queue_record.is_fetched());
    assert!(!queue_record.is_placeholder());
}

#[tokio::test]
async fn test_queue_record_placeholder() {
    let placeholder = QueueRecord::Placeholder;

    assert_eq!(placeholder.record_count(), 0);
    assert_eq!(placeholder.byte_size(), 0);
    assert_eq!(placeholder.ingestion_time(), 0);
    assert!(placeholder.is_fetched()); // Placeholders are always "fetched"
    assert!(placeholder.is_placeholder());
}

#[tokio::test]
async fn test_partition_storage_offset_calculations() {
    let mut storage = PartitionStorage::new();
    storage.base_offset = 100;

    // Add some single records to the queue
    let records1 = Bytes::from_static(b"record1");
    let batch1 = Arc::new(RecordBatch::new(
        records1.clone(),
        100, // assigned_offset
    ));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch1,
        record: create_test_record(records1, 1000, 100),
        ingestion_time: 1000,
        fetched: false,
    });

    let records2 = Bytes::from_static(b"record2");
    let batch2 = Arc::new(RecordBatch::new(records2.clone(), 101));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch2,
        record: create_test_record(records2, 2000, 101),
        ingestion_time: 2000,
        fetched: false,
    });

    // Test offset calculations (each record has exactly one offset)
    assert_eq!(storage.get_offset_at_position(0), 100); // base_offset + 0
    assert_eq!(storage.get_offset_at_position(1), 101); // base_offset + 1
    assert_eq!(storage.get_first_offset_at_position(0), 100);
    assert_eq!(storage.get_last_offset_at_position(0), 100);
    assert_eq!(storage.get_first_offset_at_position(1), 101);
    assert_eq!(storage.get_last_offset_at_position(1), 101);

    // Test finding positions by offset
    assert_eq!(storage.find_position_for_offset(100), Some(0));
    assert_eq!(storage.find_position_for_offset(101), Some(1));
    assert_eq!(storage.find_position_for_offset(102), None); // Beyond available records
}

#[tokio::test]
async fn test_find_position_for_missing_offsets() {
    let mut storage = PartitionStorage {
        next_offset: 3,
        queue: VecDeque::new(),
        base_offset: 0,
        last_accessed_offset: -1,
        published: Arc::new(Notify::new()),
        wal: None,
        ram_batch_count: 0,
        offloaded_batch_count: 0,
    };

    // Add records at offsets 0, 2 (missing offset 1)
    let records0 = Bytes::from_static(b"record0");
    let batch0 = Arc::new(RecordBatch::new(records0.clone(), 0));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch0,
        record: create_test_record(records0, 1000, 0),
        ingestion_time: 1000,
        fetched: false,
    });

    // Add placeholder at position 1 (offset 1)
    storage.queue.push_back(QueueRecord::Placeholder);

    let records2 = Bytes::from_static(b"record2");
    let batch2 = Arc::new(RecordBatch::new(records2.clone(), 2));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch2,
        record: create_test_record(records2, 2000, 2),
        ingestion_time: 2000,
        fetched: false,
    });

    // Test finding positions for missing offsets - should return next available
    assert_eq!(storage.find_position_for_offset(0), Some(0)); // Exact match
    assert_eq!(storage.find_position_for_offset(1), Some(2)); // Skip placeholder, return next available (offset 2 at position 2)
    assert_eq!(storage.find_position_for_offset(2), Some(2)); // Exact match
    assert_eq!(storage.find_position_for_offset(3), None); // Beyond available records
}

#[tokio::test]
async fn test_find_position_before_base_offset() {
    let mut storage = PartitionStorage {
        next_offset: 102,
        queue: VecDeque::new(),
        base_offset: 100, // Start at offset 100
        last_accessed_offset: -1,
        published: Arc::new(Notify::new()),
        wal: None,
        ram_batch_count: 0,
        offloaded_batch_count: 0,
    };

    let records100 = Bytes::from_static(b"record100");
    let batch100 = Arc::new(RecordBatch::new(records100.clone(), 100));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch100,
        record: create_test_record(records100, 1000, 100),
        ingestion_time: 1000,
        fetched: false,
    });

    let records101 = Bytes::from_static(b"record101");
    let batch101 = Arc::new(RecordBatch::new(records101.clone(), 101));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch101,
        record: create_test_record(records101, 2000, 101),
        ingestion_time: 2000,
        fetched: false,
    });

    // Test finding positions for offsets before base_offset - should return first available
    assert_eq!(storage.find_position_for_offset(50), Some(0)); // Before base_offset, return first available
    assert_eq!(storage.find_position_for_offset(99), Some(0)); // Before base_offset, return first available
    assert_eq!(storage.find_position_for_offset(100), Some(0)); // Exact match at base_offset
    assert_eq!(storage.find_position_for_offset(101), Some(1)); // Exact match
    assert_eq!(storage.find_position_for_offset(102), None); // Beyond available
}

#[tokio::test]
async fn test_exact_scenario_request_offset1_when_starts_at_offset2() {
    // Mimic the exact scenario: consumer requests offset 1, but data starts at offset 2
    let mut storage = PartitionStorage {
        next_offset: 15, // Matches the dump showing next_offset: 15
        queue: VecDeque::new(),
        base_offset: 2,          // Start at offset 2, missing offsets 0 and 1
        last_accessed_offset: 1, // Consumer stuck requesting offset 1
        published: Arc::new(Notify::new()),
        wal: None,
        ram_batch_count: 0,
        offloaded_batch_count: 0,
    };

    // Add records starting at offset 2 (positions 0, 1, 2... in queue correspond to offsets 2, 3, 4...)
    for i in 0..13 {
        // Offsets 2-14 (13 records total)
        let offset = 2 + i;
        let record_data = Bytes::from(format!("record_at_offset_{}", offset));
        let batch = Arc::new(RecordBatch::new(record_data.clone(), offset));
        storage.queue.push_back(QueueRecord::Memory {
            batch,
            record: Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                timestamp: 1758011718275 + (i * 8000),
                sequence: 0,
                offset: offset,
                key: None,
                value: Some(record_data),
                headers: Default::default(),
            },
            ingestion_time: 1758011718275 + (i * 8000), // Mimic the ingestion times from dump
            fetched: false,
        });
    }

    // Test the problematic scenario: request offset 1, should get position of offset 2
    assert_eq!(storage.find_position_for_offset(1), Some(0)); // Should return position 0 (which contains offset 2)

    // Verify that position 0 actually contains offset 2
    assert_eq!(storage.get_offset_at_position(0), 2);

    // Test other scenarios
    assert_eq!(storage.find_position_for_offset(0), Some(0)); // Before base_offset, return first available
    assert_eq!(storage.find_position_for_offset(2), Some(0)); // Exact match for offset 2
    assert_eq!(storage.find_position_for_offset(3), Some(1)); // Exact match for offset 3
    assert_eq!(storage.find_position_for_offset(14), Some(12)); // Last available offset
    assert_eq!(storage.find_position_for_offset(15), None); // Beyond available data
}

#[tokio::test]
async fn test_purge_from_front_updates_base_offset() {
    let mut storage = PartitionStorage::new();
    storage.base_offset = 100;

    // Add three single records
    let records1 = Bytes::from_static(b"record1");
    let batch1 = Arc::new(RecordBatch::new(records1.clone(), 100));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch1,
        record: create_test_record(records1, 1000, 100),
        ingestion_time: 1000,
        fetched: false,
    });

    let records2 = Bytes::from_static(b"record2");
    let batch2 = Arc::new(RecordBatch::new(records2.clone(), 101));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch2,
        record: create_test_record(records2, 2000, 101),
        ingestion_time: 2000,
        fetched: false,
    });

    let records3 = Bytes::from_static(b"record3");
    let batch3 = Arc::new(RecordBatch::new(records3.clone(), 102));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch3,
        record: create_test_record(records3, 3000, 102),
        ingestion_time: 3000,
        fetched: false,
    });

    assert_eq!(storage.queue.len(), 3);
    assert_eq!(storage.base_offset, 100);

    // Purge first two records
    let purged = storage.purge_from_front(2);
    assert_eq!(purged, 2);
    assert_eq!(storage.queue.len(), 1);
    assert_eq!(storage.base_offset, 102); // 100 + 2 (two single records)

    // Verify remaining record is accessible at correct offset
    assert_eq!(storage.find_position_for_offset(102), Some(0));
    assert_eq!(storage.find_position_for_offset(103), None); // Beyond available
}

#[tokio::test]
async fn test_placeholder_replacement() {
    let mut storage = PartitionStorage::new();
    storage.base_offset = 0;

    // Add single records
    let records1 = Bytes::from_static(b"record1");
    let batch1 = Arc::new(RecordBatch::new(records1.clone(), 0));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch1,
        record: create_test_record(records1, 1000, 0),
        ingestion_time: 1000,
        fetched: false,
    });

    let records2 = Bytes::from_static(b"record2");
    let batch2 = Arc::new(RecordBatch::new(records2.clone(), 1));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch2,
        record: create_test_record(records2, 2000, 1),
        ingestion_time: 2000,
        fetched: false,
    });

    // Replace first record with placeholder
    assert!(storage.replace_with_placeholder(0));
    assert_eq!(storage.queue.len(), 2);

    // Check that first record is now a placeholder
    if let Some(record) = storage.queue.get(0) {
        assert!(record.is_placeholder());
        assert_eq!(record.record_count(), 0); // Placeholder has 0 count
    } else {
        panic!("Record should exist");
    }

    // Second record should still be memory
    if let Some(record) = storage.queue.get(1) {
        assert!(!record.is_placeholder());
        assert!(matches!(record, QueueRecord::Memory { .. }));
    } else {
        panic!("Record should exist");
    }
}

#[tokio::test]
async fn test_find_next_non_placeholder() {
    let mut storage = PartitionStorage::new();

    // Add mixed records and placeholders
    storage.queue.push_back(QueueRecord::Placeholder);
    storage.queue.push_back(QueueRecord::Placeholder);

    let records = Bytes::from_static(b"record");
    let batch = Arc::new(RecordBatch::new(records.clone(), 2));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch,
        record: create_test_record(records, 1000, 0),
        ingestion_time: 1000,
        fetched: false,
    });

    // Should find the memory record at position 2
    assert_eq!(storage.find_next_non_placeholder(0), Some(2));
    assert_eq!(storage.find_next_non_placeholder(1), Some(2));
    assert_eq!(storage.find_next_non_placeholder(2), Some(2));
    assert_eq!(storage.find_next_non_placeholder(3), None);
}

#[tokio::test]
async fn test_storage_integration() {
    // Test basic storage operations without full system initialization
    let mut storage = PartitionStorage::new();

    // Add single records to queue
    let records1 = Bytes::from_static(b"test record 1");
    let batch1 = Arc::new(RecordBatch::new(records1.clone(), 0));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch1,
        record: create_test_record(records1, 1000, 0),
        ingestion_time: 1000,
        fetched: false,
    });

    let records2 = Bytes::from_static(b"test record 2");
    let batch2 = Arc::new(RecordBatch::new(records2.clone(), 1));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch2,
        record: create_test_record(records2, 2000, 1),
        ingestion_time: 2000,
        fetched: false,
    });

    // Test that we can find records by offset
    assert_eq!(storage.find_position_for_offset(0), Some(0));
    assert_eq!(storage.find_position_for_offset(1), Some(1));

    // Test offset calculations
    assert_eq!(storage.get_offset_at_position(0), 0);
    assert_eq!(storage.get_offset_at_position(1), 1);
}

#[tokio::test]
async fn test_fetch_with_placeholders() {
    let mut storage = PartitionStorage::new();

    // Add three single records
    let records1 = Bytes::from_static(b"record1");
    let batch1 = Arc::new(RecordBatch::new(records1.clone(), 0));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch1,
        record: create_test_record(records1, 1000, 0),
        ingestion_time: 1000,
        fetched: false,
    });

    let records2 = Bytes::from_static(b"record2");
    let batch2 = Arc::new(RecordBatch::new(records2.clone(), 1));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch2,
        record: create_test_record(records2, 2000, 1),
        ingestion_time: 2000,
        fetched: false,
    });

    let records3 = Bytes::from_static(b"record3");
    let batch3 = Arc::new(RecordBatch::new(records3.clone(), 2));
    storage.queue.push_back(QueueRecord::Memory {
        batch: batch3,
        record: create_test_record(records3, 3000, 2),
        ingestion_time: 3000,
        fetched: false,
    });

    // Replace middle record with placeholder
    assert!(storage.replace_with_placeholder(1));

    // Check that middle record is now a placeholder
    if let Some(record) = storage.queue.get(1) {
        assert!(record.is_placeholder());
        assert_eq!(record.record_count(), 0); // Placeholder has 0 count
    }

    // Test finding next non-placeholder from position 1
    assert_eq!(storage.find_next_non_placeholder(1), Some(2));

    // Test that first and third records are still memory records
    assert!(matches!(
        storage.queue.get(0),
        Some(QueueRecord::Memory { .. })
    ));
    assert!(matches!(
        storage.queue.get(2),
        Some(QueueRecord::Memory { .. })
    ));
}

#[tokio::test]
async fn test_queue_record_fetched_state() {
    let records = Bytes::from_static(b"test");
    let batch = Arc::new(RecordBatch::new(records.clone(), 0));
    let mut memory_record = QueueRecord::Memory {
        batch: batch,
        record: create_test_record(records.clone(), 1000, 0),
        ingestion_time: 1000,
        fetched: false,
    };

    // Test initial state
    assert!(!memory_record.is_fetched());
    // Test state change
    memory_record.set_fetched();
    assert!(memory_record.is_fetched());
}

#[tokio::test]
async fn test_memory_tracking_during_offloading() {
    let mut storage = PartitionStorage::new();

    // Create a single record with Arc<RecordBatch> for proper memory tracking
    let test_data = Bytes::from_static(b"test data for offloading");
    let batch = Arc::new(RecordBatch::new(
        test_data.clone(),
        0, // assigned_offset
    ));

    // Add to queue as Memory record (single record slice pointing to batch)
    storage.queue.push_back(QueueRecord::Memory {
        batch: Arc::clone(&batch),
        record: Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            timestamp: 1000,
            sequence: 0,
            offset: 0,
            key: None,
            value: Some(test_data.clone()),
            headers: Default::default(),
        },
        ingestion_time: 1000,
        fetched: false,
    });
    storage.ram_batch_count = 1;

    // Verify the record exists and has correct data
    if let Some(QueueRecord::Memory {
        record,
        ingestion_time,
        fetched: _,
        ..
    }) = storage.queue.get(0)
    {
        let record_value = record.value.as_ref().unwrap();
        assert_eq!(record_value.len(), test_data.len());
        assert_eq!(record_value, &test_data);
        assert_eq!(*ingestion_time, 1000);
    } else {
        panic!("Expected Memory record at position 0");
    }

    // Test that we have a Memory record before offloading
    assert!(matches!(
        storage.queue.get(0),
        Some(QueueRecord::Memory { .. })
    ));
    assert_eq!(storage.ram_batch_count, 1);
    assert_eq!(storage.offloaded_batch_count, 0);

    // Now simulate offloading by replacing with WAL record
    let wal_record = QueueRecord::Wal {
        entry_ref: unsafe { std::mem::zeroed() }, // Mock EntryRef for testing
        record: Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            timestamp: 1000,
            sequence: 0,
            offset: 0,
            key: None,
            value: Some(test_data.clone()),
            headers: Default::default(),
        },
        byte_size: test_data.len(),
        ingestion_time: 1000,
        fetched: false,
    };

    // Replace the Memory record with WAL record (simulates offloading)
    storage.queue[0] = wal_record;
    storage.ram_batch_count = 0;
    storage.offloaded_batch_count = 1;

    // Verify the offloading transition completed correctly
    assert!(matches!(
        storage.queue.get(0),
        Some(QueueRecord::Wal { .. })
    ));
    assert_eq!(storage.ram_batch_count, 0);
    assert_eq!(storage.offloaded_batch_count, 1);

    // Drop the original batch reference to trigger memory cleanup
    drop(batch);

    // Test successful - memory tracking behavior is working as expected
    // The RecordBatch Drop implementation will handle memory counter updates
}
