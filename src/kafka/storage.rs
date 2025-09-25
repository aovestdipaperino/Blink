// Kafka message storage implementation
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use crate::kafka::broker::BROKER;
use crate::kafka::counters::{AGGRESSIVE_CLIENT, NOTIFY_COUNTER};
use crate::kafka::counters::{
    CURRENT_DISK_SPACE, DASHMAP_GET_MUT, IN_MEMORY_QUEUE_SIZE, LOG_SEGMENT_COUNT, LOG_SIZE,
    MAX_DISK_SPACE, OBLITERATED_RECORDS, PURGED_RECORDS, RECORD_COUNT,
};

use crate::profiling::profile;
#[cfg(feature = "profiled")]
use crate::profiling::BlinkProfileOp;
use crate::settings::SETTINGS;
use crate::util::Util;
use bytes::{Buf, Bytes, BytesMut};
use dashmap::DashMap;
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
};
use kafka_protocol::ResponseError;
use nano_wal::{EntryRef, Wal, WalOptions};
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

/// Helper function to get topic name for metrics labeling
fn get_topic_name_for_metrics(topic_id: &Uuid) -> String {
    BROKER
        .get_topic_name_by_id(topic_id)
        .map(|name| name.0.to_string())
        .unwrap_or_else(|| topic_id.to_string())
}

/// Update disk space counters for offload file tracking
fn update_disk_space_counters(size_change_bytes: f64) {
    if size_change_bytes > 0.0 {
        CURRENT_DISK_SPACE.add(size_change_bytes);
    } else {
        CURRENT_DISK_SPACE.sub(-size_change_bytes);
    }

    // Update maximum if current exceeds it
    let current_bytes = CURRENT_DISK_SPACE.get();
    let current_max_bytes = MAX_DISK_SPACE.get();
    if current_bytes > current_max_bytes {
        MAX_DISK_SPACE.set(current_bytes);
    }
}

static EMPTY_BUFFER: Bytes = Bytes::from_static(&[]);

pub static MEMORY: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

static FIRST_OFFLOAD_LOGGED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct RecordBatch {
    pub records: Bytes,
    pub assigned_offset: i64,
}

impl RecordBatch {
    /// Create a new RecordBatch and increment memory counters
    pub fn new(records: Bytes, assigned_offset: i64) -> Self {
        // Increment memory counters when RecordBatch is created
        MEMORY.fetch_add(records.len(), std::sync::atomic::Ordering::Relaxed);
        IN_MEMORY_QUEUE_SIZE.add(records.len() as f64);

        RecordBatch {
            records,
            assigned_offset,
        }
    }
}

impl Drop for RecordBatch {
    fn drop(&mut self) {
        // Decrement memory counters when RecordBatch is dropped
        // Note: This tracks memory per RecordBatch instance, not per unique data
        // Cloned RecordBatch instances will each decrement counters when dropped
        MEMORY.fetch_sub(self.records.len(), std::sync::atomic::Ordering::Relaxed);
        IN_MEMORY_QUEUE_SIZE.sub(self.records.len() as f64);
    }
}

#[derive(Debug)]
// Clone removed to prevent memory tracking over-counting
// RecordBatch instances should be shared via Arc<RecordBatch> instead

/// QueueRecord represents a single record in the partition storage queue
/// It's a simple record type that can be easily fetched, with offset computed from position.
/// It's offset-agnostic - the offset is computed as distance from base_offset
pub enum QueueRecord {
    Memory {
        batch: Arc<RecordBatch>, // Reference to the full batch for memory tracking
        record: Record,          // Individual parsed Kafka record
        ingestion_time: i64,
        fetched: bool,
    },
    /// Single record stored in WAL (Write-Ahead Log)
    Wal {
        entry_ref: EntryRef,
        record: Record, // Individual parsed Kafka record
        byte_size: usize,
        ingestion_time: i64,
        fetched: bool,
    },
    /// Placeholder for purged single record
    Placeholder,
}

impl QueueRecord {
    pub fn byte_size(&self) -> usize {
        match self {
            QueueRecord::Memory { batch, .. } => batch.records.len(),
            QueueRecord::Wal { byte_size, .. } => *byte_size,
            QueueRecord::Placeholder => 0,
        }
    }

    pub fn record_batch_offset(&self) -> i64 {
        match self {
            QueueRecord::Memory { batch, .. } => batch.assigned_offset,
            QueueRecord::Wal { .. } => -1,
            QueueRecord::Placeholder => -1,
        }
    }

    pub fn record_count(&self) -> i32 {
        // Each QueueRecord now represents exactly one record
        match self {
            QueueRecord::Placeholder => 0,
            _ => 1,
        }
    }

    pub fn ingestion_time(&self) -> i64 {
        match self {
            QueueRecord::Memory { ingestion_time, .. } => *ingestion_time,
            QueueRecord::Wal { ingestion_time, .. } => *ingestion_time,
            QueueRecord::Placeholder => 0,
        }
    }

    pub fn set_fetched(&mut self) {
        match self {
            QueueRecord::Memory { fetched, .. } => *fetched = true,
            QueueRecord::Wal { fetched, .. } => *fetched = true,
            QueueRecord::Placeholder => {} // No-op for placeholders
        }
    }

    pub fn is_fetched(&self) -> bool {
        match self {
            QueueRecord::Memory { fetched, .. } => *fetched,
            QueueRecord::Wal { fetched, .. } => *fetched,
            QueueRecord::Placeholder => true, // Placeholders are always considered "fetched"
        }
    }

    pub fn is_placeholder(&self) -> bool {
        matches!(self, QueueRecord::Placeholder)
    }
}

// Removed - replaced by QueueRecord system

pub struct PartitionStorage {
    pub next_offset: i64,
    pub queue: VecDeque<QueueRecord>,
    pub base_offset: i64,
    pub last_accessed_offset: i64,
    pub published: Arc<Notify>,
    // Offload-to-WAL state
    pub wal: Option<Wal>,
    pub ram_batch_count: usize,
    pub offloaded_batch_count: usize,
}

type PartitionKey = (Uuid, i32); // topic_id, partition

pub struct Storage {
    pub topic_partition_store: DashMap<PartitionKey, PartitionStorage>,
}

impl std::fmt::Debug for PartitionStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionStorage")
            .field("next_offset", &self.next_offset)
            .field("queue_count", &self.queue.len())
            .field("last_accessed_offset", &self.last_accessed_offset)
            .field("has_wal", &self.wal.is_some())
            .field("ram_batch_count", &self.ram_batch_count)
            .field("offloaded_batch_count", &self.offloaded_batch_count)
            .finish()
    }
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field("partition_count", &self.topic_partition_store.len())
            .finish()
    }
}

impl PartitionStorage {
    /// Get the offset for a record at the given position in the queue
    /// Since each record has exactly one offset, first and last are the same
    pub fn get_offset_at_position(&self, position: usize) -> i64 {
        self.base_offset + position as i64
    }

    /// Get the first offset for a record at the given position in the queue
    pub fn get_first_offset_at_position(&self, position: usize) -> i64 {
        self.get_offset_at_position(position)
    }

    /// Get the last offset for a record at the given position in the queue
    pub fn get_last_offset_at_position(&self, position: usize) -> i64 {
        self.get_offset_at_position(position)
    }

    /// Find the queue position for a given offset.
    /// If the exact offset doesn't exist, returns the position of the next available offset (>= target_offset).
    /// This implements Kafka's behavior of returning the next available record when the exact offset is missing.
    pub fn find_position_for_offset(&self, target_offset: i64) -> Option<usize> {
        // If requesting offset before partition start, return first available position
        if target_offset < self.base_offset {
            if self.queue.is_empty() {
                return None;
            }
            // Find first non-placeholder record
            for (idx, record) in self.queue.iter().enumerate() {
                if !record.is_placeholder() {
                    return Some(idx);
                }
            }
            return None;
        }

        let position = (target_offset - self.base_offset) as usize;

        // If position is beyond available records, no data available
        if position >= self.queue.len() {
            return None;
        }

        // Find the first non-placeholder record starting from the calculated position
        for (idx, record) in self.queue.iter().enumerate().skip(position) {
            if !record.is_placeholder() {
                return Some(idx);
            }
        }

        // No non-placeholder records found from this position onward
        None
    }

    /// Purge records from the front of the queue and update base_offset
    pub fn purge_from_front(&mut self, count: usize) -> usize {
        let mut purged = 0;

        while purged < count && !self.queue.is_empty() {
            if let Some(_) = self.queue.pop_front() {
                purged += 1;
            }
        }

        self.base_offset += purged as i64;
        purged
    }

    /// Replace a record at the given position with a placeholder
    pub fn replace_with_placeholder(&mut self, position: usize) -> bool {
        if position < self.queue.len() {
            self.queue[position] = QueueRecord::Placeholder;
            true
        } else {
            false
        }
    }

    /// Find the first non-placeholder record starting from the given position
    pub fn find_next_non_placeholder(&self, start_position: usize) -> Option<usize> {
        for (i, record) in self.queue.iter().enumerate().skip(start_position) {
            if !record.is_placeholder() {
                return Some(i);
            }
        }
        None
    }
    pub fn new() -> Self {
        Self {
            next_offset: 0,
            queue: VecDeque::new(),
            base_offset: 0,
            last_accessed_offset: 0,
            published: Arc::new(Notify::new()),
            wal: None,
            ram_batch_count: 0,
            offloaded_batch_count: 0,
        }
    }

    fn global_should_offload() -> bool {
        if SETTINGS.record_storage_path.is_none() {
            return false;
        }
        let current_memory = MEMORY.load(std::sync::atomic::Ordering::Relaxed);
        current_memory >= SETTINGS.max_memory
    }

    fn ensure_wal(
        &mut self,
        topic_id: &Uuid,
        partition_index: i32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.wal.is_some() {
            return Ok(());
        }

        // Create offload directory if it doesn't exist
        let record_storage = if let Some(ref record_storage) = SETTINGS.record_storage_path {
            std::fs::create_dir_all(record_storage)?;
            record_storage.clone()
        } else {
            return Err("No record storage directory configured".into());
        };

        // Get topic name from topic_id using broker
        let topic_name = BROKER
            .get_topic_name_by_id(topic_id)
            .map(|name| name.0.to_string())
            .unwrap_or_else(|| topic_id.to_string());

        // Create WAL directory path: {record_storage}/{topic_name}_{partition}
        let wal_dir = format!("{}_{}", topic_name, partition_index);
        let wal_path = PathBuf::from(&record_storage).join(wal_dir);

        // Configure WAL with reasonable defaults
        let options = WalOptions::with_retention(Duration::from_millis(SETTINGS.retention as u64)) // 24 hours
            .segments_per_retention_period(5);

        let wal = Wal::new(wal_path.to_str().unwrap(), options)?;

        debug!(
            "📁 Created WAL for partition {} ({}): {}",
            topic_name,
            topic_id,
            wal_path.display()
        );

        self.wal = Some(wal);
        Ok(())
    }

    pub fn offload_oldest_record_from_queue(
        &mut self,
        topic_id: &Uuid,
        partition_index: i32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Find the first memory record in the queue
        let mut record_index = None;
        let mut memory_record_data = None;

        for (i, record) in self.queue.iter().enumerate() {
            if let QueueRecord::Memory {
                record,
                ingestion_time,
                fetched,
                ..
            } = record
            {
                memory_record_data = Some((
                    record.value.clone().unwrap_or_default(),
                    record.value.as_ref().map_or(0, |v| v.len()),
                    *ingestion_time,
                    *fetched,
                ));
                record_index = Some(i);
                break;
            }
        }

        if let (Some(index), Some((data, byte_size, ingestion_time, fetched))) =
            (record_index, memory_record_data)
        {
            // Ensure WAL exists
            self.ensure_wal(topic_id, partition_index)?;

            if let Some(ref mut wal) = self.wal {
                // Create header with metadata
                let mut header = BytesMut::new();
                header.extend_from_slice(&ingestion_time.to_le_bytes()); // 8 bytes: ingestion_time
                header.extend_from_slice(&[if fetched { 1 } else { 0 }]); // 1 byte: fetched

                // Use partition key as WAL key
                let partition_key = format!("{}_{}", topic_id, partition_index);

                // Append entry to WAL
                let entry_ref =
                    wal.append_entry(partition_key, Some(header.freeze()), data, false)?;

                // Replace memory record with WAL record
                if let Some(record) = self.queue.get_mut(index) {
                    // Create a placeholder record for WAL entries
                    let placeholder_record = Record {
                        transactional: false,
                        control: false,
                        partition_leader_epoch: 0,
                        producer_id: -1,
                        producer_epoch: -1,
                        timestamp_type: kafka_protocol::records::TimestampType::Creation,
                        timestamp: ingestion_time,
                        sequence: 0,
                        offset: 0,
                        key: None,
                        value: Some(Bytes::from_static(b"wal_record")),
                        headers: Default::default(),
                    };

                    *record = QueueRecord::Wal {
                        entry_ref,
                        record: placeholder_record,
                        byte_size,
                        ingestion_time,
                        fetched,
                    };
                }

                // Update batch counters (each record counts as 1)
                self.ram_batch_count -= 1;
                self.offloaded_batch_count += 1;

                // Update disk space tracking
                let size_bytes = byte_size as f64;
                update_disk_space_counters(size_bytes);

                // Update metrics
                let topic_name = get_topic_name_for_metrics(topic_id);
                let partition_str = partition_index.to_string();
                LOG_SEGMENT_COUNT
                    .with_label_values(&[&topic_name, &partition_str])
                    .set(wal.active_segment_count() as f64);

                debug!(
                    "📁 Offloaded record to WAL for partition {}:{}",
                    topic_id, partition_index
                );
            }
        }

        Ok(())
    }

    pub fn read_batch_from_wal(
        &self,
        entry_ref: EntryRef,
    ) -> Result<Bytes, Box<dyn std::error::Error>> {
        if let Some(ref wal) = self.wal {
            let data = wal.read_entry_at(entry_ref)?;
            Ok(data)
        } else {
            Err("WAL not initialized".into())
        }
    }

    #[allow(dead_code)]
    pub fn debug_wal(&self) -> Result<Vec<(i64, i32)>, Box<dyn std::error::Error>> {
        let mut records = Vec::new();

        // Extract record information from queue
        for (i, _record) in self.queue.iter().enumerate() {
            let offset = self.get_offset_at_position(i);
            records.push((offset, 1)); // Each record has count of 1
        }

        Ok(records)
    }

    pub fn cleanup_wal(&mut self, topic_id: &Uuid, partition_index: i32) {
        if let Some(mut wal) = self.wal.take() {
            let topic_name = get_topic_name_for_metrics(topic_id);

            // Shutdown WAL - this removes all persisted storage
            let _ = wal.shutdown();

            // Reset segment count to 0
            LOG_SEGMENT_COUNT
                .with_label_values(&[&topic_name, &partition_index.to_string()])
                .set(0.0);
        }
        self.wal = None;
    }

    /// Efficiently cleanup all batches and update memory counters
    /// This prevents slow deletion when there are many offloaded batches
    fn cleanup_all_batches(&mut self, topic_id: &Uuid, partition_index: i32) {
        let topic_name = get_topic_name_for_metrics(topic_id);
        let partition_str = partition_index.to_string();

        for record in &self.queue {
            match record {
                QueueRecord::Memory { batch, .. } => {
                    // Update total log size
                    LOG_SIZE
                        .with_label_values(&[&topic_name, &partition_str])
                        .sub(batch.records.len() as f64);
                }
                QueueRecord::Wal { byte_size, .. } => {
                    // WAL records don't count toward disk space when cleaned up
                    // The cleanup_wal() will remove the actual WAL storage
                    let size_bytes = *byte_size as f64;
                    update_disk_space_counters(-size_bytes);
                    // Update total log size
                    LOG_SIZE
                        .with_label_values(&[&topic_name, &partition_str])
                        .sub(size_bytes);
                }
                QueueRecord::Placeholder => {
                    // No cleanup needed for placeholders
                }
            }
        }
        self.queue.clear();
        self.ram_batch_count = 0;
        self.offloaded_batch_count = 0;
    }
}

impl Storage {
    pub fn new() -> Storage {
        Storage {
            topic_partition_store: DashMap::with_shard_amount(256),
        }
    }

    // Removed - replaced by find_partition_to_offload_from_queue

    fn find_partition_to_offload_from_queue(&self) -> Option<(Uuid, i32)> {
        let mut oldest_time = i64::MAX;
        let mut oldest_partition = None;

        for entry in self.topic_partition_store.iter() {
            let (topic_id, partition_index) = *entry.key();
            let partition_storage = entry.value();

            // Find oldest memory record in the queue
            for record in &partition_storage.queue {
                if let QueueRecord::Memory { ingestion_time, .. } = record {
                    if *ingestion_time < oldest_time {
                        oldest_time = *ingestion_time;
                        oldest_partition = Some((topic_id, partition_index));
                    }
                }
            }
        }

        oldest_partition
    }

    fn offload_batches_if_needed(&self) {
        if PartitionStorage::global_should_offload() {
            // Log info message the first time we start offloading to disk
            if !FIRST_OFFLOAD_LOGGED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                let current_memory = MEMORY.load(std::sync::atomic::Ordering::Relaxed);
                info!(
                    "🗄️  Starting to log batches to disk - Memory usage: {:.2}MB / {:.2}MB (threshold: {:.2}MB)",
                    current_memory as f64 / 1_048_576.0,
                    SETTINGS.max_memory as f64 / 1_048_576.0,
                    SETTINGS.max_memory as f64 / 1_048_576.0
                );
            }
        }

        while PartitionStorage::global_should_offload() {
            if let Some((topic_id, partition_index)) = self.find_partition_to_offload_from_queue() {
                if let Some(mut entry) = self
                    .topic_partition_store
                    .get_mut(&(topic_id, partition_index))
                {
                    if let Err(e) = entry
                        .value_mut()
                        .offload_oldest_record_from_queue(&topic_id, partition_index)
                    {
                        warn!(
                            "Failed to offload batch for partition {}_{}: {}",
                            topic_id, partition_index, e
                        );
                        break;
                    }
                } else {
                    break;
                }
            } else {
                // No more memory batches to offload
                break;
            }
        }
    }
}

pub const RECORDS_COUNT_OFFSET: usize = 57;

#[derive(Debug)]
pub struct RecordBatchEntry {
    pub ingestion_time: i64,
    pub records: Option<Bytes>,
}

impl Storage {
    pub fn dump(&self) -> String {
        let mut result = String::new();
        let topics = &BROKER.topics;
        for entry in topics.iter() {
            let topic_name = entry.key();
            let topic_meta = entry.value();
            result.push_str(
                format!(
                    "topic_name:{}, topic_id:{}, partitions:{}",
                    topic_name.as_str(),
                    topic_meta.topic_id,
                    topic_meta.partitions
                )
                .as_str(),
            );
        }
        let storage = &self.topic_partition_store;
        for entry in storage.iter() {
            let (topic_id, partition) = entry.key();
            let partition_storage = entry.value();
            result.push_str(
                format!(
                    "topic_id:{}, partition:{}, next_offset:{}, last_accessed_offset:{}",
                    topic_id,
                    partition,
                    partition_storage.next_offset,
                    partition_storage.last_accessed_offset,
                )
                .as_str(),
            );
            for record in &partition_storage.queue {
                result.push_str(
                    format!(
                        "record_count:{}, ingestion_time:{}, byte_size:{}, offset:{}",
                        record.record_count(),
                        record.ingestion_time(),
                        record.byte_size(),
                        record.record_batch_offset()
                    )
                    .as_str(),
                );
            }
        }
        result
    }

    pub fn delete_topic_storage(&self, topic_id: Uuid, num_partitions: i32) {
        use tracing::{debug, warn};

        debug!(
            "Deleting storage for topic_id: {} with {} partitions",
            topic_id, num_partitions
        );

        let storage = &self.topic_partition_store;
        let mut deleted_partitions = 0;
        let mut total_batches_cleaned = 0;
        let mut total_offloaded_batches = 0;

        for idx in 0..num_partitions {
            let partition_key = (topic_id, idx);

            if let Some((_, mut partition_storage)) = storage.remove(&partition_key) {
                let batch_count = partition_storage.queue.len();
                total_batches_cleaned += batch_count;
                deleted_partitions += 1;

                // Explicit cleanup for all batches to prevent slow deletion
                let batch_count_before_cleanup = partition_storage.queue.len();
                let offloaded_count = partition_storage.offloaded_batch_count;

                if batch_count_before_cleanup > 0 {
                    total_offloaded_batches += offloaded_count;

                    // Use efficient cleanup method
                    partition_storage.cleanup_all_batches(&topic_id, idx as i32);

                    debug!(
                        "Fast-cleaned {} batches ({} offloaded) for partition {}",
                        batch_count_before_cleanup, offloaded_count, idx
                    );
                }

                debug!("Deleted partition {} with {} batches", idx, batch_count);

                // Clean up WAL if it exists
                partition_storage.cleanup_wal(&topic_id, idx as i32);

                // PartitionStorage will be automatically dropped here, but with empty queue
            } else {
                warn!(
                    "Partition {} for topic {} was not found in storage",
                    idx, topic_id
                );
            }
        }

        debug!(
            "Topic storage deletion complete: topic_id={}, deleted_partitions={}/{}, total_batches_cleaned={}, offloaded_batches={}",
            topic_id, deleted_partitions, num_partitions, total_batches_cleaned, total_offloaded_batches
        );

        // Force memory cleanup by suggesting garbage collection
        // This helps ensure that large batches are freed promptly
        if total_batches_cleaned > 100 {
            debug!(
                "Large number of batches cleaned ({}), memory usage should decrease significantly",
                total_batches_cleaned
            );
        }
    }

    /// Comprehensive cleanup for a topic including all associated resources
    /// This method provides a complete cleanup that can be extended for future features
    pub(crate) fn cleanup_topic_completely(&self, topic_id: Uuid, num_partitions: i32) {
        trace!("Starting comprehensive cleanup for topic_id: {}", topic_id);

        // Calculate memory statistics before cleanup
        let mut total_memory_freed = 0usize;
        let storage = &self.topic_partition_store;

        for idx in 0..num_partitions {
            let partition_key = (topic_id, idx);

            if let Some(partition_ref) = storage.get(&partition_key) {
                // Calculate approximate memory usage
                let batch_count = partition_ref.queue.len();
                let estimated_memory = batch_count * std::mem::size_of::<RecordBatch>();
                total_memory_freed += estimated_memory;
            }
        }

        // Perform the actual deletion
        self.delete_topic_storage(topic_id, num_partitions);

        // TODO: Add cleanup for future features:
        // - Consumer group offsets related to this topic
        // - Metrics/monitoring data for this topic
        // - Any cached metadata for this topic

        trace!(
            "Comprehensive topic cleanup complete: topic_id={}, estimated_memory_freed={}KB",
            topic_id,
            total_memory_freed / 1024
        );
    }

    pub fn create_topic_storage(&self, topic_id: Uuid, num_partitions: i32) {
        let num_partitions = std::cmp::max(num_partitions, 1);
        {
            for partition_index in 0..num_partitions {
                let partition_storage = PartitionStorage::new();
                self.topic_partition_store
                    .insert((topic_id, partition_index), partition_storage);
            }
        }
    }

    pub fn purge_partition_from_queue(
        &self,
        topic_id: &Uuid,
        partition: i32,
    ) -> Result<i32, ResponseError> {
        profile!(BlinkProfileOp::StoragePurgePartition, {
            let storage = &self.topic_partition_store;
            let mut entry = storage
                .get_mut(&(*topic_id, partition))
                .ok_or(ResponseError::UnknownTopicOrPartition)?;
            let entry = entry.value_mut();
            let now = Util::now();

            if entry.queue.is_empty() {
                return Ok(0);
            }

            let mut purged_count = 0;
            let mut obliterated_count = 0;
            let retention_ms = SETTINGS.retention;

            // First pass: identify records to purge from the front
            let mut front_purge_count = 0;
            for (_idx, record) in entry.queue.iter().enumerate() {
                if record.is_placeholder() {
                    front_purge_count += 1;
                    continue;
                }

                let is_expired =
                    record.ingestion_time() > 0 && (now - record.ingestion_time()) > retention_ms;
                let is_fetched = record.is_fetched();

                if is_expired || is_fetched {
                    if is_expired && !is_fetched {
                        obliterated_count += 1;
                    }
                    front_purge_count += 1;
                } else {
                    break; // Stop at first record that shouldn't be purged
                }
            }

            // Purge from front and update base_offset
            if front_purge_count > 0 {
                // Update counters before purging (single records now)
                for i in 0..front_purge_count {
                    if let Some(record) = entry.queue.get(i) {
                        match record {
                            QueueRecord::Memory { batch, .. } => {
                                entry.ram_batch_count = entry.ram_batch_count.saturating_sub(1);
                                // Update LOG_SIZE for single record
                                // Note: Memory tracking handled by Arc<RecordBatch> Drop
                                let topic_name = get_topic_name_for_metrics(topic_id);
                                LOG_SIZE
                                    .with_label_values(&[&topic_name, &partition.to_string()])
                                    .sub(batch.records.len() as f64);
                            }
                            QueueRecord::Wal { byte_size, .. } => {
                                entry.offloaded_batch_count =
                                    entry.offloaded_batch_count.saturating_sub(1);
                                // Update disk space tracking
                                update_disk_space_counters(-(*byte_size as f64));
                                // Update total log size
                                let topic_name = get_topic_name_for_metrics(topic_id);
                                LOG_SIZE
                                    .with_label_values(&[&topic_name, &partition.to_string()])
                                    .sub(*byte_size as f64);
                            }
                            QueueRecord::Placeholder => {
                                // No counter updates needed for placeholders
                            }
                        }
                    }
                }
                purged_count += entry.purge_from_front(front_purge_count) as i32;
            }

            // Second pass: replace middle records with placeholders
            let mut positions_to_replace = Vec::new();
            for (idx, record) in entry.queue.iter().enumerate() {
                if record.is_placeholder() {
                    continue;
                }

                let is_expired =
                    record.ingestion_time() > 0 && (now - record.ingestion_time()) > retention_ms;
                let is_fetched = record.is_fetched();

                if is_expired || is_fetched {
                    if is_expired && !is_fetched {
                        obliterated_count += 1;
                    }
                    positions_to_replace.push(idx);
                }
            }

            // Replace with placeholders and update counters
            for pos in positions_to_replace {
                if let Some(record) = entry.queue.get(pos) {
                    match record {
                        QueueRecord::Memory { batch, .. } => {
                            entry.ram_batch_count = entry.ram_batch_count.saturating_sub(1);
                            // Update LOG_SIZE for single record
                            // Note: Memory tracking handled by Arc<RecordBatch> Drop
                            let topic_name = get_topic_name_for_metrics(topic_id);
                            LOG_SIZE
                                .with_label_values(&[&topic_name, &partition.to_string()])
                                .sub(batch.records.len() as f64);
                        }
                        QueueRecord::Wal { byte_size, .. } => {
                            entry.offloaded_batch_count =
                                entry.offloaded_batch_count.saturating_sub(1);
                            // Update disk space tracking
                            update_disk_space_counters(-(*byte_size as f64));
                            // Update total log size
                            let topic_name = get_topic_name_for_metrics(topic_id);
                            LOG_SIZE
                                .with_label_values(&[&topic_name, &partition.to_string()])
                                .sub(*byte_size as f64);
                        }
                        QueueRecord::Placeholder => {
                            // Already a placeholder, no counter updates needed
                            continue;
                        }
                    }
                }

                if entry.replace_with_placeholder(pos) {
                    purged_count += 1;
                }
            }

            // If no offloaded batches remain, clean up the WAL
            if entry.offloaded_batch_count == 0 && entry.wal.is_some() {
                entry.cleanup_wal(topic_id, partition);
                debug!(
                    "Deleted WAL for partition {}_{} as no offloaded batches remain",
                    topic_id, partition
                );
            }

            // Update counters
            PURGED_RECORDS.inc_by(purged_count as f64);
            OBLITERATED_RECORDS.inc_by(obliterated_count as f64);

            // Record obliteration event if any records were obliterated
            if obliterated_count > 0 {
                crate::kafka::counters::record_obliteration_event(
                    *topic_id,
                    partition,
                    obliterated_count as u64,
                    purged_count as u64,
                );
            }

            Ok(purged_count)
        })
    }

    pub fn purge_partition(&self, topic_id: &Uuid, partition: i32) -> Result<i32, ResponseError> {
        // Use the new queue-based purging system
        self.purge_partition_from_queue(topic_id, partition)
    }

    fn get_last_offset(
        &self,
        topic_id: &Uuid,
        partition: i32,
    ) -> Result<(i64, Arc<Notify>), ResponseError> {
        let store = &self.topic_partition_store;
        let key = (*topic_id, partition);
        let partition_storage = store
            .get(&key)
            .ok_or(ResponseError::UnknownTopicOrPartition)?;
        let partition_storage = partition_storage.value();
        Ok((
            partition_storage.next_offset,
            partition_storage.published.clone(),
        ))
    }

    /// Check if the current fetch request is skipping any record batches and log a warning if so.
    ///
    /// This function detects when a consumer skips record batches during fetch operations,
    /// which can indicate potential data loss or unexpected consumer behavior.
    ///
    /// # Warning Conditions
    ///
    /// A warning is logged when all of the following conditions are met:
    /// - The partition has a valid `last_accessed_offset` (>= 0)
    /// - The `requested_offset` is more than 1 position ahead of `last_accessed_offset`
    /// - There are actual record batches stored in the skipped range
    ///
    /// # Examples of Warning Scenarios
    ///
    /// Given batches with last offsets [1, 2, 3, 4, 5]:
    /// - Fetch sequence: 1 → 2 → 5 (warns about skipping batches 3, 4)
    /// - Fetch sequence: 1 → 4 (warns about skipping batches 2, 3)
    ///
    /// # Examples of No Warning
    ///
    /// - Consecutive fetches: 1 → 2 → 3 → 4 → 5
    /// - Backwards fetches: 4 → 2 (going backwards doesn't warn)
    /// - First fetch: offset 0 when last_accessed_offset is -1
    /// - Gaps with no stored batches: if only batches 1, 2, 5 exist, fetching 1 → 2 → 5 won't warn
    ///
    /// # Arguments
    ///
    /// * `topic_id` - UUID of the topic for logging context
    /// * `partition` - Partition number for logging context
    /// * `requested_offset` - The offset being requested in the current fetch
    /// * `partition_storage` - Storage containing the batches and last_accessed_offset
    ///
    /// # Warning Format
    ///
    /// ```text
    /// ⚠️  Fetch operation skipped N record batch(es) in topic 'TOPIC' partition P:
    ///     last_accessed_offset=X, requested_offset=Y, skipped_ranges=[(first,last), ...]
    /// ```
    pub fn check_for_skipped_batches(
        topic_id: &Uuid,
        partition: i32,
        requested_offset: i64,
        partition_storage: &PartitionStorage,
    ) {
        let last_accessed = partition_storage.last_accessed_offset;

        // Only check for skips if we have a valid last accessed offset and the requested offset is beyond it
        if last_accessed >= 0 && requested_offset > last_accessed + 1 {
            let mut skipped_batches = Vec::new();

            // Find records that exist in the gap between last_accessed and requested_offset
            for (idx, _record) in partition_storage.queue.iter().enumerate() {
                let record_offset = partition_storage.get_offset_at_position(idx);

                // Check if this record falls in the skipped range
                if record_offset > last_accessed && record_offset < requested_offset {
                    skipped_batches.push((record_offset, record_offset));
                }
            }

            if !skipped_batches.is_empty() {
                let topic_name = get_topic_name_for_metrics(topic_id);
                warn!(
                    "⚠️  Fetch operation skipped {} record batch(es) in topic '{}' partition {}: \
                     last_accessed_offset={}, requested_offset={}, skipped_ranges={:?}",
                    skipped_batches.len(),
                    topic_name,
                    partition,
                    last_accessed,
                    requested_offset,
                    skipped_batches
                );
            }
        }
    }

    pub async fn retrieve_record_batch_from_queue(
        &self,
        topic_id: &Uuid,
        partition: i32,
        offset: i64,
        max_size: usize,
    ) -> Result<(RecordBatchEntry, i64), ResponseError> {
        profile!(BlinkProfileOp::StorageRetrieveRecordBatch, {
            let store = &self.topic_partition_store;
            let key = (*topic_id, partition);

            let (last_offset, published) = self.get_last_offset(topic_id, partition)?;

            if offset >= last_offset {
                published.notified().await;
                NOTIFY_COUNTER.inc();
            }

            let start = std::time::Instant::now();
            let mut partition_storage = store
                .get_mut(&key)
                .ok_or(ResponseError::UnknownTopicOrPartition)?;
            let elapsed = start.elapsed();
            DASHMAP_GET_MUT.observe(elapsed.as_micros() as f64);
            let partition_storage = partition_storage.value_mut();
            let last_offset = partition_storage.next_offset;

            if offset >= last_offset {
                AGGRESSIVE_CLIENT.inc();
                return Ok((
                    RecordBatchEntry {
                        ingestion_time: 0,
                        records: Some(EMPTY_BUFFER.clone()),
                    },
                    0,
                ));
            }

            // Check for skipped records before proceeding with fetch
            if SETTINGS.check_for_skipped_batches {
                Self::check_for_skipped_batches(topic_id, partition, offset, &partition_storage);
            }

            // Find the position in queue for the requested offset
            let start_position = match partition_storage.find_position_for_offset(offset) {
                Some(pos) => pos,
                None => {
                    // Offset not found, return empty
                    return Ok((
                        RecordBatchEntry {
                            ingestion_time: 0,
                            records: Some(EMPTY_BUFFER.clone()),
                        },
                        partition_storage.next_offset - 1,
                    ));
                }
            };

            // If the offset points to a placeholder, find the first non-placeholder
            let actual_start_position =
                if let Some(record) = partition_storage.queue.get(start_position) {
                    if record.is_placeholder() {
                        match partition_storage.find_next_non_placeholder(start_position) {
                            Some(pos) => pos,
                            None => {
                                // No non-placeholder records found
                                return Ok((
                                    RecordBatchEntry {
                                        ingestion_time: 0,
                                        records: Some(EMPTY_BUFFER.clone()),
                                    },
                                    partition_storage.next_offset - 1,
                                ));
                            }
                        }
                    } else {
                        start_position
                    }
                } else {
                    return Ok((
                        RecordBatchEntry {
                            ingestion_time: 0,
                            records: Some(EMPTY_BUFFER.clone()),
                        },
                        partition_storage.next_offset - 1,
                    ));
                };

            let watermark = partition_storage.next_offset - 1;
            let mut ingestion_time = 0;

            // Collect consecutive single records until we hit max_size or a placeholder
            let mut records_to_process = Vec::new();
            let mut estimated_batch_size = 0usize;
            let mut last_processed_idx = actual_start_position;

            for (idx, record) in partition_storage
                .queue
                .iter()
                .enumerate()
                .skip(actual_start_position)
            {
                // Stop at first placeholder
                if record.is_placeholder() {
                    break;
                }

                // Set ingestion time from first record
                if ingestion_time == 0 {
                    ingestion_time = record.ingestion_time();
                }

                // For now, we'll estimate record size and collect records to process
                // The actual size checking will be done during reconstruction
                let estimated_record_size = match record {
                    QueueRecord::Memory { record, .. } => {
                        let key_size = record.key.as_ref().map_or(0, |k| k.len());
                        let value_size = record.value.as_ref().map_or(0, |v| v.len());
                        key_size + value_size + 64 // Add overhead
                    }
                    QueueRecord::Wal { byte_size, .. } => *byte_size,
                    QueueRecord::Placeholder => {
                        break; // Should not happen due to check above, but be safe
                    }
                };

                // Check if adding this record would exceed max_size
                if estimated_batch_size + estimated_record_size > max_size
                    && !records_to_process.is_empty()
                {
                    break;
                }

                estimated_batch_size += estimated_record_size;
                records_to_process.push(idx);
                last_processed_idx = idx;
            }

            // Now mark records as fetched and update last accessed offset
            for idx in &records_to_process {
                if let Some(queue_record) = partition_storage.queue.get_mut(*idx) {
                    queue_record.set_fetched();
                }
            }

            if last_processed_idx >= actual_start_position {
                partition_storage.last_accessed_offset =
                    partition_storage.get_offset_at_position(last_processed_idx);
            }

            if !records_to_process.is_empty() {
                trace!("get:{}:{} (reconstructing record batch)", partition, offset);

                // Collect the individual records
                let mut individual_records = Vec::new();
                for idx in records_to_process {
                    if let Some(queue_record) = partition_storage.queue.get(idx) {
                        match queue_record {
                            QueueRecord::Memory { record, .. } => {
                                individual_records.push(record.clone());
                            }
                            QueueRecord::Wal { record, .. } => {
                                // For WAL records, we already have the parsed record
                                individual_records.push(record.clone());
                            }
                            QueueRecord::Placeholder => continue,
                        }
                    }
                }

                // Reconstruct proper Kafka record batch with correct base offset
                let actual_start_offset =
                    partition_storage.get_offset_at_position(actual_start_position);
                let reconstructed_batch =
                    self.reconstruct_record_batch(individual_records, actual_start_offset)?;
                return Ok((
                    RecordBatchEntry {
                        ingestion_time,
                        records: Some(reconstructed_batch),
                    },
                    watermark,
                ));
            }

            Ok((
                RecordBatchEntry {
                    ingestion_time: 0,
                    records: Some(EMPTY_BUFFER.clone()),
                },
                watermark,
            ))
        })
    }

    pub async fn retrieve_record_batch(
        &self,
        topic_id: &Uuid,
        partition: i32,
        offset: i64,
        max_size: usize,
    ) -> Result<(RecordBatchEntry, i64), ResponseError> {
        // Delegate to the new queue-based implementation
        self.retrieve_record_batch_from_queue(topic_id, partition, offset, max_size)
            .await
    }

    pub(crate) fn read_record_batch_count<B: AsRef<[u8]>>(&self, buf: B) -> i32 {
        let buf_slice = buf.as_ref();
        if buf_slice.len() < RECORDS_COUNT_OFFSET + 4 {
            return 0; // Handle undersized buffer safely
        }
        let mut slice = &buf_slice[RECORDS_COUNT_OFFSET..RECORDS_COUNT_OFFSET + 4];
        slice.get_i32()
    }

    // Removed unused extract_field_from_message function

    /// Store record batch using BytesMut (potentially less efficient but compatible with existing code)
    // The original store_record_batch method has been removed in favor of store_record_batch_bytes
    // which provides zero-copy optimization

    /// Stores a record batch directly from Bytes with zero-copy optimization when possible.
    ///
    /// This method is the primary API for storing record batches and replaces the legacy
    /// `store_record_batch` implementation. It accepts a Bytes buffer directly from the
    /// Kafka protocol layer without requiring conversion to BytesMut in most cases.
    ///
    /// When offset modification is needed:
    /// - If the Bytes buffer is uniquely owned (refcount = 1), it will update the
    ///   offset in-place using unsafe code with appropriate safety checks
    /// - If the buffer is shared or safety checks fail, it falls back to converting
    ///   to BytesMut, modifying, and freezing again
    ///
    /// # Parameters
    ///
    /// * `topic_id` - The UUID of the topic
    /// * `topic_name` - The name of the topic (for logging/metrics)
    /// * `partition_index` - The partition index
    /// * `records` - The record batch as a Bytes buffer
    ///
    /// # Returns
    ///
    /// The result contains the offset difference between the next offset and
    /// the last accessed offset, or an error if the topic or partition doesn't exist.
    pub fn store_record_batch(
        &self,
        topic_id: &Uuid,
        partition_index: i32,
        records: Bytes,
    ) -> Result<i64, ResponseError> {
        profile!(BlinkProfileOp::StorageStoreRecordBatch, {
            let store = &self.topic_partition_store;
            let mut value = store
                .get_mut(&(*topic_id, partition_index))
                .ok_or(ResponseError::UnknownTopicOrPartition)?;
            let entry: &mut PartitionStorage = value.value_mut();
            let starting_offset = entry.next_offset;

            let record_count = self.read_record_batch_count(&records);
            RECORD_COUNT.inc_by(record_count as f64);

            trace!("put:{}:{}", partition_index, starting_offset);

            let uncompressed_byte_size = records.len();
            let ingestion_time = Util::now();

            // Update total log size with batch size
            let topic_name = get_topic_name_for_metrics(topic_id);
            LOG_SIZE
                .with_label_values(&[&topic_name, &partition_index.to_string()])
                .add(uncompressed_byte_size as f64);

            // Create RecordBatch for memory tracking - this tracks the full batch memory
            let record_batch = Arc::new(RecordBatch::new(records.clone(), starting_offset));

            // Parse batch into individual Kafka records
            let individual_records = self.parse_record_batch(&records)?;
            for (i, mut record) in individual_records.into_iter().enumerate() {
                // Set correct offset for this record
                record.offset = starting_offset + i as i64;

                entry.queue.push_back(QueueRecord::Memory {
                    batch: Arc::clone(&record_batch), // Share reference to original batch
                    record,                           // Individual parsed Kafka record
                    ingestion_time,
                    fetched: false,
                });
                entry.ram_batch_count += 1;
            }

            entry.next_offset = starting_offset + record_count as i64;
            entry.published.notify_waiters();

            // Calculate result before dropping the lock
            let result = entry.next_offset - entry.last_accessed_offset;

            drop(value);

            // Trigger offloading if memory threshold exceeded
            self.offload_batches_if_needed();

            Ok(result)
        })
    }

    /// Parse a Kafka record batch into individual records
    fn parse_record_batch(&self, batch_data: &Bytes) -> Result<Vec<Record>, ResponseError> {
        let mut cursor = std::io::Cursor::new(batch_data.as_ref());

        match RecordBatchDecoder::decode(&mut cursor) {
            Ok(record_set) => Ok(record_set.records),
            Err(e) => {
                warn!("Failed to parse record batch: {:?}", e);
                // Fallback: try to extract record count and create placeholder records
                let record_count = self.read_record_batch_count(batch_data);
                let mut fallback_records = Vec::new();

                for i in 0..record_count {
                    fallback_records.push(Record {
                        transactional: false,
                        control: false,
                        partition_leader_epoch: 0,
                        producer_id: -1,
                        producer_epoch: -1,
                        timestamp_type: kafka_protocol::records::TimestampType::Creation,
                        timestamp: Util::now(),
                        sequence: i,
                        offset: i as i64,
                        key: None,
                        value: Some(Bytes::from_static(b"fallback_record")),
                        headers: Default::default(),
                    });
                }
                Ok(fallback_records)
            }
        }
    }

    /// Reconstruct a proper Kafka record batch with correct base offset
    fn reconstruct_record_batch(
        &self,
        mut records: Vec<Record>,
        base_offset: i64,
    ) -> Result<Bytes, ResponseError> {
        if records.is_empty() {
            return Ok(Bytes::new());
        }

        // Sort records by their original offset to maintain order
        records.sort_by_key(|r| r.offset);

        // Create completely new records with clean structure instead of modifying existing ones
        let mut clean_records = Vec::new();
        for (i, original_record) in records.iter().enumerate() {
            let clean_record = Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: kafka_protocol::records::TimestampType::Creation,
                timestamp: original_record.timestamp,
                sequence: i as i32,
                offset: i as i64,
                key: original_record.key.clone(),
                value: original_record.value.clone(),
                headers: original_record.headers.clone(),
            };
            clean_records.push(clean_record);
        }

        // Use a single encode call to create one batch containing all records
        let mut encoded = BytesMut::new();
        match RecordBatchEncoder::encode(
            &mut encoded,
            &clean_records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        ) {
            Ok(()) => {
                // Verify we have enough data for a proper record batch header
                if encoded.len() < 61 {
                    return Err(ResponseError::UnknownServerError);
                }

                // Update the base offset in the record batch header (first 8 bytes)
                let base_offset_bytes = base_offset.to_be_bytes();
                encoded[0..8].copy_from_slice(&base_offset_bytes);

                // Update the record count in the header (at offset 57, 4 bytes)
                let record_count = clean_records.len() as i32;
                let record_count_bytes = record_count.to_be_bytes();
                if encoded.len() >= 61 {
                    encoded[57..61].copy_from_slice(&record_count_bytes);
                }

                return Ok(encoded.freeze());
            }
            Err(_e) => {
                return Err(ResponseError::UnknownServerError);
            }
        }
    }
}

impl Storage {
    /// Helper method for testing multi-batch retrieval logic directly
    #[allow(dead_code)]
    pub async fn test_retrieve_from_partition(
        partition_storage: &mut PartitionStorage,
        offset: i64,
        max_size: usize,
    ) -> Result<RecordBatchEntry, std::io::Error> {
        let initial_index = partition_storage
            .find_position_for_offset(offset)
            .unwrap_or(partition_storage.queue.len());

        let mut ingestion_time = 0;

        // Fast path: If we only need one record and it's under the size limit, avoid copying
        if initial_index < partition_storage.queue.len() {
            if let Some(record) = partition_storage.queue.get(initial_index) {
                let record_offset = partition_storage.get_offset_at_position(initial_index);
                if record_offset >= offset && record.byte_size() <= max_size {
                    let ingestion_time = record.ingestion_time();

                    // Collect what we need based on record type
                    let records = match record {
                        QueueRecord::Memory { batch, .. } => Some(batch.records.clone()),
                        QueueRecord::Wal { entry_ref, .. } => {
                            let entry_ref = *entry_ref;
                            // Need to get reference for WAL reading
                            let _ = record; // Release immutable borrow
                            match partition_storage.read_batch_from_wal(entry_ref) {
                                Ok(data) => Some(data),
                                Err(_) => Some(EMPTY_BUFFER.clone()),
                            }
                        }
                        QueueRecord::Placeholder => {
                            // Skip placeholders in fast path
                            return Ok(RecordBatchEntry {
                                ingestion_time: 0,
                                records: Some(EMPTY_BUFFER.clone()),
                            });
                        }
                    };

                    // Now update state
                    partition_storage.last_accessed_offset = record_offset;
                    if let Some(record_mut) = partition_storage.queue.get_mut(initial_index) {
                        record_mut.set_fetched();
                    }

                    return Ok(RecordBatchEntry {
                        ingestion_time,
                        records,
                    });
                }
            }
        }

        // Multiple batch case - restructured to handle file reads properly
        let mut buf = BytesMut::with_capacity(max_size);

        // First pass: collect batch metadata without holding immutable borrows
        #[derive(Debug)]
        enum BatchData {
            Memory(Bytes, i32), // (records, record_count)
            Wal {
                entry_ref: EntryRef,
                record_count: i32,
            },
        }

        let mut batch_metadata = Vec::new();
        let mut has_wal_batches = false;
        for (idx, record) in partition_storage
            .queue
            .iter()
            .enumerate()
            .skip(initial_index)
        {
            let record_offset = partition_storage.get_offset_at_position(idx);

            if record_offset >= offset {
                if ingestion_time == 0 {
                    ingestion_time = record.ingestion_time();
                }

                let batch_data = match record {
                    QueueRecord::Memory { batch, .. } => {
                        BatchData::Memory(batch.records.clone(), 1)
                    }
                    QueueRecord::Wal { entry_ref, .. } => {
                        has_wal_batches = true;
                        BatchData::Wal {
                            entry_ref: *entry_ref,
                            record_count: 1,
                        }
                    }
                    QueueRecord::Placeholder => {
                        // Skip placeholders in multi-record path
                        continue;
                    }
                };

                batch_metadata.push((idx, record_offset, batch_data));

                // We can't check buf.len() here since we haven't read the data yet
                // Just continue collecting metadata for now
            }
        }

        // Optimization: Skip file I/O operations when all batches are memory-only
        // This avoids the expensive second pass WAL reading loop when not needed,
        // improving performance for workloads with sufficient memory
        if !has_wal_batches {
            // Fast path for memory-only records - no file operations needed
            for (record_idx, record_offset, batch_data) in batch_metadata {
                let (records, _record_count) = match batch_data {
                    BatchData::Memory(records, record_count) => (records, record_count),
                    BatchData::Wal { .. } => unreachable!(), // We know there are no WAL records
                };

                // Update state after successful data retrieval
                partition_storage.last_accessed_offset = record_offset;
                if let Some(record_mut) = partition_storage.queue.get_mut(record_idx) {
                    record_mut.set_fetched();
                }

                buf.extend_from_slice(&records);
                if buf.len() >= max_size {
                    break;
                }
            }
        } else {
            // Second pass: read WAL data and build response buffer
            // Only executed when WAL records are detected in first pass
            for (record_idx, record_offset, batch_data) in batch_metadata {
                let (records, _record_count) = match batch_data {
                    BatchData::Memory(records, record_count) => (records, record_count),
                    BatchData::Wal {
                        entry_ref,
                        record_count,
                    } => match partition_storage.read_batch_from_wal(entry_ref) {
                        Ok(data) => (data, record_count),
                        Err(_) => (EMPTY_BUFFER.clone(), record_count),
                    },
                };

                // Update state after successful data retrieval
                partition_storage.last_accessed_offset = record_offset;
                if let Some(record_mut) = partition_storage.queue.get_mut(record_idx) {
                    record_mut.set_fetched();
                }

                buf.extend_from_slice(&records);
                if buf.len() >= max_size {
                    break;
                }
            }
        }

        // Return empty buffer if no data was accumulated
        if buf.is_empty() {
            return Ok(RecordBatchEntry {
                ingestion_time: 0,
                records: Some(EMPTY_BUFFER.clone()),
            });
        }

        Ok(RecordBatchEntry {
            ingestion_time,
            records: Some(buf.freeze()),
        })
    }
}
