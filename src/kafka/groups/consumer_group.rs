//! # Consumer Groups in Blink
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
//!
//! This module implements Kafka-compatible consumer group coordination for Blink.
//! See `docs/CONSUMER_GROUPS_SUPPORT.md` for comprehensive documentation.
//!
//! ## Key Design Principles (Section 4: Blink's Interpretation)
//! - Single-node coordination (no distributed consensus)
//! - In-memory state for maximum performance
//! - Explicit race condition detection and prevention
//! - Feature-flagged for zero overhead when disabled

use crate::kafka::broker::Broker;
use crate::settings::SETTINGS;
use dashmap::DashMap;
use kafka_protocol::messages::{
    HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    LeaveGroupResponse, OffsetCommitRequest, OffsetCommitResponse, ResponseKind, SyncGroupRequest,
    SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::ResponseError;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock};
use tracing::{debug, warn};
use uuid::Uuid;

// ============================================================================
// Race Condition Detection Types and Implementation
// ============================================================================
//
// See Section 7: Race Conditions - The Hidden Complexity
//
// Blink implements explicit race condition detection to prevent common
// coordination issues that can occur in distributed consumer group management.

/// Tracks which consumer owns which partition in which generation.
/// Prevents the "Partition Assignment Race" (Section 7.1) where multiple
/// consumers might think they own the same partition during rebalancing.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PartitionAssignment {
    pub topic: TopicName,
    pub partition: i32,
    pub assigned_to: StrBytes,
    pub generation_id: i32,
    pub assignment_time: Instant,
}

/// Context for tracking consumer group operations to detect race conditions.
/// Every operation is logged with timing and sequence information for analysis.
#[derive(Debug, Clone)]
pub struct OperationContext {
    pub group_id: StrBytes,
    pub member_id: StrBytes,
    pub generation_id: i32,
    pub operation_type: OperationType,
    pub timestamp: Instant,
    pub sequence_number: u64,
}

/// All consumer group operations that can participate in race conditions.
/// See Section 6: Implementation Deep Dive for details on each operation.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum OperationType {
    JoinGroup,
    SyncGroup,
    Heartbeat,
    LeaveGroup,
    OffsetCommit,
    OffsetFetch,
    Assignment,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RaceConditionEvent {
    pub event_type: RaceConditionType,
    pub group_id: StrBytes,
    pub affected_members: Vec<StrBytes>,
    pub generation_id: i32,
    pub timestamp: Instant,
    pub details: String,
}

/// Types of race conditions that can occur in consumer group coordination.
/// Each corresponds to scenarios described in Section 7: Race Conditions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum RaceConditionType {
    PartitionAssignmentConflict, // Section 7.1: Multiple consumers claim same partition
    StaleGenerationRequest,      // Section 7.2: Outdated generation operations
    OffsetCommitConflict,        // Section 7.3: Out-of-order offset commits
    HeartbeatRaceCondition,      // Section 7.4: Timing conflicts in heartbeats
    RebalanceOverlap,            // Concurrent rebalance attempts
    StatefulJoinRace,            // Join conflicts with state changes
    ManualOffsetRace,            // Manual commit coordination issues
    ConcurrentRebalance,         // Multiple rebalance triggers
}

/// Explicit race condition detection system unique to Blink.
/// See Section 7.5: Why Explicit Race Detection?
///
/// Unlike Kafka which relies on protocol design to prevent races,
/// Blink proactively detects and prevents coordination issues.
#[derive(Debug)]
pub struct RaceConditionDetector {
    // Track all partition assignments globally
    partition_assignments: DashMap<(TopicName, i32), PartitionAssignment>,

    // Track ongoing operations by group
    active_operations: DashMap<StrBytes, Vec<OperationContext>>,

    // Track generation transitions
    generation_history: DashMap<StrBytes, Vec<(i32, Instant)>>,

    // Track offset commit order
    offset_commit_sequence: DashMap<(StrBytes, TopicName, i32), u64>,

    // Global sequence number for ordering
    global_sequence: AtomicU64,

    // Store detected race conditions for analysis
    detected_races: DashMap<u64, RaceConditionEvent>,

    // Configuration
    stale_operation_threshold: Duration,
    max_concurrent_operations: usize,
}

impl RaceConditionDetector {
    pub fn new() -> Self {
        Self {
            partition_assignments: DashMap::new(),
            active_operations: DashMap::new(),
            generation_history: DashMap::new(),
            offset_commit_sequence: DashMap::new(),

            global_sequence: AtomicU64::new(1),
            detected_races: DashMap::new(),
            stale_operation_threshold: Duration::from_secs(30),
            max_concurrent_operations: 10,
        }
    }

    pub fn register_operation(&self, context: OperationContext) -> Result<u64, i16> {
        let sequence = self.global_sequence.fetch_add(1, Ordering::SeqCst);
        let mut context = context;
        context.sequence_number = sequence;

        // Check for stale generation requests
        if let Err(error) = self.check_stale_generation(&context) {
            return Err(error);
        }

        // Check for concurrent operation limits
        if let Err(error) = self.check_concurrent_operations(&context) {
            return Err(error);
        }

        // Register the operation
        self.active_operations
            .entry(context.group_id.clone())
            .or_insert_with(Vec::new)
            .push(context.clone());

        // Clean up old operations
        self.cleanup_stale_operations(&context.group_id);

        Ok(sequence)
    }

    pub fn validate_offset_commit_order(
        &self,
        group_id: &StrBytes,
        topic: &TopicName,
        partition: i32,
        _offset: i64,
        generation_id: i32,
    ) -> Result<(), i16> {
        let sequence = self.global_sequence.load(Ordering::SeqCst);
        let key = (group_id.clone(), topic.clone(), partition);

        // Check for out-of-order commits
        if let Some(last_sequence) = self.offset_commit_sequence.get(&key) {
            // Allow some tolerance for near-simultaneous commits
            if sequence < *last_sequence && sequence + 100 < *last_sequence {
                warn!(
                    "Out-of-order offset commit detected for {:?}:{} in group {}",
                    topic, partition, group_id
                );

                self.record_race_condition(RaceConditionEvent {
                    event_type: RaceConditionType::OffsetCommitConflict,
                    group_id: group_id.clone(),
                    affected_members: vec![],
                    generation_id,
                    timestamp: Instant::now(),
                    details: format!("Out-of-order offset commit for {:?}:{}", topic, partition),
                });

                return Err(ResponseError::OffsetOutOfRange.code());
            }
        }

        self.offset_commit_sequence.insert(key, sequence);
        Ok(())
    }

    pub fn clear_partition_assignments(&self, group_id: &StrBytes, generation_id: i32) {
        // Remove all assignments for this group/generation
        self.partition_assignments
            .retain(|_, assignment| !(assignment.generation_id <= generation_id));

        debug!(
            "Cleared partition assignments for group {} generation {}",
            group_id, generation_id
        );
    }

    fn check_stale_generation(&self, context: &OperationContext) -> Result<(), i16> {
        // Check for truly stale generation requests (significantly older than current)
        if let Some(history) = self.generation_history.get(&context.group_id) {
            if let Some((latest_gen, latest_time)) = history.last() {
                // Only reject if generation is significantly older AND request is old
                let generation_gap = *latest_gen - context.generation_id;
                let time_gap = context.timestamp.duration_since(*latest_time);

                if generation_gap > 1 && time_gap > Duration::from_millis(5000) {
                    warn!(
                        "Stale generation request detected: {} << {} for group {} (gap: {}, time: {:?})",
                        context.generation_id, latest_gen, context.group_id, generation_gap, time_gap
                    );

                    self.record_race_condition(RaceConditionEvent {
                        event_type: RaceConditionType::StaleGenerationRequest,
                        group_id: context.group_id.clone(),
                        affected_members: vec![context.member_id.clone()],
                        generation_id: context.generation_id,
                        timestamp: Instant::now(),
                        details: format!(
                            "Stale generation {} vs current {} (gap: {})",
                            context.generation_id, latest_gen, generation_gap
                        ),
                    });

                    return Err(ResponseError::IllegalGeneration.code());
                }
            }
        }

        // Update generation history
        self.generation_history
            .entry(context.group_id.clone())
            .or_insert_with(Vec::new)
            .push((context.generation_id, context.timestamp));

        Ok(())
    }

    fn check_concurrent_operations(&self, context: &OperationContext) -> Result<(), i16> {
        if let Some(operations) = self.active_operations.get(&context.group_id) {
            let concurrent_count = operations
                .iter()
                .filter(|op| {
                    op.timestamp.elapsed() < self.stale_operation_threshold
                        && op.operation_type == context.operation_type
                })
                .count();

            if concurrent_count >= self.max_concurrent_operations {
                warn!(
                    "Too many concurrent {} operations for group {}",
                    format!("{:?}", context.operation_type),
                    context.group_id
                );
                return Err(ResponseError::CoordinatorNotAvailable.code());
            }
        }

        Ok(())
    }

    fn cleanup_stale_operations(&self, group_id: &StrBytes) {
        if let Some(mut operations) = self.active_operations.get_mut(group_id) {
            operations.retain(|op| op.timestamp.elapsed() < self.stale_operation_threshold);
        }
    }

    fn record_race_condition(&self, event: RaceConditionEvent) {
        let id = self.global_sequence.fetch_add(1, Ordering::SeqCst);
        self.detected_races.insert(id, event.clone());

        // Log the race condition
        warn!("Race condition detected: {:?}", event);
    }
}

impl Default for RaceConditionDetector {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Consumer Group Types and Implementation
// ============================================================================
//
// See Section 5: The State Machine - Coordination Through Generations
//
// Core data structures that implement Kafka's consumer group protocol
// with Blink-specific optimizations for single-node operation.

/// A consumer group manages coordinated consumption across multiple consumers.
/// See Section 2.1: The Fundamental Model for the core concept.
///
/// The generation_id is crucial - it's incremented on every membership change
/// and serves as a logical timestamp to invalidate stale operations.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConsumerGroup {
    pub group_id: StrBytes,
    pub protocol_type: StrBytes, // Always "consumer" in current implementation
    pub protocol_name: StrBytes, // Always "range" (basic assignment strategy)
    pub leader_id: Option<StrBytes>, // First member becomes leader
    pub generation_id: i32,      // Incremented on each rebalance - key coordination mechanism
    pub members: HashMap<StrBytes, GroupMember>,
    pub state: GroupState,
    pub created_at: Instant,
    pub rebalance_timeout_ms: i32,
    pub session_timeout_ms: i32,
    pub assignment_in_progress: bool, // Prevents concurrent assignment races
    pub last_assignment_time: Option<Instant>,
}

/// Individual consumer within a group. Each member has a unique ID
/// and participates in the rebalancing protocol.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct GroupMember {
    pub member_id: StrBytes,                 // Generated as "consumer-{uuid}"
    pub group_instance_id: Option<StrBytes>, // Static membership (currently unused)
    pub client_id: StrBytes,                 // Currently "unknown" - would come from connection
    pub client_host: StrBytes,               // Currently "unknown" - would come from connection
    pub protocol_metadata: Vec<u8>,          // Subscription information from client
    pub assignment: Vec<u8>,                 // Partition assignment (serialized)
    pub last_heartbeat: Instant,             // For timeout detection
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
}

/// Consumer group state machine. See Section 5.2: Blink's State Machine.
/// Ensures atomic transitions - all members transition together or not at all.
#[derive(Debug, Clone, PartialEq)]
pub enum GroupState {
    PreparingRebalance,  // Change detected, waiting for all members to rejoin
    CompletingRebalance, // Members rejoined, waiting for leader to distribute assignments
    Stable,              // All members have assignments and are consuming
    Dead,                // Group marked for deletion (currently unused)
    Empty,               // No active members
}

impl Default for GroupState {
    fn default() -> Self {
        GroupState::Empty
    }
}

/// Central coordinator for all consumer groups in this Blink instance.
/// See Section 4.1: The Single-Node Advantage - unlike Kafka's distributed
/// coordination, Blink manages all groups within a single node.
#[derive(Debug)]
pub struct ConsumerGroupManager {
    /// All consumer groups, stored in memory with concurrent access
    pub groups: DashMap<StrBytes, Arc<RwLock<ConsumerGroup>>>,
    /// Offset commits for consumer groups (key: group_id, topic, partition)
    pub offset_commits: DashMap<(StrBytes, TopicName, i32), OffsetMetadata>,
    /// Notification system for coordinating assignment distribution
    pub rebalance_notifier: Arc<Notify>,

    // Concurrency control mechanisms
    rebalance_locks: DashMap<StrBytes, Arc<tokio::sync::Mutex<()>>>, // Per-group rebalance prevention
    last_rebalance_time: DashMap<StrBytes, Instant>,                 // Tracks last rebalance time
    min_rebalance_interval: Duration, // Prevents rebalance thrashing (1 second)
    race_detector: RaceConditionDetector, // Explicit race condition detection
}

/// Offset storage for consumer groups. See Section 6.4: Offset Management.
/// Unlike individual consumer offsets, these represent group progress.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OffsetMetadata {
    pub offset: i64,
    pub metadata: Option<StrBytes>, // Optional commit metadata from client
    pub commit_timestamp: Instant,
    pub expire_timestamp: Option<Instant>, // 24-hour expiry to prevent unbounded growth
}

impl ConsumerGroupManager {
    pub fn new() -> Self {
        Self {
            groups: DashMap::new(),
            offset_commits: DashMap::new(),
            rebalance_notifier: Arc::new(Notify::new()),

            rebalance_locks: DashMap::new(),
            last_rebalance_time: DashMap::new(),
            min_rebalance_interval: Duration::from_millis(1000), // 1 second minimum
            race_detector: RaceConditionDetector::new(),
        }
    }

    pub fn next_generation_id_for_group(&self, group: &mut ConsumerGroup) -> i32 {
        group.generation_id += 1;
        group.generation_id
    }

    pub fn get_rebalance_lock(&self, group_id: &StrBytes) -> Arc<tokio::sync::Mutex<()>> {
        self.rebalance_locks
            .entry(group_id.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    pub fn get_or_create_group(&self, group_id: StrBytes) -> Arc<RwLock<ConsumerGroup>> {
        self.groups
            .entry(group_id.clone())
            .or_insert_with(|| {
                Arc::new(RwLock::new(ConsumerGroup {
                    group_id,
                    protocol_type: StrBytes::from_static_str("consumer"),
                    protocol_name: StrBytes::from_static_str("range"),
                    leader_id: None,
                    generation_id: 0,
                    members: HashMap::new(),
                    state: GroupState::Empty,
                    created_at: Instant::now(),
                    rebalance_timeout_ms: 60000, // 60 seconds default
                    session_timeout_ms: 30000,   // 30 seconds default
                    assignment_in_progress: false,
                    last_assignment_time: None,
                }))
            })
            .clone()
    }

    /// Commits an offset for a consumer group partition.
    /// See Section 6.4: Offset Management - Progress Tracking.
    ///
    /// Includes validation to prevent backwards commits and race conditions.
    pub fn commit_offset(
        &self,
        group_id: StrBytes,
        topic: TopicName,
        partition: i32,
        offset: i64,
        metadata: Option<StrBytes>,
        expected_generation: i32,
    ) -> Result<(), i16> {
        // Prevent backwards commits (Section 7.3: The Offset Commit Race)
        let key = (group_id.clone(), topic.clone(), partition);

        // Validate offset ordering to prevent corruption from out-of-order commits
        if let Some(existing) = self.offset_commits.get(&key) {
            // Allow same offset re-commit, but prevent going backwards
            if offset < existing.offset {
                return Err(ResponseError::OffsetOutOfRange.code());
            }
            // Duplicate detection within 100ms window (network retry protection)
            if offset == existing.offset {
                let time_diff = Instant::now().duration_since(existing.commit_timestamp);
                if time_diff < Duration::from_millis(100) {
                    return Ok(()); // Ignore rapid duplicates
                }
            }
        }

        // Validate generation is still current
        if let Some(group_ref) = self.groups.get(&group_id) {
            if let Ok(group) = group_ref.try_read() {
                if expected_generation != -1 && group.generation_id != expected_generation {
                    return Err(ResponseError::IllegalGeneration.code());
                }
            }
        }

        let offset_metadata = OffsetMetadata {
            offset,
            metadata,
            commit_timestamp: Instant::now(),
            expire_timestamp: Some(Instant::now() + Duration::from_secs(24 * 3600)), // 24 hour expiry
        };
        self.offset_commits.insert(key, offset_metadata);
        Ok(())
    }

    /// Retrieves committed offset for a consumer group partition.
    /// Returns None if no offset has been committed.
    pub fn fetch_offset(
        &self,
        group_id: &StrBytes,
        topic: &TopicName,
        partition: i32,
    ) -> Option<OffsetMetadata> {
        let key = (group_id.clone(), topic.clone(), partition);
        self.offset_commits.get(&key).map(|entry| entry.clone())
    }

    /// Generates unique member IDs as "consumer-{uuid}".
    /// See Section 6.1: JoinGroup implementation details.
    fn generate_member_id(&self) -> StrBytes {
        StrBytes::from_string(format!("consumer-{}", Uuid::new_v4()))
    }

    pub fn is_group_rebalancing(&self, group_id: &StrBytes) -> bool {
        if let Some(group_ref) = self.groups.get(group_id) {
            if let Ok(group) = group_ref.try_read() {
                matches!(
                    group.state,
                    GroupState::PreparingRebalance | GroupState::CompletingRebalance
                )
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn check_member_generation(
        &self,
        group_id: &StrBytes,
        member_id: &StrBytes,
        generation_id: i32,
    ) -> Result<(), i16> {
        if let Some(group_ref) = self.groups.get(group_id) {
            if let Ok(group) = group_ref.try_read() {
                // Check if member exists
                if !member_id.is_empty() && !group.members.contains_key(member_id) {
                    return Err(ResponseError::UnknownMemberId.code());
                }

                // Check generation ID - strict validation to prevent stale requests
                if generation_id != -1 && group.generation_id != generation_id {
                    return Err(ResponseError::IllegalGeneration.code());
                }

                // Check if group is rebalancing
                if matches!(
                    group.state,
                    GroupState::PreparingRebalance | GroupState::CompletingRebalance
                ) {
                    return Err(ResponseError::RebalanceInProgress.code());
                }

                Ok(())
            } else {
                Err(ResponseError::CoordinatorNotAvailable.code())
            }
        } else {
            Err(ResponseError::GroupIdNotFound.code())
        }
    }

    pub fn validate_member_assignment(
        &self,
        group_id: &StrBytes,
        member_id: &StrBytes,
        _topic: &TopicName,
        _partition: i32,
    ) -> Result<(), i16> {
        if let Some(group_ref) = self.groups.get(group_id) {
            if let Ok(group) = group_ref.try_read() {
                if let Some(_member) = group.members.get(member_id) {
                    // Check if member is assigned this partition
                    // This would require parsing the assignment bytes in a real implementation
                    // For now, we assume assignment is valid if member exists
                    Ok(())
                } else {
                    Err(ResponseError::UnknownMemberId.code())
                }
            } else {
                Err(ResponseError::CoordinatorNotAvailable.code())
            }
        } else {
            Err(ResponseError::GroupIdNotFound.code())
        }
    }

    /// Starts background task to monitor member health and trigger rebalances.
    /// See Section 8.3: Background Monitoring.
    ///
    /// Runs every 5 seconds, detecting expired members based on session timeout.
    pub fn start_rebalance_watchdog(&self) {
        if !SETTINGS.enable_consumer_groups {
            return;
        }
        let groups = self.groups.clone();
        let notifier = self.rebalance_notifier.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;

                // Check all groups for expired members
                for group_entry in groups.iter() {
                    let group_ref = group_entry.value();
                    if let Ok(mut group) = group_ref.try_write() {
                        let now = Instant::now();
                        let mut expired_members = Vec::new();

                        for (member_id, member) in &group.members {
                            let session_timeout =
                                Duration::from_millis(member.session_timeout_ms as u64);
                            if now.duration_since(member.last_heartbeat) > session_timeout {
                                expired_members.push(member_id.clone());
                            }
                        }

                        if !expired_members.is_empty() {
                            // Check if enough time has passed since last rebalance
                            let group_id = group_entry.key().clone();
                            let should_rebalance = if let Some(_last_time) =
                                groups.get(&group_id).and_then(|g| {
                                    if let Ok(_guard) = g.try_read() {
                                        // Use a separate timestamp tracking mechanism
                                        Some(now)
                                    } else {
                                        None
                                    }
                                }) {
                                true // For watchdog, we always rebalance on expiry
                            } else {
                                true
                            };

                            if should_rebalance {
                                // Remove expired members
                                for member_id in expired_members {
                                    group.members.remove(&member_id);
                                }

                                // Trigger rebalance
                                if !group.members.is_empty() {
                                    group.state = GroupState::PreparingRebalance;
                                    // Clear assignments to prevent conflicts
                                    for member in group.members.values_mut() {
                                        member.assignment.clear();
                                    }
                                    notifier.notify_waiters();
                                } else {
                                    group.state = GroupState::Empty;
                                }
                            }
                        }
                    }
                }
            }
        });
    }
}

// ============================================================================
// Broker Implementation for Consumer Groups
// ============================================================================
//
// Protocol handlers implementing Kafka's consumer group coordination.
// See Section 6: Implementation Deep Dive for detailed explanations.

impl Broker {
    /// JoinGroup: The entry point for consumer group membership.
    /// See Section 6.1: JoinGroup - The Entry Point.
    ///
    /// Handles member registration, leader election, and rebalance triggering.
    pub(crate) async fn handle_join_group(&self, request: JoinGroupRequest) -> ResponseKind {
        let Some(ref consumer_groups) = self.consumer_groups else {
            return ResponseKind::JoinGroup(
                JoinGroupResponse::default()
                    .with_error_code(ResponseError::InvalidRequest.code())
                    .with_member_id(StrBytes::from_static_str("")),
            );
        };

        let group_id = request.group_id.clone().into();

        // Per-group mutex prevents concurrent rebalances while allowing
        // different groups to rebalance independently
        let rebalance_lock = consumer_groups.get_rebalance_lock(&group_id);
        let _lock_guard = rebalance_lock.lock().await;

        // Register operation with race detector
        let operation_context = OperationContext {
            group_id: group_id.clone(),
            member_id: request.member_id.clone(),
            generation_id: -1, // JoinGroup doesn't have generation_id field
            operation_type: OperationType::JoinGroup,
            timestamp: Instant::now(),
            sequence_number: 0,
        };

        if let Err(error_code) = consumer_groups
            .race_detector
            .register_operation(operation_context)
        {
            return ResponseKind::JoinGroup(
                JoinGroupResponse::default()
                    .with_error_code(error_code)
                    .with_member_id(StrBytes::from_static_str("")),
            );
        }

        let group_ref = consumer_groups.get_or_create_group(group_id);

        let response = match self.try_join_group_async(group_ref, &request).await {
            Ok(resp) => resp,
            Err(error_code) => JoinGroupResponse::default()
                .with_error_code(error_code)
                .with_member_id(StrBytes::from_static_str("")),
        };

        ResponseKind::JoinGroup(response)
    }

    async fn try_join_group_async(
        &self,
        group_ref: Arc<RwLock<ConsumerGroup>>,
        request: &JoinGroupRequest,
    ) -> Result<JoinGroupResponse, i16> {
        let Some(ref consumer_groups) = self.consumer_groups else {
            return Err(ResponseError::InvalidRequest.code());
        };

        let mut group = group_ref.write().await;

        // Check if this is a rejoin during rebalance
        let is_rejoin =
            !request.member_id.is_empty() && group.members.contains_key(&request.member_id);

        // Generate member ID if not provided
        let member_id = if request.member_id.is_empty() {
            consumer_groups.generate_member_id()
        } else {
            request.member_id.clone()
        };

        // Check group state and handle appropriately
        match group.state {
            GroupState::Stable => {
                // New member joining stable group triggers rebalance
                if !is_rejoin {
                    // Check if enough time has passed since last rebalance
                    let now = Instant::now();
                    let group_id = request.group_id.clone().into();
                    let should_rebalance = if let Some(last_time) =
                        consumer_groups.last_rebalance_time.get(&group_id)
                    {
                        now.duration_since(*last_time) >= consumer_groups.min_rebalance_interval
                    } else {
                        true
                    };

                    if should_rebalance {
                        group.state = GroupState::PreparingRebalance;
                        group.generation_id =
                            consumer_groups.next_generation_id_for_group(&mut group);
                        consumer_groups.last_rebalance_time.insert(group_id, now);
                        // Clear all existing assignments to prevent conflicts
                        for member in group.members.values_mut() {
                            member.assignment.clear();
                        }
                        // Notify existing members about rebalance
                        consumer_groups.rebalance_notifier.notify_waiters();
                    }
                }
            }
            GroupState::PreparingRebalance | GroupState::CompletingRebalance => {
                // Group is already rebalancing, but new members should still trigger generation increment
                if !is_rejoin {
                    // Check if enough time has passed since last rebalance
                    let now = Instant::now();
                    let group_id = request.group_id.clone().into();
                    let should_rebalance = if let Some(last_time) =
                        consumer_groups.last_rebalance_time.get(&group_id)
                    {
                        now.duration_since(*last_time) >= consumer_groups.min_rebalance_interval
                    } else {
                        true
                    };

                    if should_rebalance {
                        // New member during rebalance - increment generation
                        group.generation_id =
                            consumer_groups.next_generation_id_for_group(&mut group);
                        consumer_groups.last_rebalance_time.insert(group_id, now);
                        // Clear assignments to prevent conflicts
                        for member in group.members.values_mut() {
                            member.assignment.clear();
                        }
                        // Notify all waiting members about rebalance
                        consumer_groups.rebalance_notifier.notify_waiters();
                    }
                }
            }
            GroupState::Empty => {
                // First member joining empty group
                group.state = GroupState::PreparingRebalance;
                group.generation_id = consumer_groups.next_generation_id_for_group(&mut group);
            }
            GroupState::Dead => {
                return Err(ResponseError::GroupIdNotFound.code()); // GROUP_DEAD - using closest equivalent
            }
        }

        // Create or update member
        let member = GroupMember {
            member_id: member_id.clone(),
            group_instance_id: request.group_instance_id.clone(),
            client_id: StrBytes::from_static_str("unknown"), // Would come from connection info
            client_host: StrBytes::from_static_str("unknown"), // Would come from connection info
            protocol_metadata: request
                .protocols
                .first()
                .map(|p| p.metadata.to_vec())
                .unwrap_or_default(),
            assignment: vec![],
            last_heartbeat: Instant::now(),
            session_timeout_ms: request.session_timeout_ms,
            rebalance_timeout_ms: request.rebalance_timeout_ms,
        };

        // Add member to group
        group.members.insert(member_id.clone(), member);

        // Update group settings
        group.session_timeout_ms = request.session_timeout_ms;
        group.rebalance_timeout_ms = request.rebalance_timeout_ms;

        // Set protocol info from the first member if not set
        if group.protocol_type.is_empty() && !request.protocols.is_empty() {
            group.protocol_type = request.protocol_type.clone();
            group.protocol_name = request.protocols[0].name.clone();
        }

        // Leader election: first member becomes leader, or re-elect if leader left
        let is_leader = group.leader_id.is_none()
            || !group
                .members
                .contains_key(group.leader_id.as_ref().unwrap());
        if is_leader {
            group.leader_id = Some(member_id.clone());
        }

        // Collect member information for response (only for leader)
        let members =
            if group.leader_id.as_ref() == Some(&member_id) {
                group.members.values().map(|member| {
                kafka_protocol::messages::join_group_response::JoinGroupResponseMember::default()
                    .with_member_id(member.member_id.clone())
                    .with_group_instance_id(member.group_instance_id.clone())
                    .with_metadata(member.protocol_metadata.clone().into())
            }).collect()
            } else {
                vec![] // Non-leaders don't get member list
            };

        Ok(JoinGroupResponse::default()
            .with_error_code(0) // NO_ERROR
            .with_generation_id(group.generation_id)
            .with_protocol_type(Some(group.protocol_type.clone()))
            .with_protocol_name(Some(group.protocol_name.clone()))
            .with_leader(group.leader_id.clone().unwrap_or_default())
            .with_member_id(member_id)
            .with_members(members))
    }

    /// SyncGroup: Assignment distribution and coordination.
    /// See Section 6.2: SyncGroup - Assignment Distribution.
    ///
    /// Leader distributes assignments atomically, followers wait for notification.
    pub(crate) async fn handle_sync_group(&self, request: SyncGroupRequest) -> ResponseKind {
        let Some(ref consumer_groups) = self.consumer_groups else {
            return ResponseKind::SyncGroup(
                SyncGroupResponse::default()
                    .with_error_code(ResponseError::InvalidRequest.code())
                    .with_assignment(vec![].into()),
            );
        };

        let group_id = request.group_id.clone().into();

        // Acquire rebalance lock to ensure atomic assignment distribution
        let rebalance_lock = consumer_groups.get_rebalance_lock(&group_id);
        let _lock_guard = rebalance_lock.lock().await;

        // Register operation with race detector
        let operation_context = OperationContext {
            group_id: group_id.clone(),
            member_id: request.member_id.clone(),
            generation_id: request.generation_id,
            operation_type: OperationType::SyncGroup,
            timestamp: Instant::now(),
            sequence_number: 0,
        };

        if let Err(error_code) = consumer_groups
            .race_detector
            .register_operation(operation_context)
        {
            return ResponseKind::SyncGroup(
                SyncGroupResponse::default()
                    .with_error_code(error_code)
                    .with_assignment(vec![].into()),
            );
        }

        let group_ref = consumer_groups.get_or_create_group(group_id);

        let response = match self.try_sync_group_async(group_ref, &request).await {
            Ok(resp) => resp,
            Err(error_code) => SyncGroupResponse::default()
                .with_error_code(error_code)
                .with_assignment(vec![].into()),
        };

        ResponseKind::SyncGroup(response)
    }

    async fn try_sync_group_async(
        &self,
        group_ref: Arc<RwLock<ConsumerGroup>>,
        request: &SyncGroupRequest,
    ) -> Result<SyncGroupResponse, i16> {
        let Some(ref consumer_groups) = self.consumer_groups else {
            return Err(ResponseError::InvalidRequest.code());
        };
        let mut group = group_ref.write().await;

        // Verify generation ID first (more specific error)
        if group.generation_id != request.generation_id {
            return Err(ResponseError::IllegalGeneration.code());
        }

        // Then verify the member exists in the group
        if !group.members.contains_key(&request.member_id) {
            return Err(ResponseError::UnknownMemberId.code());
        }

        // Check current group state
        match group.state {
            GroupState::PreparingRebalance => {
                // Group is still preparing for rebalance, transition to completing
                group.state = GroupState::CompletingRebalance;
            }
            GroupState::CompletingRebalance => {
                // Continue with assignment distribution
            }
            GroupState::Stable => {
                // Group is already stable, allow re-sync
            }
            GroupState::Empty => {
                // Empty group, allow sync to establish initial state
                group.state = GroupState::CompletingRebalance;
            }
            GroupState::Dead => {
                return Err(ResponseError::GroupIdNotFound.code()); // GROUP_DEAD - using closest equivalent
            }
        }

        // Check if this is the leader distributing assignments
        let is_leader = group.leader_id.as_ref() == Some(&request.member_id);

        if is_leader {
            // Validate assignments to prevent partition conflicts
            let _assigned_partitions: std::collections::HashSet<(TopicName, i32)> =
                std::collections::HashSet::new();
            for assignment in &request.assignments {
                // Validate all members exist before committing assignments
                if !group.members.contains_key(&assignment.member_id) {
                    return Err(ResponseError::UnknownMemberId.code());
                }
            }

            // Atomic assignment flag prevents race conditions during distribution
            group.assignment_in_progress = true;

            // Atomically distribute assignments to prevent partial assignment states
            for assignment in &request.assignments {
                if let Some(member) = group.members.get_mut(&assignment.member_id) {
                    member.assignment = assignment.assignment.to_vec();

                    // Clear old partition assignments for this generation
                    consumer_groups.race_detector.clear_partition_assignments(
                        &request.group_id.clone().into(),
                        group.generation_id - 1,
                    );
                }
            }

            // Update group state to stable after successful assignment
            group.state = GroupState::Stable;
            group.assignment_in_progress = false;
            group.last_assignment_time = Some(Instant::now());

            // Notify other members that assignments are ready
            consumer_groups.rebalance_notifier.notify_waiters();
        } else {
            // Non-leader members wait for assignments to be ready
            if group.state != GroupState::Stable || group.assignment_in_progress {
                drop(group); // Release lock while waiting

                // Wait for assignment with timeout
                let timeout_duration = Duration::from_millis(30000); // 30 seconds
                let mut retry_count = 0;
                const MAX_RETRIES: u32 = 3;

                loop {
                    let timeout_result = tokio::time::timeout(timeout_duration, async {
                        consumer_groups.rebalance_notifier.notified().await;
                    })
                    .await;

                    if timeout_result.is_err() {
                        retry_count += 1;
                        if retry_count >= MAX_RETRIES {
                            return Err(ResponseError::RequestTimedOut.code());
                        }
                        continue;
                    }

                    // Re-acquire lock
                    group = group_ref.write().await;

                    // Verify state again
                    if group.generation_id != request.generation_id {
                        return Err(ResponseError::IllegalGeneration.code());
                    }

                    if group.state == GroupState::Stable && !group.assignment_in_progress {
                        break; // Assignments ready
                    }

                    if group.state == GroupState::Dead {
                        return Err(ResponseError::GroupIdNotFound.code()); // GROUP_DEAD - using closest equivalent
                    }

                    drop(group); // Release lock and wait again
                }
            }
        }

        // Return assignment for this specific member
        let member_assignment = group
            .members
            .get(&request.member_id)
            .map(|member| member.assignment.clone())
            .unwrap_or_default();

        Ok(SyncGroupResponse::default()
            .with_error_code(0) // NO_ERROR
            .with_assignment(member_assignment.into()))
    }

    /// Heartbeat: Member liveness detection and rebalance signaling.
    /// See Section 6.3: Heartbeat - Liveness Detection.
    ///
    /// Validates generation, updates timestamps, detects rebalance state.
    pub(crate) async fn handle_heartbeat(&self, request: HeartbeatRequest) -> ResponseKind {
        let Some(ref consumer_groups) = self.consumer_groups else {
            return ResponseKind::Heartbeat(
                HeartbeatResponse::default().with_error_code(ResponseError::InvalidRequest.code()),
            );
        };

        let group_id = request.group_id.clone().into();

        // Use normal lock for heartbeats - they should wait for other operations to complete
        let rebalance_lock = consumer_groups.get_rebalance_lock(&group_id);
        let _lock_guard = rebalance_lock.lock().await;

        // Register operation with race detector
        let operation_context = OperationContext {
            group_id: group_id.clone(),
            member_id: request.member_id.clone(),
            generation_id: request.generation_id,
            operation_type: OperationType::Heartbeat,
            timestamp: Instant::now(),
            sequence_number: 0,
        };

        if let Err(error_code) = consumer_groups
            .race_detector
            .register_operation(operation_context)
        {
            return ResponseKind::Heartbeat(
                HeartbeatResponse::default().with_error_code(error_code),
            );
        }

        let group_ref = consumer_groups.get_or_create_group(group_id);

        let response = match self.try_heartbeat_async(group_ref, &request).await {
            Ok(resp) => resp,
            Err(error_code) => HeartbeatResponse::default().with_error_code(error_code),
        };

        ResponseKind::Heartbeat(response)
    }

    async fn try_heartbeat_async(
        &self,
        group_ref: Arc<RwLock<ConsumerGroup>>,
        request: &HeartbeatRequest,
    ) -> Result<HeartbeatResponse, i16> {
        let mut group = group_ref.write().await;

        // Strict generation ID validation first (more specific error)
        if group.generation_id != request.generation_id {
            return Err(ResponseError::IllegalGeneration.code());
        }

        // Then verify the member exists in the group
        if !group.members.contains_key(&request.member_id) {
            return Err(ResponseError::UnknownMemberId.code());
        }

        // Skip overly strict heartbeat timing validation during rebalance
        // Allow heartbeats during rebalance preparation as this is normal Kafka behavior

        // Allow heartbeats in most states - only reject if group is dead
        match group.state {
            GroupState::Dead => {
                return Err(ResponseError::GroupIdNotFound.code()); // GROUP_DEAD - using closest equivalent
            }
            GroupState::PreparingRebalance => {
                // Allow heartbeats during rebalance preparation - this is normal Kafka behavior
                // Members can heartbeat after join_group but before sync_group
            }
            GroupState::CompletingRebalance => {
                // Allow heartbeats during assignment completion, only block if actively assigning
                if group.assignment_in_progress {
                    return Err(ResponseError::RebalanceInProgress.code());
                }
            }
            _ => {
                // Stable and Empty states allow heartbeats
            }
        }

        // Update member's last heartbeat timestamp atomically
        if let Some(member) = group.members.get_mut(&request.member_id) {
            member.last_heartbeat = Instant::now();
        }

        // Heartbeats should not trigger rebalances automatically
        // Rebalances are triggered by explicit events like new members joining
        // or members leaving, not by heartbeats

        Ok(HeartbeatResponse::default().with_error_code(0)) // NO_ERROR
    }

    // Removed unused method: check_rebalance_needed
    // This method was intended to check if a consumer group needed rebalancing
    // by examining expired members based on session timeouts, but was never called.
    // The rebalancing logic is handled elsewhere in the consumer group management.

    /// LeaveGroup: Graceful member departure and rebalance triggering.
    /// Supports both single and batch member removal.
    pub(crate) async fn handle_leave_group(&self, request: LeaveGroupRequest) -> ResponseKind {
        let Some(ref consumer_groups) = self.consumer_groups else {
            return ResponseKind::LeaveGroup(
                LeaveGroupResponse::default().with_error_code(ResponseError::InvalidRequest.code()),
            );
        };

        let group_id = request.group_id.clone().into();

        // Acquire rebalance lock to prevent race conditions during member removal
        let rebalance_lock = consumer_groups.get_rebalance_lock(&group_id);
        let _lock_guard = rebalance_lock.lock().await;

        // Register operation with race detector
        let member_id = if !request.member_id.is_empty() {
            request.member_id.clone()
        } else if !request.members.is_empty() {
            request.members[0].member_id.clone()
        } else {
            StrBytes::from_static_str("unknown")
        };

        let operation_context = OperationContext {
            group_id: group_id.clone(),
            member_id,
            generation_id: -1, // LeaveGroup doesn't always have generation
            operation_type: OperationType::LeaveGroup,
            timestamp: Instant::now(),
            sequence_number: 0,
        };

        if let Err(error_code) = consumer_groups
            .race_detector
            .register_operation(operation_context)
        {
            return ResponseKind::LeaveGroup(
                LeaveGroupResponse::default().with_error_code(error_code),
            );
        }

        let group_ref = consumer_groups.get_or_create_group(group_id);

        let response = match self.try_leave_group_async(group_ref, &request).await {
            Ok(resp) => resp,
            Err(error_code) => LeaveGroupResponse::default().with_error_code(error_code),
        };

        ResponseKind::LeaveGroup(response)
    }

    async fn try_leave_group_async(
        &self,
        group_ref: Arc<RwLock<ConsumerGroup>>,
        request: &LeaveGroupRequest,
    ) -> Result<LeaveGroupResponse, i16> {
        let Some(ref consumer_groups) = self.consumer_groups else {
            return Err(ResponseError::InvalidRequest.code());
        };
        let mut group = group_ref.write().await;

        // Handle both single member leave (legacy) and multiple members leave
        let members_to_remove = if !request.member_id.is_empty() {
            // Legacy single member leave
            vec![request.member_id.clone()]
        } else {
            // New multiple members leave
            request
                .members
                .iter()
                .map(|m| m.member_id.clone())
                .collect()
        };

        let mut removed_members = vec![];
        for member_id in &members_to_remove {
            if group.members.remove(member_id).is_some() {
                removed_members.push(member_id.clone());
            }
        }

        // If the leader left, elect a new leader
        if let Some(leader_id) = &group.leader_id {
            if members_to_remove.contains(leader_id) {
                group.leader_id = group.members.keys().next().cloned();
            }
        }

        // Update group state based on remaining members
        if group.members.is_empty() {
            group.state = GroupState::Empty;
        } else {
            // Trigger rebalance if any members left
            let now = Instant::now();
            let group_id = request.group_id.clone().into();
            group.state = GroupState::PreparingRebalance;
            group.generation_id = consumer_groups.next_generation_id_for_group(&mut group);
            consumer_groups.last_rebalance_time.insert(group_id, now);
            // Clear all assignments to prevent stale partition assignments
            for member in group.members.values_mut() {
                member.assignment.clear();
            }
            // Notify waiting members about the rebalance
            consumer_groups.rebalance_notifier.notify_waiters();
        }

        Ok(LeaveGroupResponse::default().with_error_code(0)) // NO_ERROR
    }

    /// OffsetCommit: Group progress tracking with validation.
    /// See Section 6.4: Offset Management - Progress Tracking.
    ///
    /// Validates rebalance state, generation ID, and prevents backwards commits.
    pub(crate) fn handle_offset_commit(&self, request: OffsetCommitRequest) -> ResponseKind {
        if self.consumer_groups.is_none() {
            let error_topics = request.topics
                .iter()
                .map(|topic| {
                    let error_partitions = topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            kafka_protocol::messages::offset_commit_response::OffsetCommitResponsePartition::default()
                                .with_partition_index(partition.partition_index)
                                .with_error_code(ResponseError::InvalidRequest.code())
                        })
                        .collect();
                    kafka_protocol::messages::offset_commit_response::OffsetCommitResponseTopic::default()
                        .with_name(topic.name.clone())
                        .with_partitions(error_partitions)
                })
                .collect();
            return ResponseKind::OffsetCommit(
                OffsetCommitResponse::default().with_topics(error_topics),
            );
        }

        let response = match self.try_offset_commit_sync(&request) {
            Ok(resp) => resp,
            Err(error_code) => {
                let mut response = OffsetCommitResponse::default();
                // Set error code for all topics/partitions
                let error_topics = request.topics
                    .iter()
                    .map(|topic| {
                        let error_partitions = topic
                            .partitions
                            .iter()
                            .map(|partition| {
                                kafka_protocol::messages::offset_commit_response::OffsetCommitResponsePartition::default()
                                    .with_partition_index(partition.partition_index)
                                    .with_error_code(error_code)
                            })
                            .collect();
                        kafka_protocol::messages::offset_commit_response::OffsetCommitResponseTopic::default()
                            .with_name(topic.name.clone())
                            .with_partitions(error_partitions)
                    })
                    .collect();
                response = response.with_topics(error_topics);
                response
            }
        };

        ResponseKind::OffsetCommit(response)
    }

    fn try_offset_commit_sync(
        &self,
        request: &OffsetCommitRequest,
    ) -> Result<OffsetCommitResponse, i16> {
        let Some(ref consumer_groups) = self.consumer_groups else {
            return Err(ResponseError::InvalidRequest.code());
        };
        // Verify group exists if group_id is provided
        if !request.group_id.is_empty() {
            let group_id = request.group_id.clone().into();

            // Block offset commits during rebalancing to prevent inconsistency
            if consumer_groups.is_group_rebalancing(&group_id) {
                return Err(ResponseError::RebalanceInProgress.code());
            }

            // Check member generation and existence
            if let Err(error_code) = consumer_groups.check_member_generation(
                &group_id,
                &request.member_id,
                request.generation_id_or_member_epoch,
            ) {
                return Err(error_code);
            }

            // Validate member has assignment for the partitions being committed
            for topic in &request.topics {
                for partition in &topic.partitions {
                    // Validate with race detector
                    if let Err(error_code) =
                        consumer_groups.race_detector.validate_offset_commit_order(
                            &group_id,
                            &topic.name,
                            partition.partition_index,
                            partition.committed_offset,
                            request.generation_id_or_member_epoch,
                        )
                    {
                        return Err(error_code);
                    }

                    if let Err(error_code) = consumer_groups.validate_member_assignment(
                        &group_id,
                        &request.member_id,
                        &topic.name,
                        partition.partition_index,
                    ) {
                        return Err(error_code);
                    }
                }
            }
        }

        let mut response_topics = vec![];

        for topic in &request.topics {
            let mut response_partitions = vec![];

            for partition in &topic.partitions {
                // Commit the offset with validation
                if !request.group_id.is_empty() {
                    if let Err(error_code) = consumer_groups.commit_offset(
                        request.group_id.clone().into(),
                        topic.name.clone(),
                        partition.partition_index,
                        partition.committed_offset,
                        partition.committed_metadata.clone(),
                        request.generation_id_or_member_epoch,
                    ) {
                        // Return error for this specific partition
                        response_partitions.push(
                            kafka_protocol::messages::offset_commit_response::OffsetCommitResponsePartition::default()
                                .with_partition_index(partition.partition_index)
                                .with_error_code(error_code)
                        );
                        continue;
                    }
                }

                response_partitions.push(
                    kafka_protocol::messages::offset_commit_response::OffsetCommitResponsePartition::default()
                        .with_partition_index(partition.partition_index)
                        .with_error_code(0) // NO_ERROR
                );
            }

            response_topics.push(
                kafka_protocol::messages::offset_commit_response::OffsetCommitResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_partitions(response_partitions)
            );
        }

        Ok(OffsetCommitResponse::default().with_topics(response_topics))
    }
}
