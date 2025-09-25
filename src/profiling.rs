//! Profiling module for Blink Kafka broker
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
//!
//! This module provides profiling instrumentation using the quantum-pulse crate.
//! It defines profiling points for the main produce and fetch call paths.
//!
//! The ProfileOp derive macro is always available, but the actual profiling
//! functionality is only enabled when the "profiled" feature is active.

use quantum_pulse::ProfileOp;

/// Profiling points for the Blink Kafka broker
///
/// This enum defines all interesting profiling points in the handle_produce
/// and handle_fetch call paths. Each variant represents a specific operation
/// that can be profiled using the quantum-pulse crate.
///
/// The derive macro is always available for zero compilation overhead,
/// but profiling is only active when the "profiled" feature is enabled.
#[derive(Debug, Clone, Copy, ProfileOp)]
#[allow(dead_code)]
pub enum BlinkProfileOp {
    // Produce call path profiling points
    #[category(name = "produce")]
    HandleProduce,
    ProduceValidation,
    ProduceConsumerGroupCheck,
    ProduceMemoryCheck,
    ProduceTopicResolution,
    ProducePartitionProcessing,
    ProduceStoreRecordBatch,
    ProduceResponseBuild,

    // Fetch call path profiling points
    HandleFetch,
    FetchValidation,
    FetchTopicResolution,
    FetchPartitionSetup,
    FetchRecordBatch,
    FetchStorageRetrieve,
    FetchBinarySearch,
    FetchDataCollection,
    FetchResponseBuild,
    FetchEnsurePartitions,

    // Storage layer profiling points
    StorageStoreRecordBatch,
    StorageRetrieveRecordBatch,
    StorageMemoryPressureCheck,
    StoragePurgePartition,
    StorageOffsetAssignment,
    StorageNotification,

    // Additional system profiling points
    MetricsUpdate,
    MemoryAllocation,
    ConsumerGroupRebalance,
}

/// Profile macro that conditionally compiles based on feature flag
///
/// When the "profiled" feature is enabled, this uses quantum-pulse's profiling.
/// When disabled, it's a zero-cost no-op that just executes the code block.
#[cfg(feature = "profiled")]
#[allow(unused_imports)]
pub use quantum_pulse::profile;

/// No-op profile macro when profiling is disabled
///
/// This provides the same interface as the quantum-pulse profile macro
/// but with zero runtime overhead when profiling is not needed.
#[cfg(not(feature = "profiled"))]
#[macro_export]
macro_rules! profile {
    ($op:expr, $code:block) => {
        $code
    };
}

#[cfg(not(feature = "profiled"))]
#[allow(unused_imports)]
pub use profile;

/// Async profile macro that conditionally compiles based on feature flag
///
/// When the "profiled" feature is enabled, this uses quantum-pulse's async profiling.
/// When disabled, it's a zero-cost no-op that just executes the async code block.
#[cfg(feature = "profiled")]
#[allow(unused_imports)]
pub use quantum_pulse::profile_async;

/// No-op async profile macro when profiling is disabled
///
/// This provides the same interface as the quantum-pulse profile_async macro
/// but with zero runtime overhead when profiling is not needed.
#[cfg(not(feature = "profiled"))]
#[macro_export]
macro_rules! profile_async {
    ($op:expr, $code:expr) => {
        $code
    };
}

#[cfg(not(feature = "profiled"))]
#[allow(unused_imports)]
pub use profile_async;
