//! Distributed task queue and worker executor.

/// Worker runtime that executes queued tasks.
pub mod executor;
/// axum handlers for task submission, replication and status.
pub mod handlers;
/// HTTP protocol models and endpoint constants for task APIs.
pub mod protocol;
/// Distributed queue implementation with partition ownership.
pub mod queue;
/// Registry for named async task handlers.
pub mod registry;
/// Core task types used by queue, executor and handlers.
pub mod types;

#[cfg(test)]
mod tests;
