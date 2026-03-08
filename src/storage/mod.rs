//! Distributed storage primitives.

/// Generic axum handlers for distributed map operations.
pub mod handlers;
/// In-memory distributed map implementation.
pub mod memory;
/// Partition ownership and replication assignment.
pub mod partitioner;
/// HTTP protocol models and endpoint constants for storage APIs.
pub mod protocol;

#[cfg(test)]
mod tests;
