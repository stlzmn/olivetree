//! Cluster membership based on UDP gossip.

/// Membership service implementation.
pub mod service;
/// Shared membership data types and gossip messages.
pub mod types;

#[cfg(test)]
mod tests;
