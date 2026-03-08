//! `olive-tree` is a Hazelcast-inspired distributed systems playground.
//!
//! The crate is split into modules for:
//! - cluster membership and gossip,
//! - partitioned distributed storage with replication,
//! - distributed task queue and workers,
//! - document ingestion and simple full-text search.
//!
//! The runnable end-to-end demo lives in `examples/book_search_engine.rs`.

/// Distributed task queue, execution and handler registry.
pub mod executor;
/// Book/document ingestion models and handlers.
pub mod ingestion;
/// Cluster membership and gossip.
pub mod membership;
/// Search engine, tokenization and search HTTP handlers.
pub mod search;
/// Partitioning, distributed maps and storage protocol handlers.
pub mod storage;

pub use executor::executor::TaskExecutor;
pub use executor::queue::DistributedQueue;
pub use executor::registry::TaskHandlerRegistry;
pub use executor::types::{Task, TaskEntry, TaskId, TaskStatus};
pub use ingestion::types::{IndexTaskPayload, IngestResponse, IngestStatusResponse, RawDocument};
pub use membership::service::MembershipService;
pub use membership::types::{GossipMessage, Node, NodeId, NodeState};
pub use search::types::{BookMetadata, SearchResponse, SearchResultItem};
pub use storage::memory::DistributedMap;
pub use storage::partitioner::PartitionManager;
