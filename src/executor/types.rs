use crate::membership::types::NodeId;
use serde::{Deserialize, Serialize};

/// Stable identifier of a distributed task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TaskId(pub String);

impl TaskId {
    /// Creates a new random task identifier.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

/// Lifecycle state of a task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    /// Waiting for a worker to claim execution.
    Pending,
    /// Claimed by worker, lease active.
    Running,
    /// Finished successfully.
    Completed,
    /// Finished with error.
    Failed {
        /// Error message produced by handler execution.
        error: String,
    },
}

/// Executable task payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Task {
    /// Executes a named handler with a JSON payload.
    Execute {
        /// Registered handler name.
        handler: String,
        /// JSON payload consumed by handler.
        payload: serde_json::Value,
    },
}

/// Entry stored in the distributed task queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEntry {
    /// Task payload.
    pub task: Task,
    /// Current status.
    pub status: TaskStatus,
    /// Worker node currently assigned to the task.
    pub assigned_to: Option<NodeId>,
    /// Creation timestamp in UNIX milliseconds.
    pub created_at: u64,
    /// Lease expiration timestamp in UNIX milliseconds.
    pub lease_expires: Option<u64>,
}

/// Returns current UNIX timestamp in milliseconds.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
