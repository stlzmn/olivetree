use super::types::*;
use crate::membership::types::NodeId;
use serde::{Deserialize, Serialize};

/// Public endpoint for submitting tasks.
pub const ENDPOINT_SUBMIT_TASK: &str = "/task/submit";
/// Internal endpoint for forwarding tasks to partition primary.
pub const ENDPOINT_INTERNAL_SUBMIT: &str = "/internal/submit_task";
/// Internal endpoint for local task fetch by id.
pub const ENDPOINT_TASK_INTERNAL_GET: &str = "/internal/get_task";
/// Public endpoint for task status checks.
pub const ENDPOINT_TASK_STATUS: &str = "/task/status";
/// Internal endpoint for task replication.
pub const ENDPOINT_TASK_REPLICATE: &str = "/internal/replicate_task";
/// Internal endpoint for task partition dumps used by resync.
pub const ENDPOINT_TASK_PARTITION_DUMP: &str = "/internal/task_partition";

/// Public task submission request.
#[derive(Debug, Serialize, Deserialize)]
pub struct SubmitTaskRequest {
    /// Task to enqueue.
    pub task: Task,
}

/// Response containing enqueued task id.
#[derive(Debug, Serialize, Deserialize)]
pub struct SubmitTaskResponse {
    /// Assigned task id.
    pub task_id: TaskId,
}

/// Internal task fetch response.
#[derive(Debug, Serialize, Deserialize)]
pub struct GetTaskResponse {
    /// Task entry if found.
    pub task: Option<TaskEntry>,
}

/// Payload used when forwarding a task to partition primary.
#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardTaskRequest {
    /// Target partition id.
    pub partition: u32,
    /// Task id assigned by submitter.
    pub task_id: TaskId,
    /// Task payload.
    pub task: Task,
}

/// Public task status response.
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskStatusResponse {
    /// Task id.
    pub task_id: TaskId,
    /// Current status.
    pub status: TaskStatus,
    /// Worker assigned to task, if any.
    pub assigned_to: Option<NodeId>,
    /// Creation timestamp in UNIX milliseconds.
    pub created_at: u64,
}

/// Payload used to replicate task entry to backup owners.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicateTaskRequest {
    /// Target partition id.
    pub partition: u32,
    /// Task id.
    pub task_id: TaskId,
    /// Task entry payload.
    pub entry: TaskEntry,
}

/// One entry in a partition dump response.
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskPartitionEntry {
    /// Task id.
    pub task_id: TaskId,
    /// Full task entry.
    pub entry: TaskEntry,
}

/// Response payload for dumping one task partition.
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskPartitionDumpResponse {
    /// Partition id.
    pub partition: u32,
    /// Entries stored for the partition.
    pub entries: Vec<TaskPartitionEntry>,
}
