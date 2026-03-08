use serde::{Deserialize, Serialize};

/// Internal endpoint for backup replication writes.
pub const ENDPOINT_REPLICATE: &str = "/replicate";
/// Internal endpoint used by non-primary owners to forward writes.
pub const ENDPOINT_FORWARD_PUT: &str = "/forward_put";
/// Public put endpoint.
pub const ENDPOINT_PUT: &str = "/put";
/// Public get endpoint.
pub const ENDPOINT_GET: &str = "/get";
/// Internal local-only get endpoint.
pub const ENDPOINT_GET_INTERNAL: &str = "/internal/get";
/// Internal endpoint used for partition resynchronization.
pub const ENDPOINT_PARTITION_DUMP: &str = "/internal/partition";

/// Payload for a replicated write to a backup owner.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicateRequest {
    /// Target partition id.
    pub partition: u32,
    /// Idempotency key for deduplication.
    pub op_id: String,
    /// Key encoded as string.
    pub key: String,
    /// JSON serialized value.
    pub value_json: String,
}

/// Payload for forwarding a write to the primary owner.
#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardPutRequest {
    /// Target partition id.
    pub partition: u32,
    /// Idempotency key for deduplication.
    pub op_id: String,
    /// Key encoded as string.
    pub key: String,
    /// JSON serialized value.
    pub value_json: String,
}

/// Public put request payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct PutRequest {
    /// Idempotency key for deduplication.
    pub op_id: String,
    /// Key encoded as string.
    pub key: String,
    /// JSON serialized value.
    pub value_json: String,
}

/// Response for get APIs.
#[derive(Debug, Serialize, Deserialize)]
pub struct GetResponse {
    /// Serialized value, if key exists.
    pub value_json: Option<String>,
}

/// Response for put/forward/replicate APIs.
#[derive(Debug, Serialize, Deserialize)]
pub struct PutResponse {
    /// `true` when operation succeeded.
    pub success: bool,
}

/// One key/value entry encoded as JSON strings.
#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValueJson {
    /// Key encoded as string.
    pub key: String,
    /// JSON serialized value.
    pub value_json: String,
}

/// Response payload for partition dump API.
#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionDumpResponse {
    /// Partition id being returned.
    pub partition: u32,
    /// All entries currently stored for that partition.
    pub entries: Vec<KeyValueJson>,
}
