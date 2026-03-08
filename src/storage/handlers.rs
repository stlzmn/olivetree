use axum::{
    Json,
    extract::{Extension, Path},
    http::StatusCode,
};
use serde::{Serialize, de::DeserializeOwned};
use std::hash::Hash;
use std::str::FromStr;
use std::sync::Arc;

use super::memory::DistributedMap;
use super::protocol::{
    ForwardPutRequest, GetResponse, KeyValueJson, PartitionDumpResponse, PutRequest, PutResponse,
    ReplicateRequest,
};

/// Generic `PUT` handler for distributed maps.
pub async fn handle_put<K, V>(
    Extension(map): Extension<Arc<DistributedMap<K, V>>>,
    Json(req): Json<PutRequest>,
) -> (StatusCode, Json<PutResponse>)
where
    K: ToString + FromStr + Clone + Hash + Eq + Send + Sync + 'static,
    <K as FromStr>::Err: std::fmt::Display,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let key: K = match req.key.parse() {
        Ok(k) => k,
        Err(e) => {
            tracing::error!("Failed to parse key: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(PutResponse { success: false }),
            );
        }
    };

    let value: V = match serde_json::from_str(&req.value_json) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Failed to deserialize value: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(PutResponse { success: false }),
            );
        }
    };

    match map.put_with_op(key, value, req.op_id).await {
        Ok(_) => (StatusCode::OK, Json(PutResponse { success: true })),
        Err(e) => {
            tracing::error!("Failed to put: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PutResponse { success: false }),
            )
        }
    }
}

/// Generic `GET` handler that may query remote owners.
pub async fn handle_get<K, V>(
    Extension(map): Extension<Arc<DistributedMap<K, V>>>,
    Path(key_str): Path<String>,
) -> (StatusCode, Json<GetResponse>)
where
    K: ToString + FromStr + Clone + Hash + Eq + Send + Sync + 'static,
    <K as FromStr>::Err: std::fmt::Display,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let key: K = match key_str.parse() {
        Ok(k) => k,
        Err(e) => {
            tracing::error!("Failed to parse key: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(GetResponse { value_json: None }),
            );
        }
    };

    match map.get(&key).await {
        Some(value) => match serde_json::to_string(&value) {
            Ok(value_json) => (
                StatusCode::OK,
                Json(GetResponse {
                    value_json: Some(value_json),
                }),
            ),
            Err(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetResponse { value_json: None }),
            ),
        },
        None => (
            StatusCode::NOT_FOUND,
            Json(GetResponse { value_json: None }),
        ),
    }
}

/// Internal local-only `GET` handler used by owner-to-owner reads.
pub async fn handle_get_internal<K, V>(
    Extension(map): Extension<Arc<DistributedMap<K, V>>>,
    Path(key_str): Path<String>,
) -> (StatusCode, Json<GetResponse>)
where
    K: ToString + FromStr + Clone + Hash + Eq + Send + Sync + 'static,
    <K as FromStr>::Err: std::fmt::Display,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let k: K = match key_str.parse() {
        Ok(k) => k,
        Err(e) => {
            tracing::error!("Failed to parse key: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(GetResponse { value_json: None }),
            );
        }
    };

    match map.get_local(&k) {
        Some(value) => {
            if let Ok(value_json) = serde_json::to_string(&value) {
                (
                    StatusCode::OK,
                    Json(GetResponse {
                        value_json: Some(value_json),
                    }),
                )
            } else {
                tracing::error!("Failed to deserialize value");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(GetResponse { value_json: None }),
                )
            }
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(GetResponse { value_json: None }),
        ),
    }
}

/// Internal handler that stores data on primary owner and replicates.
pub async fn handle_forward_put<K, V>(
    Extension(map): Extension<Arc<DistributedMap<K, V>>>,
    Json(req): Json<ForwardPutRequest>,
) -> (StatusCode, Json<PutResponse>)
where
    K: ToString + FromStr + Clone + Hash + Eq + Send + Sync + 'static,
    <K as FromStr>::Err: std::fmt::Display,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let key: K = match req.key.parse() {
        Ok(k) => k,
        Err(e) => {
            tracing::error!("Failed to parse key: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(PutResponse { success: false }),
            );
        }
    };

    let value: V = match serde_json::from_str(&req.value_json) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Failed to deserialize value: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(PutResponse { success: false }),
            );
        }
    };

    match map
        .store_as_primary(req.partition, req.op_id, key, value)
        .await
    {
        Ok(_) => (StatusCode::OK, Json(PutResponse { success: true })),
        Err(e) => {
            tracing::error!("Failed to put local: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PutResponse { success: false }),
            )
        }
    }
}

/// Internal handler for backup replication writes.
pub async fn handle_replicate<K, V>(
    Extension(map): Extension<Arc<DistributedMap<K, V>>>,
    Json(req): Json<ReplicateRequest>,
) -> (StatusCode, Json<PutResponse>)
where
    K: ToString + FromStr + Clone + Hash + Eq + Send + Sync + 'static,
    <K as FromStr>::Err: std::fmt::Display,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let key: K = match req.key.parse() {
        Ok(k) => k,
        Err(e) => {
            tracing::error!("Failed to parse key: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(PutResponse { success: false }),
            );
        }
    };

    let value: V = match serde_json::from_str(&req.value_json) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Failed to deserialize value: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(PutResponse { success: false }),
            );
        }
    };

    match map.store_replica(req.partition, req.op_id, key, value) {
        Ok(_) => {
            tracing::info!("Stored replica for partition {}", req.partition);
            (StatusCode::OK, Json(PutResponse { success: true }))
        }
        Err(e) => {
            tracing::error!("Failed to store replica: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PutResponse { success: false }),
            )
        }
    }
}

/// Internal handler that returns all entries for one partition.
pub async fn handle_partition_dump<K, V>(
    Extension(map): Extension<Arc<DistributedMap<K, V>>>,
    Path(partition): Path<u32>,
) -> (StatusCode, Json<PartitionDumpResponse>)
where
    K: ToString + FromStr + Clone + Hash + Eq + Send + Sync + 'static,
    <K as FromStr>::Err: std::fmt::Display,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let entries = map
        .dump_partition(partition)
        .into_iter()
        .filter_map(|(key, value)| match serde_json::to_string(&value) {
            Ok(value_json) => Some(KeyValueJson {
                key: key.to_string(),
                value_json,
            }),
            Err(e) => {
                tracing::warn!("Failed to serialize partition entry: {}", e);
                None
            }
        })
        .collect();

    (
        StatusCode::OK,
        Json(PartitionDumpResponse { partition, entries }),
    )
}
