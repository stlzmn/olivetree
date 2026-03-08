use super::protocol::*;
use super::queue::DistributedQueue;
use super::types::*;

use axum::{Extension, Json, extract::Path, http::StatusCode};
use std::sync::Arc;

pub async fn handle_submit_task(
    Extension(queue): Extension<Arc<DistributedQueue>>,
    Json(req): Json<SubmitTaskRequest>,
) -> (StatusCode, Json<SubmitTaskResponse>) {
    match queue.submit(req.task).await {
        Ok(task_id) => {
            tracing::info!("Task submitted successfully: {}", task_id.0);
            (StatusCode::OK, Json(SubmitTaskResponse { task_id }))
        }
        Err(e) => {
            tracing::error!("Failed to submit task: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(SubmitTaskResponse {
                    task_id: TaskId::new(), // Dummy ID for error response TODO: to be changed
                }),
            )
        }
    }
}

pub async fn handle_internal_submit_task(
    Extension(queue): Extension<Arc<DistributedQueue>>,
    Json(req): Json<ForwardTaskRequest>,
) -> (StatusCode, Json<SubmitTaskResponse>) {
    tracing::debug!(
        "Received forwarded task {} for partition {}",
        req.task_id.0,
        req.partition
    );

    if let Err(e) = queue
        .store_as_primary(
            req.partition,
            req.task_id.clone(),
            TaskEntry {
                task: req.task,
                status: TaskStatus::Pending,
                assigned_to: None,
                created_at: now_ms(),
                lease_expires: None,
            },
        )
        .await
    {
        tracing::error!("Failed to store forwarded task: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(SubmitTaskResponse {
                task_id: req.task_id,
            }),
        );
    }

    (
        StatusCode::OK,
        Json(SubmitTaskResponse {
            task_id: req.task_id,
        }),
    )
}

pub async fn handle_get_task_status_internal(
    Extension(queue): Extension<Arc<DistributedQueue>>,
    Path(task_id_str): Path<String>,
) -> (StatusCode, Json<Option<TaskStatusResponse>>) {
    let task_id = TaskId(task_id_str);

    match queue.get_task(&task_id).await {
        Some(entry) => (
            StatusCode::OK,
            Json(Some(TaskStatusResponse {
                task_id,
                status: entry.status,
                assigned_to: entry.assigned_to,
                created_at: entry.created_at,
            })),
        ),
        None => (StatusCode::NOT_FOUND, Json(None)),
    }
}

pub async fn handle_get_task_internal(
    Extension(queue): Extension<Arc<DistributedQueue>>,
    Path(task_id_str): Path<String>,
) -> (StatusCode, Json<GetTaskResponse>) {
    let task_id = TaskId(task_id_str);

    match queue.get_task_local(&task_id) {
        Some(entry) => (StatusCode::OK, Json(GetTaskResponse { task: Some(entry) })),
        None => (StatusCode::NOT_FOUND, Json(GetTaskResponse { task: None })),
    }
}

pub async fn handle_get_task_status(
    Extension(queue): Extension<Arc<DistributedQueue>>,
    Path(task_id_str): Path<String>,
) -> (StatusCode, Json<Option<TaskStatusResponse>>) {
    let task_id = TaskId(task_id_str);
    let partition = queue.partitioner.get_partition(&task_id.0);
    let owners = queue.partitioner.get_owners(partition);

    if !owners.is_empty() && owners[0] == queue.membership.local_node.id {
        match queue.get_task(&task_id).await {
            Some(entry) => {
                return (
                    StatusCode::OK,
                    Json(Some(TaskStatusResponse {
                        task_id,
                        status: entry.status,
                        assigned_to: entry.assigned_to,
                        created_at: entry.created_at,
                    })),
                );
            }
            None => {
                return (StatusCode::NOT_FOUND, Json(None));
            }
        }
    }

    match queue.get_task(&task_id).await {
        Some(entry) => {
            tracing::debug!("Task status query: {} -> {:?}", task_id.0, entry.status);
            (
                StatusCode::OK,
                Json(Some(TaskStatusResponse {
                    task_id,
                    status: entry.status,
                    assigned_to: entry.assigned_to,
                    created_at: entry.created_at,
                })),
            )
        }
        None => {
            tracing::debug!("Task not found: {}", task_id.0);
            (StatusCode::NOT_FOUND, Json(None))
        }
    }
}

pub async fn handle_replicate_task(
    Extension(queue): Extension<Arc<DistributedQueue>>,
    Json(req): Json<ReplicateTaskRequest>,
) -> StatusCode {
    queue.store_local(req.partition, req.task_id.clone(), req.entry);

    tracing::debug!(
        "Stored replicated task {} in partition {}",
        req.task_id.0,
        req.partition
    );

    StatusCode::OK
}

pub async fn handle_task_partition_dump(
    Extension(queue): Extension<Arc<DistributedQueue>>,
    Path(partition): Path<u32>,
) -> (StatusCode, Json<TaskPartitionDumpResponse>) {
    let entries = queue
        .dump_partition(partition)
        .into_iter()
        .map(|(task_id, entry)| TaskPartitionEntry { task_id, entry })
        .collect();

    (
        StatusCode::OK,
        Json(TaskPartitionDumpResponse { partition, entries }),
    )
}
