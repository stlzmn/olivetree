use super::protocol::{
    ENDPOINT_TASK_INTERNAL_GET, ENDPOINT_TASK_PARTITION_DUMP, ENDPOINT_TASK_REPLICATE,
    ForwardTaskRequest, GetTaskResponse, ReplicateTaskRequest, TaskPartitionDumpResponse,
};
use super::types::*;
use crate::membership::{service::MembershipService, types::NodeId};
use crate::storage::partitioner::PartitionManager;

use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;

/// Partitioned distributed queue with primary/backup ownership.
pub struct DistributedQueue {
    /// Local tasks grouped by partition id.
    pub local_tasks: Arc<DashMap<u32, DashMap<TaskId, TaskEntry>>>,
    /// Cluster membership service used for owner lookup.
    pub membership: Arc<MembershipService>,
    /// Partition ownership manager.
    pub partitioner: Arc<PartitionManager>,
    http_client: reqwest::Client,
}

impl DistributedQueue {
    /// Creates an empty queue bound to membership and partition manager.
    pub fn new(membership: Arc<MembershipService>, partitioner: Arc<PartitionManager>) -> Self {
        Self {
            local_tasks: Arc::new(DashMap::new()),
            membership,
            partitioner,
            http_client: reqwest::Client::new(),
        }
    }

    /// Submits a task and returns generated task id.
    ///
    /// If local node is not primary owner for the task partition, the task is
    /// forwarded to the primary.
    pub async fn submit(&self, task: Task) -> Result<TaskId> {
        let task_id = TaskId::new();
        let partition = self.partitioner.get_partition(&task_id.0);
        let owners = self.partitioner.get_owners(partition);

        if owners.is_empty() {
            tracing::warn!("No alive nodes, storing task locally");
            self.store_local(
                partition,
                task_id.clone(),
                TaskEntry {
                    task,
                    status: TaskStatus::Pending,
                    assigned_to: None,
                    created_at: now_ms(),
                    lease_expires: None,
                },
            );
            return Ok(task_id);
        }

        let primary = &owners[0];

        if primary == &self.membership.local_node.id {
            tracing::debug!(
                "Storing task {} in partition {} (I'm primary)",
                task_id.0,
                partition
            );
            self.store_as_primary(
                partition,
                task_id.clone(),
                TaskEntry {
                    task,
                    status: TaskStatus::Pending,
                    assigned_to: None,
                    created_at: now_ms(),
                    lease_expires: None,
                },
            )
            .await?;
        } else {
            tracing::debug!("Forwarding task {} to primary {:?}", task_id.0, primary);
            self.forward_task(primary, partition, task_id.clone(), task)
                .await?;
        }

        Ok(task_id)
    }

    /// Stores one task entry locally in a partition map.
    pub fn store_local(&self, partition: u32, task_id: TaskId, entry: TaskEntry) {
        let partition_map = self
            .local_tasks
            .entry(partition)
            .or_insert_with(|| DashMap::new());

        if !partition_map.contains_key(&task_id) {
            partition_map.insert(task_id, entry);
        }

        tracing::info!("Stored task in partition {}", partition);
    }

    /// Returns all local task entries for `partition`.
    pub fn dump_partition(&self, partition: u32) -> Vec<(TaskId, TaskEntry)> {
        let mut entries = Vec::new();
        if let Some(partition_map) = self.local_tasks.get(&partition) {
            for entry in partition_map.iter() {
                entries.push((entry.key().clone(), entry.value().clone()));
            }
        }
        entries
    }

    /// Returns `true` if local queue has at least one task in `partition`.
    pub fn has_partition(&self, partition: u32) -> bool {
        self.local_tasks
            .get(&partition)
            .map(|map| !map.is_empty())
            .unwrap_or(false)
    }

    /// Applies fetched partition entries locally if they are not present yet.
    pub fn apply_partition_entries(&self, partition: u32, entries: Vec<(TaskId, TaskEntry)>) {
        let partition_map = self
            .local_tasks
            .entry(partition)
            .or_insert_with(|| DashMap::new());
        for (task_id, entry) in entries {
            if !partition_map.contains_key(&task_id) {
                partition_map.insert(task_id, entry);
            }
        }
    }

    /// Returns local node id.
    pub fn local_node_id(&self) -> NodeId {
        self.membership.local_node.id.clone()
    }

    /// Returns number of partitions with local tasks.
    pub fn local_partition_count(&self) -> usize {
        self.local_tasks.len()
    }

    /// Returns number of local tasks across all partitions.
    pub fn local_task_count(&self) -> usize {
        self.local_tasks
            .iter()
            .map(|entry| entry.value().len())
            .sum()
    }

    /// Returns status counts as `(pending, running, completed, failed)`.
    pub fn local_task_status_counts(&self) -> (usize, usize, usize, usize) {
        let mut pending = 0;
        let mut running = 0;
        let mut completed = 0;
        let mut failed = 0;

        for partition in self.local_tasks.iter() {
            for entry in partition.value().iter() {
                match entry.status {
                    TaskStatus::Pending => pending += 1,
                    TaskStatus::Running => running += 1,
                    TaskStatus::Completed => completed += 1,
                    TaskStatus::Failed { .. } => failed += 1,
                }
            }
        }

        (pending, running, completed, failed)
    }

    /// Stores task on primary owner and replicates it to backup owners.
    pub async fn store_as_primary(
        &self,
        partition: u32,
        task_id: TaskId,
        entry: TaskEntry,
    ) -> Result<()> {
        self.store_local(partition, task_id.clone(), entry.clone());

        let owners = self.partitioner.get_owners(partition);
        for backup in owners.iter().skip(1) {
            self.replicate_task_to_backup(backup, partition, task_id.clone(), entry.clone())
                .await?;
        }

        Ok(())
    }

    async fn replicate_task_to_backup(
        &self,
        backup_node_id: &NodeId,
        partition: u32,
        task_id: TaskId,
        entry: TaskEntry,
    ) -> Result<()> {
        let node = self
            .membership
            .get_member(backup_node_id)
            .ok_or_else(|| anyhow::anyhow!("Backup node not found"))?;

        let payload = ReplicateTaskRequest {
            partition,
            task_id: task_id.clone(),
            entry,
        };

        let response = self
            .post_with_retry(
                format!("http://{}{}", node.http_addr, ENDPOINT_TASK_REPLICATE),
                &payload,
                std::time::Duration::from_millis(500),
                3,
            )
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Task replication failed: {}",
                response.status()
            ));
        }

        tracing::debug!(
            "Replicated task {} to backup {:?}",
            task_id.0,
            backup_node_id
        );

        Ok(())
    }

    async fn forward_task(
        &self,
        target: &NodeId,
        partition: u32,
        task_id: TaskId,
        task: Task,
    ) -> Result<()> {
        let node = self
            .membership
            .get_member(target)
            .ok_or_else(|| anyhow::anyhow!("Target node not found"))?;

        let payload = ForwardTaskRequest {
            partition,
            task_id,
            task,
        };

        let response = self
            .post_with_retry(
                format!("http://{}/internal/submit_task", node.http_addr),
                &payload,
                std::time::Duration::from_millis(500),
                3,
            )
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Forward failed: {}", response.status()));
        }

        Ok(())
    }

    /// Returns tasks that local workers may attempt to claim.
    pub fn my_pending_tasks(&self) -> Vec<(TaskId, TaskEntry)> {
        let my_partitions = self.get_my_primary_partitions();

        let mut tasks = Vec::new();

        for partition in my_partitions {
            if let Some(partition_map) = self.local_tasks.get(&partition) {
                for entry in partition_map.iter() {
                    let task_entry = entry.value();

                    let is_available = match task_entry.status {
                        TaskStatus::Pending => true,
                        TaskStatus::Running => {
                            if let Some(lease) = task_entry.lease_expires {
                                now_ms() > lease
                            } else {
                                false
                            }
                        }
                        _ => false,
                    };

                    if is_available {
                        tasks.push((entry.key().clone(), task_entry.clone()));
                    }
                }
            }
        }

        tasks
    }

    fn get_my_primary_partitions(&self) -> Vec<u32> {
        (0..self.partitioner.num_partitions)
            .filter(|&partition| {
                let owners = self.partitioner.get_owners(partition);
                !owners.is_empty() && owners[0] == self.membership.local_node.id
            })
            .collect()
    }

    /// Tries to atomically claim a pending task for local worker.
    pub fn try_claim_task(&self, task_id: &TaskId) -> Result<bool> {
        let partition = self.partitioner.get_partition(&task_id.0);

        if let Some(partition_map) = self.local_tasks.get(&partition) {
            if let Some(mut entry) = partition_map.get_mut(task_id) {
                if entry.status != TaskStatus::Pending {
                    return Ok(false);
                }

                entry.status = TaskStatus::Running;
                entry.assigned_to = Some(self.membership.local_node.id.clone());
                entry.lease_expires = Some(now_ms() + 30_000);

                tracing::debug!("Claimed task {}", task_id.0);
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Renews running task lease for another 30 seconds.
    pub fn renew_lease(&self, task_id: &TaskId) -> Result<()> {
        let partition = self.partitioner.get_partition(&task_id.0);

        if let Some(partition_map) = self.local_tasks.get(&partition) {
            if let Some(mut entry) = partition_map.get_mut(task_id) {
                if entry.status == TaskStatus::Running {
                    entry.lease_expires = Some(now_ms() + 30_000);
                    tracing::trace!("Renewed lease for task {}", task_id.0);
                    return Ok(());
                } else {
                    return Err(anyhow::anyhow!(
                        "Task not running (status: {:?})",
                        entry.status
                    ));
                }
            }
        }

        Err(anyhow::anyhow!("Task not found"))
    }

    /// Completes task with success or failure result.
    pub fn complete_task(&self, task_id: &TaskId, result: Result<()>) -> Result<()> {
        let partition = self.partitioner.get_partition(&task_id.0);

        if let Some(partition_map) = self.local_tasks.get(&partition) {
            if let Some(mut entry) = partition_map.get_mut(task_id) {
                match result {
                    Ok(_) => {
                        entry.status = TaskStatus::Completed;
                        tracing::info!("Task {} completed", task_id.0);
                    }
                    Err(e) => {
                        entry.status = TaskStatus::Failed {
                            error: e.to_string(),
                        };
                        tracing::error!("Task {} failed: {}", task_id.0, e);
                    }
                }
                entry.lease_expires = None;
                return Ok(());
            }
        }

        Err(anyhow::anyhow!("Task not found"))
    }

    /// Returns task entry by id, querying remote owners when needed.
    pub async fn get_task(&self, task_id: &TaskId) -> Option<TaskEntry> {
        match self.get_task_local(task_id) {
            Some(task_entry) => {
                tracing::debug!(
                    "Task: {:?} found locally at: {:?}",
                    task_id,
                    self.membership.local_node.id
                );
                Some(task_entry)
            }
            None => {
                let partition = self.partitioner.get_partition(&task_id.0);
                let owners = self.partitioner.get_owners(partition);
                if !owners.is_empty() && owners[0] != self.membership.local_node.id {
                    for owner in owners.iter() {
                        match self.fetch_remote(owner, &task_id.0).await {
                            Ok(Some(task)) => return Some(task),
                            Ok(None) => continue,
                            Err(e) => {
                                tracing::warn!("Failed to fetch remote task: {}", e);
                                continue;
                            }
                        }
                    }
                }
                None
            }
        }
    }

    async fn fetch_remote(&self, node_id: &NodeId, key: &str) -> Result<Option<TaskEntry>> {
        let node = self
            .membership
            .get_member(node_id)
            .ok_or_else(|| anyhow::anyhow!("Owner node not found: {:?}", node_id))?;

        let addr = node.http_addr;

        let url = format!(
            "http://{}{}/{}",
            addr,
            ENDPOINT_TASK_INTERNAL_GET,
            key.to_string()
        );

        let response = self
            .get_with_retry(url, std::time::Duration::from_millis(500), 3)
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("GET request failed {}", response.status()));
        }

        let get_response: GetTaskResponse = response.json().await?;

        Ok(get_response.task)
    }

    /// Fetches full partition snapshot from remote owner.
    pub async fn fetch_partition(
        &self,
        node_id: &NodeId,
        partition: u32,
    ) -> Result<Vec<(TaskId, TaskEntry)>> {
        let node = self
            .membership
            .get_member(node_id)
            .ok_or_else(|| anyhow::anyhow!("Owner node not found: {:?}", node_id))?;

        let url = format!(
            "http://{}{}/{}",
            node.http_addr, ENDPOINT_TASK_PARTITION_DUMP, partition
        );

        let response = self
            .get_with_retry(url, std::time::Duration::from_millis(500), 3)
            .await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Task partition dump failed {}",
                response.status()
            ));
        }

        let dump: TaskPartitionDumpResponse = response.json().await?;
        Ok(dump
            .entries
            .into_iter()
            .map(|entry| (entry.task_id, entry.entry))
            .collect())
    }

    /// Returns task entry from local storage only.
    pub fn get_task_local(&self, task_id: &TaskId) -> Option<TaskEntry> {
        let partition = self.partitioner.get_partition(&task_id.0);

        if let Some(partition_map) = self.local_tasks.get(&partition) {
            if let Some(entry) = partition_map.get(task_id) {
                return Some(entry.clone());
            }
        }

        None
    }

    async fn post_with_retry<T: serde::Serialize>(
        &self,
        url: String,
        payload: &T,
        timeout: std::time::Duration,
        attempts: usize,
    ) -> Result<reqwest::Response> {
        let mut delay_ms = 150u64;

        for attempt in 0..attempts {
            let response = self
                .http_client
                .post(url.clone())
                .json(payload)
                .timeout(timeout)
                .send()
                .await;

            match response {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    if attempt + 1 == attempts {
                        return Err(anyhow::anyhow!(e));
                    }
                    let jitter = rand::random::<u64>() % 50;
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms + jitter)).await;
                    delay_ms = (delay_ms * 2).min(1200);
                }
            }
        }

        Err(anyhow::anyhow!("Retry attempts exhausted"))
    }

    async fn get_with_retry(
        &self,
        url: String,
        timeout: std::time::Duration,
        attempts: usize,
    ) -> Result<reqwest::Response> {
        let mut delay_ms = 150u64;

        for attempt in 0..attempts {
            let response = self
                .http_client
                .get(url.clone())
                .timeout(timeout)
                .send()
                .await;

            match response {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    if attempt + 1 == attempts {
                        return Err(anyhow::anyhow!(e));
                    }
                    let jitter = rand::random::<u64>() % 50;
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms + jitter)).await;
                    delay_ms = (delay_ms * 2).min(1200);
                }
            }
        }

        Err(anyhow::anyhow!("Retry attempts exhausted"))
    }
}
