use super::queue::DistributedQueue;
use super::registry::TaskHandlerRegistry;
use super::types::*;

use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;

/// Background worker pool that executes tasks from [`DistributedQueue`].
pub struct TaskExecutor {
    queue: Arc<DistributedQueue>,
    handlers: Arc<TaskHandlerRegistry>,
    worker_count: usize,
}

impl TaskExecutor {
    /// Creates a task executor with fixed worker count.
    pub fn new(
        queue: Arc<DistributedQueue>,
        handlers: Arc<TaskHandlerRegistry>,
        worker_count: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            queue,
            handlers,
            worker_count,
        })
    }

    /// Starts worker tasks in the background.
    pub async fn start(self: Arc<Self>) {
        tracing::info!("Starting {} task workers", self.worker_count);

        for worker_id in 0..self.worker_count {
            let executor = self.clone();
            tokio::spawn(async move {
                executor.worker_loop(worker_id).await;
            });
        }

        tracing::info!("Task executor started with {} workers", self.worker_count);
    }

    async fn worker_loop(&self, worker_id: usize) {
        tracing::info!("Worker {} started", worker_id);

        loop {
            let tasks = self.queue.my_pending_tasks();

            if tasks.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            tracing::trace!("Worker {} found {} available tasks", worker_id, tasks.len());

            let mut claimed = false;
            for (task_id, entry) in tasks {
                match self.queue.try_claim_task(&task_id) {
                    Ok(true) => {
                        tracing::info!(
                            "Worker {} claimed task {} (handler: {:?})",
                            worker_id,
                            task_id.0,
                            match &entry.task {
                                Task::Execute { handler, .. } => handler,
                            }
                        );

                        self.execute_with_lease(&task_id, entry.task).await;

                        claimed = true;
                        break;
                    }
                    Ok(false) => {
                        tracing::trace!("Task {} already claimed by another worker", task_id.0);
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to claim task {}: {}", task_id.0, e);
                        continue;
                    }
                }
            }

            if !claimed {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    async fn execute_with_lease(&self, task_id: &TaskId, task: Task) {
        let renewal_handle = self.spawn_lease_renewal(task_id);

        let result = self.execute_task(&task).await;

        renewal_handle.abort();

        match self.queue.complete_task(task_id, result) {
            Ok(_) => {
                tracing::debug!("Task {} marked as complete", task_id.0);
            }
            Err(e) => {
                tracing::error!("Failed to complete task {}: {}", task_id.0, e);
            }
        }
    }

    fn spawn_lease_renewal(&self, task_id: &TaskId) -> tokio::task::JoinHandle<()> {
        let queue = self.queue.clone();
        let task_id = task_id.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;

                match queue.renew_lease(&task_id) {
                    Ok(_) => {
                        tracing::trace!("Renewed lease for task {}", task_id.0);
                    }
                    Err(_) => {
                        tracing::trace!("Task {} no longer needs lease renewal", task_id.0);
                        break;
                    }
                }
            }
        })
    }

    async fn execute_task(&self, task: &Task) -> Result<()> {
        match task {
            Task::Execute { handler, .. } => {
                tracing::debug!("Executing task with handler: {}", handler);
                self.handlers.execute(task).await
            }
        }
    }
}
