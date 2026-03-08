use super::types::*;

use anyhow::Result;
use dashmap::DashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Type-erased async handler function used by the task executor.
pub type TaskHandlerFn =
    Arc<dyn Fn(Task) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

/// Registry mapping handler names to executable async functions.
pub struct TaskHandlerRegistry {
    handlers: DashMap<String, TaskHandlerFn>,
}

impl TaskHandlerRegistry {
    /// Creates an empty handler registry wrapped in `Arc`.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            handlers: DashMap::new(),
        })
    }

    /// Registers a new named task handler.
    pub fn register<F, Fut>(&self, handler_name: &str, handler: F)
    where
        F: Fn(Task) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let handler_fn: TaskHandlerFn = Arc::new(move |task: Task| {
            Box::pin(handler(task)) as Pin<Box<dyn Future<Output = Result<()>> + Send>>
        });

        self.handlers.insert(handler_name.to_string(), handler_fn);

        tracing::info!("Registered task handler: {}", handler_name);
    }

    /// Executes a task by dispatching to its registered handler.
    pub async fn execute(&self, task: &Task) -> Result<()> {
        match task {
            Task::Execute { handler, payload } => {
                if let Some(handler_fn) = self.handlers.get(handler) {
                    tracing::debug!(
                        "Executing task with handler '{}' (payload size: {} bytes)",
                        handler,
                        payload.to_string().len()
                    );

                    handler_fn.value()(task.clone()).await
                } else {
                    let error = format!("Unknown task handler: {}", handler);
                    tracing::error!("{}", error);
                    Err(anyhow::anyhow!(error))
                }
            }
        }
    }

    /// Returns all registered handler names.
    pub fn list_handlers(&self) -> Vec<String> {
        self.handlers
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Returns `true` when a handler with `handler_name` exists.
    pub fn has_handler(&self, handler_name: &str) -> bool {
        self.handlers.contains_key(handler_name)
    }

    /// Returns number of registered handlers.
    pub fn handler_count(&self) -> usize {
        self.handlers.len()
    }
}

impl Default for TaskHandlerRegistry {
    fn default() -> Self {
        Self {
            handlers: DashMap::new(),
        }
    }
}
