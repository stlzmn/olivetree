use axum::Json;
use axum::extract::{DefaultBodyLimit, Path};
use axum::http::StatusCode;
use axum::{
    Router,
    extract::Extension,
    routing::{get, post},
};
use olive_tree::executor::executor::TaskExecutor;
use olive_tree::executor::handlers::{
    handle_get_task_internal, handle_get_task_status, handle_internal_submit_task,
    handle_replicate_task, handle_submit_task, handle_task_partition_dump,
};
use olive_tree::executor::protocol::{
    ENDPOINT_INTERNAL_SUBMIT, ENDPOINT_SUBMIT_TASK, ENDPOINT_TASK_INTERNAL_GET,
    ENDPOINT_TASK_PARTITION_DUMP, ENDPOINT_TASK_REPLICATE, ENDPOINT_TASK_STATUS,
};
use olive_tree::executor::queue::DistributedQueue;
use olive_tree::executor::registry::TaskHandlerRegistry;
use olive_tree::executor::types::Task;
use olive_tree::ingestion::handlers::{handle_ingest_gutenberg, handle_ingest_status};
use olive_tree::ingestion::types::RawDocument;
use olive_tree::membership::service::MembershipService;
use olive_tree::search::handlers::{handle_create_book, handle_search};
use olive_tree::search::tokenizer::tokenize_text;
use olive_tree::search::types::BookMetadata;
use olive_tree::storage::handlers::*;
use olive_tree::storage::memory::DistributedMap;
use olive_tree::storage::partitioner::PartitionManager;
use olive_tree::storage::protocol::{
    ENDPOINT_FORWARD_PUT, ENDPOINT_GET, ENDPOINT_GET_INTERNAL, ENDPOINT_PARTITION_DUMP,
    ENDPOINT_PUT, ENDPOINT_REPLICATE, ForwardPutRequest, GetResponse, PutRequest, PutResponse,
    ReplicateRequest,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{collections::HashSet, hash::Hash, str::FromStr};
use sysinfo::System;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        // .with_max_level(tracing::Level::DEBUG)
        .with_max_level(tracing::Level::INFO)
        .init();

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} --bind <addr:port> [--seed <addr:port>]", args[0]);
        eprintln!("Example: {} -- bind 127.0.0.1:5000", args[0]);
        eprintln!(
            "Example: {} --bind 127.0.0.1:5001 --seed 127.0.0.1:5000",
            args[0]
        );

        std::process::exit(1);
    }

    let mut bind_addr: Option<SocketAddr> = None;
    let mut seed_nodes: Vec<SocketAddr> = vec![];

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--bind" => {
                bind_addr = Some(args[i + 1].parse()?);
                i += 2;
            }
            "--seed" => {
                seed_nodes.push(args[i + 1].parse()?);
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let bind_addr = bind_addr.expect("--bind is required");

    tracing::info!("Starting node on {}", bind_addr);
    if !seed_nodes.is_empty() {
        tracing::info!("Seed nodes: {:?}", seed_nodes);
    } else {
        tracing::info!("Starting as seed node (founder)");
    }

    let membership = MembershipService::new(bind_addr, seed_nodes).await?;
    tracing::info!("Node ID: {:?}", membership.local_node.id);

    let replication_factor = std::env::var("REPLICATION_FACTOR")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(2);
    let partitioner =
        PartitionManager::new_with_replication(membership.clone(), replication_factor);

    let books = Arc::new(DistributedMap::<String, BookMetadata>::new_with_base(
        membership.clone(),
        partitioner.clone(),
        "/books",
    ));

    let datalake = Arc::new(DistributedMap::<String, RawDocument>::new_with_base(
        membership.clone(),
        partitioner.clone(),
        "/datalake",
    ));

    let queue = Arc::new(DistributedQueue::new(
        membership.clone(),
        partitioner.clone(),
    ));

    let index_map = Arc::new(DistributedMap::<String, Vec<String>>::new_with_base(
        membership.clone(),
        partitioner.clone(),
        "/index",
    ));
    let task_registry = TaskHandlerRegistry::new();

    let index_map_clone = index_map.clone();
    let datalake_clone = datalake.clone();
    task_registry.register("index_document", move |task| {
        let index_map = index_map_clone.clone();
        let datalake = datalake_clone.clone();
        async move {
            let Task::Execute { payload, .. } = task;
            let payload: olive_tree::ingestion::types::IndexTaskPayload =
                serde_json::from_value(payload)?;

            let Some(raw_doc) = datalake.get(&payload.book_id).await else {
                tracing::warn!("Index task skipped; missing doc {}", payload.book_id);
                return Ok(());
            };

            let tokens = tokenize_text(&raw_doc.body);
            for token in tokens {
                let mut book_ids = index_map.get(&token).await.unwrap_or_default();

                if !book_ids.contains(&payload.book_id) {
                    book_ids.push(payload.book_id.clone());
                }

                index_map.put(token, book_ids).await?;
            }

            tracing::info!("Indexed book {}", payload.book_id);
            Ok(())
        }
    });

    let worker_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    tracing::info!(
        "Starting {} task workers (auto-detected CPU cores)",
        worker_count
    );
    let executor = TaskExecutor::new(queue.clone(), task_registry, worker_count);

    executor.start().await;

    let max_body_bytes = std::env::var("MAX_BODY_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20 * 1024 * 1024);

    let app = Router::new()
        .route("/health/routes", get(handle_routes))
        .route("/health/stats", get(handle_stats))
        .route("/ingest/:book_id", post(handle_ingest_gutenberg))
        .route("/ingest/status/:book_id", get(handle_ingest_status))
        .route("/search", get(handle_search))
        .route("/books", post(handle_create_book))
        .nest(
            "/books",
            Router::new()
                .route(ENDPOINT_PUT, post(handle_put_book))
                .route(&format!("{}/:key", ENDPOINT_GET), get(handle_get_book))
                .route(
                    &format!("{}/:key", ENDPOINT_GET_INTERNAL),
                    get(handle_get_internal_book),
                )
                .route(
                    &format!("{}/:id", ENDPOINT_PARTITION_DUMP),
                    get(handle_partition_dump_book),
                )
                .route(ENDPOINT_FORWARD_PUT, post(handle_forward_put_book))
                .route(ENDPOINT_REPLICATE, post(handle_replicate_book)),
        )
        .nest(
            "/datalake",
            Router::new()
                .route(ENDPOINT_PUT, post(handle_put_datalake))
                .route(&format!("{}/:key", ENDPOINT_GET), get(handle_get_datalake))
                .route(
                    &format!("{}/:key", ENDPOINT_GET_INTERNAL),
                    get(handle_get_internal_datalake),
                )
                .route(
                    &format!("{}/:id", ENDPOINT_PARTITION_DUMP),
                    get(handle_partition_dump_datalake),
                )
                .route(ENDPOINT_FORWARD_PUT, post(handle_forward_put_datalake))
                .route(ENDPOINT_REPLICATE, post(handle_replicate_datalake)),
        )
        .nest(
            "/index",
            Router::new()
                .route(ENDPOINT_PUT, post(handle_put_index))
                .route(&format!("{}/:key", ENDPOINT_GET), get(handle_get_index))
                .route(
                    &format!("{}/:key", ENDPOINT_GET_INTERNAL),
                    get(handle_get_internal_index),
                )
                .route(
                    &format!("{}/:id", ENDPOINT_PARTITION_DUMP),
                    get(handle_partition_dump_index),
                )
                .route(ENDPOINT_FORWARD_PUT, post(handle_forward_put_index))
                .route(ENDPOINT_REPLICATE, post(handle_replicate_index)),
        )
        .route(ENDPOINT_SUBMIT_TASK, post(handle_submit_task))
        .route(
            &format!("{}/:id", ENDPOINT_TASK_STATUS),
            get(handle_get_task_status),
        )
        .route(
            &format!("{}/:id", ENDPOINT_TASK_INTERNAL_GET),
            get(handle_get_task_internal),
        )
        .route(
            &format!("{}/:id", ENDPOINT_TASK_PARTITION_DUMP),
            get(handle_task_partition_dump),
        )
        .route(ENDPOINT_INTERNAL_SUBMIT, post(handle_internal_submit_task))
        .route(ENDPOINT_TASK_REPLICATE, post(handle_replicate_task))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .layer(Extension(membership.clone()))
        .layer(Extension(books.clone()))
        .layer(Extension(datalake.clone()))
        .layer(Extension(queue.clone()))
        .layer(Extension(index_map.clone()));

    let service_clone = membership.clone();
    tokio::spawn(async move {
        service_clone.start().await;
    });

    let books_sync = books.clone();
    let datalake_sync = datalake.clone();
    let index_sync = index_map.clone();
    let queue_sync = queue.clone();
    let partitioner_sync = partitioner.clone();
    let books_partitioner = partitioner_sync.clone();
    let datalake_partitioner = partitioner_sync.clone();
    let index_partitioner = partitioner_sync.clone();

    tokio::spawn(async move {
        sync_loop("books", books_sync, books_partitioner).await;
    });
    tokio::spawn(async move {
        sync_loop("datalake", datalake_sync, datalake_partitioner).await;
    });
    tokio::spawn(async move {
        sync_loop("index", index_sync, index_partitioner).await;
    });
    tokio::spawn(async move {
        sync_queue_loop(queue_sync, partitioner_sync).await;
    });

    let stats_service = membership.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            interval.tick().await;
            let alive = stats_service.get_alive_members();
            tracing::info!("Cluster stats: {} alive nodes", alive.len());
            for node in alive {
                tracing::info!(
                    "  - {:?} gossip={} http={} (inc={})",
                    node.id,
                    node.gossip_addr,
                    node.http_addr,
                    node.incarnation
                );
            }
        }
    });

    let http_port = bind_addr.port() + 1000;
    let http_addr = SocketAddr::new(bind_addr.ip(), http_port);

    tracing::info!("HTTP server listening on {}", http_addr);
    tracing::info!("Press Ctrl+C to shutdown");

    let listener = tokio::net::TcpListener::bind(http_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn sync_loop<K, V>(
    name: &'static str,
    map: Arc<DistributedMap<K, V>>,
    partitioner: Arc<PartitionManager>,
) where
    K: ToString + FromStr + Clone + Hash + Eq + Send + Sync + 'static,
    <K as FromStr>::Err: std::fmt::Display,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));

    loop {
        interval.tick().await;
        if let Err(e) = sync_partitions(name, &map, &partitioner).await {
            tracing::warn!("Resync loop for {} failed: {}", name, e);
        }
    }
}

async fn sync_partitions<K, V>(
    name: &'static str,
    map: &DistributedMap<K, V>,
    partitioner: &PartitionManager,
) -> anyhow::Result<()>
where
    K: ToString + FromStr + Clone + Hash + Eq + Send + Sync + 'static,
    <K as FromStr>::Err: std::fmt::Display,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let mut target_partitions = HashSet::new();
    for partition in partitioner.my_primary_partitions() {
        target_partitions.insert(partition);
    }
    for partition in partitioner.my_backup_partitions() {
        target_partitions.insert(partition);
    }

    for partition in target_partitions {
        let owners = partitioner.get_owners(partition);
        if owners.is_empty() {
            continue;
        }

        let local_id = map.local_node_id();
        let is_primary = owners[0] == local_id;

        let source_owner = if is_primary {
            if map.has_partition(partition) {
                continue;
            }
            owners.iter().skip(1).next()
        } else {
            Some(&owners[0])
        };

        let Some(source_owner) = source_owner else {
            continue;
        };

        match map.fetch_partition(source_owner, partition).await {
            Ok(entries) => {
                if !entries.is_empty() {
                    map.apply_partition_entries(partition, entries);
                    tracing::info!(
                        "Resynced {} partition {} from {:?}",
                        name,
                        partition,
                        source_owner
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Resync {} partition {} failed from {:?}: {}",
                    name,
                    partition,
                    source_owner,
                    e
                );
            }
        }
    }

    Ok(())
}

async fn sync_queue_loop(queue: Arc<DistributedQueue>, partitioner: Arc<PartitionManager>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));

    loop {
        interval.tick().await;
        if let Err(e) = sync_queue_partitions(&queue, &partitioner).await {
            tracing::warn!("Queue resync loop failed: {}", e);
        }
    }
}

async fn sync_queue_partitions(
    queue: &DistributedQueue,
    partitioner: &PartitionManager,
) -> anyhow::Result<()> {
    let mut target_partitions = HashSet::new();
    for partition in partitioner.my_primary_partitions() {
        target_partitions.insert(partition);
    }
    for partition in partitioner.my_backup_partitions() {
        target_partitions.insert(partition);
    }

    let local_id = queue.local_node_id();

    for partition in target_partitions {
        let owners = partitioner.get_owners(partition);
        if owners.is_empty() {
            continue;
        }

        let is_primary = owners[0] == local_id;
        let source_owner = if is_primary {
            if queue.has_partition(partition) {
                continue;
            }
            owners.iter().skip(1).next()
        } else {
            Some(&owners[0])
        };

        let Some(source_owner) = source_owner else {
            continue;
        };

        match queue.fetch_partition(source_owner, partition).await {
            Ok(entries) => {
                if !entries.is_empty() {
                    queue.apply_partition_entries(partition, entries);
                    tracing::info!(
                        "Resynced queue partition {} from {:?}",
                        partition,
                        source_owner
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Queue resync partition {} failed from {:?}: {}",
                    partition,
                    source_owner,
                    e
                );
            }
        }
    }

    Ok(())
}

#[derive(Serialize)]
struct RoutesResponse {
    routes: Vec<&'static str>,
}

#[derive(Serialize, Clone)]
struct NodeInfo {
    node_id: String,
    gossip_addr: String,
    http_addr: String,
}

#[derive(Serialize)]
struct NodeStatsResponse {
    node_id: String,
    gossip_addr: String,
    http_addr: String,
    alive_nodes: usize,
    nodes: Vec<NodeInfo>,
    books_partitions: usize,
    books_entries: usize,
    datalake_partitions: usize,
    datalake_entries: usize,
    index_partitions: usize,
    index_entries: usize,
    queue_partitions: usize,
    queue_tasks: usize,
    queue_pending: usize,
    queue_running: usize,
    queue_completed: usize,
    queue_failed: usize,
    cpu_usage: f32,
    mem_used_mb: u64,
    mem_total_mb: u64,
}

async fn handle_routes() -> Json<RoutesResponse> {
    Json(RoutesResponse {
        routes: vec![
            "/health/routes",
            "/health/stats",
            "/ingest/:book_id",
            "/ingest/status/:book_id",
            "/search",
            "/books",
            "/books/put",
            "/books/get/:key",
            "/books/internal/get/:key",
            "/books/forward_put",
            "/books/replicate",
            "/datalake/put",
            "/datalake/get/:key",
            "/datalake/internal/get/:key",
            "/datalake/forward_put",
            "/datalake/replicate",
            "/index/put",
            "/index/get/:key",
            "/index/internal/get/:key",
            "/index/forward_put",
            "/index/replicate",
            "/submit_task",
            "/task_status/:id",
            "/internal/task/:id",
            "/internal/submit_task",
            "/internal/task_partition/:id",
            "/task_replicate",
        ],
    })
}

async fn handle_stats(
    Extension(books): Extension<Arc<DistributedMap<String, BookMetadata>>>,
    Extension(datalake): Extension<Arc<DistributedMap<String, RawDocument>>>,
    Extension(index_map): Extension<Arc<DistributedMap<String, Vec<String>>>>,
    Extension(queue): Extension<Arc<DistributedQueue>>,
    Extension(membership): Extension<Arc<MembershipService>>,
) -> Json<NodeStatsResponse> {
    let alive_members = membership.get_alive_members();
    let nodes: Vec<NodeInfo> = alive_members
        .iter()
        .map(|n| NodeInfo {
            node_id: n.id.0.clone(),
            gossip_addr: n.gossip_addr.to_string(),
            http_addr: n.http_addr.to_string(),
        })
        .collect();
    let (pending, running, completed, failed) = queue.local_task_status_counts();
    let node = &membership.local_node;
    let mut sys = System::new_all();
    sys.refresh_cpu();
    sys.refresh_memory();
    let cpu_usage = sys.global_cpu_info().cpu_usage();

    let mem_total_mb = sys.total_memory() / (1024 * 1024);
    let mem_used_mb = sys.used_memory() / (1024 * 1024);

    Json(NodeStatsResponse {
        node_id: node.id.0.clone(),
        gossip_addr: node.gossip_addr.to_string(),
        http_addr: node.http_addr.to_string(),
        alive_nodes: nodes.len(),
        nodes,
        books_partitions: books.local_partition_count(),
        books_entries: books.local_entry_count(),
        datalake_partitions: datalake.local_partition_count(),
        datalake_entries: datalake.local_entry_count(),
        index_partitions: index_map.local_partition_count(),
        index_entries: index_map.local_entry_count(),
        queue_partitions: queue.local_partition_count(),
        queue_tasks: queue.local_task_count(),
        queue_pending: pending,
        queue_running: running,
        queue_completed: completed,
        queue_failed: failed,
        cpu_usage,
        mem_used_mb,
        mem_total_mb,
    })
}

async fn handle_get_internal_book(
    map: Extension<Arc<DistributedMap<String, BookMetadata>>>,
    json: Path<String>,
) -> (StatusCode, Json<GetResponse>) {
    handle_get_internal::<String, BookMetadata>(map, json).await
}

async fn handle_get_book(
    map: Extension<Arc<DistributedMap<String, BookMetadata>>>,
    json: Path<String>,
) -> (StatusCode, Json<GetResponse>) {
    handle_get::<String, BookMetadata>(map, json).await
}

async fn handle_forward_put_book(
    map: Extension<Arc<DistributedMap<String, BookMetadata>>>,
    json: Json<ForwardPutRequest>,
) -> (StatusCode, Json<PutResponse>) {
    handle_forward_put::<String, BookMetadata>(map, json).await
}

async fn handle_put_book(
    map: Extension<Arc<DistributedMap<String, BookMetadata>>>,
    json: Json<PutRequest>,
) -> (StatusCode, Json<PutResponse>) {
    handle_put::<String, BookMetadata>(map, json).await
}

async fn handle_replicate_book(
    map: Extension<Arc<DistributedMap<String, BookMetadata>>>,
    json: Json<ReplicateRequest>,
) -> (StatusCode, Json<PutResponse>) {
    handle_replicate::<String, BookMetadata>(map, json).await
}

async fn handle_partition_dump_book(
    map: Extension<Arc<DistributedMap<String, BookMetadata>>>,
    partition: Path<u32>,
) -> (
    StatusCode,
    Json<olive_tree::storage::protocol::PartitionDumpResponse>,
) {
    handle_partition_dump::<String, BookMetadata>(map, partition).await
}

async fn handle_get_internal_datalake(
    map: Extension<Arc<DistributedMap<String, RawDocument>>>,
    json: Path<String>,
) -> (StatusCode, Json<GetResponse>) {
    handle_get_internal::<String, RawDocument>(map, json).await
}

async fn handle_get_datalake(
    map: Extension<Arc<DistributedMap<String, RawDocument>>>,
    json: Path<String>,
) -> (StatusCode, Json<GetResponse>) {
    handle_get::<String, RawDocument>(map, json).await
}

async fn handle_forward_put_datalake(
    map: Extension<Arc<DistributedMap<String, RawDocument>>>,
    json: Json<ForwardPutRequest>,
) -> (StatusCode, Json<PutResponse>) {
    handle_forward_put::<String, RawDocument>(map, json).await
}

async fn handle_put_datalake(
    map: Extension<Arc<DistributedMap<String, RawDocument>>>,
    json: Json<PutRequest>,
) -> (StatusCode, Json<PutResponse>) {
    handle_put::<String, RawDocument>(map, json).await
}

async fn handle_replicate_datalake(
    map: Extension<Arc<DistributedMap<String, RawDocument>>>,
    json: Json<ReplicateRequest>,
) -> (StatusCode, Json<PutResponse>) {
    handle_replicate::<String, RawDocument>(map, json).await
}

async fn handle_partition_dump_datalake(
    map: Extension<Arc<DistributedMap<String, RawDocument>>>,
    partition: Path<u32>,
) -> (
    StatusCode,
    Json<olive_tree::storage::protocol::PartitionDumpResponse>,
) {
    handle_partition_dump::<String, RawDocument>(map, partition).await
}

async fn handle_get_internal_index(
    map: Extension<Arc<DistributedMap<String, Vec<String>>>>,
    json: Path<String>,
) -> (StatusCode, Json<GetResponse>) {
    handle_get_internal::<String, Vec<String>>(map, json).await
}

async fn handle_get_index(
    map: Extension<Arc<DistributedMap<String, Vec<String>>>>,
    json: Path<String>,
) -> (StatusCode, Json<GetResponse>) {
    handle_get::<String, Vec<String>>(map, json).await
}

async fn handle_forward_put_index(
    map: Extension<Arc<DistributedMap<String, Vec<String>>>>,
    json: Json<ForwardPutRequest>,
) -> (StatusCode, Json<PutResponse>) {
    handle_forward_put::<String, Vec<String>>(map, json).await
}

async fn handle_put_index(
    map: Extension<Arc<DistributedMap<String, Vec<String>>>>,
    json: Json<PutRequest>,
) -> (StatusCode, Json<PutResponse>) {
    handle_put::<String, Vec<String>>(map, json).await
}

async fn handle_replicate_index(
    map: Extension<Arc<DistributedMap<String, Vec<String>>>>,
    json: Json<ReplicateRequest>,
) -> (StatusCode, Json<PutResponse>) {
    handle_replicate::<String, Vec<String>>(map, json).await
}

async fn handle_partition_dump_index(
    map: Extension<Arc<DistributedMap<String, Vec<String>>>>,
    partition: Path<u32>,
) -> (
    StatusCode,
    Json<olive_tree::storage::protocol::PartitionDumpResponse>,
) {
    handle_partition_dump::<String, Vec<String>>(map, partition).await
}
