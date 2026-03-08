use super::engine::search;
use super::types::SearchResultItem;
use super::types::{BookMetadata, SearchResponse};
use crate::executor::queue::DistributedQueue;
use crate::executor::types::Task;
use crate::storage::memory::DistributedMap;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Query parameters accepted by `GET /search`.
#[derive(Deserialize)]
pub struct SearchParams {
    /// Search query text.
    pub q: String,
    /// Page size (default: `10`).
    pub limit: Option<usize>,
    /// Number of items to skip (default: `0`).
    pub offset: Option<usize>,
}

/// Payload for creating a new book metadata record.
#[derive(Deserialize)]
pub struct CreateBookRequest {
    /// Book title.
    pub title: String,
    /// Book author.
    pub author: String,
    /// Book language code or label.
    pub language: String,
    /// Publication/release year if known.
    pub year: Option<u32>,
}

/// Response returned after creating a book.
#[derive(Serialize)]
pub struct CreateBookResponse {
    /// Assigned book id.
    pub book_id: String,
}

/// Handler for creating a book and scheduling indexing task.
pub async fn handle_create_book(
    Extension(books_map): Extension<Arc<DistributedMap<String, BookMetadata>>>,
    Extension(queue): Extension<Arc<DistributedQueue>>,
    Json(req): Json<CreateBookRequest>,
) -> (StatusCode, Json<CreateBookResponse>) {
    let book_id = uuid::Uuid::new_v4().to_string();
    let book_meta = BookMetadata {
        book_id: book_id.clone(),
        title: req.title,
        author: req.author,
        language: req.language,
        year: req.year,
        word_count: 0,   // TODO: DO POPRAWY
        unique_words: 0, // TODO: Do POPRAWY
    };

    match books_map.put(book_id.clone(), book_meta.clone()).await {
        Ok(_) => {
            tracing::debug!("Successfully created book");
            let task = Task::Execute {
                handler: "index_document".to_string(),
                payload: serde_json::to_value(&crate::ingestion::types::IndexTaskPayload {
                    book_id: book_id.clone(),
                })
                .unwrap(),
            };
            if let Err(e) = queue.submit(task).await {
                tracing::error!("Failed to submit index task: {:?}", e);
            }
            (
                StatusCode::CREATED,
                Json(CreateBookResponse {
                    book_id: book_id.clone(),
                }),
            )
        }
        Err(e) => {
            tracing::debug!("Failed to create book: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CreateBookResponse { book_id }),
            )
        }
    }
}

/// Handler for `GET /search`.
pub async fn handle_search(
    Query(params): Query<SearchParams>,
    Extension(index_map): Extension<Arc<DistributedMap<String, Vec<String>>>>,
    Extension(book_map): Extension<Arc<DistributedMap<String, BookMetadata>>>,
) -> Json<SearchResponse> {
    let results: Vec<SearchResultItem> = search(&params.q, index_map, book_map)
        .await
        .into_iter()
        .map(|(meta, score)| SearchResultItem {
            book_id: meta.book_id.to_string(),
            title: meta.title,
            author: meta.author,
            score,
        })
        .collect();
    let limit = params.limit.unwrap_or(10);
    let offset = params.offset.unwrap_or(0);
    let total_count = results.len();
    let results: Vec<SearchResultItem> = results.into_iter().skip(offset).take(limit).collect();

    Json(SearchResponse {
        query: params.q,
        filters: HashMap::new(),
        total_count,
        count: results.len(),
        results,
    })
}
