use super::types::{IndexTaskPayload, IngestResponse, IngestStatusResponse, RawDocument};
use crate::executor::queue::DistributedQueue;
use crate::executor::types::Task;
use crate::search::types::BookMetadata;
use crate::storage::memory::DistributedMap;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::{Extension, Json};
use regex::Regex;
use std::sync::Arc;

const START_MARKER: &str = "*** START OF THE PROJECT GUTENBERG EBOOK";
const END_MARKER: &str = "*** END OF THE PROJECT GUTENBERG EBOOK";

/// Downloads and ingests a Project Gutenberg book by id.
pub async fn handle_ingest_gutenberg(
    Path(book_id): Path<String>,
    Extension(datalake): Extension<Arc<DistributedMap<String, RawDocument>>>,
    Extension(books): Extension<Arc<DistributedMap<String, BookMetadata>>>,
    Extension(queue): Extension<Arc<DistributedQueue>>,
) -> (StatusCode, Json<IngestResponse>) {
    let source_url = format!(
        "https://www.gutenberg.org/cache/epub/{}/pg{}.txt",
        book_id, book_id
    );

    if datalake.get(&book_id).await.is_some() {
        return (
            StatusCode::OK,
            Json(IngestResponse {
                book_id,
                status: "already_exists".to_string(),
                source_url,
            }),
        );
    }

    let text = match fetch_gutenberg_text(&source_url).await {
        Ok(text) => text,
        Err(err) => {
            tracing::error!("Failed to download book {}: {}", book_id, err);
            return (
                StatusCode::BAD_GATEWAY,
                Json(IngestResponse {
                    book_id,
                    status: "download_failed".to_string(),
                    source_url,
                }),
            );
        }
    };

    let (header, body) = match split_gutenberg_text(&text) {
        Some(parts) => parts,
        None => {
            tracing::warn!("Failed to split book {} into header/body", book_id);
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(IngestResponse {
                    book_id,
                    status: "invalid_format".to_string(),
                    source_url,
                }),
            );
        }
    };

    let (word_count, unique_words) = compute_counts(&body);
    let metadata = parse_metadata(&header, &book_id, word_count, unique_words);

    let raw_doc = RawDocument {
        book_id: book_id.clone(),
        header,
        body,
        source_url: source_url.clone(),
    };

    if let Err(err) = datalake.put(book_id.clone(), raw_doc).await {
        tracing::error!("Failed to store book {} in datalake: {}", book_id, err);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(IngestResponse {
                book_id,
                status: "datalake_store_failed".to_string(),
                source_url,
            }),
        );
    }

    if let Err(err) = books.put(book_id.clone(), metadata).await {
        tracing::error!("Failed to store metadata for {}: {}", book_id, err);
    }

    let task = Task::Execute {
        handler: "index_document".to_string(),
        payload: serde_json::to_value(IndexTaskPayload {
            book_id: book_id.clone(),
        })
        .unwrap(),
    };

    if let Err(err) = queue.submit(task).await {
        tracing::error!("Failed to submit index task for {}: {}", book_id, err);
    }

    (
        StatusCode::ACCEPTED,
        Json(IngestResponse {
            book_id,
            status: "ingested".to_string(),
            source_url,
        }),
    )
}

/// Returns ingestion availability status for a book id.
pub async fn handle_ingest_status(
    Path(book_id): Path<String>,
    Extension(datalake): Extension<Arc<DistributedMap<String, RawDocument>>>,
) -> (StatusCode, Json<IngestStatusResponse>) {
    let status = if datalake.get(&book_id).await.is_some() {
        "available"
    } else {
        "missing"
    };

    (
        StatusCode::OK,
        Json(IngestStatusResponse {
            book_id,
            status: status.to_string(),
        }),
    )
}

async fn fetch_gutenberg_text(url: &str) -> Result<String, reqwest::Error> {
    let response = reqwest::get(url).await?;
    response.text().await
}

fn split_gutenberg_text(text: &str) -> Option<(String, String)> {
    let start_idx = text.find(START_MARKER)?;
    let end_idx = text.find(END_MARKER)?;

    if start_idx >= end_idx {
        return None;
    }

    let header = text[..start_idx].to_string();
    let body = text[start_idx + START_MARKER.len()..end_idx].to_string();
    Some((header.trim().to_string(), body.trim().to_string()))
}

fn parse_metadata(
    header: &str,
    book_id: &str,
    word_count: usize,
    unique_words: usize,
) -> BookMetadata {
    let title =
        extract_header_field(header, "Title:").unwrap_or_else(|| format!("Book {}", book_id));
    let author = extract_header_field(header, "Author:").unwrap_or_else(|| "Unknown".to_string());
    let language =
        extract_header_field(header, "Language:").unwrap_or_else(|| "unknown".to_string());
    let year = extract_year(&extract_header_field(header, "Release Date:"));

    BookMetadata {
        book_id: book_id.to_string(),
        title,
        author,
        language,
        year,
        word_count,
        unique_words,
    }
}

fn extract_header_field(header: &str, label: &str) -> Option<String> {
    header
        .lines()
        .find_map(|line| {
            if line.starts_with(label) {
                Some(line[label.len()..].trim().to_string())
            } else {
                None
            }
        })
        .filter(|value| !value.is_empty())
}

fn extract_year(field: &Option<String>) -> Option<u32> {
    let text = field.as_deref()?;
    let re = Regex::new(r"(\d{4})").unwrap();
    let year = re
        .captures(text)
        .and_then(|cap| cap.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok());
    year
}

fn compute_counts(body: &str) -> (usize, usize) {
    let re = Regex::new(r"\b[a-zA-Z]+\b").unwrap();
    let mut total = 0usize;
    let mut unique = std::collections::HashSet::new();

    for cap in re.find_iter(body) {
        total += 1;
        unique.insert(cap.as_str().to_lowercase());
    }

    (total, unique.len())
}
