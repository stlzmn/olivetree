use super::types::BookMetadata;
use crate::search::tokenizer::tokenize_query;
use crate::storage::memory::DistributedMap;
use std::collections::HashMap;
use std::sync::Arc;

/// Executes token-based search and returns `(metadata, score)` pairs.
///
/// Scoring is currently a count of matched query tokens.
pub async fn search(
    query: &str,
    index_map: Arc<DistributedMap<String, Vec<String>>>,
    books_map: Arc<DistributedMap<String, BookMetadata>>,
) -> Vec<(BookMetadata, usize)> {
    let query_tokens = tokenize_query(query);

    let mut book_scores: HashMap<String, usize> = HashMap::new();
    for token in query_tokens.iter() {
        if let Some(book_index) = index_map.get(token).await {
            for book_id in book_index {
                book_scores
                    .entry(book_id.clone())
                    .and_modify(|score| *score += 1)
                    .or_insert(1);
            }
        }
    }

    let mut results: Vec<(BookMetadata, usize)> = Vec::new();
    for (book_id, score) in book_scores.iter() {
        if let Some(metadata) = books_map.get(book_id).await {
            results.push((metadata.clone(), *score));
        }
    }

    results.sort_by(|a, b| b.1.cmp(&a.1));
    results
}
