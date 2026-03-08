use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Basic metadata returned for a book.
#[derive(Debug, Serialize, Deserialize)]
pub struct BookResult {
    /// Book identifier.
    pub book_id: String,
    /// Book title.
    pub title: String,
    /// Book author.
    pub author: String,
    /// Book language code or label.
    pub language: String,
    /// Publication/release year if available.
    pub year: Option<u32>,
}

/// API response returned by `/search`.
#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResponse {
    /// Original query string.
    pub query: String,
    /// Placeholder for extra filters.
    pub filters: HashMap<String, String>,
    /// Total matched books before pagination.
    pub total_count: usize,
    /// Number of items returned in this page.
    pub count: usize,
    /// Paginated result items.
    pub results: Vec<SearchResultItem>,
}

/// One search hit with score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResultItem {
    /// Book identifier.
    pub book_id: String,
    /// Book title.
    pub title: String,
    /// Book author.
    pub author: String,
    /// Relevance score.
    pub score: usize,
}

/// Full metadata persisted for ingested books.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookMetadata {
    /// Book identifier.
    pub book_id: String,
    /// Book title.
    pub title: String,
    /// Book author.
    pub author: String,
    /// Book language code or label.
    pub language: String,
    /// Publication/release year if available.
    pub year: Option<u32>,
    /// Number of parsed words in document body.
    pub word_count: usize,
    /// Number of unique tokens in document body.
    pub unique_words: usize,
}
