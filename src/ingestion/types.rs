use serde::{Deserialize, Serialize};

/// Raw document stored in datalake map.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawDocument {
    /// Book identifier.
    pub book_id: String,
    /// Parsed header section.
    pub header: String,
    /// Parsed body section.
    pub body: String,
    /// Source URL used during ingestion.
    pub source_url: String,
}

/// Response returned by ingest endpoint.
#[derive(Debug, Serialize)]
pub struct IngestResponse {
    /// Book identifier.
    pub book_id: String,
    /// Ingestion status string.
    pub status: String,
    /// Source URL used during ingestion.
    pub source_url: String,
}

/// Response returned by ingest status endpoint.
#[derive(Debug, Serialize)]
pub struct IngestStatusResponse {
    /// Book identifier.
    pub book_id: String,
    /// Current availability status.
    pub status: String,
}

/// Payload passed to background indexing task handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct IndexTaskPayload {
    /// Book identifier to index.
    pub book_id: String,
}
