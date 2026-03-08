//! Search indexing and query modules.

/// Query evaluation engine.
pub mod engine;
/// axum handlers for search and book creation.
pub mod handlers;
/// Text and query tokenization helpers.
pub mod tokenizer;
/// Search DTOs and metadata structures.
pub mod types;

#[cfg(test)]
mod tests;
