//! Durable storage and deterministic replay for paper-trading records.

pub mod parquet;
pub mod replay;
pub mod research;
pub mod sqlite;

#[cfg(test)]
mod engine_batch_tests;
