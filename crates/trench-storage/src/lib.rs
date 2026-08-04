//! Durable storage and deterministic replay for paper-trading records.

pub mod parquet;
pub mod replay;
pub mod research;
pub mod research_plan;
pub mod research_runs;
pub mod research_sidecar;
pub mod sqlite;

#[cfg(test)]
mod engine_batch_tests;
