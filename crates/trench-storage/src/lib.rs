//! Durable storage and deterministic replay for paper-trading records.

pub mod sqlite;

#[cfg(test)]
mod engine_batch_tests;
