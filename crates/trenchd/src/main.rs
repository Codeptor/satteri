#[cfg(unix)]
mod admin;
#[cfg(not(unix))]
#[path = "admin_stub.rs"]
mod admin;
mod app;
mod commands;
mod execution;
mod readiness;
mod writer;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    commands::Cli::parse()
        .execute()
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))
}
