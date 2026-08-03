fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    tracing::info!("trenchd paper-only workspace initialized");
    Ok(())
}
