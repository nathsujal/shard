use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Note: Uses env!("CARGO_PKG_NAME") so it auto-adapts to "shard"
    tracing_subscriber::fmt()
        .with_env_filter(format!("info,{}=debug", env!("CARGO_PKG_NAME")))
        .init();

    info!("shard initialized. Core engine ready.");
    Ok(())
}