// src/daemon/server.rs
//
// Listens on a Unix domain socket.
// Each incoming client connection is handled in its own tokio task.
// Uses newline-delimited JSON (one Request line in, one Response line out).

use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::db::schema::open_db;
use crate::queue::manager::QueueManager;

use super::handler::handle_request;

/// Path to the Unix domain socket file.
pub fn socket_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("shard")
        .join("shard.sock")
}

/// Start the daemon. Blocks forever until SIGINT or a Shutdown request.
pub async fn run_daemon(max_concurrent: usize) -> Result<()> {
    let sock_path = socket_path();

    // Ensure parent directory exists.
    if let Some(parent) = sock_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Remove stale socket file from previous run (if any).
    if sock_path.exists() {
        tokio::fs::remove_file(&sock_path).await?;
    }

    // Open DB and build shared state.
    let db = open_db().await?;
    let manager = Arc::new(Mutex::new(QueueManager::new(max_concurrent)));

    info!("Daemon listening on {}", sock_path.display());
    let listener = UnixListener::bind(&sock_path)?;

    // Shared shutdown flag.
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    loop {
        tokio::select! {
            // Accept a new client connection.
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => {
                        let manager = Arc::clone(&manager);
                        let db = db.clone();
                        let shutdown_tx = shutdown_tx.clone();

                        tokio::spawn(async move {
                            let (reader, mut writer) = stream.into_split();
                            let mut lines = BufReader::new(reader).lines();

                            // Read one request line.
                            match lines.next_line().await {
                                Ok(Some(line)) => {
                                    let response = handle_request(
                                        &line,
                                        &manager,
                                        &db,
                                        &shutdown_tx,
                                    )
                                    .await;

                                    // Write response back.
                                    let mut out = serde_json::to_string(&response)
                                        .unwrap_or_else(|_| r#"{"type":"error","message":"serialization failed"}"#.into());
                                    out.push('\n');

                                    if let Err(e) = writer.write_all(out.as_bytes()).await {
                                        error!("Failed to write response: {e}");
                                    }
                                }
                                Ok(None) => {} // client disconnected early
                                Err(e) => error!("Failed to read request: {e}"),
                            }
                        });
                    }
                    Err(e) => error!("Accept error: {e}"),
                }
            }

            // Shutdown signal received.
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("Daemon shutting down.");
                    break;
                }
            }
        }
    }

    // Clean up socket file on exit.
    let _ = tokio::fs::remove_file(&sock_path).await;
    Ok(())
}