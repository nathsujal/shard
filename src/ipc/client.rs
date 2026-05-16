// Thin client used by every CLI subcommand.
// Flow: connect socket → write JSON request line → read JSON response line → return.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::RwLock;

use super::message::{JobSummary, Request, Response};
use crate::daemon::server::socket_path;

/// Connect to the running daemon and send one request.
/// Returns the daemon's response, or an error if the daemon isn't running.
pub async fn send_request(request: &Request) -> Result<Response> {
    let path = socket_path();

    let stream = UnixStream::connect(&path).await.with_context(|| {
        format!(
            "Could not connect to shard daemon at {}.\nIs the daemon running? Start it with: shard daemon",
            path.display()
        )
    })?;

    let (reader, mut writer) = stream.into_split();

    // Serialize request as a single JSON line and send it.
    let mut line = serde_json::to_string(request)?;
    line.push('\n'); // newline-delimited JSON (NDJSON)
    writer.write_all(line.as_bytes()).await?;
    writer.flush().await?;

    // Read one response line back from daemon.
    let mut buf_reader = BufReader::new(reader);
    let mut response_line = String::new();
    buf_reader.read_line(&mut response_line).await?;

    let response: Response = serde_json::from_str(response_line.trim())
        .with_context(|| format!("Bad response from daemon: {response_line}"))?;

    Ok(response)
}

/// Connect to daemon, subscribe to updates, and stream updates into shared state.
/// Spawn this as a background task - it continuously updates `jobs` via daemon pushes.
/// `connected` is set to true when subscribe handshake succeeds, false on disconnect.
pub async fn subscribe_and_stream(
    jobs: Arc<RwLock<Vec<JobSummary>>>,
    connected: Arc<AtomicBool>,
) -> Result<()> {
    loop {
        match subscribe_inner(&jobs, &connected).await {
            Ok(()) => {
                connected.store(false, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(_) => {
                connected.store(false, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }
}

async fn subscribe_inner(
    jobs: &Arc<RwLock<Vec<JobSummary>>>,
    connected: &Arc<AtomicBool>,
) -> Result<()> {
    let stream = UnixStream::connect(&socket_path()).await?;
    let (reader, mut writer) = stream.into_split();

    // Send subscribe request
    let subscribe = r#"{"type":"subscribe","all":true}"#;
    let mut line = subscribe.to_string();
    line.push('\n');
    writer.write_all(line.as_bytes()).await?;
    writer.flush().await?;

    // Read Subscribed response
    let mut buf_reader = BufReader::new(reader);
    let mut resp_line = String::new();
    buf_reader.read_line(&mut resp_line).await?;
    let _response: Response = serde_json::from_str(resp_line.trim())?;

    // Handshake complete — daemon confirmed subscription
    connected.store(true, Ordering::Relaxed);

    // Read updates in a loop
    loop {
        let mut line = String::new();
        match buf_reader.read_line(&mut line).await {
            Ok(0) => break, // connection closed
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Ok(Response::Update { jobs: new_jobs }) =
                    serde_json::from_str::<Response>(trimmed)
                {
                    let mut state = jobs.write().await;
                    *state = new_jobs;
                }
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    Ok(())
}