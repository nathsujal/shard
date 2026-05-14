// Thin client used by every CLI subcommand.
// Flow: connect socket → write JSON request line → read JSON response line → return.

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use super::message::{Request, Response};
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