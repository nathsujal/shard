use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use futures_util::StreamExt;
use indicatif::MultiProgress;
use reqwest::Client;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;
use tracing::{info, warn};

use super::chunker::{probe_url, split_into_chunks, ChunkRange};
use super::error::DownloadError;
use super::progress::build_progress_bar;
use super::resume::{delete_state, load_state, save_state, PartState};

pub struct DownloadEngine {
    client: Client,
    connections: u8,
}

impl DownloadEngine {
    pub fn new(connections: u8) -> Self {
        let client = Client::builder()
            .user_agent("shard/1.0")
            .build()
            .expect("failed to build HTTP client");

        Self { client, connections }
    }

    pub fn with_client(client: Client, connections: u8) -> Self {
        Self { client, connections }
    }

    /// Download a file. Returns (bytes_downloaded, total_size).
    pub async fn download(&self, url: &str, output_path: &PathBuf) -> Result<(u64, u64)> {
        info!("Probing: {url}");

        let probe = probe_url(&self.client, url).await?;

        if !probe.supports_ranges || probe.total_size == 0 {
            warn!("Server doesn't support ranges — falling back to single stream");
            return self.download_single(url, output_path).await;
        }

        info!(
            "Parallel download: {} bytes across {} chunks",
            probe.total_size, self.connections
        );

        self.download_chunked(url, output_path, probe.total_size).await
    }

    // Single-stream fallback

    /// Returns (bytes_downloaded, total_size). total_size may equal bytes_downloaded
    /// if Content-Length was known, or 0 if unknown.
    async fn download_single(&self, url: &str, output_path: &PathBuf) -> Result<(u64, u64)> {
        let response = self.client.get(url).send().await?;

        if !response.status().is_success() {
            return Err(DownloadError::HttpError {
                status: response.status().as_u16(),
            }
            .into());
        }

        let total_size = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let pb = build_progress_bar(if total_size > 0 { Some(total_size) } else { None });
        let mut file = tokio::fs::File::create(output_path).await?;
        let mut stream = response.bytes_stream();
        let mut downloaded: u64 = 0;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
            pb.set_position(downloaded);
        }

        file.flush().await?;
        pb.finish_with_message("Done");
        println!("\nSaved → {}", output_path.display());

        Ok((downloaded, if total_size > 0 { total_size } else { downloaded }))
    }

    // Parallel chunked download

    async fn download_chunked(
        &self,
        url: &str,
        output_path: &PathBuf,
        total_size: u64,
    ) -> Result<(u64, u64)> {
        let state = load_state(output_path).await.unwrap_or_else(|| {
            PartState::new(url, total_size, self.connections)
        });

        let pending = state.pending_chunks();
        if pending.is_empty() {
            println!("Already complete: {}", output_path.display());
            delete_state(output_path).await;
            return Ok((total_size, total_size));
        }

        let already_done = self.connections as usize - pending.len();
        if already_done > 0 {
            info!("Resuming: {already_done}/{} chunks already done", self.connections);
        }

        // Pre-allocate output file at full size.
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(output_path)
            .await?;
        file.set_len(total_size).await?;
        drop(file);

        let mp = MultiProgress::new();
        let pb = mp.add(build_progress_bar(Some(total_size)));

        // Set progress bar to already-completed bytes.
        let already_bytes: u64 = split_into_chunks(total_size, self.connections)
            .iter()
            .filter(|c| !pending.contains(&c.index))
            .map(|c| c.len())
            .sum();
        pb.set_position(already_bytes);

        let state = Arc::new(Mutex::new(state));
        let all_chunks = split_into_chunks(total_size, self.connections);
        let pending_chunks: Vec<ChunkRange> = all_chunks
            .into_iter()
            .filter(|c| pending.contains(&c.index))
            .collect();

        let mut handles = Vec::new();

        for chunk in pending_chunks {
            let client = self.client.clone();
            let url = url.to_string();
            let path = output_path.clone();
            let pb = pb.clone();
            let state = Arc::clone(&state);

            handles.push(tokio::spawn(async move {
                fetch_chunk(&client, &url, &path, chunk, pb, state).await
            }));
        }

        for handle in handles {
            handle.await??;
        }

        pb.finish_with_message("Done");
        delete_state(output_path).await;
        println!("\nSaved → {}", output_path.display());

        Ok((total_size, total_size))
    }
}

// Per-chunk fetch

async fn fetch_chunk(
    client: &Client,
    url: &str,
    output_path: &PathBuf,
    chunk: ChunkRange,
    pb: indicatif::ProgressBar,
    state: Arc<Mutex<PartState>>,
) -> Result<()> {
    let range_header = format!("bytes={}-{}", chunk.start, chunk.end);

    let response = client
        .get(url)
        .header(reqwest::header::RANGE, &range_header)
        .send()
        .await?;

    if !response.status().is_success() && response.status().as_u16() != 206 {
        return Err(DownloadError::HttpError {
            status: response.status().as_u16(),
        }
        .into());
    }

    let mut file = OpenOptions::new()
        .write(true)
        .open(output_path)
        .await?;

    file.seek(SeekFrom::Start(chunk.start)).await?;

    let mut stream = response.bytes_stream();

    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        file.write_all(&bytes).await?;
        pb.inc(bytes.len() as u64);
    }

    file.flush().await?;

    let mut s = state.lock().await;
    s.mark_done(chunk.index);
    save_state(output_path, &s).await?;

    Ok(())
}