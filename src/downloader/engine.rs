use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar};
use reqwest::Client;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use super::chunker::{probe_url, split_into_chunks, ChunkRange};
use super::error::DownloadError;
use super::progress::build_progress_bar;
use super::resume::{delete_state, load_state, save_state, PartState};

use crate::queue::job::JobControl;

const SAVE_EVERY_N_CHUNKS: usize = 5;

/// Add default headers based on URL to help with servers that require them
fn add_default_headers(url: &str, mut request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    // Extract scheme and host from URL using simple parsing
    if let Some(host_start) = url.find("://") {
        let after_scheme = &url[host_start + 3..];
        if let Some(slash_pos) = after_scheme.find('/') {
            let host = &after_scheme[..slash_pos];
            let referer = format!("https://{}", host);
            request = request.header("Referer", &referer);
        } else {
            let referer = format!("https://{}", after_scheme);
            request = request.header("Referer", &referer);
        }
    }
    request
}

pub struct DownloadEngine {
    client: Client,
    connections: u8,
}

impl DownloadEngine {
    pub fn new(connections: u8) -> Self {
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
            .pool_max_idle_per_host(10)
            .tcp_keepalive(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(300))
            .tcp_nodelay(true)
            .build()
            .expect("failed to build HTTP client");

        Self { client, connections }
    }

    pub fn with_client(client: Client, connections: u8) -> Self {
        Self { client, connections }
    }

    /// Download a file. Returns (bytes_downloaded, total_size).
    pub async fn download(
        &self,
        url: &str,
        output_path: &PathBuf,
        downloaded: Option<Arc<AtomicU64>>,
        total_size_atomic: Option<Arc<AtomicU64>>,
        headers: &[(String, String)],
        control_rx: tokio::sync::watch::Receiver<JobControl>,
    ) -> Result<(u64, u64)> {
        // Check if .part state exists — resume from pause, skip probe
        if let Some(state) = load_state(output_path).await {
            if state.url == url {
                info!("Resuming download — {} chunks done, skipping probe", state.completed_chunks.len());

                // Update total_size for UI
                if let Some(ref ts) = total_size_atomic {
                    ts.store(state.total_size, Ordering::Relaxed);
                }

                // Set downloaded bytes from completed chunks
                let all_chunks = split_into_chunks(state.total_size, self.connections);
                let done_bytes: u64 = all_chunks
                    .iter()
                    .filter(|c| state.completed_chunks.contains(&c.index))
                    .map(|c| c.len())
                    .sum();
                if let Some(ref dl) = downloaded {
                    dl.store(done_bytes, Ordering::Relaxed);
                }

                return self.download_chunked(url, output_path, state.total_size, downloaded, headers, control_rx).await;
            } else {
                warn!("State file URL mismatch, re-probing");
            }
        }

        info!("Probing: {url}");

        let probe = probe_url(&self.client, url, headers).await?;

        // Update total_size immediately so UI can show progress
        if let Some(ref ts) = total_size_atomic {
            ts.store(probe.total_size, Ordering::Relaxed);
        }

        if !probe.supports_ranges || probe.total_size == 0 {
            warn!("Server doesn't support ranges — falling back to single stream");
            return self.download_single(url, output_path, downloaded, headers, control_rx).await;
        }

        info!(
            "Parallel download: {} bytes across {} chunks",
            probe.total_size, self.connections
        );

        self.download_chunked(url, output_path, probe.total_size, downloaded, headers, control_rx).await
    }

    // Single-stream fallback

    /// Returns (bytes_downloaded, total_size). total_size may equal bytes_downloaded
    /// if Content-Length was known, or 0 if unknown.
    async fn download_single(
        &self,
        url: &str,
        output_path: &PathBuf,
        downloaded: Option<Arc<AtomicU64>>,
        headers: &[(String, String)],
        _control_rx: tokio::sync::watch::Receiver<JobControl>,
    ) -> Result<(u64, u64)> {
        let mut request = add_default_headers(url, self.client.get(url));
        
        // Add user headers
        for (key, value) in headers {
            request = request.header(key.as_str(), value.as_str());
        }
        let response = request.send().await?;

        let status = response.status();
        debug!("GET response status: {}", status);
        
        // Log relevant headers for debugging
        if let Some(cl) = response.headers().get(reqwest::header::CONTENT_LENGTH) {
            debug!("  Content-Length: {:?}", cl.to_str());
        }
        if let Some(ct) = response.headers().get(reqwest::header::CONTENT_TYPE) {
            debug!("  Content-Type: {:?}", ct.to_str());
        }

        if !status.is_success() {
            warn!("Download failed with status {} for URL: {}", status, url);
            return Err(DownloadError::HttpError {
                status: status.as_u16(),
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
        let file = tokio::fs::File::create(output_path).await?;
        let mut file = BufWriter::new(file);
        let mut stream = response.bytes_stream();
        let mut bytes_written: u64 = 0;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            bytes_written += chunk.len() as u64;
            pb.set_position(bytes_written);
            if let Some(ref dl) = downloaded {
                dl.store(bytes_written, std::sync::atomic::Ordering::Relaxed);
            }
        }

        file.flush().await?;
        pb.finish_with_message("Done");
        info!("Saved → {}", output_path.display());

        Ok((bytes_written, if total_size > 0 { total_size } else { bytes_written }))
    }

    // Parallel chunked download

    async fn download_chunked(
        &self,
        url: &str,
        output_path: &PathBuf,
        total_size: u64,
        downloaded: Option<Arc<AtomicU64>>,
        headers: &[(String, String)],
        control_rx: tokio::sync::watch::Receiver<JobControl>,
    ) -> Result<(u64, u64)> {
        let state = load_state(output_path).await.unwrap_or_else(|| {
            PartState::new(url, total_size, self.connections)
        });

        let pending: Vec<usize> = state.pending_indices().collect();
        if pending.is_empty() {
            info!("Already complete: {}", output_path.display());
            delete_state(output_path).await;
            return Ok((total_size, total_size));
        }

        let already_done = self.connections as usize - pending.len();
        if already_done > 0 {
            info!("Resuming: {already_done}/{} chunks already done", self.connections);
        }

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(output_path)
            .await?;
        file.set_len(total_size).await?;
        let file = Arc::new(file);

        let show_progress = std::env::var("SHARD_TUI_MODE").is_err();
        let (_mp, pb): (Option<MultiProgress>, Option<ProgressBar>) = if show_progress {
            let mp = MultiProgress::new();
            let pb = mp.add(build_progress_bar(Some(total_size)));
            (Some(mp), Some(pb))
        } else {
            (None, None)
        };

        let state = Arc::new(Mutex::new(state));
        let completed_count = Arc::new(Mutex::new(0usize));
        let all_chunks = split_into_chunks(total_size, self.connections);

        let already_bytes: u64 = all_chunks
            .iter()
            .filter(|c| !pending.contains(&c.index))
            .map(|c| c.len())
            .sum();
        if let Some(ref pb) = pb {
            pb.set_position(already_bytes);
        }
        let pending_chunks: Vec<ChunkRange> = all_chunks
            .into_iter()
            .filter(|c| pending.contains(&c.index))
            .collect();

        let mut handles = Vec::new();

        for chunk in pending_chunks {
            let client = self.client.clone();
            let url = url.to_string();
            let output_path = output_path.clone();
            let file = Arc::clone(&file);
            let pb = pb.clone();
            let state = Arc::clone(&state);
            let completed_count = Arc::clone(&completed_count);
            let downloaded = downloaded.clone();
            let headers = headers.to_vec();
            let control_rx = control_rx.clone();

            handles.push(tokio::spawn(async move {
                fetch_chunk(&client, &url, &output_path, file, chunk, pb, state, completed_count, downloaded, &headers, control_rx).await
            }));
        }

        for handle in handles {
            handle.await??;
        }

        if let Some(ref pb) = pb {
            pb.finish_with_message("Done");
        }
        delete_state(output_path).await;
        info!("Saved → {}", output_path.display());

        Ok((total_size, total_size))
    }
}

// Per-chunk fetch

async fn fetch_chunk(
    client: &Client,
    url: &str,
    output_path: &PathBuf,
    file: Arc<tokio::fs::File>,
    chunk: ChunkRange,
    pb: Option<ProgressBar>,
    state: Arc<Mutex<PartState>>,
    completed_count: Arc<Mutex<usize>>,
    downloaded: Option<Arc<AtomicU64>>,
    headers: &[(String, String)],
    control_rx: tokio::sync::watch::Receiver<crate::queue::job::JobControl>,
) -> Result<()> {
    let range_header = format!("bytes={}-{}", chunk.start, chunk.end);

    let mut request = add_default_headers(url, client.get(url));
    for (key, value) in headers {
        request = request.header(key.as_str(), value.as_str());
    }
    let response = request
        .header(reqwest::header::RANGE, &range_header)
        .send()
        .await?;

    if !response.status().is_success() && response.status().as_u16() != 206 {
        return Err(DownloadError::HttpError {
            status: response.status().as_u16(),
        }
        .into());
    }

    let mut file = BufWriter::new(file.try_clone().await?);
    file.seek(SeekFrom::Start(chunk.start)).await?;

    let mut stream = response.bytes_stream();

    while let Some(bytes) = stream.next().await {
        // Check for cancel
        if *control_rx.borrow() == JobControl::Cancel {
            let err = std::io::Error::new(std::io::ErrorKind::Interrupted, "Cancelled");
            return Err(anyhow!(err));
        }
        
        // Check for pause - save state and exit gracefully
        if *control_rx.borrow() == JobControl::Pause {
            // Save current progress
            let state_snapshot = {
                let s = state.lock().await;
                PartState { url: s.url.clone(), total_size: s.total_size, num_chunks: s.num_chunks, completed_chunks: s.completed_chunks.clone() }
            };
            save_state(output_path, &state_snapshot).await?;
            return Err(anyhow!("Paused"));
        }

        let bytes = bytes?;
        file.write_all(&bytes).await?;
        if let Some(ref pb) = pb {
            pb.inc(bytes.len() as u64);
        }
        if let Some(ref dl) = downloaded {
            dl.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }

    file.flush().await?;

    let should_save = {
        let mut s = state.lock().await;
        s.mark_done(chunk.index);
        let mut count = completed_count.lock().await;
        *count += 1;
        *count % SAVE_EVERY_N_CHUNKS == 0
    };

    if should_save {
        let state_snapshot = {
            let s = state.lock().await;
            PartState { url: s.url.clone(), total_size: s.total_size, num_chunks: s.num_chunks, completed_chunks: s.completed_chunks.clone() }
        };
        save_state(output_path, &state_snapshot).await?;
    }

    Ok(())
}