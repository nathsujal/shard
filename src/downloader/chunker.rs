use reqwest::Client;
use tracing::{debug, warn};

use super::error::DownloadError;

/// A single byte range: [start, end] inclusive.
#[derive(Debug, Clone)]
pub struct ChunkRange {
    pub index: usize,
    pub start: u64,
    pub end: u64,
}

impl ChunkRange {
    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }
}

/// Result of probing a URL before downloading.
#[derive(Debug)]
pub struct ServerProbe {
    pub total_size: u64,
    pub supports_ranges: bool,
}

/// HEAD request → get file size + check Accept-Ranges header.
pub async fn probe_url(client: &Client, url: &str, headers: &[(String, String)]) -> Result<ServerProbe, DownloadError> {
    let mut request = client.head(url);
    
    // Add default headers
    if let Some(host_start) = url.find("://") {
        let after_scheme = &url[host_start + 3..];
        if let Some(slash_pos) = after_scheme.find('/') {
            let host = &after_scheme[..slash_pos];
            request = request.header("Referer", &format!("https://{}", host));
        } else {
            request = request.header("Referer", &format!("https://{}", after_scheme));
        }
    }
    
    // Add user-provided headers
    for (key, value) in headers {
        request = request.header(key.as_str(), value.as_str());
    }
    let response = request.send().await?;

    let status = response.status();
    debug!("Probe response status: {}", status);
    
    // Log all response headers for debugging
    for (name, value) in response.headers() {
        debug!("  {}: {:?}", name.as_str(), value.to_str().unwrap_or("[binary]"));
    }

    if !status.is_success() {
        warn!("Probe failed with status {} for URL: {}", status, url);
        return Err(DownloadError::HttpError {
            status: status.as_u16(),
        });
    }

    let total_size = response
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    let supports_ranges = response
        .headers()
        .get(reqwest::header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map(|v| v != "none")
        .unwrap_or(false);

    debug!("Probe OK: size={}, ranges={}", total_size, supports_ranges);

    Ok(ServerProbe {
        total_size,
        supports_ranges,
    })
}

/// Split `total_size` bytes into `n` evenly-sized `ChunkRange`s.
pub fn split_into_chunks(total_size: u64, n: u8) -> Vec<ChunkRange> {
    let n = n as u64;
    let chunk_size = total_size / n;

    (0..n)
        .map(|i| {
            let start = i * chunk_size;
            let end = if i == n - 1 {
                total_size - 1 // last chunk absorbs remainder
            } else {
                start + chunk_size - 1
            };
            ChunkRange {
                index: i as usize,
                start,
                end,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_cover_full_file() {
        let chunks = split_into_chunks(100, 4);
        assert_eq!(chunks[0].start, 0);
        assert_eq!(chunks[3].end, 99);

        // No gaps
        for w in chunks.windows(2) {
            assert_eq!(w[0].end + 1, w[1].start);
        }
    }

    #[test]
    fn last_chunk_absorbs_remainder() {
        let chunks = split_into_chunks(101, 4);
        assert_eq!(chunks[3].end, 100);
    }
}