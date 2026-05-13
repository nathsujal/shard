use thiserror::Error;

#[derive(Error, Debug)]
pub enum DownloadError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Server does not support range requests")]
    RangeNotSupported,
    #[error("Content length mismatch: expected {expected}, got {actual}")]
    LengthMismatch { expected: u64, actual: u64 },
    #[error("Resume metadata corrupted or incompatible")]
    MetadataCorrupted,
    #[error("Max retries exceeded for chunk {chunk_id}")]
    MaxRetries { chunk_id: usize },
    #[error("Download cancelled")]
    Cancelled,
}

impl DownloadError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Http(e) => e.is_timeout() || e.is_connect() || e.is_request(),
            Self::Io(e) => matches!(
                e.kind(),
                std::io::ErrorKind::Interrupted | std::io::ErrorKind::WouldBlock
            ),
            Self::RangeNotSupported
            | Self::LengthMismatch { .. }
            | Self::MetadataCorrupted
            | Self::MaxRetries { .. }
            | Self::Cancelled => false,
        }
    }
}