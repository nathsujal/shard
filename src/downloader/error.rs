use thiserror::Error;

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("HTTP error: {status}")]
    HttpError { status: u16 },

    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[allow(dead_code)]
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),
}