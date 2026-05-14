use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::watch;

/// Unique job identifier.
pub type JobId = u64;

#[derive(Debug, Clone, PartialEq)]
pub enum JobStatus {
    Pending,
    Active,
    Paused,
    Done,
    Failed(String),
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatus::Pending => write!(f, "Pending"),
            JobStatus::Active => write!(f, "Active"),
            JobStatus::Paused => write!(f, "Paused"),
            JobStatus::Done => write!(f, "Done"),
            JobStatus::Failed(e) => write!(f, "Failed: {e}"),
        }
    }
}

/// Control signals sent to a running job.
#[derive(Debug, Clone, PartialEq)]
pub enum JobControl {
    Run,
    Pause,
    Cancel,
}

/// A single download job.
pub struct DownloadJob {
    pub id: JobId,
    pub url: String,
    pub output_path: PathBuf,
    pub connections: u8,
    pub total_size: u64,

    /// Bytes downloaded so far — updated atomically by engine tasks.
    pub downloaded: Arc<AtomicU64>,

    /// Current status — readable by UI, writable by manager.
    pub status: JobStatus,

    /// Channel to send pause/cancel signals into the running task.
    pub control_tx: watch::Sender<JobControl>,
    pub control_rx: watch::Receiver<JobControl>,
}

impl DownloadJob {
    pub fn new(id: JobId, url: String, output_path: PathBuf, connections: u8) -> Self {
        let (control_tx, control_rx) = watch::channel(JobControl::Run);

        Self {
            id,
            url,
            output_path,
            connections,
            total_size: 0,
            downloaded: Arc::new(AtomicU64::new(0)),
            status: JobStatus::Pending,
            control_tx,
            control_rx,
        }
    }

    pub fn bytes_downloaded(&self) -> u64 {
        self.downloaded.load(Ordering::Relaxed)
    }

    pub fn progress_pct(&self) -> f32 {
        if self.total_size == 0 {
            return 0.0;
        }
        (self.bytes_downloaded() as f32 / self.total_size as f32) * 100.0
    }

    /// Send a control signal to the running task.
    pub fn send_control(&self, signal: JobControl) {
        let _ = self.control_tx.send(signal);
    }
}

impl std::fmt::Debug for DownloadJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadJob")
            .field("id", &self.id)
            .field("url", &self.url)
            .field("status", &self.status)
            .field("progress", &format!("{:.1}%", self.progress_pct()))
            .finish()
    }
}