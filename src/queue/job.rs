//! Job state machine: `DownloadJob`, `JobStatus`, `JobControl` signals via watch channel.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

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
    pub url: Arc<String>,
    pub output_path: PathBuf,
    pub connections: u8,
    pub total_size: u64,
    pub total_size_atomic: Arc<AtomicU64>,
    pub headers: Vec<(String, String)>,

    /// Bytes downloaded so far — updated atomically by engine tasks.
    pub downloaded: Arc<AtomicU64>,

    /// Chunks completed — updated atomically by engine.
    pub chunks_done: Arc<AtomicU8>,

    /// Speed tracking.
    last_downloaded: AtomicU64,
    last_sample_time: std::sync::Mutex<Instant>,
    cached_speed: std::sync::Mutex<u64>,

    /// Current status — readable by UI, writable by manager.
    pub status: JobStatus,

    /// When download started (set when status becomes Active)
    pub started_at: Option<Instant>,

    /// When download ended (set on pause / completion / failure)
    pub ended_at: Option<Instant>,

    /// Channel to send pause/cancel signals into the running task.
    pub control_tx: watch::Sender<JobControl>,
    pub control_rx: watch::Receiver<JobControl>,
}

impl DownloadJob {
    pub fn new(id: JobId, url: String, output_path: PathBuf, connections: u8, headers: Vec<(String, String)>) -> Self {
        let (control_tx, control_rx) = watch::channel(JobControl::Run);

        Self {
            id,
            url: Arc::new(url),
            output_path,
            connections,
            total_size: 0,
            total_size_atomic: Arc::new(AtomicU64::new(0)),
            headers,
            downloaded: Arc::new(AtomicU64::new(0)),
            chunks_done: Arc::new(AtomicU8::new(0)),
            last_downloaded: AtomicU64::new(0),
            last_sample_time: std::sync::Mutex::new(Instant::now()),
            cached_speed: std::sync::Mutex::new(0),
            status: JobStatus::Pending,
            started_at: None,
            ended_at: None,
            control_tx,
            control_rx,
        }
    }

    pub fn bytes_downloaded(&self) -> u64 {
        self.downloaded.load(Ordering::Relaxed)
    }

    pub fn total_size_loaded(&self) -> u64 {
        self.total_size_atomic.load(Ordering::Relaxed)
    }

    pub fn progress_pct(&self) -> f32 {
        let total = self.total_size_loaded();
        if total == 0 {
            return 0.0;
        }
        (self.bytes_downloaded() as f32 / total as f32) * 100.0
    }

    /// Set the total file size (called after probe).
    pub fn set_total_size(&mut self, size: u64) {
        self.total_size = size;
        self.total_size_atomic.store(size, Ordering::Relaxed);
    }

    /// Update speed calculation — call periodically from engine.
    pub fn update_speed(&self) {
        let now = Instant::now();
        let current = self.bytes_downloaded();
        
        let mut last_time = self.last_sample_time.lock().unwrap();
        let mut last_bytes = self.last_downloaded.load(Ordering::Relaxed);
        let mut cached = self.cached_speed.lock().unwrap();
        
        let elapsed = now.duration_since(*last_time).as_secs();
        if elapsed >= 1 {
            if current > last_bytes {
                *cached = (current - last_bytes) / elapsed;
            }
            last_bytes = current;
            *last_time = now;
            self.last_downloaded.store(last_bytes, Ordering::Relaxed);
        }
    }

    /// Get current speed in bytes per second.
    pub fn speed_bps(&self) -> u64 {
        *self.cached_speed.lock().unwrap()
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
            .field("url", &*self.url)
            .field("status", &self.status)
            .field("progress", &format!("{:.1}%", self.progress_pct()))
            .finish()
    }
}