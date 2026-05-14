use serde::{Deserialize, Serialize};

/// Messages sent FROM the CLI client TO the daemon.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    /// Add one or more URLs to the queue.
    Add {
        urls: Vec<String>,
        output_dir: String,
        connections: u8,
    },
    /// Pause a running job.
    Pause { id: u64 },
    /// Cancel a job (running or pending).
    Cancel { id: u64 },
    /// Get current queue status. If all=true, include Done/Failed/Cancelled.
    Status { all: bool },
    /// Shut the daemon down cleanly.
    Shutdown,
}

/// Messages sent FROM the daemon BACK to the CLI client.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    /// Job(s) successfully queued. Returns their IDs.
    Queued { ids: Vec<u64> },
    /// Generic success (pause, cancel, shutdown).
    Ok { message: String },
    /// Current list of all jobs in the queue.
    JobList { jobs: Vec<JobSummary> },
    /// Something went wrong.
    Error { message: String },
}

/// Snapshot of a single job's state — safe to serialize and send over socket.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobSummary {
    pub id: u64,
    pub url: String,
    pub output_path: String,
    pub status: String,
    pub total_size: u64,
    pub downloaded: u64,
    pub progress_pct: f32,
}