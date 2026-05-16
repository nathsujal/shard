use serde::{Deserialize, Serialize};

/// Messages sent FROM the CLI client TO the daemon.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    /// Add one or more URLs to the queue.
    Add {
        urls: Vec<String>,
        output_dir: Option<String>,
        connections: u8,
        headers: Vec<(String, String)>,
    },
    /// Pause a running job.
    Pause { id: u64 },
    /// Resume a paused job.
    Resume { id: u64 },
    /// Cancel a job (running or pending).
    Cancel { id: u64 },
    /// Get current queue status. If all=true, include Done/Failed/Cancelled.
    Status { all: bool },
    /// Subscribe to real-time job updates (keeps connection open).
    Subscribe { all: bool },
    /// Unsubscribe from real-time updates.
    Unsubscribe,
    /// Shut the daemon down cleanly.
    Shutdown,
    /// Fetch completed job history.
    History {
        limit: u32,
    },
    /// Prune old history entries.
    Prune {
        days: u32,
        statuses: Vec<String>,
        dry_run: bool,
    },
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
    /// Confirmation of subscription.
    Subscribed,
    /// Push update for subscribed clients.
    Update { jobs: Vec<JobSummary> },
    /// Results of a prune operation.
    PruneResult { count: u64, message: String },
    /// Something went wrong.
    Error { message: String },
}

/// Snapshot of a single job's state — safe to serialize and send over socket.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct JobSummary {
    pub id: u64,
    pub url: String,
    pub output_path: String,
    pub status: String,
    pub total_size: u64,
    pub downloaded: u64,
    pub progress_pct: f32,
    pub speed_bps: Option<u64>,
    pub chunks_total: u8,
    pub chunks_done: u8,
    pub started_at: Option<i64>,  // epoch seconds when download started
    pub ended_at: Option<i64>,  // epoch seconds when download ended (paused/completed/failed); None if still active
    pub added_at: Option<i64>,
    pub error_msg: Option<String>,
}