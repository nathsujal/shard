use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use reqwest::Client;
use sqlx::SqlitePool;
use tokio::sync::{broadcast, RwLock, Semaphore};
use tracing::{error, info};

use crate::db::schema::update_job_status;
use crate::downloader::engine::DownloadEngine;
use crate::ipc::message::JobSummary;

use super::job::{DownloadJob, JobControl, JobId, JobStatus};

/// Events emitted by QueueManager for subscribers.
#[derive(Debug, Clone)]
pub enum ManagerEvent {
    JobsChanged,
}

pub struct QueueManager {
    client: Client,
    jobs: Arc<RwLock<VecDeque<DownloadJob>>>,
    job_index: Arc<RwLock<HashMap<JobId, usize>>>,
    semaphore: Arc<Semaphore>,
    next_id: Arc<AtomicU64>,
    event_tx: broadcast::Sender<ManagerEvent>,
    runner_running: Arc<AtomicBool>,
    // ── NEW: fired by download tasks when they finish, so server can re-trigger
    //         run_all_with_db to pick up any Pending jobs that were waiting for a slot.
    pub run_trigger_tx: broadcast::Sender<()>,
}

impl QueueManager {
    pub fn new(max_concurrent: usize) -> Self {
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
            .pool_max_idle_per_host(10)
            .tcp_keepalive(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(300))
            .tcp_nodelay(true)
            .build()
            .expect("failed to build HTTP client");

        let (event_tx, _) = broadcast::channel(256);
        let (run_trigger_tx, _) = broadcast::channel(16); // NEW

        Self {
            client,
            jobs: Arc::new(RwLock::new(VecDeque::new())),
            job_index: Arc::new(RwLock::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            next_id: Arc::new(AtomicU64::new(1)),
            event_tx,
            runner_running: Arc::new(AtomicBool::new(false)),
            run_trigger_tx, // NEW
        }
    }

    pub fn event_tx(&self) -> broadcast::Sender<ManagerEvent> {
        self.event_tx.clone()
    }

    /// Returns sender so server.rs can subscribe and re-trigger run_all_with_db
    /// when a download slot frees up and Pending jobs are waiting.
    pub fn run_trigger_tx(&self) -> broadcast::Sender<()> {
        self.run_trigger_tx.clone()
    }

    fn emit_event(&self) {
        let _ = self.event_tx.send(ManagerEvent::JobsChanged);
    }

    #[allow(dead_code)]
    pub async fn add(&self, url: String, output_path: PathBuf, connections: u8, headers: Vec<(String, String)>) -> JobId {
        let id = self.next_id.fetch_add(1, Ordering::AcqRel);
        self.add_with_id(id, url, output_path, connections, headers, None, 0, 0, None, None).await;
        id
    }

    pub async fn add_with_id(
        &self,
        id: JobId,
        url: String,
        output_path: PathBuf,
        connections: u8,
        headers: Vec<(String, String)>,
        initial_status: Option<JobStatus>,
        initial_total_size: u64,
        initial_downloaded: u64,
        initial_started_at: Option<Instant>,
        initial_ended_at: Option<Instant>,
    ) {
        let mut job = DownloadJob::new(id, url.clone(), output_path, connections, headers);
        if let Some(ref status) = initial_status {
            job.status = status.clone();
            if *status == JobStatus::Paused {
                job.control_tx.send_modify(|c| *c = JobControl::Pause);
            }
        }
        if initial_total_size > 0 {
            job.total_size = initial_total_size;
            job.total_size_atomic.store(initial_total_size, Ordering::Relaxed);
        }
        if initial_downloaded > 0 {
            job.downloaded.store(initial_downloaded, Ordering::Relaxed);
        }
        if let Some(ts) = initial_started_at {
            job.started_at = Some(ts);
        }
        if let Some(ts) = initial_ended_at {
            job.ended_at = Some(ts);
        }
        info!("Queued job #{id}: {url}");
        let mut jobs = self.jobs.write().await;
        let index = jobs.len();
        jobs.push_back(job);
        self.job_index.write().await.insert(id, index);
        self.next_id.fetch_max(id + 1, Ordering::AcqRel);
        self.emit_event();
    }

    // ── Control signals ───────────────────────────────────────────────────────

    pub async fn pause(&self, id: JobId) {
        {
            let mut jobs = self.jobs.write().await;
            let index = self.job_index.read().await.get(&id).copied();
            if let Some(job) = index.and_then(|i| jobs.get_mut(i)) {
                match &job.status {
                    JobStatus::Active => {
                        job.send_control(JobControl::Pause);
                        info!("Paused active job #{id}");
                    }
                    _ => return, // Pending/Paused/terminal — no-op
                }
            }
        }
        self.emit_event();
    }

    pub async fn cancel(&self, id: JobId) {
        // Immediately update in-memory status so TUI reflects cancel without
        // waiting for the download task to asynchronously process the signal.
        // The spawned task still handles DB update + run_trigger when it exits.
        {
            let mut jobs = self.jobs.write().await;
            let index = self.job_index.read().await.get(&id).copied();
            if let Some(job) = index.and_then(|i| jobs.get_mut(i)) {
                match &job.status {
                    JobStatus::Paused | JobStatus::Pending => {
                        // Not running — set directly, no task to notify.
                        job.status = JobStatus::Failed("Cancelled".into());
                        job.ended_at = Some(std::time::Instant::now());
                        info!("Cancelled paused/pending job #{id}");
                    }
                    JobStatus::Active => {
                        // Signal the running task AND set status immediately so
                        // TUI doesn't lag waiting for the task's cancel check loop.
                        job.send_control(JobControl::Cancel);
                        job.status = JobStatus::Failed("Cancelled".into());
                        job.ended_at = Some(std::time::Instant::now());
                        info!("Cancelled active job #{id}");
                    }
                    _ => return, // already terminal
                }
            } else {
                return; // not found
            }
        } // jobs write lock dropped here

        self.job_index.write().await.remove(&id);
        // For Paused/Pending jobs there's no running task to fire run_trigger,
        // so do it here — in case another Pending job is waiting for a slot.
        let _ = self.run_trigger_tx.send(());
        self.emit_event();
    }

    pub async fn resume(&self, id: JobId) {
        // ── Try to grab a semaphore slot BEFORE touching job state.
        //    acquire_owned().await would block here while the QueueManager mutex
        //    is held by the caller (handler.rs), starving every other IPC request
        //    (including TUI Status polls) → UI freeze.
        //    If no slot is free right now, flip the job to Pending so the
        //    run_trigger_rx arm in server.rs picks it up the moment one opens.
        let permit = match Arc::clone(&self.semaphore).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                // No slot available. Set job back to Pending so the runner
                // starts it automatically when a running download completes.
                let mut jobs = self.jobs.write().await;
                if let Some(&index) = self.job_index.read().await.get(&id) {
                    if let Some(job) = jobs.get_mut(index) {
                        if job.status == JobStatus::Paused {
                            job.send_control(JobControl::Run); // clear Pause signal
                            job.status = JobStatus::Pending;
                            job.ended_at = None;
                            info!("Resume #{id}: no slot, queued as Pending");
                        }
                    }
                }
                self.emit_event();
                return;
            }
        };

        let job_data = {
            let mut jobs = self.jobs.write().await;
            if let Some(&index) = self.job_index.read().await.get(&id) {
                if let Some(job) = jobs.get_mut(index) {
                    if job.status == JobStatus::Paused {
                        job.send_control(JobControl::Run);
                        job.status = JobStatus::Active;
                        job.ended_at = None;
                        info!("Resumed job #{id}");
                        Some((
                            job.id,
                            job.url.clone(),
                            job.output_path.clone(),
                            job.connections,
                            job.control_rx.clone(),
                            Arc::clone(&job.downloaded),
                            Arc::clone(&job.total_size_atomic),
                            job.headers.clone(),
                        ))
                    } else {
                        info!("Job #{id} not paused (status: {})", job.status);
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some((id, url, output_path, connections, control_rx, downloaded, total_size_atomic, headers)) = job_data {
            let jobs = Arc::clone(&self.jobs);
            let job_index = Arc::clone(&self.job_index);
            let client = self.client.clone();
            let event_tx = self.event_tx();
            let run_trigger_tx = self.run_trigger_tx.clone(); // NEW

            tokio::spawn(async move {
                let _permit = permit;
                let _ = event_tx.send(ManagerEvent::JobsChanged);

                if *control_rx.borrow() == JobControl::Cancel {
                    set_status(&jobs, id, JobStatus::Failed("Cancelled".into())).await;
                    remove_from_index(&job_index, id).await;
                    let _ = event_tx.send(ManagerEvent::JobsChanged);
                    let _ = run_trigger_tx.send(()); // NEW: free slot, wake runner
                    return;
                }

                let engine = crate::downloader::engine::DownloadEngine::with_client(client, connections);
                match engine.download(&url, &output_path, Some(downloaded), Some(total_size_atomic), &headers, control_rx).await {
                    Ok((downloaded, total_size)) => {
                        info!("Job #{id} complete — {downloaded}/{total_size} bytes");
                        set_status(&jobs, id, JobStatus::Done).await;
                        {
                            let mut jobs = jobs.write().await;
                            if let Some(job) = jobs.iter_mut().find(|j| j.id == id) {
                                job.set_total_size(total_size);
                            }
                        }
                        remove_from_index(&job_index, id).await;
                        let _ = event_tx.send(ManagerEvent::JobsChanged);
                        let _ = run_trigger_tx.send(()); // NEW: wake runner
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        if err_str.contains("Paused") {
                            info!("Job #{id} paused");
                            set_status(&jobs, id, JobStatus::Paused).await;
                        } else if err_str.contains("Cancelled") {
                            info!("Job #{id} cancelled");
                            set_status(&jobs, id, JobStatus::Failed("Cancelled".into())).await;
                            remove_from_index(&job_index, id).await;
                        } else {
                            error!("Job #{id} failed: {e}");
                            set_status(&jobs, id, JobStatus::Failed(e.to_string())).await;
                            remove_from_index(&job_index, id).await;
                        }
                        let _ = event_tx.send(ManagerEvent::JobsChanged);
                        let _ = run_trigger_tx.send(()); // NEW: wake runner
                    }
                }
            });
        }

        self.emit_event();
    }

    #[allow(dead_code)]
    pub async fn move_up(&self, id: JobId) {
        let mut jobs = self.jobs.write().await;
        if let Some(pos) = jobs.iter().position(|j| j.id == id) {
            if pos > 0 {
                jobs.swap(pos, pos - 1);
                self.job_index.write().await.clear();
                for (i, job) in jobs.iter().enumerate() {
                    self.job_index.write().await.insert(job.id, i);
                }
            }
        }
    }

    // ── Status snapshot ───────────────────────────────────────────────────────

    pub async fn job_summaries(&self) -> Vec<JobSummary> {
        let jobs = self.jobs.read().await;
        jobs.iter()
            .map(|j| {
                let is_active = matches!(j.status, JobStatus::Active);
                if is_active {
                    j.update_speed();
                }
                let speed = if is_active {
                    let sp = j.speed_bps();
                    if sp > 0 { Some(sp) } else { None }
                } else {
                    None
                };
                let now_sys = std::time::SystemTime::now();
                let started_at = j.started_at.map(|i| {
                    let elapsed = i.elapsed();
                    now_sys
                        .checked_sub(elapsed)
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0)
                });
                let ended_at = j.ended_at.map(|i| {
                    let elapsed = i.elapsed();
                    now_sys
                        .checked_sub(elapsed)
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0)
                });
                JobSummary {
                    id: j.id,
                    url: j.url.as_str().to_string(),
                    output_path: j.output_path.to_string_lossy().to_string(),
                    status: j.status.to_string(),
                    total_size: j.total_size_loaded(),
                    downloaded: j.bytes_downloaded(),
                    progress_pct: j.progress_pct(),
                    speed_bps: speed,
                    chunks_total: j.connections,
                    chunks_done: j.chunks_done.load(Ordering::Relaxed),
                    started_at,
                    ended_at,
                    added_at: None,
                    error_msg: None,
                }
            })
            .collect()
    }

    #[allow(dead_code)]
    pub async fn print_status(&self) {
        let jobs = self.jobs.read().await;
        if jobs.is_empty() {
            println!("Queue is empty.");
            return;
        }
        println!("\n{:<5} {:<50} {:<10} {:<8}", "ID", "URL", "STATUS", "PROGRESS");
        println!("{}", "─".repeat(80));
        for job in jobs.iter() {
            let url_ref: &str = &job.url;
            let url_short = if url_ref.len() > 48 {
                format!("{}…", &url_ref[..47])
            } else {
                url_ref.to_string()
            };
            println!(
                "{:<5} {:<50} {:<10} {:.1}%",
                job.id,
                url_short,
                job.status,
                job.progress_pct()
            );
        }
        println!();
    }

    // ── Run loops ─────────────────────────────────────────────────────────────

    #[allow(dead_code)]
    pub async fn run_all(&self) -> Result<()> {
        self.run_all_inner(None).await
    }

    pub async fn run_all_with_db(&self, db: &SqlitePool) -> Result<()> {
        if self.runner_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return Ok(());
        }
        let result = self.run_all_inner(Some(db.clone())).await;
        self.runner_running.store(false, Ordering::Release);
        result
    }

    async fn run_all_inner(&self, db: Option<SqlitePool>) -> Result<()> {
        loop {
            // Find first Pending job and mark Active.
            let job_data = {
                let mut jobs = self.jobs.write().await;
                if let Some(job) = jobs
                    .iter_mut()
                    .find(|j| matches!(j.status, JobStatus::Pending))
                {
                    job.status = JobStatus::Active;
                    job.ended_at = None;
                    if job.started_at.is_none() {
                        job.started_at = Some(std::time::Instant::now());
                    }
                    Some((
                        job.id,
                        job.url.clone(),
                        job.output_path.clone(),
                        job.connections,
                        job.control_rx.clone(),
                        Arc::clone(&job.downloaded),
                        Arc::clone(&job.total_size_atomic),
                        job.headers.clone(),
                    ))
                } else {
                    None
                }
            };

            let (id, url, output_path, connections, control_rx, downloaded, total_size_atomic, headers) =
                match job_data {
                    Some(d) => d,
                    None => break, // no more Pending jobs
                };

            // ── KEY FIX ───────────────────────────────────────────────────────
            // Use try_acquire instead of blocking acquire.
            // Blocking here holds the QueueManager mutex (via &self), which
            // starves Status requests from the TUI → UI freeze.
            // If no permit is available, roll the job back to Pending and exit.
            // The run_trigger_tx channel will re-fire run_all_with_db when a
            // running download completes and a slot opens up.
            let permit = match Arc::clone(&self.semaphore).try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    // Roll back: slot not free yet. Runner re-triggered on completion.
                    let mut jobs = self.jobs.write().await;
                    if let Some(job) = jobs.iter_mut().find(|j| j.id == id) {
                        job.status = JobStatus::Pending;
                        job.started_at = None;
                    }
                    break;
                }
            };
            // ─────────────────────────────────────────────────────────────────

            let jobs = Arc::clone(&self.jobs);
            let job_index = Arc::clone(&self.job_index);
            let client = self.client.clone();
            let db = db.clone();
            let event_tx = self.event_tx();
            let run_trigger_tx = self.run_trigger_tx.clone(); // NEW

            tokio::spawn(async move {
                let _permit = permit; // dropped at end of task → releases semaphore slot

                if let Some(ref pool) = db {
                    let _ = update_job_status(pool, id as i64, "Active", 0, 0).await;
                }
                let _ = event_tx.send(ManagerEvent::JobsChanged);

                if *control_rx.borrow() == JobControl::Cancel {
                    set_status(&jobs, id, JobStatus::Failed("Cancelled".into())).await;
                    remove_from_index(&job_index, id).await;
                    let _ = event_tx.send(ManagerEvent::JobsChanged);
                    if let Some(ref pool) = db {
                        let _ = update_job_status(pool, id as i64, "Cancelled", 0, 0).await;
                    }
                    let _ = run_trigger_tx.send(());
                    return;
                }

                if *control_rx.borrow() == JobControl::Pause {
                    set_status(&jobs, id, JobStatus::Paused).await;
                    let _ = event_tx.send(ManagerEvent::JobsChanged);
                    if let Some(ref pool) = db {
                        let _ = update_job_status(
                            pool,
                            id as i64,
                            "Paused",
                            downloaded.load(Ordering::Relaxed),
                            total_size_atomic.load(Ordering::Relaxed),
                        )
                        .await;
                    }
                    let _ = run_trigger_tx.send(());
                    return;
                }

                let engine = DownloadEngine::with_client(client, connections);
                match engine
                    .download(
                        &url,
                        &output_path,
                        Some(downloaded.clone()),
                        Some(total_size_atomic.clone()),
                        &headers,
                        control_rx,
                    )
                    .await
                {
                    Ok((dl, total_size)) => {
                        info!("Job #{id} complete — {dl}/{total_size} bytes");
                        set_status(&jobs, id, JobStatus::Done).await;
                        {
                            let mut jobs = jobs.write().await;
                            if let Some(job) = jobs.iter_mut().find(|j| j.id == id) {
                                job.set_total_size(total_size);
                            }
                        }
                        remove_from_index(&job_index, id).await;
                        let _ = event_tx.send(ManagerEvent::JobsChanged);
                        if let Some(ref pool) = db {
                            let _ = update_job_status(pool, id as i64, "Done", dl, total_size).await;
                        }
                        // Slot freed — wake runner
                        let _ = run_trigger_tx.send(());
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        if err_str.contains("Paused") {
                            info!("Job #{id} paused");
                            set_status(&jobs, id, JobStatus::Paused).await;
                            let _ = event_tx.send(ManagerEvent::JobsChanged);
                            if let Some(ref pool) = db {
                                let _ = update_job_status(
                                    pool,
                                    id as i64,
                                    "Paused",
                                    downloaded.load(Ordering::Relaxed),
                                    total_size_atomic.load(Ordering::Relaxed),
                                )
                                .await;
                            }
                            // Paused job frees its slot — wake runner for next Pending
                            let _ = run_trigger_tx.send(());
                        } else if err_str.contains("Cancelled") {
                            info!("Job #{id} cancelled");
                            set_status(&jobs, id, JobStatus::Failed("Cancelled".into())).await;
                            remove_from_index(&job_index, id).await;
                            let _ = event_tx.send(ManagerEvent::JobsChanged);
                            if let Some(ref pool) = db {
                                let _ = update_job_status(pool, id as i64, "Cancelled", 0, 0).await;
                            }
                            let _ = run_trigger_tx.send(());
                        } else {
                            error!("Job #{id} failed: {e}");
                            set_status(&jobs, id, JobStatus::Failed(e.to_string())).await;
                            remove_from_index(&job_index, id).await;
                            let _ = event_tx.send(ManagerEvent::JobsChanged);
                            if let Some(ref pool) = db {
                                let _ =
                                    update_job_status(pool, id as i64, &format!("Failed: {e}"), 0, 0)
                                        .await;
                            }
                            let _ = run_trigger_tx.send(());
                        }
                    }
                }
            });
        }

        Ok(())
    }
}

async fn set_status(jobs: &RwLock<VecDeque<DownloadJob>>, id: JobId, status: JobStatus) {
    let mut jobs = jobs.write().await;
    if let Some(job) = jobs.iter_mut().find(|j| j.id == id) {
        match &status {
            JobStatus::Paused | JobStatus::Done | JobStatus::Failed(_) => {
                job.ended_at = Some(std::time::Instant::now());
            }
            _ => {}
        }
        job.status = status;
    }
}

/// Remove job from index map only. Job stays in VecDeque (visible in TUI until session ends).
async fn remove_from_index(job_index: &Arc<RwLock<HashMap<JobId, usize>>>, id: JobId) {
    job_index.write().await.remove(&id);
}