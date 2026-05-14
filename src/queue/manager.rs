use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use reqwest::Client;
use sqlx::SqlitePool;
use tokio::sync::{Mutex, Semaphore};
use tracing::{error, info};

use crate::db::schema::update_job_status;
use crate::downloader::engine::DownloadEngine;
use crate::ipc::message::JobSummary;

use super::job::{DownloadJob, JobControl, JobId, JobStatus};

pub struct QueueManager {
    client: Client,
    jobs: Arc<Mutex<VecDeque<DownloadJob>>>,
    semaphore: Arc<Semaphore>,
    next_id: Arc<AtomicU64>,
    max_concurrent: usize,
}

impl QueueManager {
    pub fn new(max_concurrent: usize) -> Self {
        let client = Client::builder()
            .user_agent("shard/1.0")
            .build()
            .expect("failed to build HTTP client");

        Self {
            client,
            jobs: Arc::new(Mutex::new(VecDeque::new())),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            next_id: Arc::new(AtomicU64::new(1)),
            max_concurrent,
        }
    }

    // ── Job addition ─────────────────────────────────────────────────────────

    pub async fn add(&self, url: String, output_path: PathBuf, connections: u8) -> JobId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.add_with_id(id, url, output_path, connections).await;
        id
    }

    pub async fn add_with_id(
        &self,
        id: JobId,
        url: String,
        output_path: PathBuf,
        connections: u8,
    ) {
        let job = DownloadJob::new(id, url.clone(), output_path, connections);
        info!("Queued job #{id}: {url}");
        self.jobs.lock().await.push_back(job);
        self.next_id.fetch_max(id + 1, Ordering::Relaxed);
    }

    // ── Control signals ───────────────────────────────────────────────────────

    pub async fn pause(&self, id: JobId) {
        let jobs = self.jobs.lock().await;
        if let Some(job) = jobs.iter().find(|j| j.id == id) {
            job.send_control(JobControl::Pause);
            info!("Paused job #{id}");
        }
    }

    pub async fn cancel(&self, id: JobId) {
        let jobs = self.jobs.lock().await;
        if let Some(job) = jobs.iter().find(|j| j.id == id) {
            job.send_control(JobControl::Cancel);
            info!("Cancelled job #{id}");
        }
    }

    #[allow(dead_code)]
    pub async fn move_up(&self, id: JobId) {
        let mut jobs = self.jobs.lock().await;
        if let Some(pos) = jobs.iter().position(|j| j.id == id) {
            if pos > 0 {
                jobs.swap(pos, pos - 1);
            }
        }
    }

    // ── Status snapshot ───────────────────────────────────────────────────────

    pub async fn job_summaries(&self) -> Vec<JobSummary> {
        let jobs = self.jobs.lock().await;
        jobs.iter()
            .map(|j| JobSummary {
                id: j.id,
                url: j.url.clone(),
                output_path: j.output_path.to_string_lossy().to_string(),
                status: j.status.to_string(),
                total_size: j.total_size,
                downloaded: j.bytes_downloaded(),
                progress_pct: j.progress_pct(),
            })
            .collect()
    }

    pub async fn print_status(&self) {
        let jobs = self.jobs.lock().await;
        if jobs.is_empty() {
            println!("Queue is empty.");
            return;
        }
        println!("\n{:<5} {:<50} {:<10} {:<8}", "ID", "URL", "STATUS", "PROGRESS");
        println!("{}", "─".repeat(80));
        for job in jobs.iter() {
            let url_short = if job.url.len() > 48 {
                format!("{}…", &job.url[..47])
            } else {
                job.url.clone()
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

    pub async fn run_all(&self) -> Result<()> {
        self.run_all_inner(None).await
    }

    pub async fn run_all_with_db(&self, db: &SqlitePool) -> Result<()> {
        self.run_all_inner(Some(db.clone())).await
    }

    async fn run_all_inner(&self, db: Option<SqlitePool>) -> Result<()> {
        loop {
            let job_data = {
                let mut jobs = self.jobs.lock().await;
                jobs.iter_mut()
                    .find(|j| j.status == JobStatus::Pending)
                    .map(|j| {
                        j.status = JobStatus::Active;
                        (
                            j.id,
                            j.url.clone(),
                            j.output_path.clone(),
                            j.connections,
                            j.control_rx.clone(),
                        )
                    })
            };

            let (id, url, output_path, connections, control_rx) = match job_data {
                Some(d) => d,
                None => break,
            };

            let permit = Arc::clone(&self.semaphore).acquire_owned().await?;
            let jobs = Arc::clone(&self.jobs);
            let client = self.client.clone();
            let db = db.clone();

            tokio::spawn(async move {
                let _permit = permit;

                if let Some(ref pool) = db {
                    let _ = update_job_status(pool, id as i64, "Active", 0, 0).await;
                }

                if *control_rx.borrow() == JobControl::Cancel {
                    set_status(&jobs, id, JobStatus::Failed("Cancelled".into())).await;
                    if let Some(ref pool) = db {
                        let _ = update_job_status(pool, id as i64, "Cancelled", 0, 0).await;
                    }
                    return;
                }

                let engine = DownloadEngine::with_client(client, connections);
                match engine.download(&url, &output_path).await {
                    Ok((downloaded, total_size)) => {
                        info!("Job #{id} complete — {downloaded}/{total_size} bytes");
                        set_status(&jobs, id, JobStatus::Done).await;
                        if let Some(ref pool) = db {
                            let _ = update_job_status(
                                pool, id as i64, "Done", downloaded, total_size,
                            ).await;
                        }
                    }
                    Err(e) => {
                        error!("Job #{id} failed: {e}");
                        set_status(&jobs, id, JobStatus::Failed(e.to_string())).await;
                        if let Some(ref pool) = db {
                            let _ = update_job_status(
                                pool,
                                id as i64,
                                &format!("Failed: {e}"),
                                0,
                                0,
                            ).await;
                        }
                    }
                }
            });
        }

        for _ in 0..self.max_concurrent {
            let _ = self.semaphore.acquire().await;
        }

        Ok(())
    }
}

async fn set_status(jobs: &Mutex<VecDeque<DownloadJob>>, id: JobId, status: JobStatus) {
    let mut jobs = jobs.lock().await;
    if let Some(job) = jobs.iter_mut().find(|j| j.id == id) {
        job.status = status;
    }
}