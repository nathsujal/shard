//! Unix socket listener, subscribe handling, daemon startup/shutdown lifecycle.

use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::config::Settings;
use crate::db::schema::{
    archive_jobs, enable_wal_mode, fail_active_jobs, load_pending_jobs, migrate_db, open_db,
    prune_history, update_job_status,
};
use crate::downloader::resume::load_state;
use crate::ipc::message::Response;
use crate::queue::job::JobStatus;
use crate::queue::manager::QueueManager;

use super::handler::handle_request;

/// Path to the Unix domain socket file.
pub fn socket_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("shard")
        .join("shard.sock")
}

/// Start the daemon. Blocks forever until SIGINT or a Shutdown request.
pub async fn run_daemon(max_concurrent: usize) -> Result<()> {
    let sock_path = socket_path();

    if let Some(parent) = sock_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    if sock_path.exists() {
        tokio::fs::remove_file(&sock_path).await?;
    }

    let db = open_db().await?;
    enable_wal_mode(&db).await?;
    fail_active_jobs(&db).await?;
    migrate_db(&db).await?;

    let settings = Settings::load()?;
    if settings.prune_enabled {
        let count = prune_history(&db, settings.prune_days, &settings.prune_statuses).await?;
        if count > 0 {
            info!("Pruned {} old jobs from history", count);
        }
    }

    let manager = Arc::new(Mutex::new(QueueManager::new(max_concurrent)));

    // Load pending jobs from DB into queue.
    let pending = load_pending_jobs(&db).await?;
    let pending_count = pending.len();
    if pending_count > 0 {
        let mgr = manager.lock().await;
        for job in &pending {
            let initial_status = match job.status.as_str() {
                "Paused" => Some(JobStatus::Paused),
                _ => None,
            };
            let (initial_started_at, initial_ended_at) =
                persisted_instants(job.started_at, job.ended_at);

            let recovered_size = if job.status == "Paused" && job.total_size == 0 {
                let path: std::path::PathBuf = job.output_path.clone().into();
                match load_state(&path).await {
                    Some(state) => {
                        info!(
                            "Recovered size from .part for job #{}: {} bytes",
                            job.id, state.total_size
                        );
                        state.total_size
                    }
                    None => job.total_size,
                }
            } else {
                job.total_size
            };

            mgr.add_with_id(
                job.id,
                job.url.clone(),
                job.output_path.clone().into(),
                job.chunks_total,
                vec![],
                initial_status,
                recovered_size,
                job.downloaded,
                initial_started_at,
                initial_ended_at,
            )
            .await;

            if recovered_size != job.total_size {
                let _ = update_job_status(
                    &db,
                    job.id as i64,
                    job.status.as_str(),
                    job.downloaded,
                    recovered_size,
                )
                .await;
            }
        }
        info!("Loaded {pending_count} pending jobs from DB");
    }

    // Grab channel senders while holding manager lock, then release.
    let (event_tx, run_trigger_tx) = {
        let mgr = manager.lock().await;
        (mgr.event_tx(), mgr.run_trigger_tx())
    };

    // ── NEW: subscribe to run_trigger so we can re-start the runner when a
    //         download slot frees up and Pending jobs are still waiting.
    let mut run_trigger_rx = run_trigger_tx.subscribe();

    info!("Daemon listening on {}", sock_path.display());
    let listener = UnixListener::bind(&sock_path)?;

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    loop {
        tokio::select! {
            // Accept a new client connection.
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => {
                        let manager = Arc::clone(&manager);
                        let db = db.clone();
                        let shutdown_tx = shutdown_tx.clone();
                        let mut event_rx = event_tx.subscribe();

                        tokio::spawn(async move {
                            let (reader, mut writer) = stream.into_split();
                            let mut lines = BufReader::new(reader).lines();

                            match lines.next_line().await {
                                Ok(Some(line)) => {
                                    if line.contains("\"subscribe\"") {
                                        // Persistent connection for subscriber
                                        let sub_response = serde_json::to_string(&Response::Subscribed)
                                            .unwrap_or_else(|_| r#"{"type":"error","message":"serialization failed"}"#.into());
                                        let mut out = sub_response;
                                        out.push('\n');
                                        if let Err(e) = writer.write_all(out.as_bytes()).await {
                                            error!("Failed to write subscribe response: {e}");
                                            return;
                                        }

                                        let mut last_jobs: Option<Vec<crate::ipc::message::JobSummary>> = None;

                                        loop {
                                            let mut got_event = false;
                                            let _ = got_event;

                                            tokio::select! {
                                                _ = event_rx.recv() => {}

                                                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                                                    got_event = true;
                                                }

                                                cmd = lines.next_line() => {
                                                    match cmd {
                                                        Ok(Some(cmd_line)) => {
                                                            let response = handle_request(
                                                                &cmd_line,
                                                                &manager,
                                                                &db,
                                                                &shutdown_tx,
                                                            ).await;

                                                            let mut out = serde_json::to_string(&response)
                                                                .unwrap_or_else(|_| r#"{"type":"error","message":"serialization failed"}"#.into());
                                                            out.push('\n');

                                                            if let Err(e) = writer.write_all(out.as_bytes()).await {
                                                                error!("Failed to write command response: {e}");
                                                                break;
                                                            }
                                                            got_event = true;
                                                        }
                                                        Ok(None) => break,
                                                        Err(e) => {
                                                            error!("Failed to read command: {e}");
                                                            break;
                                                        }
                                                    }
                                                }
                                            }

                                            if got_event {
                                                let mgr = manager.lock().await;
                                                let jobs = mgr.job_summaries().await;
                                                let changed = match &last_jobs {
                                                    Some(prev) => &jobs != prev,
                                                    None => true,
                                                };
                                                if changed {
                                                    let update = Response::Update { jobs: jobs.clone() };
                                                    let mut json = serde_json::to_string(&update)
                                                        .unwrap_or_else(|_| r#"{"type":"error","message":"serialization failed"}"#.into());
                                                    json.push('\n');
                                                    if let Err(e) = writer.write_all(json.as_bytes()).await {
                                                        error!("Failed to push update: {e}");
                                                        break;
                                                    }
                                                    last_jobs = Some(jobs);
                                                }
                                            }
                                        }
                                    } else {
                                        // One-shot request
                                        let response = handle_request(
                                            &line,
                                            &manager,
                                            &db,
                                            &shutdown_tx,
                                        )
                                        .await;

                                        let mut out = serde_json::to_string(&response)
                                            .unwrap_or_else(|_| r#"{"type":"error","message":"serialization failed"}"#.into());
                                        out.push('\n');

                                        if let Err(e) = writer.write_all(out.as_bytes()).await {
                                            error!("Failed to write response: {e}");
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => error!("Failed to read request: {e}"),
                            }
                        });
                    }
                    Err(e) => error!("Accept error: {e}"),
                }
            }

            // ── NEW: a download task completed and fired run_trigger_tx.
            //         Re-run the queue runner so any Pending jobs pick up the freed slot.
            //         This is the fix for the deadlock: run_all_inner now uses
            //         try_acquire (non-blocking), so it exits immediately when slots are
            //         full. This arm re-triggers it once a slot actually frees.
            _ = run_trigger_rx.recv() => {
                let manager = Arc::clone(&manager);
                let db = db.clone();
                tokio::spawn(async move {
                    let mgr = manager.lock().await;
                    if let Err(e) = mgr.run_all_with_db(&db).await {
                        error!("re-trigger run_all error: {e}");
                    }
                });
            }

            // Shutdown signal received.
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("Daemon shutting down.");
                    break;
                }
            }
        }
    }

    if let Err(e) = archive_jobs(&db).await {
        error!("Failed to archive jobs on shutdown: {e}");
    } else {
        info!("Archived terminal jobs to history");
    }

    let _ = tokio::fs::remove_file(&sock_path).await;
    Ok(())
}

/// Convert epoch seconds to Instant for restored in-memory job timestamps.
fn persisted_instants(
    started_at: Option<i64>,
    ended_at: Option<i64>,
) -> (Option<Instant>, Option<Instant>) {
    let now = Instant::now();
    let sys_now = SystemTime::now();

    let started = started_at.and_then(|epoch| {
        let then = SystemTime::UNIX_EPOCH + Duration::from_secs(epoch as u64);
        sys_now
            .duration_since(then)
            .ok()
            .map(|elapsed| now - elapsed)
    });

    let ended = ended_at.and_then(|epoch| {
        let then = SystemTime::UNIX_EPOCH + Duration::from_secs(epoch as u64);
        sys_now
            .duration_since(then)
            .ok()
            .map(|elapsed| now - elapsed)
    });

    (started, ended)
}