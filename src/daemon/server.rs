// src/daemon/server.rs
//
// Listens on a Unix domain socket.
// Each incoming client connection is handled in its own tokio task.
// Uses newline-delimited JSON (one Request line in, one Response line out).

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

    // Ensure parent directory exists.
    if let Some(parent) = sock_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Remove stale socket file from previous run (if any).
    if sock_path.exists() {
        tokio::fs::remove_file(&sock_path).await?;
    }

    // Open DB and build shared state.
    let db = open_db().await?;

    // Enable WAL mode for better concurrent performance.
    enable_wal_mode(&db).await?;

    // Mark Active jobs as Failed (they were interrupted by daemon death).
    // Must run BEFORE migration so migrate_db moves them to history.
    fail_active_jobs(&db).await?;

    // Run migration: create indexes, move terminal rows to history.
    migrate_db(&db).await?;

    // Load settings and prune old history.
    let settings = Settings::load()?;
    if settings.prune_enabled {
        let count = prune_history(&db, settings.prune_days, &settings.prune_statuses).await?;
        if count > 0 {
            info!("Pruned {} old jobs from history", count);
        }
    }

    let manager = Arc::new(Mutex::new(QueueManager::new(max_concurrent)));

    // Load pending jobs from DB into queue (do NOT auto-start).
    let pending = load_pending_jobs(&db).await?;
    let pending_count = pending.len();
    if pending_count > 0 {
        let mgr = manager.lock().await;
        for job in &pending {
            let initial_status = match job.status.as_str() {
                "Paused" => Some(JobStatus::Paused),
                _ => None,
            };
            let (initial_started_at, initial_ended_at) = persisted_instants(
                job.started_at,
                job.ended_at,
            );

            // Recover total_size from .part file for paused jobs with unknown size.
            // Happens when previous buggy version saved Paused with 0 bytes.
            let recovered_size = if job.status == "Paused" && job.total_size == 0 {
                let path: std::path::PathBuf = job.output_path.clone().into();
                match load_state(&path).await {
                    Some(state) => {
                        info!("Recovered size from .part for job #{}: {} bytes", job.id, state.total_size);
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

            // Persist recovered size back to DB so next restart doesn't need .part
            if recovered_size != job.total_size {
                let _ = update_job_status(
                    &db,
                    job.id as i64,
                    job.status.as_str(),
                    job.downloaded,
                    recovered_size,
                ).await;
            }
        }
        info!("Loaded {pending_count} pending jobs from DB");
    }
    let event_tx = {
        let mgr = manager.lock().await;
        mgr.event_tx()
    };

    info!("Daemon listening on {}", sock_path.display());
    let listener = UnixListener::bind(&sock_path)?;

    // Shared shutdown flag.
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

                            // Read first request line.
                            match lines.next_line().await {
                                Ok(Some(line)) => {
                                    // Check if this is a subscribe request
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

                                        // After initial full state, push updates on change
                                        let mut last_jobs: Option<Vec<crate::ipc::message::JobSummary>> = None;

                                        loop {
                                            let mut got_event = false;
                                            let _ = got_event; // silence unused warning if we don't use it in all branches

                                            tokio::select! {
                                                // Check for events from manager
                                                _ = event_rx.recv() => {}

                                                // Periodic check for progress even without events
                                                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                                                    got_event = true;
                                                }

                                                // Check for incoming commands from client
                                                cmd = lines.next_line() => {
                                                    match cmd {
                                                        Ok(Some(cmd_line)) => {
                                                            // Handle pause/cancel/etc on this connection
                                                            let response = handle_request(
                                                                &cmd_line,
                                                                &manager,
                                                                &db,
                                                                &shutdown_tx,
                                                            ).await;

                                                            let mut out = serde_json::to_string(&response)
                                                                .unwrap_or_else(|_| r#"{"type":"error","message":"serialization failed"}"#.into());
                                                            out.push('\n');

                                                            // Push the updated state after command
                                                            if let Err(e) = writer.write_all(out.as_bytes()).await {
                                                                error!("Failed to write command response: {e}");
                                                                break;
                                                            }
                                                            got_event = true;
                                                        }
                                                        Ok(None) => break, // client disconnected
                                                        Err(e) => {
                                                            error!("Failed to read command: {e}");
                                                            break;
                                                        }
                                                    }
                                                }
                                            }

                                            // After any event/command/timer, check state and push update
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
                                        // One-shot request (existing behavior)
                                        let response = handle_request(
                                            &line,
                                            &manager,
                                            &db,
                                            &shutdown_tx,
                                        )
                                        .await;

                                        // Write response back.
                                        let mut out = serde_json::to_string(&response)
                                            .unwrap_or_else(|_| r#"{"type":"error","message":"serialization failed"}"#.into());
                                        out.push('\n');

                                        if let Err(e) = writer.write_all(out.as_bytes()).await {
                                            error!("Failed to write response: {e}");
                                        }
                                    }
                                }
                                Ok(None) => {} // client disconnected early
                                Err(e) => error!("Failed to read request: {e}"),
                            }
                        });
                    }
                    Err(e) => error!("Accept error: {e}"),
                }
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

    // Archive terminal jobs before cleanup.
    if let Err(e) = archive_jobs(&db).await {
        error!("Failed to archive jobs on shutdown: {e}");
    } else {
        info!("Archived terminal jobs to history");
    }

    // Clean up socket file on exit.
    let _ = tokio::fs::remove_file(&sock_path).await;
    Ok(())
}

/// Convert epoch seconds to Instant for restored in-memory job timestamps.
fn persisted_instants(started_at: Option<i64>, ended_at: Option<i64>) -> (Option<Instant>, Option<Instant>) {
    let now = Instant::now();
    let sys_now = SystemTime::now();

    let started = started_at.and_then(|epoch| {
        let then = SystemTime::UNIX_EPOCH + Duration::from_secs(epoch as u64);
        sys_now.duration_since(then).ok().map(|elapsed| now - elapsed)
    });

    let ended = ended_at.and_then(|epoch| {
        let then = SystemTime::UNIX_EPOCH + Duration::from_secs(epoch as u64);
        sys_now.duration_since(then).ok().map(|elapsed| now - elapsed)
    });

    (started, ended)
}