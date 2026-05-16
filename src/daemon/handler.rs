// Parses incoming JSON request, calls the right QueueManager method,
// persists changes to DB, and returns a Response.

use std::path::PathBuf;
use std::sync::Arc;

use sqlx::SqlitePool;
use tokio::sync::{watch, Mutex};
use tracing::{error, info};

use crate::config::Settings;
use crate::db::schema::{insert_job, load_history, prune_history, update_job_status};
use crate::ipc::message::{JobSummary, Request, Response};
use crate::queue::manager::QueueManager;
use crate::utils::filename_from_url;

/// Parse `line` as a JSON Request, execute it, return a Response.
pub async fn handle_request(
    line: &str,
    manager: &Arc<Mutex<QueueManager>>,
    db: &SqlitePool,
    shutdown_tx: &watch::Sender<bool>,
) -> Response {
    let request: Request = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            return Response::Error {
                message: format!("Bad request JSON: {e}"),
            }
        }
    };

    match request {
        // Add URLs
        Request::Add {
                urls,
                output_dir,
                connections,
                headers,
            } => {
                let base = if let Some(dir) = output_dir {
                    PathBuf::from(dir)
                } else {
                    Settings::load()
                        .unwrap_or_default()
                        .download_dir
                };
            let mut ids = Vec::new();

            for url in &urls {
                let filename = filename_from_url(url);
                let output_path = base.join(&filename);
                let output_str = output_path.to_string_lossy().to_string();

                // Persist to DB first — get stable integer ID.
                let db_id = match insert_job(db, url, &output_str, connections).await {
                    Ok(id) => id,
                    Err(e) => {
                        error!("DB insert failed for {url}: {e}");
                        return Response::Error {
                            message: format!("DB error: {e}"),
                        };
                    }
                };

                // Add to in-memory queue using the DB id so they stay in sync.
                {
                    let mgr = manager.lock().await;
                    mgr.add_with_id(db_id as u64, url.clone(), output_path, connections, headers.clone(), None, 0, 0, None, None)
                        .await;
                }

                info!("Queued #{db_id}: {url}");
                ids.push(db_id as u64);
            }

            // Kick off downloads in background (non-blocking).
            let manager_clone = Arc::clone(manager);
            let db_clone = db.clone();
            tokio::spawn(async move {
                let mgr = manager_clone.lock().await;
                if let Err(e) = mgr.run_all_with_db(&db_clone).await {
                    error!("run_all error: {e}");
                }
            });

            Response::Queued { ids }
        }

        // Pause
        Request::Pause { id } => {
            let mgr = manager.lock().await;
            mgr.pause(id).await;

            if let Err(e) = update_job_status(db, id as i64, "Paused", 0, 0).await {
                error!("DB update failed on pause: {e}");
            }

            Response::Ok {
                message: format!("Job #{id} paused"),
            }
        }

        Request::Resume { id } => {
            let mgr = manager.lock().await;
            mgr.resume(id).await;

            if let Err(e) = update_job_status(db, id as i64, "Active", 0, 0).await {
                error!("DB update failed on resume: {e}");
            }

            Response::Ok {
                message: format!("Job #{id} resumed"),
            }
        }

        // Cancel
        Request::Cancel { id } => {
            let mgr = manager.lock().await;
            mgr.cancel(id).await;

            if let Err(e) = update_job_status(db, id as i64, "Cancelled", 0, 0).await {
                error!("DB update status failed on cancel: {e}");
            }

            Response::Ok {
                message: format!("Job #{id} cancelled"),
            }
        }

        // Status
        Request::Status { all } => {
            let mgr = manager.lock().await;
            let mut jobs: Vec<JobSummary> = mgr.job_summaries().await;

            if !all {
                // Default: hide Done, Failed, Cancelled
                jobs.retain(|j| {
                    j.status != "Done"
                        && j.status != "Cancelled"
                        && !j.status.starts_with("Failed")
                });
            }

            Response::JobList { jobs }
        }

        // Shutdown
        Request::Shutdown => {
            let _ = shutdown_tx.send(true);
            Response::Ok {
                message: "Daemon shutting down".into(),
            }
        }

        // Subscribe / Unsubscribe handled directly in server for persistent connections
        Request::Subscribe { .. } | Request::Unsubscribe => {
            Response::Error {
                message: "Subscribe only valid over persistent connection".into(),
            }
        }

        // History
        Request::History { limit } => {
            let jobs = match load_history(db, limit).await {
                Ok(jobs) => jobs,
                Err(e) => {
                    error!("Failed to load history: {e}");
                    return Response::Error {
                        message: format!("Failed to load history: {e}"),
                    };
                }
            };
            Response::JobList { jobs }
        }

        // Prune
        Request::Prune {
            days,
            statuses,
            dry_run,
        } => {
            if dry_run {
                return Response::PruneResult {
                    count: 0,
                    message: format!("Dry run — would prune history older than {days} days with statuses {statuses:?}"),
                };
            }
            match prune_history(db, days, &statuses).await {
                Ok(count) => Response::PruneResult {
                    count,
                    message: format!("Pruned {count} old jobs from history"),
                },
                Err(e) => Response::Error {
                    message: format!("Prune failed: {e}"),
                },
            }
        }
    }
}