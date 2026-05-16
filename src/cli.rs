use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;
use tracing::error;
 
use crate::config::Settings;
use crate::daemon::server::{run_daemon, socket_path};
use crate::downloader::engine::DownloadEngine;
use crate::ipc::client::send_request;
use crate::ipc::message::{Request, Response};
use crate::ui::run_tui;
use crate::utils::filename_from_url;
 
#[derive(Parser)]
#[command(name = "shard", about = "High-performance terminal download manager")]
#[command(version, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}
 
#[derive(Subcommand)]
pub enum Commands {
    /// Download a single file immediately (bypasses queue and daemon)
    Get {
        url: String,
        #[arg(short, long)]
        output: Option<PathBuf>,
        #[arg(short = 'n', long, default_value = "4")]
        connections: u8,
    },
 
    /// Start the background daemon (run this first)
    Daemon {
        /// Max simultaneous downloads
        #[arg(short, long, default_value = "3")]
        concurrent: usize,
    },
 
    /// Add one or more URLs to the queue
    Add {
        urls: Vec<String>,
        #[arg(short, long)]
        dir: Option<PathBuf>,
        #[arg(short = 'n', long, default_value = "4")]
        connections: u8,
        /// Custom headers (format: "Key: Value")
        #[arg(short = 'H', long)]
        header: Vec<String>,
    },
 
    /// Show current queue status (active and pending only)
    Queue {
        /// Also show Done, Failed, and Cancelled jobs
        #[arg(long)]
        all: bool,
    },
 
    /// Pause a running job by ID
    Pause { id: u64 },
 
    /// Cancel a job by ID
    Cancel { id: u64 },
 
    /// Open the live TUI (daemon must be running)
    Tui,

    /// Dev mode: auto-start daemon, open TUI, kill daemon on exit
    Dev {
        /// Max simultaneous downloads
        #[arg(short, long, default_value = "3")]
        concurrent: usize,
    },
 
    /// Shut the daemon down
    Shutdown,
 
    /// Show current config
    Config,

    /// Show completed job history
    History {
        /// How many recent entries to show
        #[arg(long, default_value = "50")]
        limit: u32,
    },

    /// Prune old job history
    Prune {
        /// Only show what would be deleted (no actual deletion)
        #[arg(long)]
        dry_run: bool,
        /// Override configured retention days
        #[arg(long)]
        days: Option<u32>,
    },
}
 
impl Cli {
    pub async fn run(self) -> Result<()> {
        let settings = Settings::load()?;
 
        match self.command {
            // ── Direct download — no daemon needed ───────────────────────────
            Commands::Get {
                url,
                output,
                connections,
            } => {
                let output_path = resolve_output(&url, output, &settings.download_dir);
                let engine = DownloadEngine::new(connections);
                use tokio::sync::watch;
                let (_tx, rx) = watch::channel(crate::queue::job::JobControl::Run);
                engine.download(&url, &output_path, None, None, &[], rx).await?;
            }
 
            // ── Start daemon — blocks until shutdown ─────────────────────────
            Commands::Daemon { concurrent } => {
                println!("Starting shard daemon (max {} concurrent)…", concurrent);
                run_daemon(concurrent).await?;
            }
 
// ── IPC commands — thin clients, send request and print result ───
            Commands::Add {
                urls,
                dir,
                connections,
                header,
            } => {
                let output_dir = dir
                    .unwrap_or_else(|| settings.download_dir.clone())
                    .to_string_lossy()
                    .to_string();

                let headers: Vec<(String, String)> = header
                    .iter()
                    .filter_map(|h| {
                        let parts: Vec<&str> = h.splitn(2, ':').collect();
                        if parts.len() == 2 {
                            Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
                        } else {
                            eprintln!("Warning: invalid header format '{}', expected 'Key: Value'", h);
                            None
                        }
                    })
                    .collect();

                let response = send_request(&Request::Add {
                    urls,
                    output_dir: Some(output_dir),
                    connections,
                    headers,
                })
                .await?;
                print_response(response);
            }

            Commands::Tui => {
                run_tui().await?;
            }

            Commands::Dev { concurrent } => {
                std::env::set_var("SHARD_TUI_MODE", "1");
                let sock_path = socket_path();

                // Always start fresh daemon — clean stale socket first
                if sock_path.exists() {
                    let _ = std::fs::remove_file(&sock_path);
                }

                println!("Starting dev daemon…");
                let mut handle = tokio::spawn(async move {
                    let _ = run_daemon(concurrent).await;
                });

                for _ in 0..50 {
                    if sock_path.exists() { break; }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                if !sock_path.exists() {
                    eprintln!("Error: daemon failed to start");
                    handle.abort();
                    return Ok(());
                }

                let tui_result = run_tui().await;

                println!("Shutting down dev daemon…");
                // Graceful shutdown via IPC so archive_jobs() runs
                if let Err(e) = send_request(&Request::Shutdown).await {
                    error!("Shutdown IPC failed (daemon may already be gone): {e}");
                    handle.abort();
                } else {
                    tokio::time::timeout(Duration::from_secs(3), &mut handle).await.ok();
                }
                let _ = tokio::fs::remove_file(&sock_path).await;

                tui_result?;
            }
 
            Commands::Shutdown => {
                let response = send_request(&Request::Shutdown).await?;
                print_response(response);
            }

            Commands::Queue { all } => {
                let response = send_request(&Request::Status { all }).await?;
                print_response(response);
            }

            Commands::Pause { id } => {
                let response = send_request(&Request::Pause { id }).await?;
                print_response(response);
            }

            Commands::Cancel { id } => {
                let response = send_request(&Request::Cancel { id }).await?;
                print_response(response);
            }

            Commands::Config => {
                println!("{:#?}", settings);
            }

            Commands::History { limit } => {
                let response = send_request(&Request::History { limit }).await?;
                print_response(response);
            }

            Commands::Prune { dry_run, days } => {
                let days = days.unwrap_or(settings.prune_days);
                let statuses = settings.prune_statuses.clone();
                let response = send_request(&Request::Prune {
                    days,
                    statuses,
                    dry_run,
                })
                .await?;
                print_response(response);
            }
        }
 
        Ok(())
    }
}
 
// ── Helpers ──────────────────────────────────────────────────────────────────
 
fn resolve_output(url: &str, output: Option<PathBuf>, download_dir: &PathBuf) -> PathBuf {
    output.unwrap_or_else(|| download_dir.join(filename_from_url(url)))
}
 
/// Pretty-print daemon responses to stdout.
fn print_response(response: Response) {
    match response {
        Response::Queued { ids } => {
            for id in ids {
                println!("Queued #{id}");
            }
        }
        Response::Ok { message } => println!("{message}"),
        Response::JobList { jobs } => {
            if jobs.is_empty() {
                println!("Queue is empty.");
                return;
            }
            println!("\n{:<5} {:<50} {:<12} {:<8}", "ID", "URL", "STATUS", "PROGRESS");
            println!("{}", "─".repeat(82));
            for job in &jobs {
                let url_short = if job.url.len() > 48 {
                    format!("{}…", &job.url[..47])
                } else {
                    job.url.clone()
                };
                println!(
                    "{:<5} {:<50} {:<12} {:.1}%",
                    job.id, url_short, job.status, job.progress_pct
                );
            }
            println!();
        }
        Response::PruneResult { count: _, message } => {
            println!("{message}");
        }
        Response::Error { message } => eprintln!("Error: {message}"),
        Response::Subscribed => println!("Subscribed to real-time updates"),
        Response::Update { jobs } => {
            if jobs.is_empty() {
                println!("Queue is empty.");
                return;
            }
            println!("\n{:<5} {:<50} {:<12} {:<8}", "ID", "URL", "STATUS", "PROGRESS");
            println!("{}", "─".repeat(82));
            for job in &jobs {
                let url_short = if job.url.len() > 48 {
                    format!("{}…", &job.url[..47])
                } else {
                    job.url.clone()
                };
                println!(
                    "{:<5} {:<50} {:<12} {:.1}%",
                    job.id, url_short, job.status, job.progress_pct
                );
            }
            println!();
        }
    }
}