use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
 
use crate::config::Settings;
use crate::daemon::server::run_daemon;
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
 
    /// Shut the daemon down
    Shutdown,
 
    /// Show current config
    Config,
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
                engine.download(&url, &output_path).await?;
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
            } => {
                let output_dir = dir
                    .unwrap_or_else(|| settings.download_dir.clone())
                    .to_string_lossy()
                    .to_string();
 
                let response = send_request(&Request::Add {
                    urls,
                    output_dir,
                    connections,
                })
                .await?;
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
 
            Commands::Tui => {
                run_tui().await?;
            }
 
            Commands::Shutdown => {
                let response = send_request(&Request::Shutdown).await?;
                print_response(response);
            }
 
            Commands::Config => {
                println!("{:#?}", settings);
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
        Response::Error { message } => eprintln!("Error: {message}"),
    }
}