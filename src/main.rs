//! High-performance terminal download manager.
//!
//! shard runs as a background daemon process, accepts commands via Unix socket IPC,
//! and provides a real-time TUI for interactive use. The CLI is parsed by clap.

#![deny(unsafe_code)]

mod cli;
mod config;
mod daemon;
mod db;
mod downloader;
mod ipc;
mod queue;
mod ui;
mod utils;

use anyhow::Result;
use clap::Parser;
use tracing_appender::non_blocking::WorkerGuard;

use crate::cli::Cli;

fn init_logging() -> WorkerGuard {
    let log_dir = ".logs";

    std::fs::create_dir_all(log_dir).ok();

    // Clear log for fresh run
    let log_path = format!("{}/shard.log", log_dir);
    let _ = std::fs::File::create(&log_path);

    let file_appender = tracing_appender::rolling::never(log_dir, "shard.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_ansi(false)
        .init();

    guard
}

fn main() -> Result<()> {
    let _guard = init_logging();

    tokio::runtime::Runtime::new()?.block_on(async {
        let cli = Cli::parse();
        cli.run().await
    })
}
 