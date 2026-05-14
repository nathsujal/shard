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
 
use crate::cli::Cli;
 
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
 
    let cli = Cli::parse();
    cli.run().await
}
 