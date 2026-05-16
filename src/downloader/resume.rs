//! Resume state persistence: `.part` file format for pause/resume across daemon restarts.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::fs;

/// Persisted state for a partial download.
#[derive(Debug, Serialize, Deserialize)]
pub struct PartState {
    pub url: String,
    pub total_size: u64,
    pub num_chunks: u8,
    /// Indices of chunks already fully written to disk.
    pub completed_chunks: HashSet<usize>,
}

impl PartState {
    pub fn new(url: &str, total_size: u64, num_chunks: u8) -> Self {
        Self {
            url: url.to_string(),
            total_size,
            num_chunks,
            completed_chunks: HashSet::new(),
        }
    }

    pub fn mark_done(&mut self, index: usize) {
        self.completed_chunks.insert(index);
    }

    pub fn pending_indices(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.num_chunks as usize)
            .filter(|i| !self.completed_chunks.contains(i))
    }
}

/// `.part` file lives alongside the output file: `file.zip` → `file.zip.part`
pub fn part_path(output_path: &Path) -> PathBuf {
    let mut p = output_path.to_path_buf();
    let mut name = p.file_name().unwrap_or_default().to_os_string();
    name.push(".part");
    p.set_file_name(name);
    p
}

pub async fn load_state(output_path: &Path) -> Option<PartState> {
    let path = part_path(output_path);
    let contents = fs::read_to_string(&path).await.ok()?;
    serde_json::from_str(&contents).ok()
}

pub async fn save_state(output_path: &Path, state: &PartState) -> Result<()> {
    let path = part_path(output_path);
    let contents = serde_json::to_string(state)?;
    fs::write(&path, contents).await?;
    Ok(())
}

pub async fn delete_state(output_path: &Path) {
    let path = part_path(output_path);
    let _ = fs::remove_file(path).await;
}