use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub struct Settings {
    /// Default download directory
    pub download_dir: PathBuf,

    /// Max parallel connections per download
    pub max_connections: u8,

    /// Speed limit in bytes/sec (0 = unlimited)
    pub speed_limit: u64,

    /// Auto-prune completed jobs on daemon startup
    pub prune_enabled: bool,

    /// Keep job history for N days (older deleted on prune)
    pub prune_days: u32,

    /// Which statuses to prune (e.g. ["Done", "Failed"])
    pub prune_statuses: Vec<String>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            download_dir: dirs_next(),
            max_connections: 4,
            speed_limit: 0,
            prune_enabled: true,
            prune_days: 30,
            prune_statuses: vec!["Done".into(), "Failed".into()],
        }
    }
}

impl Settings {
    pub fn load() -> Result<Self> {
        let config_path = config_path();

        if config_path.exists() {
            let content = std::fs::read_to_string(&config_path)?;
            let settings: Settings = toml::from_str(&content)?;
            return Ok(settings);
        }

        Ok(Settings::default())
    }
}

fn config_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config/shard/config.toml")
}

fn dirs_next() -> PathBuf {
    dirs::download_dir()
        .unwrap_or_else(|| PathBuf::from("."))
}