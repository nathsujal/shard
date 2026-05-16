//! SQLite persistence via sqlx: insert, update, archive, prune, migration.

use anyhow::Result;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::path::PathBuf;
use tracing::info;

use crate::ipc::message::JobSummary;

/// Returns the path where we store the SQLite DB.
pub fn db_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("shard")
        .join("shard.db")
}

/// Open (or create) the SQLite pool and run migrations.
pub async fn open_db() -> Result<SqlitePool> {
    let path = db_path();

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await?;

    // Create active jobs table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS jobs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            url          TEXT    NOT NULL,
            output_path  TEXT    NOT NULL,
            connections  INTEGER NOT NULL,
            total_size   INTEGER NOT NULL DEFAULT 0,
            downloaded   INTEGER NOT NULL DEFAULT 0,
            status       TEXT    NOT NULL DEFAULT 'Pending',
            created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&pool)
    .await?;

    // Create job_history table for completed jobs
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS job_history (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            url           TEXT    NOT NULL,
            output_path   TEXT    NOT NULL,
            connections   INTEGER NOT NULL,
            total_size    INTEGER NOT NULL DEFAULT 0,
            downloaded    INTEGER NOT NULL DEFAULT 0,
            status        TEXT    NOT NULL,
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
            completed_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&pool)
    .await?;

    Ok(pool)
}

/// Enable WAL mode for better concurrent read/write performance.
pub async fn enable_wal_mode(pool: &SqlitePool) -> Result<()> {
    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(pool)
        .await?;
    Ok(())
}

/// Run one-time migration: add indexes, move existing terminal jobs to history.
pub async fn migrate_db(pool: &SqlitePool) -> Result<()> {
    // Create indexes
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)",

    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_job_history_completed_at ON job_history(completed_at)",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_job_history_status ON job_history(status)",
    )
    .execute(pool)
    .await?;

    // Move existing terminal jobs from old single-table layout to history
    sqlx::query(
        "INSERT OR IGNORE INTO job_history
         (id, url, output_path, connections, total_size, downloaded, status, created_at, updated_at, completed_at)
         SELECT id, url, output_path, connections, total_size, downloaded, status, created_at, updated_at, updated_at
         FROM jobs
         WHERE status IN ('Done', 'Cancelled') OR status LIKE 'Failed%'",
    )
    .execute(pool)
    .await?;

    // Remove moved rows from active jobs table
    sqlx::query(
        "DELETE FROM jobs WHERE status IN ('Done', 'Cancelled') OR status LIKE 'Failed%'",
    )
    .execute(pool)
    .await?;

    info!("DB migration complete");
    Ok(())
}

/// Insert a new job row. Returns the assigned row id.
pub async fn insert_job(
    pool: &SqlitePool,
    url: &str,
    output_path: &str,
    connections: u8,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO jobs (url, output_path, connections, status)
         VALUES (?, ?, ?, 'Pending')",
    )
    .bind(url)
    .bind(output_path)
    .bind(connections as i64)
    .execute(pool)
    .await?;

    Ok(row.last_insert_rowid())
}

/// Update status + progress for a job.
pub async fn update_job_status(
    pool: &SqlitePool,
    id: i64,
    status: &str,
    downloaded: u64,
    total_size: u64,
) -> Result<()> {
    sqlx::query(
        "UPDATE jobs SET status = ?, downloaded = ?, total_size = ?,
         updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(status)
    .bind(downloaded as i64)
    .bind(total_size as i64)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Load recoverable jobs on daemon startup — only Pending (not Paused).
pub async fn load_pending_jobs(pool: &SqlitePool) -> Result<Vec<JobSummary>> {
    let rows = sqlx::query_as::<_, DbJobRow>(
        "SELECT id, url, output_path, connections, total_size, downloaded, status, created_at, updated_at
         FROM jobs WHERE status IN ('Pending', 'Paused')
         ORDER BY id ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| r.into()).collect())
}

/// Batch-move terminal jobs to history on daemon shutdown.
/// Active → Paused (resumable on next start).
/// Done/Failed/Cancelled → job_history.
/// Paused/Pending stay in jobs (survive restart).
pub async fn archive_jobs(pool: &SqlitePool) -> Result<u64> {
    // 1. Active → Paused (in-flight downloads become resumable)
    sqlx::query(
        "UPDATE jobs SET status = 'Paused', updated_at = CURRENT_TIMESTAMP
         WHERE status = 'Active'",
    )
    .execute(pool)
    .await?;

    // 2. Copy terminal jobs to history
    sqlx::query(
        "INSERT OR REPLACE INTO job_history
         (id, url, output_path, connections, total_size, downloaded,
          status, created_at, updated_at, completed_at)
         SELECT id, url, output_path, connections, total_size, downloaded,
                status, created_at, updated_at, CURRENT_TIMESTAMP
         FROM jobs
         WHERE status IN ('Done', 'Cancelled') OR status LIKE 'Failed%'",
    )
    .execute(pool)
    .await?;

    // 3. Remove terminal rows from active table
    let result = sqlx::query(
        "DELETE FROM jobs
         WHERE status IN ('Done', 'Cancelled') OR status LIKE 'Failed%'",
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// Delete old rows from job_history based on config.
pub async fn prune_history(
    pool: &SqlitePool,
    days: u32,
    statuses: &[String],
) -> Result<u64> {
    if statuses.is_empty() {
        return Ok(0);
    }

    let conditions: Vec<String> = statuses.iter().map(|s| {
        if s == "Failed" {
            "status LIKE 'Failed%'".to_string()
        } else {
            format!("status = '{}'", s)
        }
    }).collect();

    let sql = format!(
        "DELETE FROM job_history WHERE completed_at < datetime('now', '-{} days') AND ({})",
        days,
        conditions.join(" OR "),
    );

    let result = sqlx::raw_sql(&sql).execute(pool).await?;
    Ok(result.rows_affected())
}

/// Load completed job history for display.
pub async fn load_history(pool: &SqlitePool, limit: u32) -> Result<Vec<JobSummary>> {
    let rows = sqlx::query_as::<_, DbHistoryRow>(
        "SELECT id, url, output_path, connections, total_size, downloaded, status, completed_at
         FROM job_history
         ORDER BY completed_at DESC
         LIMIT ?",
    )
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| r.into()).collect())
}

/// Mark all paused jobs as failed (called on daemon shutdown).
/// Mark Active jobs as Failed — called on daemon startup.
/// Jobs that were downloading when the daemon died should not remain Active forever.
pub async fn fail_active_jobs(pool: &SqlitePool) -> Result<u64> {
    let result = sqlx::query(
        "UPDATE jobs SET status = 'Failed: Daemon shutdown',
         updated_at = CURRENT_TIMESTAMP
         WHERE status = 'Active'",
    )
    .execute(pool)
    .await?;

    let count = result.rows_affected();
    if count > 0 {
        info!("Marked {} active jobs as failed", count);
    }
    Ok(count)
}

// Internal row types

#[derive(sqlx::FromRow)]
struct DbJobRow {
    pub id: i64,
    pub url: String,
    pub output_path: String,
    pub connections: i64,
    pub total_size: i64,
    pub downloaded: i64,
    pub status: String,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

/// Parse "YYYY-MM-DD HH:MM:SS" SQLite datetime to Unix epoch seconds.
fn sqlite_datetime_to_epoch(s: &str) -> Option<i64> {
    let parts: Vec<&str> = s.split([' ', '-', ':']).collect();
    if parts.len() != 6 {
        return None;
    }
    let year: i64 = parts[0].parse().ok()?;
    let month: usize = parts[1].parse().ok()?;
    let day: i64 = parts[2].parse().ok()?;
    let hour: i64 = parts[3].parse().ok()?;
    let min: i64 = parts[4].parse().ok()?;
    let sec: i64 = parts[5].parse().ok()?;

    // Days from 1970-01-01
    let mut days = (year - 1970) * 365;
    // Leap days
    for y in 1970..year {
        if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 { days += 1; }
    }
    // Month days
    let month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for (i, &md) in month_days.iter().enumerate().take(month.saturating_sub(1)) {
        days += md;
        if i == 1 && ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) { days += 1; }
    }
    days += day.saturating_sub(1);

    Some(days * 86400 + hour * 3600 + min * 60 + sec)
}

impl From<DbJobRow> for JobSummary {
    fn from(r: DbJobRow) -> Self {
        let progress_pct = if r.total_size > 0 {
            (r.downloaded as f32 / r.total_size as f32) * 100.0
        } else {
            0.0
        };
        JobSummary {
            id: r.id as u64,
            url: r.url,
            output_path: r.output_path,
            status: r.status,
            total_size: r.total_size as u64,
            downloaded: r.downloaded as u64,
            progress_pct,
            speed_bps: None,
            chunks_total: r.connections as u8,
            chunks_done: 0,
            started_at: r.created_at.as_deref().and_then(sqlite_datetime_to_epoch),
            ended_at: r.updated_at.as_deref().and_then(sqlite_datetime_to_epoch),
            added_at: None,
            error_msg: None,
        }
    }
}

/// Row type for job_history queries (includes completed_at).
#[derive(sqlx::FromRow)]
struct DbHistoryRow {
    pub id: i64,
    pub url: String,
    pub output_path: String,
    pub connections: i64,
    pub total_size: i64,
    pub downloaded: i64,
    pub status: String,
    #[allow(dead_code)]
    pub completed_at: String, // used by sqlx::FromRow derive, never read in Rust code
}

impl From<DbHistoryRow> for JobSummary {
    fn from(r: DbHistoryRow) -> Self {
        let progress_pct = if r.total_size > 0 {
            (r.downloaded as f32 / r.total_size as f32) * 100.0
        } else {
            0.0
        };
        JobSummary {
            id: r.id as u64,
            url: r.url,
            output_path: r.output_path,
            status: r.status,
            total_size: r.total_size as u64,
            downloaded: r.downloaded as u64,
            progress_pct,
            speed_bps: None,
            chunks_total: r.connections as u8,
            chunks_done: 0,
            started_at: None,
            ended_at: None,
            added_at: None,
            error_msg: None,
        }
    }
}