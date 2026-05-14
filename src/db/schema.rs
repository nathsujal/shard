// SQLite persistence layer via sqlx.
// Daemon loads jobs on startup (to recover from crash/restart).
// Jobs are written on add, updated on status change, deleted on cancel/done.

use anyhow::Result;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::path::PathBuf;

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

    // Ensure parent directory exists.
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await?;

    // Create table if it doesn't exist.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS jobs (
            id           INTEGER PRIMARY KEY,
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

    Ok(pool)
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

/// Delete a job row (used on cancel).
pub async fn delete_job(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("DELETE FROM jobs WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Load recoverable jobs on daemon startup — excludes Done, Failed, Cancelled.
pub async fn load_pending_jobs(pool: &SqlitePool) -> Result<Vec<JobSummary>> {
    let rows = sqlx::query_as::<_, DbJobRow>(
        "SELECT id, url, output_path, connections, total_size, downloaded, status
         FROM jobs WHERE status NOT IN ('Done', 'Cancelled')
         AND status NOT LIKE 'Failed%'
         ORDER BY id ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| r.into()).collect())
}

/// Load every job (all statuses) — used by `shard queue --all`.
pub async fn load_all_jobs(pool: &SqlitePool) -> Result<Vec<JobSummary>> {
    let rows = sqlx::query_as::<_, DbJobRow>(
        "SELECT id, url, output_path, connections, total_size, downloaded, status
         FROM jobs ORDER BY id ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| r.into()).collect())
}

// Internal row type

#[derive(sqlx::FromRow)]
struct DbJobRow {
    pub id: i64,
    pub url: String,
    pub output_path: String,
    #[allow(dead_code)]
    pub connections: i64,
    pub total_size: i64,
    pub downloaded: i64,
    pub status: String,
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
        }
    }
}