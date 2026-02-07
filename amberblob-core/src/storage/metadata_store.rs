use crate::error::{AmberError, Result};
use crate::slot_manager::Slot;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;

/// Blob metadata as stored in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobMeta {
    pub path: String,
    pub version: i64,
    pub blob_id: String,
    pub size: u64,
    pub chunks: Vec<ChunkRef>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub tombstoned_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Chunk reference stored in the chunks JSON array
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub id: String,   // chunk hash (SHA256)
    pub len: u64,     // chunk length
}

/// Archive information for a specific chunk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobChunkArchive {
    pub pk: i64,
    pub blob_id: String,
    pub chunk_id: String,
    pub length: u64,
    pub archived_at: chrono::DateTime<chrono::Utc>,
    pub target_path: String,
    pub target_version: Option<i32>,
    pub target_range_start: i64,
    pub target_range_end: i64,
}

pub struct MetadataStore {
    slot: Arc<Slot>,
}

impl MetadataStore {
    pub fn new(slot: Arc<Slot>) -> Result<Self> {
        let store = Self { slot };
        store.init_schema()?;
        Ok(store)
    }

    fn get_conn(&self) -> Result<Connection> {
        let db_path = self.slot.meta_db_path();
        let conn = Connection::open(&db_path)?;
        Ok(conn)
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self.get_conn()?;

        // Main blobs table - matches RFC schema
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blobs (
                pk INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL,
                version INTEGER NOT NULL,
                blob_id TEXT NOT NULL,
                size INTEGER NOT NULL,
                chunks TEXT NOT NULL,
                created_at TEXT NOT NULL,
                tombstoned_at TEXT,
                UNIQUE (path, version)
            )",
            [],
        )?;

        // Index for blob_id lookups
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_blobs_blob_id ON blobs(blob_id)",
            [],
        )?;

        // Index for path queries (excluding tombstoned)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_blobs_path ON blobs(path, tombstoned_at)",
            [],
        )?;

        // Chunk archives table - matches RFC schema
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blob_chunk_archives (
                pk INTEGER PRIMARY KEY AUTOINCREMENT,
                blob_id TEXT NOT NULL,
                chunk_id TEXT NOT NULL,
                length INTEGER NOT NULL,
                archived_at TEXT NOT NULL,
                target_path TEXT NOT NULL,
                target_version INTEGER,
                target_range_start INTEGER NOT NULL,
                target_range_end INTEGER NOT NULL,
                UNIQUE (blob_id, chunk_id),
                FOREIGN KEY (blob_id) REFERENCES blobs(blob_id)
            )",
            [],
        )?;

        // Index for chunk_id queries
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_blob_chunk_archives_chunk
             ON blob_chunk_archives(chunk_id)",
            [],
        )?;

        // Slot metadata table (for internal use)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS slot_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )?;

        Ok(())
    }

    /// Store a new blob metadata
    pub fn put_blob(&self, meta: &BlobMeta) -> Result<()> {
        let conn = self.get_conn()?;
        let chunks_json = serde_json::to_string(&meta.chunks)?;

        conn.execute(
            "INSERT OR REPLACE INTO blobs (
                path, version, blob_id, size, chunks, created_at, tombstoned_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                meta.path,
                meta.version,
                meta.blob_id,
                meta.size as i64,
                chunks_json,
                meta.created_at.to_rfc3339(),
                meta.tombstoned_at.map(|t| t.to_rfc3339()),
            ],
        )?;

        Ok(())
    }

    /// Get a specific blob by path and version
    pub fn get_blob(&self, path: &str, version: i64) -> Result<Option<BlobMeta>> {
        let conn = self.get_conn()?;

        let row: Option<(String, i64, String, String, Option<String>)> = conn
            .query_row(
                "SELECT blob_id, size, chunks, created_at, tombstoned_at
                 FROM blobs WHERE path = ?1 AND version = ?2",
                [path, &version.to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        match row {
            Some((blob_id, size, chunks_json, created_at, tombstoned_at)) => {
                let chunks: Vec<ChunkRef> = serde_json::from_str(&chunks_json)?;
                Ok(Some(BlobMeta {
                    path: path.to_string(),
                    version,
                    blob_id,
                    size: size as u64,
                    chunks,
                    created_at: chrono::DateTime::parse_from_rfc3339(&created_at)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                        .with_timezone(&chrono::Utc),
                    tombstoned_at: tombstoned_at
                        .map(|t| chrono::DateTime::parse_from_rfc3339(&t))
                        .transpose()
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                        .map(|t| t.with_timezone(&chrono::Utc)),
                }))
            }
            None => Ok(None),
        }
    }

    /// Get the latest version of a blob (non-tombstoned)
    pub fn get_latest_blob(&self, path: &str) -> Result<Option<BlobMeta>> {
        let conn = self.get_conn()?;

        let row: Option<(i64, String, i64, String, String)> = conn
            .query_row(
                "SELECT version, blob_id, size, chunks, created_at
                 FROM blobs
                 WHERE path = ?1 AND tombstoned_at IS NULL
                 ORDER BY version DESC
                 LIMIT 1",
                [path],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        match row {
            Some((version, blob_id, size, chunks_json, created_at)) => {
                let chunks: Vec<ChunkRef> = serde_json::from_str(&chunks_json)?;
                Ok(Some(BlobMeta {
                    path: path.to_string(),
                    version,
                    blob_id,
                    size: size as u64,
                    chunks,
                    created_at: chrono::DateTime::parse_from_rfc3339(&created_at)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                        .with_timezone(&chrono::Utc),
                    tombstoned_at: None,
                }))
            }
            None => Ok(None),
        }
    }

    /// Get the maximum version for a path
    pub fn get_max_version(&self, path: &str) -> Result<i64> {
        let conn = self.get_conn()?;

        let version: Option<i64> = conn
            .query_row(
                "SELECT MAX(version) FROM blobs WHERE path = ?1",
                [path],
                |row| row.get(0),
            )
            .optional()?;

        Ok(version.unwrap_or(0))
    }

    /// Soft delete - set tombstone timestamp
    pub fn delete_blob(&self, path: &str, version: i64) -> Result<bool> {
        let conn = self.get_conn()?;
        let tombstone_time = chrono::Utc::now().to_rfc3339();

        let affected = conn.execute(
            "UPDATE blobs SET tombstoned_at = ?1 WHERE path = ?2 AND version = ?3 AND tombstoned_at IS NULL",
            params![tombstone_time, path, version],
        )?;

        Ok(affected > 0)
    }

    /// Hard delete - actually remove from database
    pub fn hard_delete_blob(&self, path: &str, version: i64) -> Result<bool> {
        let conn = self.get_conn()?;

        // First delete from archives
        conn.execute(
            "DELETE FROM blob_chunk_archives WHERE blob_id = (
                SELECT blob_id FROM blobs WHERE path = ?1 AND version = ?2
            )",
            [path, &version.to_string()],
        )?;

        let affected = conn.execute(
            "DELETE FROM blobs WHERE path = ?1 AND version = ?2",
            [path, &version.to_string()],
        )?;

        Ok(affected > 0)
    }

    /// List blobs with optional prefix (excluding tombstoned by default)
    pub fn list_blobs(
        &self,
        prefix: &str,
        limit: usize,
        include_tombstoned: bool,
    ) -> Result<Vec<BlobMeta>> {
        let conn = self.get_conn()?;
        let pattern = format!("{}%", prefix);

        let sql = if include_tombstoned {
            "SELECT path, version, blob_id, size, chunks, created_at, tombstoned_at
             FROM blobs WHERE path LIKE ?1 ORDER BY path, version DESC LIMIT ?2"
        } else {
            "SELECT path, version, blob_id, size, chunks, created_at, tombstoned_at
             FROM blobs WHERE path LIKE ?1 AND tombstoned_at IS NULL
             ORDER BY path, version DESC LIMIT ?2"
        };

        let mut stmt = conn.prepare(sql)?;

        let rows = stmt.query_map([pattern, limit.to_string()], |row| {
            let path: String = row.get(0)?;
            let version: i64 = row.get(1)?;
            let blob_id: String = row.get(2)?;
            let size: i64 = row.get(3)?;
            let chunks_json: String = row.get(4)?;
            let created_at: String = row.get(5)?;
            let tombstoned_at: Option<String> = row.get(6)?;

            let chunks: Vec<ChunkRef> = serde_json::from_str(&chunks_json)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

            Ok(BlobMeta {
                path,
                version,
                blob_id,
                size: size as u64,
                chunks,
                created_at: chrono::DateTime::parse_from_rfc3339(&created_at)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                    .with_timezone(&chrono::Utc),
                tombstoned_at: tombstoned_at
                    .map(|t| chrono::DateTime::parse_from_rfc3339(&t))
                    .transpose()
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                    .map(|t| t.with_timezone(&chrono::Utc)),
            })
        })?;

        let mut blobs = Vec::new();
        for row in rows {
            blobs.push(row?);
        }

        Ok(blobs)
    }

    /// Get all versions of a specific path
    pub fn list_versions(&self, path: &str) -> Result<Vec<BlobMeta>> {
        let conn = self.get_conn()?;

        let mut stmt = conn.prepare(
            "SELECT path, version, blob_id, size, chunks, created_at, tombstoned_at
             FROM blobs WHERE path = ?1 ORDER BY version DESC"
        )?;

        let rows = stmt.query_map([path], |row| {
            let path: String = row.get(0)?;
            let version: i64 = row.get(1)?;
            let blob_id: String = row.get(2)?;
            let size: i64 = row.get(3)?;
            let chunks_json: String = row.get(4)?;
            let created_at: String = row.get(5)?;
            let tombstoned_at: Option<String> = row.get(6)?;

            let chunks: Vec<ChunkRef> = serde_json::from_str(&chunks_json)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

            Ok(BlobMeta {
                path,
                version,
                blob_id,
                size: size as u64,
                chunks,
                created_at: chrono::DateTime::parse_from_rfc3339(&created_at)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                    .with_timezone(&chrono::Utc),
                tombstoned_at: tombstoned_at
                    .map(|t| chrono::DateTime::parse_from_rfc3339(&t))
                    .transpose()
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                    .map(|t| t.with_timezone(&chrono::Utc)),
            })
        })?;

        let mut blobs = Vec::new();
        for row in rows {
            blobs.push(row?);
        }

        Ok(blobs)
    }

    /// Get the latest blob_id for anti-entropy comparison
    pub fn get_latest_blob_id(&self) -> Result<Option<Ulid>> {
        let conn = self.get_conn()?;

        let blob_id_str: Option<String> = conn
            .query_row(
                "SELECT blob_id FROM blobs ORDER BY blob_id DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;

        match blob_id_str {
            Some(s) => Ulid::from_string(&s)
                .map(Some)
                .map_err(|e| AmberError::Internal(e.to_string())),
            None => Ok(None),
        }
    }

    /// Get blobs newer than a specific blob_id (for anti-entropy)
    pub fn get_blobs_after(&self, after_blob_id: &str) -> Result<Vec<BlobMeta>> {
        let conn = self.get_conn()?;

        let mut stmt = conn.prepare(
            "SELECT path, version, blob_id, size, chunks, created_at, tombstoned_at
             FROM blobs WHERE blob_id > ?1 ORDER BY blob_id"
        )?;

        let rows = stmt.query_map([after_blob_id], |row| {
            let path: String = row.get(0)?;
            let version: i64 = row.get(1)?;
            let blob_id: String = row.get(2)?;
            let size: i64 = row.get(3)?;
            let chunks_json: String = row.get(4)?;
            let created_at: String = row.get(5)?;
            let tombstoned_at: Option<String> = row.get(6)?;

            let chunks: Vec<ChunkRef> = serde_json::from_str(&chunks_json)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

            Ok(BlobMeta {
                path,
                version,
                blob_id,
                size: size as u64,
                chunks,
                created_at: chrono::DateTime::parse_from_rfc3339(&created_at)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                    .with_timezone(&chrono::Utc),
                tombstoned_at: tombstoned_at
                    .map(|t| chrono::DateTime::parse_from_rfc3339(&t))
                    .transpose()
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                    .map(|t| t.with_timezone(&chrono::Utc)),
            })
        })?;

        let mut blobs = Vec::new();
        for row in rows {
            blobs.push(row?);
        }

        Ok(blobs)
    }

    // === Archive Operations ===

    /// Record a chunk as archived
    pub fn record_chunk_archived(
        &self,
        blob_id: &str,
        chunk_id: &str,
        length: u64,
        target_path: &str,
        target_version: Option<i32>,
        target_range_start: i64,
        target_range_end: i64,
    ) -> Result<()> {
        let conn = self.get_conn()?;
        let archived_at = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "INSERT OR REPLACE INTO blob_chunk_archives (
                blob_id, chunk_id, length, archived_at, target_path,
                target_version, target_range_start, target_range_end
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                blob_id,
                chunk_id,
                length as i64,
                archived_at,
                target_path,
                target_version,
                target_range_start,
                target_range_end,
            ],
        )?;

        Ok(())
    }

    /// Get archive info for a specific chunk
    pub fn get_chunk_archive(&self, blob_id: &str, chunk_id: &str) -> Result<Option<BlobChunkArchive>> {
        let conn = self.get_conn()?;

        let row: Option<(i64, i64, String, String, Option<i32>, i64, i64)> = conn
            .query_row(
                "SELECT pk, length, archived_at, target_path, target_version,
                        target_range_start, target_range_end
                 FROM blob_chunk_archives WHERE blob_id = ?1 AND chunk_id = ?2",
                [blob_id, chunk_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;

        match row {
            Some((pk, length, archived_at, target_path, target_version, range_start, range_end)) => {
                Ok(Some(BlobChunkArchive {
                    pk,
                    blob_id: blob_id.to_string(),
                    chunk_id: chunk_id.to_string(),
                    length: length as u64,
                    archived_at: chrono::DateTime::parse_from_rfc3339(&archived_at)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                        .with_timezone(&chrono::Utc),
                    target_path,
                    target_version,
                    target_range_start: range_start,
                    target_range_end: range_end,
                }))
            }
            None => Ok(None),
        }
    }

    /// Get all archived chunks for a blob
    pub fn get_blob_archives(&self, blob_id: &str) -> Result<Vec<BlobChunkArchive>> {
        let conn = self.get_conn()?;

        let mut stmt = conn.prepare(
            "SELECT pk, chunk_id, length, archived_at, target_path,
                    target_version, target_range_start, target_range_end
             FROM blob_chunk_archives WHERE blob_id = ?1"
        )?;

        let rows = stmt.query_map([blob_id], |row| {
            let pk: i64 = row.get(0)?;
            let chunk_id: String = row.get(1)?;
            let length: i64 = row.get(2)?;
            let archived_at: String = row.get(3)?;
            let target_path: String = row.get(4)?;
            let target_version: Option<i32> = row.get(5)?;
            let range_start: i64 = row.get(6)?;
            let range_end: i64 = row.get(7)?;

            Ok(BlobChunkArchive {
                pk,
                blob_id: blob_id.to_string(),
                chunk_id,
                length: length as u64,
                archived_at: chrono::DateTime::parse_from_rfc3339(&archived_at)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                    .with_timezone(&chrono::Utc),
                target_path,
                target_version,
                target_range_start: range_start,
                target_range_end: range_end,
            })
        })?;

        let mut archives = Vec::new();
        for row in rows {
            archives.push(row?);
        }

        Ok(archives)
    }

    /// Remove archive record (e.g., after restoring from archive)
    pub fn remove_chunk_archive(&self, blob_id: &str, chunk_id: &str) -> Result<bool> {
        let conn = self.get_conn()?;

        let affected = conn.execute(
            "DELETE FROM blob_chunk_archives WHERE blob_id = ?1 AND chunk_id = ?2",
            [blob_id, chunk_id],
        )?;

        Ok(affected > 0)
    }
}

// Legacy type aliases for backward compatibility during migration
pub type ObjectMeta = BlobMeta;
pub type ChunkInfo = ChunkRef;
