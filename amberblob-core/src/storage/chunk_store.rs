use crate::error::{AmberError, Result};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncWriteExt;

/// ChunkStore handles blob-specific chunk storage
/// Following RFC: chunks are stored in blobs/{blob_id}/chunks/{chunk_id}
pub struct ChunkStore {
    base_path: PathBuf,
}

impl ChunkStore {
    pub fn new(base_path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_path)?;
        Ok(Self { base_path })
    }

    /// Get the base path for the store
    pub fn base_path(&self) -> &PathBuf {
        &self.base_path
    }

    /// Store a chunk for a specific blob
    /// Returns the chunk ID (SHA256 hash)
    pub async fn put_chunk(
        &self,
        blob_id: &str,
        chunk_index: usize,
        data: Bytes,
    ) -> Result<String> {
        let chunk_id = compute_hash(&data);
        let chunk_path = self.chunk_path(blob_id, &chunk_id);

        // Create blob chunks directory if needed
        let blob_chunks_dir = self.blob_chunks_dir(blob_id);
        fs::create_dir_all(&blob_chunks_dir).await?;

        // Check if chunk already exists
        if chunk_path.exists() {
            return Ok(chunk_id);
        }

        // Write to temporary file first, then rename for atomicity
        let temp_path = chunk_path.with_extension("tmp");
        let mut file = fs::File::create(&temp_path).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;
        drop(file);

        fs::rename(&temp_path, &chunk_path).await?;

        tracing::debug!(
            "Stored chunk {} for blob {} (index {})",
            chunk_id,
            blob_id,
            chunk_index
        );
        Ok(chunk_id)
    }

    /// Retrieve a chunk by blob_id and chunk_id
    pub async fn get_chunk(&self, blob_id: &str, chunk_id: &str) -> Result<Bytes> {
        let chunk_path = self.chunk_path(blob_id, chunk_id);

        if !chunk_path.exists() {
            return Err(AmberError::ChunkNotFound(chunk_id.to_string()));
        }

        let data = fs::read(&chunk_path).await?;
        Ok(Bytes::from(data))
    }

    /// Check if a chunk exists
    pub fn chunk_exists(&self, blob_id: &str, chunk_id: &str) -> bool {
        self.chunk_path(blob_id, chunk_id).exists()
    }

    /// Delete a specific chunk
    pub async fn delete_chunk(&self, blob_id: &str, chunk_id: &str) -> Result<()> {
        let chunk_path = self.chunk_path(blob_id, chunk_id);
        if chunk_path.exists() {
            fs::remove_file(&chunk_path).await?;
        }
        Ok(())
    }

    /// Delete all chunks for a blob
    pub async fn delete_blob_chunks(&self, blob_id: &str) -> Result<()> {
        let blob_chunks_dir = self.blob_chunks_dir(blob_id);
        if blob_chunks_dir.exists() {
            fs::remove_dir_all(&blob_chunks_dir).await?;
        }
        Ok(())
    }

    /// Get the path to a chunk
    fn chunk_path(&self, blob_id: &str, chunk_id: &str) -> PathBuf {
        self.blob_chunks_dir(blob_id).join(chunk_id)
    }

    /// Get the directory for a blob's chunks
    fn blob_chunks_dir(&self, blob_id: &str) -> PathBuf {
        self.base_path.join("blobs").join(blob_id).join("chunks")
    }

    /// List all chunks for a blob
    pub async fn list_blob_chunks(&self, blob_id: &str) -> Result<Vec<String>> {
        let blob_chunks_dir = self.blob_chunks_dir(blob_id);

        if !blob_chunks_dir.exists() {
            return Ok(Vec::new());
        }

        let mut chunks = Vec::new();
        let mut entries = fs::read_dir(&blob_chunks_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    chunks.push(name.to_string());
                }
            }
        }

        Ok(chunks)
    }
}

/// Compute SHA256 hash of data
pub fn compute_hash(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Verify that data matches the expected hash
pub fn verify_hash(data: &[u8], expected_hash: &str) -> Result<()> {
    let actual_hash = compute_hash(data);
    if actual_hash != expected_hash {
        return Err(AmberError::HashMismatch {
            expected: expected_hash.to_string(),
            actual: actual_hash,
        });
    }
    Ok(())
}

/// Legacy ChunkStore for backward compatibility
/// This is the old content-addressed storage that used a shared chunks directory
pub struct LegacyChunkStore {
    base_path: PathBuf,
}

impl LegacyChunkStore {
    pub fn new(base_path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_path)?;
        Ok(Self { base_path })
    }

    /// Store a chunk and return its SHA256 hash
    pub async fn put(&self, data: Bytes) -> Result<String> {
        let hash = compute_hash(&data);
        let chunk_path = self.chunk_path(&hash);

        // Check if chunk already exists (content-addressed)
        if chunk_path.exists() {
            return Ok(hash);
        }

        // Write to temporary file first, then rename for atomicity
        let temp_path = chunk_path.with_extension("tmp");
        let mut file = fs::File::create(&temp_path).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;
        drop(file);

        fs::rename(&temp_path, &chunk_path).await?;

        tracing::debug!("Stored chunk with hash {}", hash);
        Ok(hash)
    }

    /// Retrieve a chunk by its hash
    pub async fn get(&self, hash: &str) -> Result<Bytes> {
        let chunk_path = self.chunk_path(hash);

        if !chunk_path.exists() {
            return Err(AmberError::ChunkNotFound(hash.to_string()));
        }

        let data = fs::read(&chunk_path).await?;
        Ok(Bytes::from(data))
    }

    /// Check if a chunk exists
    pub fn exists(&self, hash: &str) -> bool {
        self.chunk_path(hash).exists()
    }

    /// Delete a chunk
    pub async fn delete(&self, hash: &str) -> Result<()> {
        let chunk_path = self.chunk_path(hash);
        if chunk_path.exists() {
            fs::remove_file(&chunk_path).await?;
        }
        Ok(())
    }

    fn chunk_path(&self, hash: &str) -> PathBuf {
        // Use first 2 chars as subdirectory to avoid too many files in one dir
        let prefix = &hash[..2.min(hash.len())];
        self.base_path.join("chunks").join(prefix).join(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_chunk_store() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(temp_dir.path().to_path_buf()).unwrap();

        let blob_id = "test_blob_123";
        let data = Bytes::from("test data content");
        let chunk_id = compute_hash(&data);

        // Store chunk
        let returned_id = store.put_chunk(blob_id, 0, data.clone()).await.unwrap();
        assert_eq!(returned_id, chunk_id);

        // Retrieve chunk
        let retrieved = store.get_chunk(blob_id, &chunk_id).await.unwrap();
        assert_eq!(retrieved, data);

        // Check existence
        assert!(store.chunk_exists(blob_id, &chunk_id));

        // List chunks
        let chunks = store.list_blob_chunks(blob_id).await.unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], chunk_id);

        // Delete chunk
        store.delete_chunk(blob_id, &chunk_id).await.unwrap();
        assert!(!store.chunk_exists(blob_id, &chunk_id));
    }

    #[test]
    fn test_compute_hash() {
        let data = b"hello world";
        let hash = compute_hash(data);
        assert_eq!(hash.len(), 64); // SHA256 hex string is 64 chars
    }
}
