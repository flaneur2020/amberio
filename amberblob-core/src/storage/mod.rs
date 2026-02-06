//! Storage modules for AmberBlob
//!
//! Provides content-addressed chunk storage and metadata management.

pub mod chunk_store;
pub mod metadata_store;

pub use chunk_store::{ChunkStore, compute_hash, verify_hash};
pub use metadata_store::{MetadataStore, ObjectMeta, ChunkInfo};
