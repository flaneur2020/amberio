//! Storage modules for Amberio
//!
//! Provides filesystem part storage and metadata management.

pub mod archive_store;
pub mod metadata_store;
pub mod part_store;

pub use archive_store::{
    ArchiveListPage, ArchiveStore, RedisArchiveStore, S3ArchiveStore, parse_redis_archive_url,
    parse_s3_archive_url, read_archive_range_bytes, set_default_s3_archive_store,
};
pub use metadata_store::{
    BlobHead, BlobMeta, HeadKind, MetadataStore, PartEntry, PartIndexState, TombstoneMeta,
};
pub use part_store::{PartStore, PutPartResult, compute_hash, verify_hash};
