//! AmberBlob Core - Core library for lightweight object storage for edge cloud nodes
//!
//! A fixed-topology, leaderless object storage system using:
//! - 2048 fixed slots per group
//! - 2PC for consistency
//! - SHA256 content-addressed chunks
//! - SQLite for local metadata

pub mod chunk_store;
pub mod config;
pub mod error;
pub mod etcd_store;
pub mod metadata_store;
pub mod node;
pub mod slot_manager;
pub mod two_phase_commit;

pub use chunk_store::{ChunkStore, compute_hash, verify_hash};
pub use config::{Config, NodeConfig, EtcdConfig, DiskConfig, ReplicationConfig, ArchiveConfig, S3Config};
pub use error::{AmberError, Result};
pub use etcd_store::{EtcdStore, SlotEvent};
pub use metadata_store::{MetadataStore, ObjectMeta, ChunkInfo};
pub use node::{Node, NodeInfo, NodeStatus};
pub use slot_manager::{SlotManager, Slot, SlotInfo, SlotHealth, ReplicaStatus, slot_for_key, TOTAL_SLOTS, CHUNK_SIZE};
pub use two_phase_commit::{TwoPhaseCommit, TwoPhaseParticipant, Transaction, TransactionState, Vote};
