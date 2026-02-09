use crate::{
    AmberError, BlobMeta, MetadataStore, PartIndexState, RegistryBuilder, Result, SlotManager,
    slot_for_key,
};
use chrono::Utc;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitClusterDiskConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitClusterNodeConfig {
    pub node_id: String,
    pub bind_addr: String,
    #[serde(default)]
    pub advertise_addr: Option<String>,
    pub disks: Vec<InitClusterDiskConfig>,
}

impl InitClusterNodeConfig {
    pub fn effective_address(&self) -> String {
        self.advertise_addr
            .clone()
            .unwrap_or_else(|| self.bind_addr.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitClusterReplicationConfig {
    pub min_write_replicas: usize,
    pub total_slots: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitClusterArchiveConfig {
    pub archive_type: String,
    pub s3: Option<InitClusterArchiveS3Config>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitClusterArchiveS3Config {
    pub bucket: String,
    pub region: String,
    pub credentials: InitClusterArchiveS3Credentials,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitClusterArchiveS3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitClusterScanConfig {
    #[serde(default)]
    pub enabled: bool,
    pub redis_mock: Option<InitClusterScanRedisMockConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitClusterScanRedisMockConfig {
    pub url: String,
    pub list_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitClusterScanEntry {
    pub path: String,
    pub size_bytes: u64,
    pub etag: String,
    pub archive_url: String,
    #[serde(default = "default_part_size")]
    pub part_size: u64,
    #[serde(default)]
    pub updated_at: Option<String>,
}

fn default_part_size() -> u64 {
    64 * 1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitClusterBootstrapState {
    pub initialized_at: String,
    pub current_node: String,
    pub nodes: Vec<InitClusterNodeConfig>,
    pub replication: InitClusterReplicationConfig,
    pub archive: Option<InitClusterArchiveConfig>,
    pub initialized_by: String,
}

#[derive(Debug, Clone)]
pub struct InitClusterOperationRequest {
    pub current_node: String,
    pub nodes: Vec<InitClusterNodeConfig>,
    pub replication: InitClusterReplicationConfig,
    pub archive: Option<InitClusterArchiveConfig>,
    pub init_scan: Option<InitClusterScanConfig>,
}

#[derive(Debug, Clone)]
pub struct InitClusterOperationResult {
    pub bootstrap_state: InitClusterBootstrapState,
    pub won_bootstrap_race: bool,
}

#[derive(Clone)]
pub struct InitClusterOperation {
    registry_builder: RegistryBuilder,
}

impl InitClusterOperation {
    pub fn new(registry_builder: RegistryBuilder) -> Self {
        Self { registry_builder }
    }

    pub async fn run(
        &self,
        request: InitClusterOperationRequest,
    ) -> Result<InitClusterOperationResult> {
        let bootstrap_store = self.registry_builder.build().await?;

        if let Some(existing) = bootstrap_store.get_bootstrap_state().await? {
            let bootstrap = decode_bootstrap(&existing)?;
            ensure_local_layout(&request.current_node, &bootstrap)?;
            return Ok(InitClusterOperationResult {
                bootstrap_state: bootstrap,
                won_bootstrap_race: false,
            });
        }

        let proposed = InitClusterBootstrapState {
            initialized_at: Utc::now().to_rfc3339(),
            current_node: request.current_node.clone(),
            nodes: request.nodes.clone(),
            replication: request.replication.clone(),
            archive: request.archive.clone(),
            initialized_by: request.current_node.clone(),
        };

        let payload = serde_json::to_vec(&proposed).map_err(|error| {
            AmberError::Internal(format!("Failed to encode bootstrap state: {}", error))
        })?;

        let won_bootstrap_race = bootstrap_store
            .set_bootstrap_state_if_absent(&payload)
            .await?;

        let active_bytes = bootstrap_store
            .get_bootstrap_state()
            .await?
            .ok_or_else(|| {
                AmberError::Internal(
                    "Bootstrap state is missing after initialization attempt".to_string(),
                )
            })?;

        let bootstrap_state = decode_bootstrap(&active_bytes)?;
        ensure_local_layout(&request.current_node, &bootstrap_state)?;

        if won_bootstrap_race {
            run_optional_init_scan(&request.current_node, &request.init_scan, &bootstrap_state)
                .await?;
        }

        Ok(InitClusterOperationResult {
            bootstrap_state,
            won_bootstrap_race,
        })
    }
}

fn decode_bootstrap(payload: &[u8]) -> Result<InitClusterBootstrapState> {
    serde_json::from_slice(payload).map_err(|error| {
        AmberError::Internal(format!(
            "Failed to decode bootstrap state from registry: {}",
            error
        ))
    })
}

fn ensure_local_layout(
    current_node: &str,
    bootstrap: &InitClusterBootstrapState,
) -> Result<InitClusterNodeConfig> {
    let node = bootstrap
        .nodes
        .iter()
        .find(|node| node.node_id == current_node)
        .ok_or_else(|| {
            AmberError::Config(format!(
                "current_node '{}' not found in initialized cluster",
                current_node
            ))
        })?
        .clone();

    for disk in &node.disks {
        let amberio_dir = disk.path.join("amberio");
        std::fs::create_dir_all(&amberio_dir)?;
        tracing::info!("Ensured directory exists: {:?}", amberio_dir);
    }

    Ok(node)
}

async fn run_optional_init_scan(
    current_node: &str,
    init_scan: &Option<InitClusterScanConfig>,
    bootstrap: &InitClusterBootstrapState,
) -> Result<()> {
    let init_scan = match init_scan {
        Some(scan) if scan.enabled => scan,
        _ => return Ok(()),
    };

    let redis_mock = match init_scan.redis_mock.as_ref() {
        Some(mock) => mock,
        None => {
            tracing::warn!("init_scan enabled but init_scan.redis_mock is not configured");
            return Ok(());
        }
    };

    let node = bootstrap
        .nodes
        .iter()
        .find(|node| node.node_id == current_node)
        .ok_or_else(|| {
            AmberError::Config(format!(
                "current_node '{}' not found in initialized cluster",
                current_node
            ))
        })?;

    let data_dir = node
        .disks
        .first()
        .map(|disk| disk.path.clone())
        .ok_or_else(|| AmberError::Config("current node has no configured disks".to_string()))?;

    let slot_manager = SlotManager::new(current_node.to_string(), data_dir)?;

    let client = redis::Client::open(redis_mock.url.as_str()).map_err(|error| {
        AmberError::Config(format!(
            "init_scan redis mock connection config error: {}",
            error
        ))
    })?;

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| {
            AmberError::Internal(format!("init_scan redis mock connection failed: {}", error))
        })?;

    let raw_entries: Vec<String> =
        conn.lrange(&redis_mock.list_key, 0, -1)
            .await
            .map_err(|error| {
                AmberError::Internal(format!("init_scan redis mock LRANGE failed: {}", error))
            })?;

    if raw_entries.is_empty() {
        tracing::info!(
            "init_scan enabled but redis list '{}' is empty",
            redis_mock.list_key
        );
        return Ok(());
    }

    let mut imported = 0usize;
    for raw in raw_entries {
        let entry: InitClusterScanEntry = serde_json::from_str(&raw).map_err(|error| {
            AmberError::Config(format!("invalid init_scan entry JSON: {} ({})", raw, error))
        })?;

        let normalized_path = normalize_blob_path(&entry.path)?;
        let slot_id = slot_for_key(&normalized_path, bootstrap.replication.total_slots);

        if !slot_manager.has_slot(slot_id).await {
            slot_manager.init_slot(slot_id).await?;
        }

        let slot = slot_manager.get_slot(slot_id).await?;
        let metadata_store = MetadataStore::new(slot)?;
        let generation = metadata_store.next_generation(&normalized_path)?;

        let part_size = entry.part_size.max(1);
        let part_count = if entry.size_bytes == 0 {
            0
        } else {
            entry.size_bytes.div_ceil(part_size) as u32
        };

        let updated_at = entry
            .updated_at
            .as_deref()
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        let meta = BlobMeta {
            path: normalized_path.clone(),
            slot_id,
            generation,
            version: generation,
            size_bytes: entry.size_bytes,
            etag: entry.etag.clone(),
            part_size,
            part_count,
            part_index_state: PartIndexState::None,
            archive_url: Some(entry.archive_url.clone()),
            updated_at,
        };

        let applied = metadata_store.upsert_meta(&meta)?;
        if applied {
            imported += 1;
        }

        tracing::info!(
            "init_scan imported path={} slot={} generation={} applied={}",
            normalized_path,
            slot_id,
            generation,
            applied
        );
    }

    tracing::info!("init_scan imported {} objects", imported);
    Ok(())
}

fn normalize_blob_path(path: &str) -> Result<String> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Err(AmberError::InvalidRequest(
            "blob path cannot be empty".to_string(),
        ));
    }

    let mut components = Vec::new();
    for component in trimmed.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(AmberError::InvalidRequest(format!(
                "invalid blob path component: {}",
                component
            )));
        }
        components.push(component);
    }

    Ok(components.join("/"))
}
