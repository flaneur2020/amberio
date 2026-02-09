use amberio_core::{AmberError, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub current_node: String,
    pub registry: RegistryConfig,
    pub initial_cluster: InitialClusterConfig,
    pub archive: Option<ArchiveConfig>,
    #[serde(default)]
    pub init_scan: Option<InitScanConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitialClusterConfig {
    pub nodes: Vec<InitialNodeConfig>,
    pub replication: ReplicationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitialNodeConfig {
    pub node_id: String,
    pub bind_addr: String,
    #[serde(default)]
    pub advertise_addr: Option<String>,
    pub disks: Vec<DiskConfig>,
}

impl InitialNodeConfig {
    pub fn effective_address(&self) -> String {
        self.advertise_addr
            .clone()
            .unwrap_or_else(|| self.bind_addr.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub node: RuntimeNodeConfig,
    pub replication: ReplicationConfig,
    pub registry: RegistryConfig,
    pub archive: Option<ArchiveConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeNodeConfig {
    pub node_id: String,
    pub bind_addr: String,
    pub advertise_addr: String,
    pub disks: Vec<DiskConfig>,
}

/// Registry backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    pub backend: RegistryBackend,
    #[serde(default)]
    pub namespace: Option<String>,
    pub etcd: Option<EtcdConfig>,
    pub redis: Option<RedisConfig>,
}

impl RegistryConfig {
    pub fn namespace_or_default(&self) -> &str {
        self.namespace
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("default")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RegistryBackend {
    Etcd,
    Redis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtcdConfig {
    pub endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    pub url: String,
    #[serde(default = "default_redis_pool_size")]
    pub pool_size: usize,
}

fn default_redis_pool_size() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveConfig {
    pub archive_type: String,
    pub s3: Option<S3Config>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub credentials: S3Credentials,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub min_write_replicas: usize,
    pub total_slots: u16,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            min_write_replicas: 3,
            total_slots: 2048,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitScanConfig {
    #[serde(default)]
    pub enabled: bool,
    pub redis_mock: Option<InitScanRedisMockConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitScanRedisMockConfig {
    pub url: String,
    pub list_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitScanEntry {
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
pub struct BootstrapState {
    pub initialized_at: String,
    pub current_node: String,
    pub nodes: Vec<InitialNodeConfig>,
    pub replication: ReplicationConfig,
    pub archive: Option<ArchiveConfig>,
    pub initialized_by: String,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self> {
        let settings = ::config::Config::builder()
            .add_source(::config::File::with_name(path))
            .add_source(::config::Environment::with_prefix("AMBERIO"))
            .build()
            .map_err(|e| AmberError::Config(e.to_string()))?;

        let config: Config = settings
            .try_deserialize()
            .map_err(|e| AmberError::Config(e.to_string()))?;

        Ok(config)
    }

    pub fn runtime_from_bootstrap(&self, bootstrap: &BootstrapState) -> Result<RuntimeConfig> {
        let current_node = bootstrap
            .nodes
            .iter()
            .find(|node| node.node_id == self.current_node)
            .ok_or_else(|| {
                AmberError::Config(format!(
                    "current_node '{}' not found in bootstrap nodes",
                    self.current_node
                ))
            })?
            .clone();

        Ok(RuntimeConfig {
            node: RuntimeNodeConfig {
                node_id: current_node.node_id.clone(),
                bind_addr: current_node.bind_addr.clone(),
                advertise_addr: current_node.effective_address(),
                disks: current_node.disks,
            },
            replication: bootstrap.replication.clone(),
            registry: self.registry.clone(),
            archive: bootstrap.archive.clone(),
        })
    }

    pub fn local_bootstrap_state(&self) -> BootstrapState {
        BootstrapState {
            initialized_at: chrono::Utc::now().to_rfc3339(),
            current_node: self.current_node.clone(),
            nodes: self.initial_cluster.nodes.clone(),
            replication: self.initial_cluster.replication.clone(),
            archive: self.archive.clone(),
            initialized_by: self.current_node.clone(),
        }
    }
}
