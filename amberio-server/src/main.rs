mod config;
use clap::{Parser, Subcommand};
use config::{BootstrapState, Config, InitScanEntry, RegistryBackend};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod server;
use amberio_core::{BlobMeta, MetadataStore, PartIndexState, SlotManager, slot_for_key};
use server::run_server;
use redis::AsyncCommands;

#[derive(Parser)]
#[command(name = "amberio")]
#[command(about = "Lightweight object storage for edge cloud nodes")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the server
    Server {
        /// Path to configuration file
        #[arg(short, long, default_value = "config.yaml")]
        config: String,

        /// Run initialization flow only, then exit
        #[arg(long)]
        init: bool,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "amberio=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Server { config, init } => {
            tracing::info!("Starting Amberio server with config: {}", config);

            let cfg = match Config::from_file(&config) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Failed to load config: {}", e);
                    std::process::exit(1);
                }
            };

            let bootstrap_state = match ensure_initialized(&cfg).await {
                Ok(state) => state,
                Err(error) => {
                    tracing::error!("Initialization failed: {}", error);
                    std::process::exit(1);
                }
            };

            if init {
                tracing::info!(
                    "Initialization completed for node {} (init-only mode)",
                    cfg.current_node
                );
                return;
            }

            let runtime_config = match cfg.runtime_from_bootstrap(&bootstrap_state) {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::error!("Failed to build runtime config: {}", error);
                    std::process::exit(1);
                }
            };

            tracing::info!(
                "Node ID: {}, Bind: {}, Slots: {}",
                runtime_config.node.node_id,
                runtime_config.node.bind_addr,
                runtime_config.replication.total_slots
            );

            if let Err(e) = run_server(runtime_config).await {
                tracing::error!("Server error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

async fn ensure_initialized(config: &Config) -> amberio_core::Result<BootstrapState> {
    let registry_client = match config.registry.backend {
        RegistryBackend::Etcd => {
            let etcd_cfg = config.registry.etcd.as_ref().ok_or_else(|| {
                amberio_core::AmberError::Config(
                    "etcd configuration is required for etcd backend".to_string(),
                )
            })?;
            InitRegistry::Etcd(
                amberio_core::EtcdRegistry::new(&etcd_cfg.endpoints, config.registry.namespace_or_default())
                    .await?,
            )
        }
        RegistryBackend::Redis => {
            let redis_cfg = config.registry.redis.as_ref().ok_or_else(|| {
                amberio_core::AmberError::Config(
                    "redis configuration is required for redis backend".to_string(),
                )
            })?;
            InitRegistry::Redis(
                amberio_core::RedisRegistry::new(&redis_cfg.url, config.registry.namespace_or_default())
                    .await?,
            )
        }
    };

    if let Some(existing) = registry_client.get_bootstrap_state().await? {
        tracing::info!("Bootstrap state already exists in registry; reusing persisted state");
        ensure_local_layout(config, &existing)?;
        return Ok(existing);
    }

    tracing::info!(
        "No bootstrap state found. Attempting first-wins initialization for node {}",
        config.current_node
    );

    let proposed = config.local_bootstrap_state();

    let (active, won) = registry_client
        .upsert_bootstrap_state_first_wins(&proposed)
        .await?;

    if won {
        tracing::info!(
            "Node {} won initialization race and persisted bootstrap state",
            config.current_node
        );
    } else {
        tracing::info!(
            "Node {} lost initialization race; using persisted bootstrap state",
            config.current_node
        );
    }

    ensure_local_layout(config, &active)?;

    if won {
        run_optional_init_scan(config, &active).await?;
    }

    Ok(active)
}

fn ensure_local_layout(config: &Config, bootstrap: &BootstrapState) -> amberio_core::Result<()> {
    let node = bootstrap
        .nodes
        .iter()
        .find(|node| node.node_id == config.current_node)
        .ok_or_else(|| {
            amberio_core::AmberError::Config(format!(
                "current_node '{}' not found in initialized cluster",
                config.current_node
            ))
        })?;

    for disk in &node.disks {
        let amberio_dir = disk.path.join("amberio");
        std::fs::create_dir_all(&amberio_dir)?;
        tracing::info!("Ensured directory exists: {:?}", amberio_dir);
    }

    Ok(())
}

async fn run_optional_init_scan(
    config: &Config,
    bootstrap: &BootstrapState,
) -> amberio_core::Result<()> {
    let init_scan = match config.init_scan.as_ref() {
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

    let current_node = bootstrap
        .nodes
        .iter()
        .find(|node| node.node_id == config.current_node)
        .ok_or_else(|| {
            amberio_core::AmberError::Config(format!(
                "current_node '{}' not found in initialized cluster",
                config.current_node
            ))
        })?;

    let data_dir = current_node
        .disks
        .first()
        .map(|disk| disk.path.clone())
        .ok_or_else(|| {
            amberio_core::AmberError::Config("current node has no configured disks".to_string())
        })?;

    let slot_manager = SlotManager::new(config.current_node.clone(), data_dir)?;

    let client = redis::Client::open(redis_mock.url.as_str()).map_err(|error| {
        amberio_core::AmberError::Config(format!(
            "init_scan redis mock connection config error: {}",
            error
        ))
    })?;

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| {
            amberio_core::AmberError::Internal(format!(
                "init_scan redis mock connection failed: {}",
                error
            ))
        })?;

    let raw_entries: Vec<String> = conn
        .lrange(&redis_mock.list_key, 0, -1)
        .await
        .map_err(|error| {
            amberio_core::AmberError::Internal(format!(
                "init_scan redis mock LRANGE failed: {}",
                error
            ))
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
        let entry: InitScanEntry = serde_json::from_str(&raw).map_err(|error| {
            amberio_core::AmberError::Config(format!(
                "invalid init_scan entry JSON: {} ({})",
                raw, error
            ))
        })?;

        let normalized_path = normalize_scan_path(&entry.path)?;
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
            ((entry.size_bytes + part_size - 1) / part_size) as u32
        };

        let updated_at = entry
            .updated_at
            .as_deref()
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);

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

fn normalize_scan_path(path: &str) -> amberio_core::Result<String> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Err(amberio_core::AmberError::InvalidRequest(
            "blob path cannot be empty".to_string(),
        ));
    }

    let mut components = Vec::new();
    for component in trimmed.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(amberio_core::AmberError::InvalidRequest(format!(
                "invalid blob path component: {}",
                component
            )));
        }
        components.push(component);
    }

    Ok(components.join("/"))
}

enum InitRegistry {
    Etcd(amberio_core::EtcdRegistry),
    Redis(amberio_core::RedisRegistry),
}

impl InitRegistry {
    async fn get_bootstrap_state(&self) -> amberio_core::Result<Option<BootstrapState>> {
        let bytes = match self {
            Self::Etcd(registry) => registry.get_bootstrap_bytes().await?,
            Self::Redis(registry) => registry.get_bootstrap_bytes().await?,
        };

        let state = match bytes {
            Some(payload) => Some(serde_json::from_slice::<BootstrapState>(&payload).map_err(
                |error| {
                    amberio_core::AmberError::Internal(format!(
                        "Failed to decode bootstrap state from registry: {}",
                        error
                    ))
                },
            )?),
            None => None,
        };

        Ok(state)
    }

    async fn upsert_bootstrap_state_first_wins(
        &self,
        proposed: &BootstrapState,
    ) -> amberio_core::Result<(BootstrapState, bool)> {
        let payload = serde_json::to_vec(proposed).map_err(|error| {
            amberio_core::AmberError::Internal(format!(
                "Failed to encode bootstrap state: {}",
                error
            ))
        })?;

        let won = match self {
            Self::Etcd(registry) => registry.create_bootstrap_bytes_if_absent(&payload).await?,
            Self::Redis(registry) => registry.set_bootstrap_bytes_if_absent(&payload).await?,
        };

        let active = self.get_bootstrap_state().await?.ok_or_else(|| {
            amberio_core::AmberError::Internal(
                "Bootstrap state is missing after initialization attempt".to_string(),
            )
        })?;

        if won {
            Ok((active, true))
        } else {
            Ok((active, false))
        }
    }
}
