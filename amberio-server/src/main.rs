mod config;
use clap::{Parser, Subcommand};
use config::Config;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod server;
use amberio_core::InitClusterOperation;
use server::run_server;

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

        /// Override current_node from config at runtime
        #[arg(long)]
        current_node: Option<String>,

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
        Commands::Server {
            config,
            current_node,
            init,
        } => {
            tracing::info!("Starting Amberio server with config: {}", config);

            let mut cfg = match Config::from_file(&config) {
                Ok(c) => c,
                Err(error) => {
                    tracing::error!("Failed to load config: {}", error);
                    std::process::exit(1);
                }
            };

            if let Some(override_node) = current_node {
                tracing::info!(
                    "Overriding current_node from '{}' to '{}' via CLI",
                    cfg.current_node,
                    override_node
                );
                cfg.current_node = override_node;
            }

            let registry_builder = cfg.registry_builder();

            let init_operation = InitClusterOperation::new(registry_builder.clone());
            let init_result = match init_operation.run(cfg.to_init_cluster_request()).await {
                Ok(result) => result,
                Err(error) => {
                    tracing::error!("Initialization failed: {}", error);
                    std::process::exit(1);
                }
            };

            if init_result.won_bootstrap_race {
                tracing::info!(
                    "Node {} won initialization race and persisted bootstrap state",
                    cfg.current_node
                );
            } else {
                tracing::info!(
                    "Node {} reused existing bootstrap state from registry",
                    cfg.current_node
                );
            }

            if init {
                tracing::info!(
                    "Initialization completed for node {} (init-only mode)",
                    cfg.current_node
                );
                return;
            }

            let runtime_config = match cfg.runtime_from_bootstrap(&init_result.bootstrap_state) {
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

            let registry = match registry_builder.build().await {
                Ok(registry) => registry,
                Err(error) => {
                    tracing::error!("Failed to create runtime registry: {}", error);
                    std::process::exit(1);
                }
            };

            if let Err(error) = run_server(runtime_config, registry).await {
                tracing::error!("Server error: {}", error);
                std::process::exit(1);
            }
        }
    }
}
