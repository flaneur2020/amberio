use crate::config::{Config, RegistryBackend};
use amberblob_core::{
    AmberError, Result,
    Node, NodeInfo,
    SlotManager, SlotInfo, SlotHealth, ReplicaStatus, slot_for_key, TOTAL_SLOTS, CHUNK_SIZE,
    ChunkStore, compute_hash,
    MetadataStore, BlobMeta, ChunkRef,
    EtcdRegistry, RedisRegistry, Registry,
    TwoPhaseCommit, TwoPhaseParticipant, Vote,
};
use axum::{
    body::Bytes,
    extract::{Path, State, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;

pub struct ServerState {
    pub node: Arc<Node>,
    pub slot_manager: Arc<SlotManager>,
    pub chunk_store: Arc<ChunkStore>,
    pub registry: Arc<dyn Registry>,
    pub twopc_coordinator: Arc<TwoPhaseCommit>,
    pub twopc_participant: Arc<TwoPhaseParticipant>,
    pub config: Config,
}

#[derive(Debug, Serialize)]
struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GetObjectQuery {
    #[serde(default)]
    version: Option<i64>,
    #[serde(default)]
    start: Option<u64>,
    #[serde(default)]
    end: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ObjectResponse {
    path: String,
    version: i64,
    blob_id: String,
    size: u64,
    chunks: Vec<ChunkRef>,
    created_at: String,
    tombstoned_at: Option<String>,
}

#[derive(Debug, Serialize)]
struct WriteResponse {
    path: String,
    version: i64,
    blob_id: String,
    chunks_stored: usize,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default = "default_prefix")]
    prefix: String,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    include_tombstoned: bool,
}

#[derive(Debug, Deserialize)]
struct PutObjectQuery {
    #[serde(default)]
    version: Option<i64>,
}

fn default_prefix() -> String {
    "".to_string()
}

fn default_limit() -> usize {
    100
}

pub async fn run_server(config: Config) -> Result<()> {
    let node_config = config.node.clone();

    // Collect disk paths for Node
    let disk_paths: Vec<std::path::PathBuf> = node_config.disks.iter().map(|d| d.path.clone()).collect();

    let node = Arc::new(Node::new(
        node_config.node_id.clone(),
        node_config.group_id.clone(),
        node_config.bind_addr.clone(),
        disk_paths,
    )?);

    // Initialize slot manager with first disk
    let data_dir = if !node_config.disks.is_empty() {
        node_config.disks[0].path.clone()
    } else {
        std::path::PathBuf::from("/tmp/amberblob")
    };
    let slot_manager = Arc::new(SlotManager::new(
        node_config.node_id.clone(),
        data_dir.clone(),
    )?);

    // Initialize chunk store at data directory
    let chunk_store = Arc::new(ChunkStore::new(data_dir)?);

    // Connect to registry (etcd or redis)
    let registry: Arc<dyn Registry> = match config.registry.backend {
        RegistryBackend::Etcd => {
            let etcd_config = config.registry.etcd.as_ref()
                .ok_or_else(|| AmberError::Config("etcd configuration is required for etcd backend".to_string()))?;
            Arc::new(EtcdRegistry::new(&etcd_config.endpoints, &node_config.group_id).await?)
        }
        RegistryBackend::Redis => {
            let redis_config = config.registry.redis.as_ref()
                .ok_or_else(|| AmberError::Config("redis configuration is required for redis backend".to_string()))?;
            Arc::new(RedisRegistry::new(&redis_config.url, &node_config.group_id).await?)
        }
    };

    // Initialize 2PC
    let twopc_coordinator = Arc::new(TwoPhaseCommit::new(node_config.node_id.clone()));
    let twopc_participant = Arc::new(TwoPhaseParticipant::new(node_config.node_id.clone()));

    let state = Arc::new(ServerState {
        node: node.clone(),
        slot_manager,
        chunk_store,
        registry: registry.clone(),
        twopc_coordinator,
        twopc_participant,
        config,
    });

    // Register node in registry
    let node_info = node.info().await;
    registry.register_node(&node_info).await?;

    // Assign slots (simplified - in production this would come from a bootstrap process)
    assign_slots(&state).await?;

    // Start background tasks
    let health_check_state = state.clone();
    tokio::spawn(async move {
        health_check_loop(health_check_state).await;
    });

    // Build router
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/objects/*path", get(get_object).put(put_object).delete(delete_object))
        .route("/objects", get(list_objects))
        .route("/slots/:slot_id", get(get_slot_info))
        .route("/nodes", get(list_nodes))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&node_config.bind_addr).await?;
    tracing::info!("Server listening on {}", node_config.bind_addr);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn assign_slots(state: &Arc<ServerState>) -> Result<()> {
    // Get all nodes and determine slot assignment
    let nodes = state.registry.get_nodes().await?;

    // Simple assignment: divide slots evenly among nodes
    let node_count = nodes.len().max(1);
    let slots_per_node = TOTAL_SLOTS as usize / node_count;
    let my_node_id = state.node.node_id();

    let my_index = nodes.iter().position(|n| n.node_id == my_node_id).unwrap_or(0);
    let start_slot = my_index * slots_per_node;
    let end_slot = if my_index == node_count - 1 {
        TOTAL_SLOTS as usize
    } else {
        start_slot + slots_per_node
    };

    let my_slots: Vec<u16> = (start_slot..end_slot).map(|i| i as u16).collect();

    // Initialize each slot
    for slot_id in &my_slots {
        state.slot_manager.init_slot(*slot_id).await?;
    }

    state.node.assign_slots(my_slots).await;

    tracing::info!(
        "Node {} assigned slots {} to {}",
        my_node_id,
        start_slot,
        end_slot - 1
    );

    Ok(())
}

async fn health_check_loop(state: Arc<ServerState>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

    loop {
        interval.tick().await;

        let slots = state.slot_manager.get_assigned_slots().await;
        for slot_id in slots {
            // Use latest blob_id instead of seq for health reporting
            let blob_id = match get_latest_blob_id_for_slot(&state, slot_id).await {
                Ok(Some(id)) => id,
                _ => continue,
            };

            let health = SlotHealth {
                slot_id,
                node_id: state.node.node_id().to_string(),
                seq: blob_id,  // Using blob_id as the sequence identifier
                status: ReplicaStatus::Healthy,
                last_updated: chrono::Utc::now(),
            };

            if let Err(e) = state.registry.report_health(&health).await {
                tracing::warn!("Failed to report health for slot {}: {}", slot_id, e);
            }
        }
    }
}

async fn get_latest_blob_id_for_slot(state: &Arc<ServerState>, slot_id: u16) -> Result<Option<String>> {
    let slot = state.slot_manager.get_slot(slot_id).await?;
    let store = MetadataStore::new(slot)?;
    store.get_latest_blob_id().map(|opt| opt.map(|ulid| ulid.to_string()))
}

async fn health_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let info = state.node.info().await;
    let slots = state.slot_manager.get_assigned_slots().await;

    let response = serde_json::json!({
        "node_id": info.node_id,
        "group_id": info.group_id,
        "status": info.status,
        "slots_count": slots.len(),
    });

    (StatusCode::OK, axum::Json(response))
}

async fn get_object(
    State(state): State<Arc<ServerState>>,
    Path(path): Path<String>,
    Query(query): Query<GetObjectQuery>,
) -> impl IntoResponse {
    let slot_id = slot_for_key(&path, TOTAL_SLOTS);

    // Check if we have the slot locally
    if state.slot_manager.has_slot(slot_id).await {
        // Read from local storage
        match get_blob_local(&state, &path, query.version, slot_id).await {
            Ok(meta) => {
                // Handle range request
                let start = query.start.unwrap_or(0) as usize;
                let end = query.end.map(|e| e as usize).unwrap_or(meta.size as usize);

                // Stream the blob data
                match stream_blob(&state, &meta, start, end).await {
                    Ok(data) => (StatusCode::OK, data).into_response(),
                    Err(e) => {
                        let resp = ApiResponse::<()> {
                            success: false,
                            data: None,
                            error: Some(e.to_string()),
                        };
                        (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(resp)).into_response()
                    }
                }
            }
            Err(e) => {
                let resp = ApiResponse::<()> {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                };
                (StatusCode::NOT_FOUND, axum::Json(resp)).into_response()
            }
        }
    } else {
        // Proxy to a replica that has the slot
        match proxy_to_replica(&state, slot_id, &path, query.version).await {
            Ok(response) => response,
            Err(e) => {
                let resp = ApiResponse::<()> {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                };
                (StatusCode::NOT_FOUND, axum::Json(resp)).into_response()
            }
        }
    }
}

async fn get_blob_local(
    state: &Arc<ServerState>,
    path: &str,
    version: Option<i64>,
    slot_id: u16,
) -> Result<BlobMeta> {
    let slot = state.slot_manager.get_slot(slot_id).await?;
    let store = MetadataStore::new(slot)?;

    match version {
        Some(v) => store
            .get_blob(path, v)?
            .ok_or_else(|| AmberError::ObjectNotFound(format!("{}@v{}", path, v))),
        None => store
            .get_latest_blob(path)?
            .ok_or_else(|| AmberError::ObjectNotFound(path.to_string())),
    }
}

async fn stream_blob(
    state: &Arc<ServerState>,
    meta: &BlobMeta,
    start: usize,
    end: usize,
) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    let mut current_pos = 0usize;

    for (_index, chunk) in meta.chunks.iter().enumerate() {
        let chunk_start = current_pos;
        let chunk_end = current_pos + chunk.len as usize;

        // Check if this chunk overlaps with the requested range
        if chunk_end > start && chunk_start < end {
            let data = state.chunk_store.get_chunk(&meta.blob_id, &chunk.id).await?;

            let chunk_offset_start = start.saturating_sub(chunk_start);
            let chunk_offset_end = if end < chunk_end {
                end - chunk_start
            } else {
                chunk.len as usize
            };

            result.extend_from_slice(&data[chunk_offset_start..chunk_offset_end]);
        }

        current_pos = chunk_end;
    }

    Ok(result)
}

async fn proxy_to_replica(
    state: &Arc<ServerState>,
    slot_id: u16,
    path: &str,
    version: Option<i64>,
) -> Result<Response> {
    // Get healthy replicas from registry
    let replicas = state.registry.get_healthy_replicas(slot_id).await?;

    if replicas.is_empty() {
        return Err(AmberError::InsufficientReplicas {
            required: 1,
            found: 0,
        });
    }

    // Pick the first healthy replica
    let target_node = &replicas[0].0;

    // Get node info to find address
    let nodes = state.registry.get_nodes().await?;
    let target = nodes
        .into_iter()
        .find(|n| &n.node_id == target_node)
        .ok_or_else(|| AmberError::Internal("Target node not found".to_string()))?;

    // Forward request
    let client = reqwest::Client::new();
    let mut url = format!("http://{}/objects/{}", target.address, path);
    if let Some(v) = version {
        url.push_str(&format!("?version={}", v));
    }

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| AmberError::Http(e.to_string()))?;

    let status = StatusCode::from_u16(response.status().as_u16())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let body = response
        .bytes()
        .await
        .map_err(|e| AmberError::Http(e.to_string()))?;

    Ok((status, body).into_response())
}

async fn put_object(
    State(state): State<Arc<ServerState>>,
    Path(path): Path<String>,
    Query(query): Query<PutObjectQuery>,
    body: Bytes,
) -> impl IntoResponse {
    let slot_id = slot_for_key(&path, TOTAL_SLOTS);

    // Get healthy replicas
    let replicas = match state.registry.get_healthy_replicas(slot_id).await {
        Ok(r) => r,
        Err(e) => {
            let resp = ApiResponse::<()> {
                success: false,
                data: None,
                error: Some(e.to_string()),
            };
            return (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(resp)).into_response();
        }
    };

    if replicas.len() < state.config.replication.min_write_replicas {
        let resp = ApiResponse::<()> {
            success: false,
            data: None,
            error: Some(format!(
                "Insufficient replicas: need {}, found {}",
                state.config.replication.min_write_replicas,
                replicas.len()
            )),
        };
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(resp)).into_response();
    }

    // Get the slot for version calculation and storage
    let slot = match state.slot_manager.get_slot(slot_id).await {
        Ok(s) => s,
        Err(e) => {
            let resp = ApiResponse::<()> {
                success: false,
                data: None,
                error: Some(format!("Slot not available: {}", e)),
            };
            return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(resp)).into_response();
        }
    };

    let store = match MetadataStore::new(slot) {
        Ok(s) => s,
        Err(e) => {
            let resp = ApiResponse::<()> {
                success: false,
                data: None,
                error: Some(e.to_string()),
            };
            return (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(resp)).into_response();
        }
    };

    // Determine version
    let version = match query.version {
        Some(v) => v,
        None => {
            let max = store.get_max_version(&path).unwrap_or(0);
            max + 1
        }
    };

    // Generate blob_id (ULID)
    let blob_id = Ulid::new().to_string();

    // Split body into chunks and store
    let chunks = match store_chunks(&state, &blob_id, &body).await {
        Ok(c) => c,
        Err(e) => {
            // Clean up any stored chunks on failure
            let _ = state.chunk_store.delete_blob_chunks(&blob_id).await;
            let resp = ApiResponse::<()> {
                success: false,
                data: None,
                error: Some(e.to_string()),
            };
            return (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(resp)).into_response();
        }
    };

    // Create blob metadata
    let meta = BlobMeta {
        path: path.clone(),
        version,
        blob_id: blob_id.clone(),
        size: body.len() as u64,
        chunks,
        created_at: chrono::Utc::now(),
        tombstoned_at: None,
    };

    // Perform 2PC
    let participants: Vec<String> = replicas.iter().map(|(id, _)| id.clone()).collect();

    match perform_2pc(&state, participants, slot_id, meta).await {
        Ok(_) => {
            let resp = WriteResponse {
                path,
                version,
                blob_id,
                chunks_stored: body.len() / CHUNK_SIZE + 1,
            };
            let api_resp: ApiResponse<WriteResponse> = ApiResponse {
                success: true,
                data: Some(resp),
                error: None,
            };
            (StatusCode::CREATED, axum::Json(api_resp)).into_response()
        }
        Err(e) => {
            // Chunks become orphaned - will be cleaned up later
            let resp = ApiResponse::<()> {
                success: false,
                data: None,
                error: Some(format!("2PC failed: {}", e)),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(resp)).into_response()
        }
    }
}

async fn store_chunks(
    state: &Arc<ServerState>,
    blob_id: &str,
    data: &Bytes,
) -> Result<Vec<ChunkRef>> {
    let mut chunks = Vec::new();

    for (index, chunk_data) in data.chunks(CHUNK_SIZE).enumerate() {
        let chunk_id = compute_hash(chunk_data);
        let len = chunk_data.len() as u64;

        // Store chunk using new blob-specific API
        state.chunk_store.put_chunk(blob_id, index, Bytes::copy_from_slice(chunk_data)).await?;

        chunks.push(ChunkRef {
            id: chunk_id,
            len,
        });
    }

    Ok(chunks)
}

async fn perform_2pc(
    state: &Arc<ServerState>,
    participants: Vec<String>,
    slot_id: u16,
    meta: BlobMeta,
) -> Result<()> {
    let tx_id = state
        .twopc_coordinator
        .begin_transaction(participants.clone(), slot_id, meta.clone())
        .await?;

    // Phase 1: Prepare
    for participant in &participants {
        let tx = state
            .twopc_coordinator
            .get_transaction(&tx_id)
            .await?
            .ok_or_else(|| AmberError::TwoPhaseCommit("Transaction lost".to_string()))?;

        let vote = if participant.as_str() == state.node.node_id() {
            // Local prepare
            state.twopc_participant.prepare(&tx).await?
        } else {
            // Remote prepare - in production, this would be an RPC
            // For now, assume remote nodes vote Yes
            Vote::Yes
        };

        state
            .twopc_coordinator
            .record_vote(&tx_id, participant, vote)
            .await?;
    }

    // Phase 2: Commit or Abort
    let can_commit = state.twopc_coordinator.can_commit(&tx_id).await?;

    if can_commit {
        state.twopc_coordinator.commit(&tx_id).await?;

        // Apply locally if we're a participant
        if participants.contains(&state.node.node_id().to_string()) {
            if let Some(tx) = state.twopc_coordinator.get_transaction(&tx_id).await? {
                // Store metadata locally
                let slot = state.slot_manager.get_slot(slot_id).await?;
                let store = MetadataStore::new(slot)?;
                store.put_blob(&tx.blob_meta)?;
            }
        }
    } else {
        state.twopc_coordinator.abort(&tx_id).await?;
        return Err(AmberError::TwoPhaseCommit("Transaction aborted".to_string()));
    }

    Ok(())
}

async fn delete_object(
    State(state): State<Arc<ServerState>>,
    Path(path): Path<String>,
    Query(query): Query<GetObjectQuery>,  // Reuse query to get optional version
) -> impl IntoResponse {
    let slot_id = slot_for_key(&path, TOTAL_SLOTS);

    if !state.slot_manager.has_slot(slot_id).await {
        let resp = ApiResponse::<()> {
            success: false,
            data: None,
            error: Some("Slot not local".to_string()),
        };
        return (StatusCode::BAD_REQUEST, axum::Json(resp)).into_response();
    }

    match state.slot_manager.get_slot(slot_id).await {
        Ok(slot) => {
            let store = match MetadataStore::new(slot) {
                Ok(s) => s,
                Err(e) => {
                    let resp = ApiResponse::<()> {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    };
                    return (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(resp)).into_response();
                }
            };

            // If no version specified, delete the latest version
            let version = match query.version {
                Some(v) => v,
                None => {
                    match store.get_max_version(&path) {
                        Ok(v) if v > 0 => v,
                        _ => {
                            let resp = ApiResponse::<()> {
                                success: false,
                                data: None,
                                error: Some("Blob not found".to_string()),
                            };
                            return (StatusCode::NOT_FOUND, axum::Json(resp)).into_response();
                        }
                    }
                }
            };

            match store.delete_blob(&path, version) {
                Ok(true) => {
                    let resp = ApiResponse {
                        success: true,
                        data: Some(serde_json::json!({
                            "deleted": true,
                            "path": path,
                            "version": version,
                            "tombstoned": true
                        })),
                        error: None,
                    };
                    (StatusCode::OK, axum::Json(resp)).into_response()
                }
                Ok(false) => {
                    let resp = ApiResponse::<()> {
                        success: false,
                        data: None,
                        error: Some("Blob not found or already deleted".to_string()),
                    };
                    (StatusCode::NOT_FOUND, axum::Json(resp)).into_response()
                }
                Err(e) => {
                    let resp = ApiResponse::<()> {
                        success: false,
                        data: None,
                        error: Some(e.to_string()),
                    };
                    (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(resp)).into_response()
                }
            }
        }
        Err(e) => {
            let resp = ApiResponse::<()> {
                success: false,
                data: None,
                error: Some(e.to_string()),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(resp)).into_response()
        }
    }
}

async fn list_objects(
    State(state): State<Arc<ServerState>>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    let mut all_objects = Vec::new();

    // List from all local slots
    let slots = state.slot_manager.get_assigned_slots().await;
    for slot_id in slots {
        if let Ok(slot) = state.slot_manager.get_slot(slot_id).await {
            if let Ok(store) = MetadataStore::new(slot) {
                if let Ok(objects) = store.list_blobs(&query.prefix, query.limit, query.include_tombstoned) {
                    all_objects.extend(objects);
                }
            }
        }
    }

    // Convert to response format
    let responses: Vec<ObjectResponse> = all_objects
        .into_iter()
        .map(|meta| ObjectResponse {
            path: meta.path,
            version: meta.version,
            blob_id: meta.blob_id,
            size: meta.size,
            chunks: meta.chunks,
            created_at: meta.created_at.to_rfc3339(),
            tombstoned_at: meta.tombstoned_at.map(|t| t.to_rfc3339()),
        })
        .collect();

    let resp = ApiResponse {
        success: true,
        data: Some(responses),
        error: None,
    };

    (StatusCode::OK, axum::Json(resp))
}

async fn get_slot_info(
    State(state): State<Arc<ServerState>>,
    Path(slot_id): Path<u16>,
) -> impl IntoResponse {
    match state.registry.get_slot(slot_id).await {
        Ok(Some(info)) => {
            let resp: ApiResponse<SlotInfo> = ApiResponse {
                success: true,
                data: Some(info),
                error: None,
            };
            (StatusCode::OK, axum::Json(resp)).into_response()
        }
        Ok(None) => {
            let resp = ApiResponse::<()> {
                success: false,
                data: None,
                error: Some("Slot not found".to_string()),
            };
            (StatusCode::NOT_FOUND, axum::Json(resp)).into_response()
        }
        Err(e) => {
            let resp = ApiResponse::<()> {
                success: false,
                data: None,
                error: Some(e.to_string()),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(resp)).into_response()
        }
    }
}

async fn list_nodes(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    match state.registry.get_nodes().await {
        Ok(nodes) => {
            let resp: ApiResponse<Vec<NodeInfo>> = ApiResponse {
                success: true,
                data: Some(nodes),
                error: None,
            };
            (StatusCode::OK, axum::Json(resp)).into_response()
        }
        Err(e) => {
            let resp = ApiResponse::<()> {
                success: false,
                data: None,
                error: Some(e.to_string()),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(resp)).into_response()
        }
    }
}
