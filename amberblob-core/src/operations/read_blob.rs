use crate::{
    AmberError, BlobHead, BlobMeta, Coordinator, HeadKind, MetadataStore, NodeInfo, PartRef,
    PartStore, Result, SlotManager,
};
use bytes::Bytes;
use chrono::Utc;
use std::sync::Arc;

#[derive(Clone)]
pub struct ReadBlobOperation {
    slot_manager: Arc<SlotManager>,
    part_store: Arc<PartStore>,
    coordinator: Arc<Coordinator>,
}

#[derive(Debug, Clone)]
pub struct ReadBlobOperationRequest {
    pub slot_id: u16,
    pub path: String,
    pub replicas: Vec<NodeInfo>,
    pub local_node_id: String,
    pub include_body: bool,
}

#[derive(Debug, Clone)]
pub struct ReadBlobOperationResult {
    pub meta: BlobMeta,
    pub body: Option<Bytes>,
}

#[derive(Debug, Clone)]
pub enum ReadBlobOperationOutcome {
    Found(ReadBlobOperationResult),
    NotFound,
    Deleted,
}

impl ReadBlobOperation {
    pub fn new(
        slot_manager: Arc<SlotManager>,
        part_store: Arc<PartStore>,
        coordinator: Arc<Coordinator>,
    ) -> Self {
        Self {
            slot_manager,
            part_store,
            coordinator,
        }
    }

    pub async fn run(&self, request: ReadBlobOperationRequest) -> Result<ReadBlobOperationOutcome> {
        let ReadBlobOperationRequest {
            slot_id,
            path,
            replicas,
            local_node_id,
            include_body,
        } = request;

        let head = self
            .ensure_head_available(slot_id, &path, &replicas, &local_node_id)
            .await?;
        let Some(head) = head else {
            return Ok(ReadBlobOperationOutcome::NotFound);
        };

        if head.head_kind == HeadKind::Tombstone {
            return Ok(ReadBlobOperationOutcome::Deleted);
        }

        let meta = head
            .meta
            .ok_or_else(|| AmberError::Internal("meta payload missing".to_string()))?;

        if !include_body {
            return Ok(ReadBlobOperationOutcome::Found(ReadBlobOperationResult {
                meta,
                body: None,
            }));
        }

        let peer_nodes: Vec<NodeInfo> = replicas
            .into_iter()
            .filter(|node| node.node_id != local_node_id)
            .collect();

        let mut body = Vec::with_capacity(meta.size_bytes as usize);
        for part in &meta.parts {
            let bytes = if self.part_store.part_exists(slot_id, &path, &part.sha256) {
                match self.part_store.get_part(slot_id, &path, &part.sha256).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        tracing::warn!(
                            "Failed to read local part. slot={} path={} sha={} error={}",
                            slot_id,
                            path,
                            part.sha256,
                            error
                        );
                        self.fetch_part_from_peers_and_store(&peer_nodes, slot_id, &path, part)
                            .await?
                    }
                }
            } else {
                self.fetch_part_from_peers_and_store(&peer_nodes, slot_id, &path, part)
                    .await?
            };

            body.extend_from_slice(&bytes);
        }

        Ok(ReadBlobOperationOutcome::Found(ReadBlobOperationResult {
            meta,
            body: Some(Bytes::from(body)),
        }))
    }

    pub async fn fetch_remote_head(
        &self,
        address: &str,
        slot_id: u16,
        path: &str,
    ) -> Result<Option<BlobHead>> {
        let head_url = self.coordinator.internal_head_url(address, slot_id, path)?;
        let response = self
            .coordinator
            .client()
            .get(head_url)
            .send()
            .await
            .map_err(|error| AmberError::Http(error.to_string()))?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !response.status().is_success() {
            return Err(AmberError::Http(format!(
                "internal head fetch failed: status={} path={}",
                response.status(),
                path
            )));
        }

        #[derive(serde::Deserialize)]
        struct InternalHeadResponse {
            found: bool,
            head_kind: Option<String>,
            generation: Option<i64>,
            head_sha256: Option<String>,
            meta: Option<BlobMeta>,
            tombstone: Option<crate::TombstoneMeta>,
        }

        let payload: InternalHeadResponse = response
            .json()
            .await
            .map_err(|error| AmberError::Http(error.to_string()))?;

        if !payload.found {
            return Ok(None);
        }

        let head_kind = match payload.head_kind.as_deref() {
            Some("meta") => HeadKind::Meta,
            Some("tombstone") => HeadKind::Tombstone,
            _ => return Err(AmberError::Internal("invalid remote head kind".to_string())),
        };

        let generation = payload
            .generation
            .ok_or_else(|| AmberError::Internal("missing remote generation".to_string()))?;

        let head_sha256 = payload
            .head_sha256
            .ok_or_else(|| AmberError::Internal("missing remote head_sha256".to_string()))?;

        Ok(Some(BlobHead {
            path: path.to_string(),
            generation,
            head_kind,
            head_sha256,
            updated_at: Utc::now(),
            meta: payload.meta,
            tombstone: payload.tombstone,
        }))
    }

    pub async fn apply_remote_head_locally(
        &self,
        slot_id: u16,
        path: &str,
        head: &BlobHead,
    ) -> Result<()> {
        let store = self.ensure_store(slot_id).await?;

        match head.head_kind {
            HeadKind::Meta => {
                let mut meta = head
                    .meta
                    .clone()
                    .ok_or_else(|| AmberError::Internal("missing meta payload".to_string()))?;
                meta.path = path.to_string();
                meta.slot_id = slot_id;
                meta.generation = head.generation;
                if meta.version == 0 {
                    meta.version = meta.generation;
                }

                for part in &mut meta.parts {
                    if part.external_path.is_none() {
                        if let Ok(part_path) =
                            self.part_store.part_path(slot_id, path, &part.sha256)
                        {
                            part.external_path = Some(part_path.to_string_lossy().to_string());
                        }
                    }
                    store.upsert_part_entry(path, part)?;
                }

                let inline_data = serde_json::to_vec(&meta)?;
                store.upsert_meta_with_payload(&meta, &inline_data, &head.head_sha256)?;
            }
            HeadKind::Tombstone => {
                let mut tombstone = head
                    .tombstone
                    .clone()
                    .ok_or_else(|| AmberError::Internal("missing tombstone payload".to_string()))?;
                tombstone.path = path.to_string();
                tombstone.slot_id = slot_id;
                tombstone.generation = head.generation;

                let inline_data = serde_json::to_vec(&tombstone)?;
                store.insert_tombstone_with_payload(&tombstone, &inline_data, &head.head_sha256)?;
            }
        }

        Ok(())
    }

    pub async fn repair_path_from_head(
        &self,
        source: &NodeInfo,
        slot_id: u16,
        path: &str,
        remote_head: &BlobHead,
    ) -> Result<()> {
        if let HeadKind::Meta = remote_head.head_kind {
            let meta = remote_head
                .meta
                .clone()
                .ok_or_else(|| AmberError::Internal("missing meta payload".to_string()))?;

            for part in &meta.parts {
                if self.part_store.part_exists(slot_id, path, &part.sha256) {
                    continue;
                }

                let part_url = self.coordinator.internal_part_url(
                    &source.address,
                    slot_id,
                    &part.sha256,
                    path,
                )?;
                let response = self
                    .coordinator
                    .client()
                    .get(part_url)
                    .send()
                    .await
                    .map_err(|error| AmberError::Http(error.to_string()))?;

                if !response.status().is_success() {
                    return Err(AmberError::Http(format!(
                        "failed to fetch part {} from source {}: {}",
                        part.sha256,
                        source.node_id,
                        response.status()
                    )));
                }

                let bytes = response
                    .bytes()
                    .await
                    .map_err(|error| AmberError::Http(error.to_string()))?;

                let put_result = self
                    .part_store
                    .put_part(slot_id, path, &part.sha256, bytes)
                    .await?;

                let store = self.ensure_store(slot_id).await?;
                let mut local_part = part.clone();
                local_part.external_path = Some(put_result.part_path.to_string_lossy().to_string());
                store.upsert_part_entry(path, &local_part)?;
            }
        }

        self.apply_remote_head_locally(slot_id, path, remote_head)
            .await
    }

    async fn ensure_head_available(
        &self,
        slot_id: u16,
        path: &str,
        replicas: &[NodeInfo],
        local_node_id: &str,
    ) -> Result<Option<BlobHead>> {
        let store = self.ensure_store(slot_id).await?;
        if let Some(head) = store.get_current_head(path)? {
            return Ok(Some(head));
        }

        for node in replicas.iter().filter(|node| node.node_id != local_node_id) {
            if let Some(remote_head) = self.fetch_remote_head(&node.address, slot_id, path).await? {
                self.apply_remote_head_locally(slot_id, path, &remote_head)
                    .await?;
                return Ok(Some(remote_head));
            }
        }

        Ok(None)
    }

    async fn fetch_part_from_peers_and_store(
        &self,
        peers: &[NodeInfo],
        slot_id: u16,
        path: &str,
        part: &PartRef,
    ) -> Result<Bytes> {
        for peer in peers {
            let part_url =
                self.coordinator
                    .internal_part_url(&peer.address, slot_id, &part.sha256, path)?;

            let response = match self.coordinator.client().get(part_url).send().await {
                Ok(response) => response,
                Err(_) => continue,
            };

            if !response.status().is_success() {
                continue;
            }

            let bytes = match response.bytes().await {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };

            let put_result = self
                .part_store
                .put_part(slot_id, path, &part.sha256, bytes.clone())
                .await?;

            let store = self.ensure_store(slot_id).await?;
            let mut local_part = part.clone();
            local_part.external_path = Some(put_result.part_path.to_string_lossy().to_string());
            store.upsert_part_entry(path, &local_part)?;

            return Ok(bytes);
        }

        Err(AmberError::PartNotFound(part.sha256.clone()))
    }

    async fn ensure_store(&self, slot_id: u16) -> Result<MetadataStore> {
        if !self.slot_manager.has_slot(slot_id).await {
            self.slot_manager.init_slot(slot_id).await?;
        }

        let slot = self.slot_manager.get_slot(slot_id).await?;
        MetadataStore::new(slot)
    }
}
