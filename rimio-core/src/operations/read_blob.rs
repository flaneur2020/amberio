use crate::{
    BlobHead, BlobMeta, ClusterClient, HeadKind, MetadataStore, NodeInfo, PART_SIZE, PartStore,
    Result, RimError, SlotManager, compute_hash,
};
use bytes::Bytes;
use reqwest::header::HeaderMap;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct ReadBlobOperation {
    slot_manager: Arc<SlotManager>,
    part_store: Arc<PartStore>,
    cluster_client: Arc<ClusterClient>,
}

#[derive(Debug, Clone, Copy)]
pub struct ReadByteRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone)]
pub struct ReadBlobOperationRequest {
    pub slot_id: u16,
    pub path: String,
    pub replicas: Vec<NodeInfo>,
    pub local_node_id: String,
    pub include_body: bool,
    pub range: Option<ReadByteRange>,
}

#[derive(Debug, Clone)]
pub struct ReadBlobOperationResult {
    pub meta: BlobMeta,
    pub body: Option<Bytes>,
    pub body_range: Option<ReadByteRange>,
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
        cluster_client: Arc<ClusterClient>,
    ) -> Self {
        Self {
            slot_manager,
            part_store,
            cluster_client,
        }
    }

    pub async fn run(&self, request: ReadBlobOperationRequest) -> Result<ReadBlobOperationOutcome> {
        let ReadBlobOperationRequest {
            slot_id,
            path,
            replicas,
            local_node_id,
            include_body,
            range,
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
            .ok_or_else(|| RimError::Internal("meta payload missing".to_string()))?;

        if !include_body {
            return Ok(ReadBlobOperationOutcome::Found(ReadBlobOperationResult {
                meta,
                body: None,
                body_range: None,
            }));
        }

        if meta.size_bytes == 0 {
            if range.is_some() {
                return Err(RimError::InvalidRequest(
                    "range not satisfiable for empty blob".to_string(),
                ));
            }

            return Ok(ReadBlobOperationOutcome::Found(ReadBlobOperationResult {
                meta,
                body: Some(Bytes::new()),
                body_range: None,
            }));
        }

        let body_range = resolve_effective_range(meta.size_bytes, range)?;
        let part_size = meta.part_size.max(1);

        let first_part = body_range.start / part_size;
        let last_part = body_range.end / part_size;

        let peer_nodes: Vec<NodeInfo> = replicas
            .into_iter()
            .filter(|node| node.node_id != local_node_id)
            .collect();

        let mut body = Vec::with_capacity((body_range.end - body_range.start + 1) as usize);
        for part_no_u64 in first_part..=last_part {
            let part_no = u32::try_from(part_no_u64)
                .map_err(|_| RimError::Internal(format!("part index overflow: {}", part_no_u64)))?;

            let bytes = self
                .read_part_bytes(&peer_nodes, slot_id, &path, &meta, part_no)
                .await?;

            let part_start = part_no_u64 * part_size;
            let slice_start = if part_no_u64 == first_part {
                (body_range.start - part_start) as usize
            } else {
                0
            };
            let slice_end_exclusive = if part_no_u64 == last_part {
                ((body_range.end - part_start) + 1) as usize
            } else {
                bytes.len()
            };

            if slice_start > slice_end_exclusive || slice_end_exclusive > bytes.len() {
                return Err(RimError::Internal(format!(
                    "invalid part slice: path={} generation={} part_no={} start={} end={} len={}",
                    path,
                    meta.generation,
                    part_no,
                    slice_start,
                    slice_end_exclusive,
                    bytes.len()
                )));
            }

            body.extend_from_slice(&bytes[slice_start..slice_end_exclusive]);
        }

        Ok(ReadBlobOperationOutcome::Found(ReadBlobOperationResult {
            meta,
            body: Some(Bytes::from(body)),
            body_range: Some(body_range),
        }))
    }

    pub async fn fetch_remote_head(
        &self,
        node_id: &str,
        slot_id: u16,
        path: &str,
    ) -> Result<Option<BlobHead>> {
        self.cluster_client
            .fetch_remote_head(node_id, slot_id, path)
            .await
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
                    .ok_or_else(|| RimError::Internal("missing meta payload".to_string()))?;
                meta.path = path.to_string();
                meta.slot_id = slot_id;
                meta.generation = head.generation;
                if meta.version == 0 {
                    meta.version = meta.generation;
                }
                if meta.part_size == 0 {
                    meta.part_size = PART_SIZE as u64;
                }
                if meta.part_count == 0 && meta.size_bytes > 0 {
                    meta.part_count = meta.size_bytes.div_ceil(meta.part_size.max(1)) as u32;
                }

                let inline_data = serde_json::to_vec(&meta)?;
                store.upsert_meta_with_payload(&meta, &inline_data, &head.head_sha256)?;
            }
            HeadKind::Tombstone => {
                let mut tombstone = head
                    .tombstone
                    .clone()
                    .ok_or_else(|| RimError::Internal("missing tombstone payload".to_string()))?;
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
        source_node_id: &str,
        slot_id: u16,
        path: &str,
        remote_head: &BlobHead,
    ) -> Result<()> {
        if let HeadKind::Meta = remote_head.head_kind {
            let meta = remote_head
                .meta
                .clone()
                .ok_or_else(|| RimError::Internal("missing meta payload".to_string()))?;

            let store = self.ensure_store(slot_id).await?;

            for part_no in 0..meta.part_count {
                let already_local = match store.get_part_entry(path, meta.generation, part_no)? {
                    Some(entry) => {
                        if let Some(external_path) = entry.external_path {
                            Path::new(&external_path).exists()
                        } else {
                            self.part_store.part_exists(
                                slot_id,
                                path,
                                meta.generation,
                                part_no,
                                &entry.sha256,
                            )
                        }
                    }
                    None => false,
                };

                if already_local {
                    continue;
                }

                let payload = self
                    .cluster_client
                    .fetch_part_by_index(source_node_id, slot_id, path, meta.generation, part_no)
                    .await?;

                let sha256 = resolve_part_sha256(Some(&payload.headers), &payload.bytes, None);
                let bytes = payload.bytes;

                let put_result = self
                    .part_store
                    .put_part(
                        slot_id,
                        path,
                        meta.generation,
                        part_no,
                        &sha256,
                        bytes.clone(),
                    )
                    .await?;

                store.upsert_part_entry(
                    path,
                    meta.generation,
                    part_no,
                    &sha256,
                    bytes.len() as u64,
                    Some(put_result.part_path.to_string_lossy().as_ref()),
                    None,
                )?;
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
            if let Some(remote_head) = self.fetch_remote_head(&node.node_id, slot_id, path).await? {
                self.apply_remote_head_locally(slot_id, path, &remote_head)
                    .await?;
                return Ok(Some(remote_head));
            }
        }

        Ok(None)
    }

    async fn read_part_bytes(
        &self,
        peers: &[NodeInfo],
        slot_id: u16,
        path: &str,
        meta: &BlobMeta,
        part_no: u32,
    ) -> Result<Bytes> {
        let store = self.ensure_store(slot_id).await?;

        if let Some(entry) = store.get_part_entry(path, meta.generation, part_no)? {
            if let Ok(local) = self
                .read_local_part(
                    slot_id,
                    path,
                    meta.generation,
                    part_no,
                    &entry.sha256,
                    entry.external_path.as_deref(),
                )
                .await
            {
                return Ok(local);
            }

            if let Some(archive_url) = entry.archive_url.as_deref().or(meta.archive_url.as_deref())
            {
                match self
                    .fetch_part_from_archive_and_store(
                        slot_id,
                        path,
                        meta,
                        part_no,
                        Some(entry.sha256.as_str()),
                        archive_url,
                    )
                    .await
                {
                    Ok(bytes) => return Ok(bytes),
                    Err(error) => {
                        tracing::warn!(
                            "archive fallback failed. slot={} path={} generation={} part_no={} archive_url={} error={}",
                            slot_id,
                            path,
                            meta.generation,
                            part_no,
                            archive_url,
                            error
                        );
                    }
                }
            }

            return self
                .fetch_part_from_peers_and_store(
                    peers,
                    slot_id,
                    path,
                    meta.generation,
                    part_no,
                    Some(entry.sha256.as_str()),
                )
                .await;
        }

        if let Some(archive_url) = meta.archive_url.as_deref() {
            match self
                .fetch_part_from_archive_and_store(slot_id, path, meta, part_no, None, archive_url)
                .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(error) => {
                    tracing::warn!(
                        "archive fallback failed without local index. slot={} path={} generation={} part_no={} archive_url={} error={}",
                        slot_id,
                        path,
                        meta.generation,
                        part_no,
                        archive_url,
                        error
                    );
                }
            }
        }

        self.fetch_part_from_peers_and_store(peers, slot_id, path, meta.generation, part_no, None)
            .await
    }

    async fn read_local_part(
        &self,
        slot_id: u16,
        path: &str,
        generation: i64,
        part_no: u32,
        sha256: &str,
        external_path: Option<&str>,
    ) -> Result<Bytes> {
        if self
            .part_store
            .part_exists(slot_id, path, generation, part_no, sha256)
        {
            return self
                .part_store
                .get_part(slot_id, path, generation, part_no, sha256)
                .await;
        }

        if let Some(external_path) = external_path {
            if Path::new(external_path).exists() {
                let bytes = tokio::fs::read(external_path).await?;
                return Ok(Bytes::from(bytes));
            }
        }

        Err(RimError::PartNotFound(format!(
            "path={} generation={} part_no={}",
            path, generation, part_no
        )))
    }

    async fn fetch_part_from_archive_and_store(
        &self,
        slot_id: u16,
        path: &str,
        meta: &BlobMeta,
        part_no: u32,
        expected_sha256: Option<&str>,
        archive_url: &str,
    ) -> Result<Bytes> {
        let (range_start, range_end) = part_byte_range(meta, part_no)?;
        let bytes = fetch_archive_range_bytes(archive_url, range_start, range_end).await?;

        let expected_length = (range_end - range_start + 1) as usize;
        if bytes.len() != expected_length {
            return Err(RimError::Internal(format!(
                "archive range length mismatch: archive_url={} path={} part_no={} expected={} actual={}",
                archive_url,
                path,
                part_no,
                expected_length,
                bytes.len()
            )));
        }

        let sha256 = resolve_part_sha256(None, &bytes, expected_sha256);
        if let Some(expected) = expected_sha256 {
            if sha256 != expected {
                return Err(RimError::HashMismatch {
                    expected: expected.to_string(),
                    actual: sha256,
                });
            }
        }

        let put_result = self
            .part_store
            .put_part(
                slot_id,
                path,
                meta.generation,
                part_no,
                &sha256,
                bytes.clone(),
            )
            .await?;

        let store = self.ensure_store(slot_id).await?;
        store.upsert_part_entry(
            path,
            meta.generation,
            part_no,
            &sha256,
            bytes.len() as u64,
            Some(put_result.part_path.to_string_lossy().as_ref()),
            Some(archive_url),
        )?;

        Ok(bytes)
    }

    async fn fetch_part_from_peers_and_store(
        &self,
        peers: &[NodeInfo],
        slot_id: u16,
        path: &str,
        generation: i64,
        part_no: u32,
        expected_sha256: Option<&str>,
    ) -> Result<Bytes> {
        for peer in peers {
            let payload = if let Some(sha256) = expected_sha256 {
                self.cluster_client
                    .fetch_part_by_sha(&peer.node_id, slot_id, sha256, path, generation, part_no)
                    .await
            } else {
                self.cluster_client
                    .fetch_part_by_index(&peer.node_id, slot_id, path, generation, part_no)
                    .await
            };

            let payload = match payload {
                Ok(payload) => payload,
                Err(_) => continue,
            };

            let sha256 =
                resolve_part_sha256(Some(&payload.headers), &payload.bytes, expected_sha256);
            let bytes = payload.bytes;
            if let Some(expected_sha256) = expected_sha256 {
                if sha256 != expected_sha256 {
                    continue;
                }
            }

            let put_result = match self
                .part_store
                .put_part(slot_id, path, generation, part_no, &sha256, bytes.clone())
                .await
            {
                Ok(result) => result,
                Err(_) => continue,
            };

            let store = self.ensure_store(slot_id).await?;
            store.upsert_part_entry(
                path,
                generation,
                part_no,
                &sha256,
                bytes.len() as u64,
                Some(put_result.part_path.to_string_lossy().as_ref()),
                None,
            )?;

            return Ok(bytes);
        }

        Err(RimError::PartNotFound(format!(
            "path={} generation={} part_no={}",
            path, generation, part_no
        )))
    }

    async fn ensure_store(&self, slot_id: u16) -> Result<MetadataStore> {
        if !self.slot_manager.has_slot(slot_id).await {
            self.slot_manager.init_slot(slot_id).await?;
        }

        let slot = self.slot_manager.get_slot(slot_id).await?;
        MetadataStore::new(slot)
    }
}

fn resolve_effective_range(
    size_bytes: u64,
    requested: Option<ReadByteRange>,
) -> Result<ReadByteRange> {
    match requested {
        Some(range) => {
            if range.start > range.end || range.end >= size_bytes {
                return Err(RimError::InvalidRequest(format!(
                    "range not satisfiable: start={} end={} size={}",
                    range.start, range.end, size_bytes
                )));
            }
            Ok(range)
        }
        None => Ok(ReadByteRange {
            start: 0,
            end: size_bytes - 1,
        }),
    }
}

fn resolve_part_sha256(
    headers: Option<&HeaderMap>,
    body: &[u8],
    expected_sha256: Option<&str>,
) -> String {
    if let Some(headers) = headers {
        if let Some(value) = headers.get("x-rimio-sha256") {
            if let Ok(value) = value.to_str() {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    let actual = compute_hash(body);
                    if actual == trimmed {
                        return trimmed.to_string();
                    }
                    return actual;
                }
            }
        }
    }

    if let Some(expected) = expected_sha256 {
        let actual = compute_hash(body);
        if actual == expected {
            return expected.to_string();
        }
        return actual;
    }

    compute_hash(body)
}

fn part_byte_range(meta: &BlobMeta, part_no: u32) -> Result<(u64, u64)> {
    let part_size = meta.part_size.max(1);
    let start = part_no as u64 * part_size;
    if start >= meta.size_bytes {
        return Err(RimError::InvalidRequest(format!(
            "part_no out of range: part_no={} size_bytes={} part_size={}",
            part_no, meta.size_bytes, part_size
        )));
    }

    let end = (start + part_size - 1).min(meta.size_bytes - 1);
    Ok((start, end))
}

async fn fetch_archive_range_bytes(archive_url: &str, start: u64, end: u64) -> Result<Bytes> {
    crate::read_archive_range_bytes(archive_url, start, end).await
}
