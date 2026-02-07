# AmberBlob Roadmap

This document tracks the implementation status of the AmberBlob design as specified in [RFC 0001](./rfc/0001-initial-design.md).

## Implementation Status Overview

| Component | Status | Notes |
|-----------|--------|-------|
| Basic HTTP API | 🟡 Partial | Needs versioning support |
| Content-addressed Chunks | 🟢 Implemented | Uses SHA256, but storage layout differs from RFC |
| SQLite Metadata | 🟡 Partial | Schema differs significantly from RFC |
| Two-Phase Commit | 🟡 Framework | Core logic exists but needs integration |
| Registry (etcd/Redis) | 🟢 Implemented | Pluggable registry backends working |
| Multi-Version Support | 🔴 Not Started | No versioning in current implementation |
| Chunk-Level Archiving | 🔴 Not Started | Currently object-level only |
| Tombstone (Soft Delete) | 🔴 Not Started | Currently hard delete |
| Anti-Entropy | 🔴 Not Started | Framework only |
| Directory Structure | 🔴 Mismatch | Needs restructuring per RFC |

Legend:
- 🟢 Implemented: Matches RFC specification
- 🟡 Partial: Partially implemented or differs from RFC
- 🔴 Not Started: Not yet implemented

---

## Detailed Gap Analysis

### 1. Directory Structure

**Current Implementation:**
```
<disk>/amberblob/slot_<id>/
├── chunks/           # Shared content-addressed chunk storage
│   └── <hash_prefix>/<hash>
└── meta/
    └── metadata.db   # SQLite database
```

**RFC Specification:**
```
<disk>/slots/<slot_id>/
├── meta.sqlite3          # blob 元信息数据库
└── blobs/                # blob 存储根目录
    └── <blob_id>/        # 每个 blob 独立目录
        └── chunks/       # blob 的 chunk 存储目录
            ├── <chunk_id_1>
            └── ...
```

**Required Changes:**
- [ ] Rename `slot_<id>` to `slots/<slot_id>` format
- [ ] Move chunks from shared directory to per-blob structure
- [ ] Rename `meta/metadata.db` to `meta.sqlite3`
- [ ] Create `blobs/<blob_id>/chunks/` directory structure

### 2. Database Schema

#### 2.1 Objects/Blobs Table

**Current (`objects`):**
```sql
CREATE TABLE objects (
    path TEXT PRIMARY KEY,           -- ❌ Single path, no versioning
    size INTEGER NOT NULL,
    chunks TEXT NOT NULL,
    seq TEXT NOT NULL,               -- ❌ Separate seq (not using ULID from blob_id)
    created_at TEXT NOT NULL,
    modified_at TEXT NOT NULL,       -- ❌ Extra field not in RFC
    archived INTEGER NOT NULL DEFAULT 0,  -- ❌ Object-level archive
    archive_location TEXT            -- ❌ Object-level archive location
);
```

**RFC (`blobs`):**
```sql
CREATE TABLE blobs (
    pk INTEGER PRIMARY KEY AUTOINCREMENT,  -- ✅ Missing
    path TEXT NOT NULL,              -- ✅ Same
    version INTEGER NOT NULL,        -- ❌ Missing - critical feature
    blob_id TEXT NOT NULL,           -- ❌ Missing - replaces seq
    size INTEGER NOT NULL,           -- ✅ Same
    chunks TEXT NOT NULL,            -- ✅ Same
    created_at DATETIME NOT NULL,    -- 🟡 Type mismatch (TEXT vs DATETIME)
    tombstoned_at DATETIME,          -- ❌ Missing - soft delete support
    UNIQUE (path, version)           -- ❌ Missing - version constraint
);

CREATE INDEX idx_blobs_blob_id ON blobs(blob_id);  -- ❌ Missing
```

**Required Changes:**
- [ ] Rename table `objects` → `blobs`
- [ ] Add `pk` autoincrement primary key
- [ ] Add `version` column
- [ ] Replace `seq` with `blob_id` (ULID)
- [ ] Remove `modified_at` column
- [ ] Add `tombstoned_at` for soft delete
- [ ] Change primary key from `path` to `UNIQUE (path, version)`
- [ ] Add index on `blob_id`
- [ ] Update all queries to use `(path, version)` instead of just `path`

#### 2.2 Chunk Archives Table

**Current:** Not implemented (archive is object-level in `objects` table)

**RFC (`blob_chunk_archives`):**
```sql
CREATE TABLE blob_chunk_archives (
    pk INTEGER PRIMARY KEY AUTOINCREMENT,
    blob_id TEXT NOT NULL,
    chunk_id TEXT NOT NULL,
    length INTEGER NOT NULL,
    archived_at DATETIME NOT NULL,
    target_path TEXT NOT NULL,
    target_version INT,
    target_range_start INTEGER,
    target_range_end INTEGER,
    UNIQUE (blob_id, chunk_id),
    FOREIGN KEY (blob_id) REFERENCES blobs(blob_id)
);
```

**Required Changes:**
- [ ] Create new `blob_chunk_archives` table
- [ ] Migrate archive logic from object-level to chunk-level
- [ ] Update archive/restore operations to track per-chunk

### 3. Data Model Changes

**Current `ObjectMeta`:**
```rust
pub struct ObjectMeta {
    pub path: String,
    pub size: u64,
    pub chunks: Vec<ChunkInfo>,
    pub seq: String,                    // ❌ Should be derived from blob_id
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,     // ❌ Not in RFC
    pub archived: bool,                 // ❌ Object-level
    pub archive_location: Option<String>, // ❌ Object-level
}

pub struct ChunkInfo {
    pub hash: String,
    pub size: u64,
    pub offset: u64,                    // ✅ Useful, keep
}
```

**RFC Structure:**
```rust
// Renamed to BlobMeta
pub struct BlobMeta {
    pub path: String,
    pub version: i64,                   // ❌ Missing
    pub blob_id: String,                // ULID, replaces seq
    pub size: u64,
    pub chunks: Vec<ChunkRef>,          // JSON in DB
    pub created_at: DateTime<Utc>,
    pub tombstoned_at: Option<DateTime<Utc>>,  // ❌ Missing
}

pub struct ChunkRef {
    pub id: String,                     // hash
    pub len: u64,                       // renamed from size
}
```

**Required Changes:**
- [ ] Rename `ObjectMeta` → `BlobMeta`
- [ ] Add `version` field
- [ ] Replace `seq` with `blob_id` (ULID)
- [ ] Remove `modified_at`
- [ ] Remove `archived` and `archive_location` (moved to chunk table)
- [ ] Add `tombstoned_at`
- [ ] Rename `ChunkInfo` → `ChunkRef` with `id` and `len` fields

### 4. API Changes

**Current API:**
```
PUT /objects/{path}
GET /objects/{path}?start=&end=
DELETE /objects/{path}
GET /objects?prefix=&limit=
```

**RFC API Requirements:**
```
PUT /objects/{path}?version=       # Optional version parameter
GET /objects/{path}?version=       # Read specific version
DELETE /objects/{path}             # Soft delete (tombstone)
GET /objects?prefix=&limit=        # Filter tombstoned
```

**Required Changes:**
- [ ] Add optional `version` query parameter to PUT
- [ ] Add optional `version` query parameter to GET
- [ ] Change DELETE to set tombstone instead of hard delete
- [ ] Update list to filter tombstoned blobs by default
- [ ] Add API to list versions of a path
- [ ] Add API to restore/undelete tombstoned blobs

### 5. Write Flow Changes

**Current Flow:**
1. Client → any node
2. Query etcd for slot replicas
3. Check replica count
4. Store chunks (content-addressed)
5. Create metadata with new `seq` (ULID)
6. Perform 2PC
7. Return success

**RFC Flow:**
1. Client → any node (with optional version)
2. Query etcd for slot replicas
3. Check replica count
4. Generate new `blob_id` (ULID)
5. Determine version (current max + 1 if not specified)
6. Store chunks in `blobs/{blob_id}/chunks/`
7. Perform 2PC with blob_id as sequence
8. Return success

**Key Differences:**
- [ ] Generate `blob_id` (ULID) early in the process
- [ ] Support optional version parameter from client
- [ ] Store chunks per-blob instead of shared content-addressed
- [ ] Use `blob_id` instead of separate `seq` field

### 6. Read Flow Changes

**Current:** Read by path only

**RFC:** Support versioned reads

**Required Changes:**
- [ ] Accept optional `version` parameter
- [ ] If version not specified, read latest non-tombstoned version
- [ ] Support reading specific version even if tombstoned

### 7. Anti-Entropy / Recovery

**Current:** Framework exists but not fully implemented

**RFC:**
- Replica compares `blob_id` (ULID) with other replicas
- Fetches missing or newer blobs
- Handles tombstone propagation

**Required Changes:**
- [ ] Implement blob comparison by `blob_id` (ULID ordering)
- [ ] Implement missing blob sync
- [ ] Implement tombstone sync

### 8. Slot Manager Changes

**Current:**
- Tracks `seq` separately per slot
- Stores chunks in shared directory

**RFC:**
- Use `blob_id` from blobs table for consistency
- No separate slot seq tracking needed

**Required Changes:**
- [ ] Remove separate slot seq tracking
- [ ] Use max `blob_id` from blobs table for anti-entropy

---

## Implementation Phases

### Phase 1: Schema Migration (Critical)
- [ ] Update database schema to match RFC
- [ ] Rename `objects` → `blobs` with new columns
- [ ] Create `blob_chunk_archives` table
- [ ] Update `MetadataStore` to use new schema
- [ ] Add migration path for existing data

### Phase 2: Directory Structure
- [ ] Restructure storage layout
- [ ] Move to per-blob chunk storage
- [ ] Update `ChunkStore` to support new layout
- [ ] Update path resolution logic

### Phase 3: Versioning Support
- [ ] Add version field to all APIs
- [ ] Update write flow to support versions
- [ ] Update read flow to handle versions
- [ ] Add list versions API

### Phase 4: Tombstone & Soft Delete
- [ ] Implement soft delete
- [ ] Update list to filter tombstoned
- [ ] Add undelete API
- [ ] Add garbage collection for old tombstoned blobs

### Phase 5: Chunk-Level Archiving
- [ ] Implement `blob_chunk_archives` tracking
- [ ] Migrate from object-level to chunk-level archive
- [ ] Update archive/restore logic
- [ ] Implement S3 integration for chunk archiving

### Phase 6: Anti-Entropy
- [ ] Implement full anti-entropy protocol
- [ ] Add blob comparison logic
- [ ] Implement background sync

---

## Notes

### Naming Conventions
The RFC uses `blob` terminology while the current code uses `object`. The migration should:
1. Rename database tables and columns
2. Rename Rust structs (`ObjectMeta` → `BlobMeta`)
3. Keep HTTP API paths as `/objects/*` for backward compatibility (or add `/blobs/*` as alias)

### Backward Compatibility
Considerations for migrating existing deployments:
- Database migration script needed
- Chunk storage layout change requires data migration or dual-read support
- API can remain compatible with optional new parameters

### Testing Requirements
Each phase should include:
- Unit tests for new schema operations
- Integration tests for versioned reads/writes
- Migration tests for existing data
- End-to-end tests for anti-entropy
