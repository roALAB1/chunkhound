# ChunkHound SurrealDB Integration - Remaining TODOs

## High Priority

### 1. Enable Embeddings (requires API key)

**Status:** BLOCKED
**Blocker:** Need `VOYAGE_API_KEY` environment variable

**Steps:**
```bash
export VOYAGE_API_KEY=<key>
chunkhound index .
chunkhound search "semantic query"  # Should work with embeddings
```

**Files to verify:**
- `chunkhound/providers/embeddings/voyageai_provider.py`
- Embedding storage in SurrealDB `embedding` table

---

### 2. MCP Server Testing

**Status:** NOT TESTED

**Steps:**
```bash
chunkhound mcp  # Start MCP server with SurrealDB backend
```

**Verify:**
- MCP tools connect to SurrealDB correctly
- Search returns results
- No async/threading issues

---

### 3. SurrealDB Vector Index for Semantic Search

**Status:** NOT IMPLEMENTED

SurrealDB supports vector indexes. Need to:

1. Create vector index in schema:
```sql
DEFINE INDEX idx_embedding_vector ON embedding FIELDS vector HNSW M=16 EF=150 DIM 1024;
```

2. Implement `_executor_search_semantic()` in surrealdb_provider.py

3. Use SurrealDB's vector similarity search:
```sql
SELECT * FROM embedding WHERE vector <|> $query_vector;
```

**Note:** Dimensions depend on VoyageAI model (voyage-code-3 = 1024 dims)

---

## Medium Priority

### 4. Schema Migration Strategy

**Issue:** Currently requires `REMOVE TABLE` to update schema, losing all data.

**Solution needed:**
- Migration scripts for SurrealDB
- Schema version tracking
- Safe upgrade path

---

### 5. Batch Insert Performance

**Current:** Single-row inserts in loops
**Desired:** Batch inserts for better performance

**Files:**
- `surrealdb_provider.py:_executor_insert_chunk()`
- `surrealdb_provider.py:_executor_insert_embedding()`

---

### 6. Connection Pooling

**Current:** Single connection
**Desired:** Connection pool for concurrent requests

---

## Low Priority

### 7. SurrealDB-specific CLI Arguments

Add support for:
- `--surrealdb-namespace`
- `--surrealdb-database`
- `--surrealdb-username`
- `--surrealdb-password`

Currently only configurable via config file.

---

### 8. Embedded SurrealDB Support

Support SurrealDB embedded mode (`memory://` or `file://`) for local-only usage without Docker.

---

### 9. Documentation Updates

Update ChunkHound docs to mention SurrealDB as a supported backend.

---

## Completed

- [x] SurrealDB SDK v2 compatibility
- [x] WebSocket URL handling
- [x] Schemaless table support
- [x] Regex search functionality
- [x] Indexing with --no-embeddings
- [x] Basic CRUD operations