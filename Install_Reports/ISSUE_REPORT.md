# ChunkHound SurrealDB Integration - Issue Report

## Critical Issues

### ISSUE-001: SCHEMAFULL Tables Incompatible with SurrealDB 3.x Python SDK

**Severity:** HIGH
**Status:** WORKAROUND APPLIED
**Component:** surrealdb_provider.py

**Description:**
Using `DEFINE TABLE ... SCHEMAFULL` with SurrealDB 3.x and the Python SDK v2 causes parameter binding to fail. Parameters appear as `NONE` in SurrealDB even when correctly provided.

**Impact:**
- Cannot use type validation at database level
- Cannot enforce schema constraints

**Workaround:**
Using schemaless tables (`DEFINE TABLE name;` without SCHEMAFULL)

**Root Cause:**
SurrealDB 3.x has stricter validation that doesn't work correctly with the Python SDK's parameter serialization.

**Recommendation:**
Report to SurrealDB team or investigate parameter serialization format.

---

## Medium Issues

### ISSUE-002: DEFINE FIELD IF NOT EXISTS Doesn't Update Existing Fields

**Severity:** MEDIUM
**Status:** WORKAROUND APPLIED
**Component:** surrealdb_provider.py

**Description:**
`DEFINE FIELD IF NOT EXISTS` only creates fields that don't exist. It won't update the type of an existing field.

**Impact:**
- Schema changes require dropping tables
- Data loss during schema updates

**Workaround:**
`REMOVE TABLE IF EXISTS` before recreating with new schema

**Recommendation:**
Implement schema migration system with version tracking.

---

### ISSUE-003: Path() Corrupts WebSocket URLs

**Severity:** MEDIUM
**Status:** FIXED
**Component:** database_factory.py, database.py

**Description:**
Passing WebSocket URLs through `Path()` converts `ws://` to `ws:/` (single slash) and may append path suffixes.

**Impact:**
- Connection failures with cryptic "hostname isn't provided" errors

**Fix Applied:**
Added URL scheme detection before Path() conversion in:
- `_db_root_path_for_config()`
- `verify_database_exists()`

---

### ISSUE-004: Async Method in Non-Async Thread Context

**Severity:** MEDIUM
**Status:** FIXED
**Component:** surrealdb_provider.py

**Description:**
`_executor_search_regex()` used `asyncio.get_event_loop()` inside a worker thread, causing "no current event loop" error.

**Impact:**
- Regex search completely broken

**Fix Applied:**
Removed async wrapper, made method synchronous since SurrealDB v2 SDK is synchronous.

---

## Low Issues

### ISSUE-005: Return Type Mismatch in verify_database_exists

**Severity:** LOW
**Status:** FIXED
**Component:** api/cli/utils/database.py

**Description:**
Function was typed as returning `Path` but needed to return `str` for URLs.

**Fix Applied:**
Changed return type to `Path | str`

---

### ISSUE-006: SQL Query Creates Nested Objects

**Severity:** LOW
**Status:** FIXED
**Component:** surrealdb_provider.py

**Description:**
Query `SELECT chunk.id, chunk.content FROM chunk` creates nested objects instead of flat results.

**Impact:**
- Regex search returned all None values

**Fix Applied:**
Changed to `SELECT id, content FROM chunk`

---

## Potential Future Issues

### ISSUE-007: Semantic Search Not Implemented for SurrealDB

**Severity:** N/A (NOT BLOCKING)
**Status:** PENDING
**Component:** surrealdb_provider.py

**Description:**
`_executor_search_semantic()` not implemented. Semantic search will not work until this is added.

**Dependencies:**
- Embeddings must be generated first
- Vector index creation
- SurrealDB vector similarity query syntax

---

### ISSUE-008: Single Connection Limits Concurrency

**Severity:** LOW
**Status:** ACCEPTED
**Component:** surrealdb_provider.py

**Description:**
Using single connection limits ability to handle concurrent requests.

**Impact:**
- Potential bottleneck under load
- MCP server may have issues with multiple clients

**Recommendation:**
Consider connection pooling for production use.

---

## Summary

| Severity | Count | Fixed | Workaround | Pending |
|----------|-------|-------|------------|---------|
| HIGH     | 1     | 0     | 1          | 0       |
| MEDIUM   | 3     | 2     | 1          | 0       |
| LOW      | 2     | 2     | 0          | 0       |
| N/A      | 2     | 0     | 0          | 2       |

**Overall Status:** FUNCTIONAL with workarounds applied