# ChunkHound SurrealDB Integration - Session Log

**Session Date:** 2026-03-16
**Duration:** ~2 hours

## Initial State

- ChunkHound installed via `uv tool install --force --editable .`
- SurrealDB 3.x running in Docker on port 8000
- Configuration file `.chunkhound.json` present with SurrealDB settings

## Errors Encountered and Resolutions

### Error 1: SurrealDB SDK Version Mismatch

**Error:**
```
SurrealDB is equal 1.0.8
```

**Root Cause:** pyproject.toml had `surrealdb>=0.5.0` which installed v1.0.8, incompatible with SurrealDB 3.x.

**Fix:** Changed dependency to `surrealdb>=2.0.0a1`

---

### Error 2: SCHEMAFULL Parameter Binding Failure

**Error:**
```
Couldn't coerce value for field `file.modified_time` of `file:xyz`: Expected `float` but found `NONE`
Couldn't coerce value for field `file.path` of `file:xyz`: Expected `string` but found `NONE`
```

**Root Cause:** SurrealDB 3.x SCHEMAFULL tables have strict validation that doesn't work correctly with Python SDK parameter binding.

**Fix:** Changed from:
```sql
DEFINE TABLE IF NOT EXISTS file SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS file.path ON file TYPE string;
```

To:
```sql
DEFINE TABLE IF NOT EXISTS file;  -- schemaless
```

Also discovered that `DEFINE FIELD IF NOT EXISTS` won't update existing fields - had to `REMOVE TABLE` first.

---

### Error 3: WebSocket URL Corruption

**Error:**
```
ws:/127.0.0.1:8000/surrealdb/rpc isn't a valid URI: hostname isn't provided
```

**Root Cause:** Code was wrapping WebSocket URLs in `Path()` which:
1. Removed one slash from `ws://` → `ws:/`
2. Appended `/surrealdb` suffix meant for file paths

**Fix:** Added URL scheme detection in multiple locations:
- `_db_root_path_for_config()` in database_factory.py
- `verify_database_exists()` in database.py

---

### Error 4: Async Event Loop in Thread

**Error:**
```
There is no current event loop in thread 'serial-db_0'
```

**Root Cause:** `_executor_search_regex()` used `asyncio.get_event_loop()` inside a thread that doesn't have one.

**Fix:** Removed async wrapper, made the method fully synchronous since SurrealDB v2 SDK is synchronous.

---

### Error 5: Regex Search Returning Empty Results

**Root Cause:** SQL query used `chunk.id`, `chunk.content` etc. which created nested objects in result:
```python
{'chunk': {'id': None, 'content': None}, 'file_path': '...'}
```

**Fix:** Changed query to use direct field names:
```sql
SELECT id, content, start_line, end_line, ... FROM chunk;
```

---

## Key Code Changes

### surrealdb_provider.py

**Lines 168-182:** Schema definition changed from SCHEMAFULL to schemaless

**Lines 1003-1060:** Fixed `_executor_search_regex()`:
- Removed async wrapper
- Fixed SELECT query field names
- Corrected result parsing

### database_factory.py

**Lines 47-66:** Added URL scheme check in `_db_root_path_for_config()`

**Lines 125-145:** Preserve URLs when updating config.database.path

### database.py (CLI utils)

**Lines 14-46:** Added URL handling in `verify_database_exists()`, return type changed to `Path | str`

---

## Test Results

```
$ chunkhound index . --no-embeddings
Processed: 1522 files
Total chunks: 63339
Time: 37.35s

$ chunkhound search --regex "SurrealDB" --no-embeddings
Results: 10 of 461 (showing 1-10)
```