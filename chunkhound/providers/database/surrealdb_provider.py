"""SurrealDB provider implementation for ChunkHound - concrete database provider using SurrealDB.

# FILE_CONTEXT: Multi-model database with built-in vector search
# CRITICAL: Uses SurrealDB's native KNN vector search via MTREE indexes
# PERFORMANCE: Real-time indexing, graph relationships, live queries

## PERFORMANCE_CHARACTERISTICS
- Vector search: MTREE index with cosine/Euclidean similarity
- Graph queries: Native graph traversal for code relationships
- Live queries: Real-time notifications for index updates
"""

import asyncio
import json
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from loguru import logger

from chunkhound.core.models import Chunk, Embedding, File
from chunkhound.core.types.common import ChunkType, Language
from chunkhound.core.types import FileId, FilePath, Timestamp
from chunkhound.core.utils import normalize_path_for_lookup
from chunkhound.embeddings import EmbeddingManager
from chunkhound.providers.database.like_utils import escape_like_pattern
from chunkhound.providers.database.serial_database_provider import (
    SerialDatabaseProvider,
)
from chunkhound.providers.database.serial_executor import (
    _executor_local,
    track_operation,
)

# Type hinting only
if TYPE_CHECKING:
    from chunkhound.core.config.database_config import DatabaseConfig


class SurrealDBProvider(SerialDatabaseProvider):
    """SurrealDB implementation of DatabaseProvider protocol.

    # CLASS_CONTEXT: Multi-model database with documents, graphs, and vectors
    # CONSTRAINT: Inherits from SerialDatabaseProvider for thread safety
    # PERFORMANCE: Uses native vector search via MTREE indexes
    """

    def __init__(
        self,
        db_path: Path | str,
        base_directory: Path,
        embedding_manager: "EmbeddingManager | None" = None,
        config: "DatabaseConfig | None" = None,
    ):
        """Initialize SurrealDB provider.

        Args:
            db_path: Connection URL (e.g., "ws://127.0.0.1:8000/rpc") or path for embedded
            base_directory: Base directory for path normalization
            embedding_manager: Optional embedding manager for vector generation
            config: Database configuration with SurrealDB-specific settings
        """
        super().__init__(db_path, base_directory, embedding_manager, config)

        self.provider_type = "surrealdb"

        # SurrealDB connection settings
        self._url = str(db_path) if db_path else "ws://127.0.0.1:8000/rpc"
        self._namespace = getattr(config, "surrealdb_namespace", "chunkhound") if config else "chunkhound"
        self._database = getattr(config, "surrealdb_database", "code_index") if config else "code_index"
        self._username = getattr(config, "surrealdb_username", "root") if config else "root"
        self._password = getattr(config, "surrealdb_password", "root") if config else "root"

        # Default embedding dimensions (nomic-embed-text = 768)
        self._embedding_dims = 768

        # Connection state
        self._client: Any = None
        self._connected = False

        # ID counters (SurrealDB uses string IDs, but we track numeric for compatibility)
        self._file_id_counter = 0
        self._chunk_id_counter = 0
        self._embedding_id_counter = 0

        # ID maps for converting between int IDs and SurrealDB record IDs
        self._file_id_map: dict[int, str] = {}
        self._chunk_id_map: dict[int, str] = {}
        self._embedding_id_map: dict[int, str] = {}

    def _create_connection(self) -> Any:
        """Create and return a SurrealDB connection.

        Returns:
            SurrealDB client object
        """
        try:
            from surrealdb import Surreal
        except ImportError:
            raise ImportError(
                "surrealdb package not installed. "
                "Install with: pip install surrealdb"
            )

        client = Surreal(self._url)
        logger.debug(f"Created SurrealDB client for {self._url}")
        return client

    def _get_schema_sql(self) -> list[str] | None:
        """Get SurrealQL statements for creating the schema."""
        return None  # Schema is created in _executor_create_schema

    @property
    def connection(self) -> Any | None:
        """Database connection."""
        return self._client

    @property
    def db_path(self) -> Path | str:
        """Database connection URL or identifier."""
        return self._url

    @property
    def is_connected(self) -> bool:
        """Check if database connection is active."""
        return self._connected

    def connect(self) -> None:
        """Establish database connection and initialize schema."""
        try:
            super().connect()
            logger.info(f"SurrealDB connected: {self._url}/{self._namespace}/{self._database}")
        except Exception as e:
            logger.error(f"SurrealDB connection failed: {e}")
            raise

    def _executor_connect(self, conn: Any, state: dict[str, Any]) -> None:
        """Executor method for connect - runs in DB thread."""
        try:
            # SurrealDB blocking client connects automatically on instantiation
            # Just need to signin and use the namespace/database
            conn.signin({
                "username": self._username,
                "password": self._password,
            })
            conn.use(
                namespace=self._namespace,
                database=self._database,
            )

            self._connected = True
            self._client = conn

            # Create schema
            self._executor_create_schema(conn, state)

        except Exception as e:
            logger.error(f"SurrealDB executor connect failed: {e}")
            raise

    def _executor_create_schema(self, conn: Any, state: dict[str, Any]) -> None:
        """Create SurrealDB schema for files, chunks, and embeddings."""
        schema_sql = """
        -- Files table (schemaless for SurrealDB 3.x compatibility)
        DEFINE TABLE IF NOT EXISTS file;

        -- Unique index on file path
        DEFINE INDEX IF NOT EXISTS idx_file_path ON file FIELDS path UNIQUE;

        -- Chunks table (schemaless for SurrealDB 3.x compatibility)
        DEFINE TABLE IF NOT EXISTS chunk;

        -- Index on file_id for chunk lookups
        DEFINE INDEX IF NOT EXISTS idx_chunk_file_id ON chunk FIELDS file_id;
        DEFINE INDEX IF NOT EXISTS idx_chunk_type ON chunk FIELDS chunk_type;

        -- Embeddings table (schemaless for SurrealDB 3.x compatibility)
        DEFINE TABLE IF NOT EXISTS embedding;
        DEFINE FIELD IF NOT EXISTS embedding.chunk_id ON embedding TYPE record<chunk>;
        DEFINE FIELD IF NOT EXISTS embedding.vector ON embedding TYPE array<float>;
        DEFINE FIELD IF NOT EXISTS embedding.provider ON embedding TYPE string;
        DEFINE FIELD IF NOT EXISTS embedding.model ON embedding TYPE string;
        DEFINE FIELD IF NOT EXISTS embedding.dims ON embedding TYPE number;
        DEFINE FIELD IF NOT EXISTS embedding.created_time ON embedding TYPE float;

        -- Index on chunk_id for embedding lookups
        DEFINE INDEX IF NOT EXISTS idx_embedding_chunk_id ON embedding FIELDS chunk_id;
        DEFINE INDEX IF NOT EXISTS idx_embedding_provider_model ON embedding FIELDS provider, model;
        """

        try:
            conn.query(schema_sql)
            logger.debug("SurrealDB schema created")
        except Exception as e:
            logger.warning(f"Schema creation warning: {e}")

    def _executor_create_indexes(self, conn: Any, state: dict[str, Any]) -> None:
        """Create additional indexes (handled in schema)."""
        pass

    def create_vector_index(
        self, provider: str, model: str, dims: int, metric: str = "cosine"
    ) -> None:
        """Create vector index for specific provider/model/dims combination."""
        self._embedding_dims = dims

        index_name = f"idx_embedding_vector_{provider}_{model}_{dims}"
        distance = "EUCLIDEAN" if metric == "euclidean" else "COSINE"

        # MTREE index for vector search
        create_index = f"""
        DEFINE INDEX IF NOT EXISTS {index_name} ON embedding FIELDS vector
            MTREE DIMENSION {dims} DIST {distance};
        """

        try:
            self._client.query(create_index)
            logger.info(f"Created vector index: {index_name}")
        except Exception as e:
            logger.error(f"Failed to create vector index: {e}")

    def drop_vector_index(
        self, provider: str, model: str, dims: int, metric: str = "cosine"
    ) -> str:
        """Drop vector index for specific provider/model/dims combination."""
        index_name = f"idx_embedding_vector_{provider}_{model}_{dims}"

        drop_sql = f"REMOVE INDEX IF EXISTS {index_name} ON embedding;"

        try:
            self._client.query(drop_sql)
            logger.info(f"Dropped vector index: {index_name}")
            return index_name
        except Exception as e:
            logger.error(f"Failed to drop vector index: {e}")
            return ""

    # ========================================================================
    # File Operations
    # ========================================================================

    def insert_file(self, file: File) -> int:
        """Insert file record and return file ID."""
        return self._execute_in_db_thread_sync("insert_file", file)

    def _executor_insert_file(self, conn: Any, state: dict[str, Any], file: File) -> int:
        """Insert file in executor thread."""
        self._file_id_counter += 1
        file_id = self._file_id_counter

        query = """
        CREATE file SET
            path = $path,
            size = $size,
            modified_time = $modified_time,
            content_hash = $content_hash,
            indexed_time = $indexed_time,
            language = $language,
            encoding = $encoding,
            line_count = $line_count;
        """

        # Ensure mtime is a valid float - explicitly handle None/Timestamp cases
        mtime_value = file.mtime
        if mtime_value is None:
            mtime_value = time.time()
        else:
            mtime_value = float(mtime_value)
        
        # Debug logging
        logger.debug(f"DEBUG: file.path={file.path}, mtype={type(file.mtime)}, mtime_value={mtime_value}")
        
        params = {
            "path": str(file.path),
            "size": file.size_bytes or 0,
            "modified_time": mtime_value,
            "content_hash": file.content_hash or "",
            "indexed_time": time.time(),
            "language": file.language.value if file.language else None,
            "encoding": getattr(file, "encoding", "utf-8") or "utf-8",
            "line_count": getattr(file, "line_count", None),
        }
        
        logger.debug(f"DEBUG params: modified_time={params.get('modified_time')}")

        result = conn.query(query, params)
        if isinstance(result, str):
            logger.error(f"SurrealDB insert file error: {result}")
        elif result and isinstance(result, list) and len(result) > 0:
            record = result[0]
            if isinstance(record, dict):
                record_id = record.get("id")
                self._file_id_map[file_id] = record_id
        return file_id


    def get_file_by_path(
        self, path: str, as_model: bool = False
    ) -> dict[str, Any] | File | None:
        """Get file record by path."""
        return self._execute_in_db_thread_sync("get_file_by_path", path, as_model)

    def _executor_get_file_by_path(
        self, conn: Any, state: dict[str, Any], path: str, as_model: bool
    ) -> dict[str, Any] | File | None:
        """Get file by path in executor thread."""
        query = "SELECT * FROM file WHERE path = $path LIMIT 1;"

        result = conn.query(query, {"path": path})
        if result and isinstance(result, list) and len(result) > 0:
            return self._row_to_file(result[0], as_model)
        return None


    def get_file_by_id(
        self, file_id: int, as_model: bool = False
    ) -> dict[str, Any] | File | None:
        """Get file record by ID."""
        return self._execute_in_db_thread_sync("get_file_by_id", file_id, as_model)

    def _executor_get_file_by_id(
        self, conn: Any, state: dict[str, Any], file_id: int, as_model: bool
    ) -> dict[str, Any] | File | None:
        """Get file by ID in executor thread."""
        record_id = self._file_id_map.get(file_id)
        if not record_id:
            return None

        result = conn.select(record_id)
        if result:
            return self._row_to_file(result[0], as_model)
        return None


    def update_file(self, file_id: int, **kwargs: Any) -> None:
        """Update file record with new values."""
        self._execute_in_db_thread_sync("update_file", file_id, kwargs)

    def _executor_update_file(
        self, conn: Any, state: dict[str, Any], file_id: int, kwargs: dict[str, Any]
    ) -> None:
        """Update file in executor thread."""
        record_id = self._file_id_map.get(file_id)
        if not record_id:
            return

        set_clauses = ", ".join(f"{k} = ${k}" for k in kwargs.keys())
        query = f"UPDATE {record_id} SET {set_clauses};"
        kwargs["updated_time"] = time.time()

        conn.query(query, kwargs)


    def delete_file_completely(self, file_path: str) -> bool:
        """Delete a file and all its chunks/embeddings completely."""
        return self._execute_in_db_thread_sync("delete_file_completely", file_path)

    def _executor_delete_file_completely(
        self, conn: Any, state: dict[str, Any], file_path: str
    ) -> bool:
        """Delete file and related records in executor thread."""
        # Get file ID first
        async def _delete():
            result = conn.query(
                "SELECT id FROM file WHERE path = $path;",
                {"path": file_path}
            )
            if not result or not isinstance(result, list) or len(result) == 0:
                return False

            file_record_id = result[0].get("id")

            # Delete embeddings for chunks in this file
            conn.query("""
                DELETE embedding WHERE chunk_id IN (
                    SELECT id FROM chunk WHERE file_id = $file_id
                );
            """, {"file_id": file_record_id})

            # Delete chunks
            conn.query(
                "DELETE chunk WHERE file_id = $file_id;",
                {"file_id": file_record_id}
            )

            # Delete file
            conn.query(
                "DELETE file WHERE id = $file_id;",
                {"file_id": file_record_id}
            )

            return True

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(_delete())

    async def delete_file_completely_async(self, file_path: str) -> bool:
        """Delete a file and all its chunks/embeddings completely (asynchronous)."""
        return self._execute_in_db_thread_sync("delete_file_completely", file_path)

    async def insert_file_async(self, file: File) -> int:
        """Insert file record and return file ID (asynchronous)."""
        return self.insert_file(file)

    async def get_file_by_path_async(
        self, path: str, as_model: bool = False
    ) -> dict[str, Any] | File | None:
        """Get file record by path (asynchronous)."""
        return self.get_file_by_path(path, as_model)

    async def update_file_async(self, file_id: int, **kwargs: Any) -> None:
        """Update file record with new values (asynchronous)."""
        self.update_file(file_id, **kwargs)

    # ========================================================================
    # Chunk Operations
    # ========================================================================

    def insert_chunk(self, chunk: Chunk) -> int:
        """Insert chunk record and return chunk ID."""
        return self._execute_in_db_thread_sync("insert_chunk", chunk)

    def _executor_insert_chunk(
        self, conn: Any, state: dict[str, Any], chunk: Chunk
    ) -> int:
        """Insert chunk in executor thread."""
        self._chunk_id_counter += 1
        chunk_id = self._chunk_id_counter

        # Get file record ID
        file_record_id = self._file_id_map.get(chunk.file_id)
        if not file_record_id:
            file_record_id = f"file:{chunk.file_id}"

        query = """
        CREATE chunk SET
            file_id = $file_id,
            content = $content,
            start_line = $start_line,
            end_line = $end_line,
            chunk_type = $chunk_type,
            language = $language,
            name = $name,
            metadata = $metadata,
            created_time = $created_time;
        """

        params = {
            "file_id": file_record_id,
            "content": chunk.code,
            "start_line": chunk.start_line,
            "end_line": chunk.end_line,
            "chunk_type": chunk.chunk_type.value if isinstance(chunk.chunk_type, ChunkType) else str(chunk.chunk_type),
            "language": chunk.language.value if isinstance(chunk.language, Language) else str(chunk.language),
            "name": chunk.symbol,
            "metadata": json.dumps(chunk.metadata) if chunk.metadata else None,
            "created_time": time.time(),
        }

        result = conn.query(query, params)
        if result and isinstance(result, list) and len(result) > 0:
            record = result[0]
            if isinstance(record, dict):
                record_id = record.get("id")
                self._chunk_id_map[chunk_id] = record_id
        return chunk_id


    def insert_chunks_batch(self, chunks: list[Chunk]) -> list[int]:
        """Insert multiple chunks in batch and return chunk IDs."""
        return self._execute_in_db_thread_sync("insert_chunks_batch", chunks)

    def _executor_insert_chunks_batch(
        self, conn: Any, state: dict[str, Any], chunks: list[Chunk]
    ) -> list[int]:
        """Insert chunks batch in executor thread."""
        ids = []
        for chunk in chunks:
            chunk_id = self._executor_insert_chunk(conn, state, chunk)
            ids.append(chunk_id)
        return ids

    def get_chunk_by_id(
        self, chunk_id: int, as_model: bool = False
    ) -> dict[str, Any] | Chunk | None:
        """Get chunk record by ID."""
        return self._execute_in_db_thread_sync("get_chunk_by_id", chunk_id, as_model)

    def _executor_get_chunk_by_id(
        self, conn: Any, state: dict[str, Any], chunk_id: int, as_model: bool
    ) -> dict[str, Any] | Chunk | None:
        """Get chunk by ID in executor thread."""
        record_id = self._chunk_id_map.get(chunk_id)
        if not record_id:
            return None

        result = conn.select(record_id)
        if result:
            return self._row_to_chunk(result[0], as_model)
        return None


    def get_chunks_by_file_id(
        self, file_id: int, as_model: bool = False
    ) -> list[dict[str, Any] | Chunk]:
        """Get all chunks for a specific file."""
        return self._execute_in_db_thread_sync("get_chunks_by_file_id", file_id, as_model)

    def _executor_get_chunks_by_file_id(
        self, conn: Any, state: dict[str, Any], file_id: int, as_model: bool
    ) -> list[dict[str, Any] | Chunk]:
        """Get chunks by file ID in executor thread."""
        file_record_id = self._file_id_map.get(file_id, f"file:{file_id}")

        result = conn.query(
            "SELECT * FROM chunk WHERE file_id = $file_id ORDER BY start_line;",
            {"file_id": file_record_id}
        )
        if result and result[0]:
            return [self._row_to_chunk(row, as_model) for row in result[0]]
        return []


    async def get_chunks_by_file_id_async(
        self, file_id: int, as_model: bool = False
    ) -> list[dict[str, Any] | Chunk]:
        """Get all chunks for a specific file (asynchronous)."""
        return self.get_chunks_by_file_id(file_id, as_model)

    async def insert_chunks_batch_async(self, chunks: list[Chunk]) -> list[int]:
        """Insert multiple chunks in batch and return chunk IDs (asynchronous)."""
        return self.insert_chunks_batch(chunks)

    async def delete_chunks_batch_async(self, chunk_ids: list[int]) -> None:
        """Delete chunks by IDs (asynchronous)."""
        self._execute_in_db_thread_sync("delete_chunks_batch", chunk_ids)

    def _executor_delete_chunks_batch(
        self, conn: Any, state: dict[str, Any], chunk_ids: list[int]
    ) -> None:
        """Delete chunks batch in executor thread."""
        for chunk_id in chunk_ids:
            record_id = self._chunk_id_map.get(chunk_id)
            if record_id:
                conn.query(f"DELETE {record_id};")
                del self._chunk_id_map[chunk_id]


    def delete_file_chunks(self, file_id: int) -> None:
        """Delete all chunks for a file."""
        self._execute_in_db_thread_sync("delete_file_chunks", file_id)

    def _executor_delete_file_chunks(
        self, conn: Any, state: dict[str, Any], file_id: int
    ) -> None:
        """Delete file chunks in executor thread."""
        file_record_id = self._file_id_map.get(file_id, f"file:{file_id}")

        conn.query(
            "DELETE chunk WHERE file_id = $file_id;",
            {"file_id": file_record_id}
        )


    def delete_chunks_batch(self, chunk_ids: list[int]) -> None:
        """Delete multiple chunks by IDs."""
        self._execute_in_db_thread_sync("delete_chunks_batch", chunk_ids)

    def delete_chunk(self, chunk_id: int) -> None:
        """Delete a single chunk by ID."""
        self._execute_in_db_thread_sync("delete_chunks_batch", [chunk_id])

    def update_chunk(self, chunk_id: int, **kwargs: Any) -> None:
        """Update chunk record with new values."""
        self._execute_in_db_thread_sync("update_chunk", chunk_id, kwargs)

    def _executor_update_chunk(
        self, conn: Any, state: dict[str, Any], chunk_id: int, kwargs: dict[str, Any]
    ) -> None:
        """Update chunk in executor thread."""
        record_id = self._chunk_id_map.get(chunk_id)
        if not record_id:
            return

        set_clauses = ", ".join(f"{k} = ${k}" for k in kwargs.keys())
        query = f"UPDATE {record_id} SET {set_clauses};"

        conn.query(query, kwargs)


    # ========================================================================
    # Embedding Operations
    # ========================================================================

    def insert_embedding(self, embedding: Embedding) -> int:
        """Insert embedding record and return embedding ID."""
        return self._execute_in_db_thread_sync("insert_embedding", embedding)

    def _executor_insert_embedding(
        self, conn: Any, state: dict[str, Any], embedding: Embedding
    ) -> int:
        """Insert embedding in executor thread."""
        self._embedding_id_counter += 1
        embedding_id = self._embedding_id_counter

        chunk_record_id = self._chunk_id_map.get(embedding.chunk_id, f"chunk:{embedding.chunk_id}")

        query = """
        CREATE embedding SET
            chunk_id = $chunk_id,
            vector = $vector,
            provider = $provider,
            model = $model,
            dims = $dims,
            created_time = $created_time;
        """

        params = {
            "chunk_id": chunk_record_id,
            "vector": embedding.vector,
            "provider": embedding.provider,
            "model": embedding.model,
            "dims": len(embedding.vector),
            "created_time": time.time(),
        }

        result = conn.query(query, params)
        if result and isinstance(result, list) and len(result) > 0:
            record = result[0]
            if isinstance(record, dict):
                record_id = record.get("id")
                self._embedding_id_map[embedding_id] = record_id
        return embedding_id


    def insert_embeddings_batch(
        self,
        embeddings_data: list[dict],
        batch_size: int | None = None,
        connection: Any = None,
    ) -> int:
        """Insert multiple embedding vectors with optimization."""
        return self._execute_in_db_thread_sync("insert_embeddings_batch", embeddings_data, batch_size)

    def _executor_insert_embeddings_batch(
        self,
        conn: Any,
        state: dict[str, Any],
        embeddings_data: list[dict],
        batch_size: int | None,
    ) -> int:
        """Insert embeddings batch in executor thread."""
        count = 0
        for emb_data in embeddings_data:
            embedding = Embedding(
                chunk_id=emb_data["chunk_id"],
                vector=emb_data["vector"],
                provider=emb_data["provider"],
                model=emb_data["model"],
            )
            self._executor_insert_embedding(conn, state, embedding)
            count += 1
        return count

    def get_embedding_by_chunk_id(
        self, chunk_id: int, provider: str, model: str
    ) -> Embedding | None:
        """Get embedding for specific chunk, provider, and model."""
        return self._execute_in_db_thread_sync(
            "get_embedding_by_chunk_id", chunk_id, provider, model
        )

    def _executor_get_embedding_by_chunk_id(
        self,
        conn: Any,
        state: dict[str, Any],
        chunk_id: int,
        provider: str,
        model: str,
    ) -> Embedding | None:
        """Get embedding by chunk ID in executor thread."""
        chunk_record_id = self._chunk_id_map.get(chunk_id, f"chunk:{chunk_id}")

        result = conn.query("""
            SELECT * FROM embedding
            WHERE chunk_id = $chunk_id
            AND provider = $provider
            AND model = $model
            LIMIT 1;
        """, {
            "chunk_id": chunk_record_id,
            "provider": provider,
            "model": model,
        })
        if result and isinstance(result, list) and len(result) > 0:
            row = result[0]
            return Embedding(
                id=0,  # We don't track embedding IDs the same way
                chunk_id=chunk_id,
                vector=row.get("vector", []),
                provider=row.get("provider", ""),
                model=row.get("model", ""),
            )
        return None


    def get_existing_embeddings(
        self, chunk_ids: list[int], provider: str, model: str
    ) -> set[int]:
        """Get set of chunk IDs that already have embeddings for given provider/model."""
        return self._execute_in_db_thread_sync(
            "get_existing_embeddings", chunk_ids, provider, model
        )

    def _executor_get_existing_embeddings(
        self,
        conn: Any,
        state: dict[str, Any],
        chunk_ids: list[int],
        provider: str,
        model: str,
    ) -> set[int]:
        """Get existing embeddings in executor thread."""
        # Build list of chunk record IDs
        chunk_record_ids = [
            self._chunk_id_map.get(cid, f"chunk:{cid}")
            for cid in chunk_ids
        ]

        async def _get():
            result = conn.query("""
                SELECT chunk_id FROM embedding
                WHERE chunk_id IN $chunk_ids
                AND provider = $provider
                AND model = $model;
            """, {
                "chunk_ids": chunk_record_ids,
                "provider": provider,
                "model": model,
            })

            existing = set()
            if result and result[0]:
                for row in result[0]:
                    chunk_record_id = row.get("chunk_id")
                    # Find the int ID from the map
                    for int_id, rec_id in self._chunk_id_map.items():
                        if rec_id == chunk_record_id:
                            existing.add(int_id)
                            break
            return existing

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(_get())

    def delete_embeddings_by_chunk_id(self, chunk_id: int) -> None:
        """Delete all embeddings for a specific chunk."""
        self._execute_in_db_thread_sync("delete_embeddings_by_chunk_id", chunk_id)

    def _executor_delete_embeddings_by_chunk_id(
        self, conn: Any, state: dict[str, Any], chunk_id: int
    ) -> None:
        """Delete embeddings by chunk ID in executor thread."""
        chunk_record_id = self._chunk_id_map.get(chunk_id, f"chunk:{chunk_id}")

        conn.query(
            "DELETE embedding WHERE chunk_id = $chunk_id;",
            {"chunk_id": chunk_record_id}
        )


    def get_all_chunks_with_metadata(self) -> list[dict[str, Any]]:
        """Get all chunks with their metadata including file paths."""
        return self._execute_in_db_thread_sync("get_all_chunks_with_metadata")

    def _executor_get_all_chunks_with_metadata(
        self, conn: Any, state: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get all chunks with metadata in executor thread."""
        result = conn.query("""
            SELECT
                chunk.id,
                chunk.content,
                chunk.start_line,
                chunk.end_line,
                chunk.chunk_type,
                chunk.language,
                chunk.name,
                chunk.metadata,
                file.path AS file_path
            FROM chunk
            FETCH file_id;
        """)
        if result and result[0]:
            return [
                {
                    "id": row.get("id"),
                    "code": row.get("content"),
                    "start_line": row.get("start_line"),
                    "end_line": row.get("end_line"),
                    "chunk_type": row.get("chunk_type"),
                    "language": row.get("language"),
                    "symbol": row.get("name"),
                    "file_path": row.get("file_path"),
                    "metadata": json.loads(row.get("metadata", "{}")),
                }
                for row in result[0]
            ]
        return []


    # ========================================================================
    # Search Operations
    # ========================================================================

    def search_semantic(
        self,
        query_embedding: list[float],
        provider: str,
        model: str,
        page_size: int = 10,
        offset: int = 0,
        threshold: float | None = None,
        path_filter: str | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Perform semantic vector search using SurrealDB KNN."""
        return self._execute_in_db_thread_sync(
            "search_semantic",
            query_embedding,
            provider,
            model,
            page_size,
            offset,
            threshold,
            path_filter,
        )

    def _executor_search_semantic(
        self,
        conn: Any,
        state: dict[str, Any],
        query_embedding: list[float],
        provider: str,
        model: str,
        page_size: int,
        offset: int,
        threshold: float | None,
        path_filter: str | None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Perform semantic search in executor thread."""
        # Build query with vector similarity
        query = f"""
        LET $query_vec = {query_embedding};

        SELECT
            embedding.id,
            embedding.chunk_id,
            embedding.vector,
            embedding.provider,
            embedding.model,
            vector::similarity::cosine(embedding.vector, $query_vec) AS score,
            chunk.content,
            chunk.start_line,
            chunk.end_line,
            chunk.chunk_type,
            chunk.language,
            chunk.name,
            file.path AS file_path
        FROM embedding
        WHERE provider = $provider
        AND model = $model
        {"AND file_path =~ $path_filter" if path_filter else ""}
        ORDER BY score DESC
        LIMIT {page_size}
        START {offset};
        """

        params = {
            "provider": provider,
            "model": model,
        }
        if path_filter:
            params["path_filter"] = f".*{path_filter}.*"

        async def _search():
            result = conn.query(query, params)
            results = []
            if result and result[0]:
                for row in result[0]:
                    score = row.get("score", 0)
                    if threshold is None or score >= threshold:
                        results.append({
                            "id": row.get("id"),
                            "chunk_id": row.get("chunk_id"),
                            "code": row.get("content"),
                            "start_line": row.get("start_line"),
                            "end_line": row.get("end_line"),
                            "chunk_type": row.get("chunk_type"),
                            "language": row.get("language"),
                            "symbol": row.get("name"),
                            "file_path": row.get("file_path"),
                            "score": score,
                        })

            pagination = {
                "page_size": page_size,
                "offset": offset,
                "total": len(results),
            }
            return results, pagination

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(_search())

    def find_similar_chunks(
        self,
        chunk_id: int,
        provider: str,
        model: str,
        limit: int = 10,
        threshold: float | None = None,
        path_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """Find chunks similar to the given chunk using its embedding."""
        return self._execute_in_db_thread_sync(
            "find_similar_chunks",
            chunk_id,
            provider,
            model,
            limit,
            threshold,
            path_filter,
        )

    def _executor_find_similar_chunks(
        self,
        conn: Any,
        state: dict[str, Any],
        chunk_id: int,
        provider: str,
        model: str,
        limit: int,
        threshold: float | None,
        path_filter: str | None,
    ) -> list[dict[str, Any]]:
        """Find similar chunks in executor thread."""
        # First get the embedding for this chunk
        embedding = self._executor_get_embedding_by_chunk_id(
            conn, state, chunk_id, provider, model
        )
        if not embedding:
            return []

        # Then search for similar
        results, _ = self._executor_search_semantic(
            conn, state,
            embedding.vector,
            provider, model,
            limit + 1,  # +1 because the chunk itself will be included
            0,
            threshold,
            path_filter,
        )

        # Filter out the original chunk
        return [r for r in results if r.get("chunk_id") != chunk_id][:limit]

    def search_by_embedding(
        self,
        query_embedding: list[float],
        provider: str,
        model: str,
        limit: int = 10,
        threshold: float | None = None,
        path_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """Find chunks similar to the given embedding vector."""
        results, _ = self.search_semantic(
            query_embedding,
            provider,
            model,
            page_size=limit,
            threshold=threshold,
            path_filter=path_filter,
        )
        return results

    def search_regex(
        self,
        pattern: str,
        page_size: int = 10,
        offset: int = 0,
        path_filter: str | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Perform regex search on code content."""
        return self._execute_in_db_thread_sync(
            "search_regex", pattern, page_size, offset, path_filter
        )

    def _executor_search_regex(
        self,
        conn: Any,
        state: dict[str, Any],
        pattern: str,
        page_size: int,
        offset: int,
        path_filter: str | None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Perform regex search in executor thread."""
        # SurrealDB doesn't have native regex, so we use string matching
        # and filter in Python
        query = """
            SELECT
                id,
                content,
                start_line,
                end_line,
                chunk_type,
                language,
                name,
                file_id.path AS file_path
            FROM chunk;
        """
        if path_filter:
            query = f"""
                SELECT
                    id,
                    content,
                    start_line,
                    end_line,
                    chunk_type,
                    language,
                    name,
                    file_id.path AS file_path
                FROM chunk
                WHERE file_id.path =~ $path_filter;
            """

        result = conn.query(query, {"path_filter": f".*{path_filter}.*"} if path_filter else {})

        results = []
        regex = re.compile(pattern, re.IGNORECASE)
        # Result is a list of rows directly
        if result:
            for row in result:
                content = row.get("content", "")
                if regex.search(content):
                    results.append({
                        "id": row.get("id"),
                        "code": content,
                        "start_line": row.get("start_line"),
                        "end_line": row.get("end_line"),
                        "chunk_type": row.get("chunk_type"),
                        "language": row.get("language"),
                        "symbol": row.get("name"),
                        "file_path": row.get("file_path"),
                    })

        # Apply pagination
        total = len(results)
        paginated = results[offset:offset + page_size]

        pagination = {
            "page_size": page_size,
            "offset": offset,
            "total": total,
        }
        return paginated, pagination

    async def search_regex_async(
        self,
        pattern: str,
        page_size: int = 10,
        offset: int = 0,
        path_filter: str | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Perform regex search on code content (asynchronous)."""
        return self.search_regex(pattern, page_size, offset, path_filter)

    def search_text(
        self, query: str, page_size: int = 10, offset: int = 0
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Perform full-text search on code content."""
        return self._execute_in_db_thread_sync("search_text", query, page_size, offset)

    def _executor_search_text(
        self,
        conn: Any,
        state: dict[str, Any],
        query: str,
        page_size: int,
        offset: int,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Perform text search in executor thread."""
        async def _search():
            result = conn.query("""
                SELECT
                    chunk.id,
                    chunk.content,
                    chunk.start_line,
                    chunk.end_line,
                    chunk.chunk_type,
                    chunk.language,
                    chunk.name,
                    file.path AS file_path
                FROM chunk
                WHERE content =~ $query
                FETCH file_id
                LIMIT $limit
                START $offset;
            """, {
                "query": f".*{query}.*",
                "limit": page_size,
                "offset": offset,
            })

            results = []
            if result and result[0]:
                for row in result[0]:
                    results.append({
                        "id": row.get("id"),
                        "code": row.get("content"),
                        "start_line": row.get("start_line"),
                        "end_line": row.get("end_line"),
                        "chunk_type": row.get("chunk_type"),
                        "language": row.get("language"),
                        "symbol": row.get("name"),
                        "file_path": row.get("file_path"),
                    })

            pagination = {
                "page_size": page_size,
                "offset": offset,
                "total": len(results),
            }
            return results, pagination

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(_search())

    def get_chunks_in_range(
        self, file_id: int, start_line: int, end_line: int
    ) -> list[dict[str, Any]]:
        """Get chunks overlapping a line range within a file."""
        return self._execute_in_db_thread_sync(
            "get_chunks_in_range", file_id, start_line, end_line
        )

    def _executor_get_chunks_in_range(
        self,
        conn: Any,
        state: dict[str, Any],
        file_id: int,
        start_line: int,
        end_line: int,
    ) -> list[dict[str, Any]]:
        """Get chunks in range in executor thread."""
        file_record_id = self._file_id_map.get(file_id, f"file:{file_id}")

        async def _get():
            result = conn.query("""
                SELECT * FROM chunk
                WHERE file_id = $file_id
                AND start_line <= $end_line
                AND end_line >= $start_line
                ORDER BY start_line;
            """, {
                "file_id": file_record_id,
                "start_line": start_line,
                "end_line": end_line,
            })

            if result and result[0]:
                return [self._row_to_chunk(row, False) for row in result[0]]
            return []

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(_get())

    # ========================================================================
    # Statistics and Monitoring
    # ========================================================================

    def get_stats(self) -> dict[str, int]:
        """Get database statistics."""
        return self._execute_in_db_thread_sync("get_stats")

    def _executor_get_stats(
        self, conn: Any, state: dict[str, Any]
    ) -> dict[str, int]:
        """Get stats in executor thread."""
        result = conn.query("""
            LET $file_count = (SELECT count() FROM file GROUP ALL)[0].count OR 0;
            LET $chunk_count = (SELECT count() FROM chunk GROUP ALL)[0].count OR 0;
            LET $embedding_count = (SELECT count() FROM embedding GROUP ALL)[0].count OR 0;

            RETURN {
                file_count: $file_count,
                chunk_count: $chunk_count,
                embedding_count: $embedding_count
            };
        """)
        if result and isinstance(result, list) and len(result) > 0:
            return result[0]
        return {"file_count": 0, "chunk_count": 0, "embedding_count": 0}

    async def get_stats_async(self) -> dict[str, int]:
        """Get database statistics (asynchronous)."""
        return self.get_stats()

    def get_file_stats(self, file_id: int) -> dict[str, Any]:
        """Get statistics for a specific file."""
        return self._execute_in_db_thread_sync("get_file_stats", file_id)

    def _executor_get_file_stats(
        self, conn: Any, state: dict[str, Any], file_id: int
    ) -> dict[str, Any]:
        """Get file stats in executor thread."""
        file_record_id = self._file_id_map.get(file_id, f"file:{file_id}")

        result = conn.query("""
            LET $chunk_count = (SELECT count() FROM chunk WHERE file_id = $file_id GROUP ALL)[0].count OR 0;
            LET $embedding_count = (SELECT count() FROM embedding WHERE chunk_id.file_id = $file_id GROUP ALL)[0].count OR 0;

            RETURN {
                chunk_count: $chunk_count,
                embedding_count: $embedding_count
            };
        """, {"file_id": file_record_id})
        if result and isinstance(result, list) and len(result) > 0:
            return result[0]
        return {"chunk_count": 0, "embedding_count": 0}

    def get_provider_stats(self, provider: str, model: str) -> dict[str, Any]:
        """Get statistics for a specific embedding provider/model."""
        return self._execute_in_db_thread_sync("get_provider_stats", provider, model)

    def _executor_get_provider_stats(
        self, conn: Any, state: dict[str, Any], provider: str, model: str
    ) -> dict[str, Any]:
        """Get provider stats in executor thread."""
        result = conn.query("""
            LET $count = (SELECT count() FROM embedding WHERE provider = $provider AND model = $model GROUP ALL)[0].count OR 0;

            RETURN {
                count: $count,
                provider: $provider,
                model: $model
            };
        """, {"provider": provider, "model": model})
        if result and isinstance(result, list) and len(result) > 0:
            return result[0]
        return {"count": 0, "provider": provider, "model": model}

    # ========================================================================
    # Transaction and Bulk Operations
    # ========================================================================

    def execute_query(
        self, query: str, params: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a SurrealQL query and return results."""
        return self._execute_in_db_thread_sync("execute_query", query, params)

    def _executor_execute_query(
        self, conn: Any, state: dict[str, Any], query: str, params: list[Any] | None
    ) -> list[dict[str, Any]]:
        """Execute query in executor thread."""
        result = conn.query(query, params or {})
        if result:
            return result[0] if isinstance(result, list) and len(result) > 0 else result
        return []


    def begin_transaction(self) -> None:
        """Begin a database transaction."""
        # SurrealDB handles transactions differently
        pass

    def commit_transaction(self, force_checkpoint: bool = False) -> None:
        """Commit the current transaction."""
        pass

    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        pass

    async def begin_transaction_async(self) -> None:
        """Begin a database transaction (asynchronous)."""
        pass

    async def commit_transaction_async(self, force_checkpoint: bool = False) -> None:
        """Commit the current transaction (asynchronous)."""
        pass

    async def rollback_transaction_async(self) -> None:
        """Rollback the current transaction (asynchronous)."""
        pass

    # ========================================================================
    # File Processing Integration
    # ========================================================================

    async def process_file(
        self, file_path: Path, skip_embeddings: bool = False
    ) -> dict[str, Any]:
        """Process a file end-to-end: parse, chunk, and store in database."""
        # Delegate to indexing coordinator
        if self._indexing_coordinator:
            return await self._indexing_coordinator.process_file(file_path, skip_embeddings)
        raise RuntimeError("Indexing coordinator not initialized")

    async def process_directory(
        self,
        directory: Path,
        patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> dict[str, Any]:
        """Process all supported files in a directory."""
        if self._indexing_coordinator:
            return await self._indexing_coordinator.process_directory(
                directory, patterns, exclude_patterns
            )
        raise RuntimeError("Indexing coordinator not initialized")

    # ========================================================================
    # Health and Diagnostics
    # ========================================================================

    def optimize_tables(self) -> None:
        """Optimize tables (SurrealDB self-manages)."""
        pass

    def should_optimize(self, operation: str = "") -> bool:
        """Check if optimization is warranted."""
        return False

    def health_check(self) -> dict[str, Any]:
        """Perform health check and return status information."""
        return self._execute_in_db_thread_sync("health_check")

    def _executor_health_check(
        self, conn: Any, state: dict[str, Any]
    ) -> dict[str, Any]:
        """Health check in executor thread."""
        try:
            result = conn.query("INFO FOR DB;")
            return {
                "status": "healthy",
                "connected": True,
                "url": self._url,
                "namespace": self._namespace,
                "database": self._database,
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "connected": False,
                "error": str(e),
            }


    def get_connection_info(self) -> dict[str, Any]:
        """Get information about the database connection."""
        return {
            "provider": "surrealdb",
            "url": self._url,
            "namespace": self._namespace,
            "database": self._database,
            "connected": self._connected,
        }

    # ========================================================================
    # Helpers
    # ========================================================================

    def _row_to_file(self, row: dict[str, Any], as_model: bool) -> dict[str, Any] | File:
        """Convert SurrealDB row to File model or dict."""
        if as_model:
            return File(
                id=0,  # Will be set by caller
                path=FilePath(row.get("path", "")),
                mtime=Timestamp(row.get("modified_time", 0.0)),
                size_bytes=row.get("size", 0),
                content_hash=row.get("content_hash"),
                language=Language(row.get("language")) if row.get("language") else Language.UNKNOWN,
            )
        return {
            "id": 0,  # Will be set by caller
            "path": row.get("path", ""),
            "size_bytes": row.get("size", 0),
            "mtime": row.get("modified_time"),
            "content_hash": row.get("content_hash"),
            "language": row.get("language"),
        }

    def _row_to_chunk(self, row: dict[str, Any], as_model: bool) -> dict[str, Any] | Chunk:
        """Convert SurrealDB row to Chunk model or dict."""
        chunk_type = row.get("chunk_type", "code")
        language = row.get("language", "text")

        if as_model:
            return Chunk(
                id=0,
                file_id=0,
                code=row.get("content", ""),
                start_line=row.get("start_line", 0),
                end_line=row.get("end_line", 0),
                chunk_type=ChunkType(chunk_type) if chunk_type in [e.value for e in ChunkType] else ChunkType.CODE,
                language=Language(language) if language in [e.value for e in Language] else Language.TEXT,
                symbol=row.get("name"),
                metadata=json.loads(row.get("metadata", "{}")),
            )
        return {
            "id": 0,
            "file_id": 0,
            "code": row.get("content", ""),
            "start_line": row.get("start_line", 0),
            "end_line": row.get("end_line", 0),
            "chunk_type": chunk_type,
            "language": language,
            "symbol": row.get("name"),
            "metadata": json.loads(row.get("metadata", "{}")),
        }

    def get_base_directory(self) -> Path:
        """Get the base directory for path normalization."""
        return self._base_directory

    def create_schema(self) -> None:
        """Create database schema (handled in connect)."""
        pass

    def create_indexes(self) -> None:
        """Create database indexes (handled in connect)."""
        pass