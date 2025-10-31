"""PostgreSQL implementation of the cache interface."""

from __future__ import annotations

import json
import logging
from typing import Any

from psycopg.abc import Query
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .interface import CacheInterface

logger = logging.getLogger(__name__)


class PostgresCache(CacheInterface):
    """PostgreSQL-based implementation of the cache interface."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "climatesense_cache",
        user: str = "postgres",
        password: str | None = None,
        min_connections: int = 1,
        max_connections: int = 20,
    ):
        """
        Initialize PostgreSQL cache with connection parameters.

        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            user: Database user
            password: Database password
            min_connections: Minimum connections in pool
            max_connections: Maximum connections in pool
        """
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        try:
            # Create connection string for psycopg3
            conninfo = f"host={host} port={port} dbname={database} user={user}"
            if password:
                conninfo += f" password={password}"

            self.connection_pool = ConnectionPool(
                conninfo=conninfo,
                min_size=min_connections,
                max_size=max_connections,
            )
            self.logger.info(f"Connected to PostgreSQL at {host}:{port}/{database}")
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        """Create cache table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cache_entries (
            cache_key TEXT PRIMARY KEY,
            step TEXT NOT NULL,
            uri TEXT NOT NULL,
            success BOOLEAN NOT NULL DEFAULT TRUE,
            payload JSONB COMPRESSION lz4 NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """

        create_indexes_sql: list[Query] = [
            "CREATE INDEX IF NOT EXISTS idx_step ON cache_entries(step);",
            "CREATE INDEX IF NOT EXISTS idx_step_success ON cache_entries(step, success);",
            "CREATE INDEX IF NOT EXISTS idx_created_at ON cache_entries(created_at);",
            "CREATE INDEX IF NOT EXISTS idx_uri ON cache_entries(uri);",
        ]

        try:
            with self.connection_pool.connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    for index_sql in create_indexes_sql:
                        cursor.execute(index_sql)
                    connection.commit()
                    self.logger.debug("Ensured cache table and indexes exist")
        except Exception as e:
            self.logger.error(f"Error creating cache table: {e}")
            raise

    def get(self, uri: str, step: str) -> dict[str, Any] | None:
        """Get cached data for a URI and step."""
        try:
            cache_key = self.generate_cache_key(uri, step)

            with self.connection_pool.connection() as connection:
                with connection.cursor(row_factory=dict_row) as cursor:
                    cursor.execute(
                        "SELECT payload FROM cache_entries WHERE cache_key = %s",
                        (cache_key,),
                    )
                    result = cursor.fetchone()

            if result is None:
                self.logger.debug(f"Cache miss for {step} - {uri}")
                return None

            payload = result["payload"]

            if payload is not None:
                self.logger.debug(f"Cache hit for {step} - {uri}")
                return payload
            else:
                self.logger.warning(f"Invalid cache value for {step} - {uri}")
                return None

        except Exception as e:
            self.logger.error(f"Error reading from cache for {step} - {uri}: {e}")
            return None

    def set(
        self,
        uri: str,
        step: str,
        payload: dict[str, Any],
    ) -> bool:
        """Store data in cache for a URI and step."""
        try:
            cache_key = self.generate_cache_key(uri, step)
            success = bool(payload.get("success", True))

            with self.connection_pool.connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO cache_entries (cache_key, step, uri, success, payload)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (cache_key)
                        DO UPDATE SET
                            success = EXCLUDED.success,
                            payload = EXCLUDED.payload
                        """,
                        (cache_key, step, uri, success, json.dumps(payload)),
                    )
                    connection.commit()

            self.logger.debug(f"Cached data for {step} - {uri} (success: {success})")
            return True

        except Exception as e:
            self.logger.error(f"Error writing to cache for {step} - {uri}: {e}")
            return False

    def delete(self, uri: str, step: str) -> bool:
        """Delete cached data for a URI and step."""
        try:
            cache_key = self.generate_cache_key(uri, step)

            with self.connection_pool.connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM cache_entries WHERE cache_key = %s", (cache_key,)
                    )
                    deleted_count = cursor.rowcount
                    connection.commit()

            if deleted_count > 0:
                self.logger.debug(f"Deleted cache entry for {step} - {uri}")
                return True
            else:
                self.logger.debug(f"No cache entry found to delete for {step} - {uri}")
                return False

        except Exception as e:
            self.logger.error(f"Error deleting from cache for {step} - {uri}: {e}")
            return False

    def get_many(self, uris: list[str], step: str) -> dict[str, dict[str, Any]]:
        """Get cached data for multiple URIs and a single step."""
        if not uris:
            return {}

        try:
            cache_keys = [self.generate_cache_key(uri, step) for uri in uris]
            uri_to_key = {uri: self.generate_cache_key(uri, step) for uri in uris}
            key_to_uri = {key: uri for uri, key in uri_to_key.items()}

            with self.connection_pool.connection() as connection:
                with connection.cursor(row_factory=dict_row) as cursor:
                    cursor.execute(
                        "SELECT cache_key, payload FROM cache_entries WHERE cache_key = ANY(%s)",
                        (cache_keys,),
                    )
                    results = cursor.fetchall()

            # Map results back to URIs
            uri_payloads: dict[str, dict[str, Any]] = {}
            for result in results:
                cache_key = result["cache_key"]
                payload = result["payload"]
                uri = key_to_uri[cache_key]
                if payload is not None:
                    uri_payloads[uri] = payload

            self.logger.debug(
                f"Batch cache lookup for {step}: {len(uri_payloads)}/{len(uris)} hits"
            )
            return uri_payloads

        except Exception as e:
            self.logger.error(f"Error reading batch from cache for {step}: {e}")
            return {}

    def set_many(
        self,
        uri_step_payloads: list[tuple[str, str, dict[str, Any]]],
    ) -> bool:
        """Store data in cache for multiple URI-step combinations in a single batch operation."""
        if not uri_step_payloads:
            return True

        try:
            batch_data: list[tuple[str, str, str, bool, str]] = []
            for uri, step, payload in uri_step_payloads:
                cache_key = self.generate_cache_key(uri, step)
                success = bool(payload.get("success", True))
                batch_data.append((cache_key, step, uri, success, json.dumps(payload)))

            with self.connection_pool.connection() as connection:
                with connection.cursor() as cursor:
                    cursor.executemany(
                        """
                        INSERT INTO cache_entries (cache_key, step, uri, success, payload)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (cache_key)
                        DO UPDATE SET
                            success = EXCLUDED.success,
                            payload = EXCLUDED.payload
                        """,
                        batch_data,
                    )
                    connection.commit()

            self.logger.debug(f"Batch cached {len(uri_step_payloads)} entries")
            return True

        except Exception as e:
            self.logger.error(f"Error batch writing to cache: {e}")
            return False

    def ping(self) -> bool:
        """Test PostgreSQL connection."""
        try:
            with self.connection_pool.connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return cursor.fetchone() is not None
        except Exception:
            return False

    def close(self) -> None:
        """Close PostgreSQL connection pool."""
        try:
            self.connection_pool.close()
            self.logger.info("Closed PostgreSQL connection pool")
        except Exception as e:
            self.logger.warning(f"Error closing PostgreSQL connection pool: {e}")

    def __enter__(self) -> PostgresCache:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()
