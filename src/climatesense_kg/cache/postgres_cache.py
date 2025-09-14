"""PostgreSQL implementation of the cache interface."""

import json
import logging
from typing import Any

import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

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
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        try:
            self.connection_pool = SimpleConnectionPool(
                min_connections,
                max_connections,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
            self.logger.info(
                f"Connected to PostgreSQL at {self.host}:{self.port}/{self.database}"
            )
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

        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_step ON cache_entries(step);",
            "CREATE INDEX IF NOT EXISTS idx_step_success ON cache_entries(step, success);",
            "CREATE INDEX IF NOT EXISTS idx_created_at ON cache_entries(created_at);",
            "CREATE INDEX IF NOT EXISTS idx_uri ON cache_entries(uri);",
        ]

        connection: psycopg2.extensions.connection | None = None
        try:
            connection = self.connection_pool.getconn()
            if connection is None:
                raise RuntimeError("Connection pool returned None")
            with connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                for index_sql in create_indexes_sql:
                    cursor.execute(index_sql)
                connection.commit()
                self.logger.debug("Ensured cache table and indexes exist")
        except Exception as e:
            if connection:
                connection.rollback()
            self.logger.error(f"Error creating cache table: {e}")
            raise
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    def _determine_success(self, payload: dict[str, Any]) -> bool:
        """Determine if the payload represents a successful operation."""
        if payload.get("extraction_error"):
            return False
        if payload.get("error"):
            return False
        if payload.get("error_details"):
            return False
        return True

    def get(self, uri: str, step: str) -> dict[str, Any] | None:
        """Get cached data for a URI and step."""
        connection = None
        try:
            cache_key = self.generate_cache_key(uri, step)
            connection = self.connection_pool.getconn()

            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT payload FROM cache_entries WHERE cache_key = %s",
                    (cache_key,),
                )
                result = cursor.fetchone()

            if result is None:
                self.logger.debug(f"Cache miss for {step} - {uri}")
                return None

            payload = self.extract_payload(result["payload"])

            if payload is not None:
                self.logger.debug(f"Cache hit for {step} - {uri}")
                return payload
            else:
                self.logger.warning(f"Invalid cache value for {step} - {uri}")
                return None

        except Exception as e:
            self.logger.error(f"Error reading from cache for {step} - {uri}: {e}")
            return None
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    def set(
        self,
        uri: str,
        step: str,
        payload: dict[str, Any],
    ) -> bool:
        """Store data in cache for a URI and step."""
        connection = None
        try:
            cache_key = self.generate_cache_key(uri, step)
            cache_value = self.create_cache_value(step, payload)
            success = self._determine_success(payload)

            connection = self.connection_pool.getconn()

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
                    (cache_key, step, uri, success, json.dumps(cache_value)),
                )
                connection.commit()

            self.logger.debug(f"Cached data for {step} - {uri} (success: {success})")
            return True

        except Exception as e:
            if connection:
                connection.rollback()
            self.logger.error(f"Error writing to cache for {step} - {uri}: {e}")
            return False
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    def delete(self, uri: str, step: str) -> bool:
        """Delete cached data for a URI and step."""
        connection = None
        try:
            cache_key = self.generate_cache_key(uri, step)
            connection = self.connection_pool.getconn()

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
            if connection:
                connection.rollback()
            self.logger.error(f"Error deleting from cache for {step} - {uri}: {e}")
            return False
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    def ping(self) -> bool:
        """Test PostgreSQL connection."""
        connection = None
        try:
            connection = self.connection_pool.getconn()
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return cursor.fetchone() is not None
        except Exception:
            return False
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    def close(self) -> None:
        """Close PostgreSQL connection pool."""
        try:
            if hasattr(self, "connection_pool") and self.connection_pool:
                self.connection_pool.closeall()
                self.logger.info("Closed PostgreSQL connection pool")
        except Exception as e:
            self.logger.warning(f"Error closing PostgreSQL connection pool: {e}")

    def __enter__(self) -> "PostgresCache":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()
