"""Redis implementation of the cache interface."""

import json
import logging
from typing import Any

import redis

from .interface import CacheInterface

logger = logging.getLogger(__name__)


class RedisCache(CacheInterface):
    """Redis-based implementation of the cache interface."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: str | None = None,
        db: int = 0,
        env: str = "dev",
        decode_responses: bool = True,
    ):
        """
        Initialize Redis cache with connection parameters.

        Args:
            host: Redis host
            port: Redis port
            password: Redis password (optional)
            db: Redis database number
            env: Environment namespace for cache keys
            decode_responses: Whether to decode responses to strings
        """
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.env = env

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self.redis_client = redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password,
            db=self.db,
            decode_responses=decode_responses,
            max_connections=20,
            retry_on_timeout=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            health_check_interval=30,
        )

        self._test_connection()

    def _test_connection(self) -> None:
        """Test Redis connection."""
        try:
            self.redis_client.ping()  # pyright: ignore[reportUnknownMemberType]
            self.logger.info(f"Connected to Redis at {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

    def get(self, uri: str, step: str) -> dict[str, Any] | None:
        """Get cached data for a URI and step."""
        try:
            cache_key = self.generate_cache_key(uri, step, self.env)
            raw_value = self.redis_client.get(cache_key)

            if raw_value is None:
                self.logger.debug(f"Cache miss for {step} - {uri}")
                return None

            if isinstance(raw_value, str):
                cache_value = json.loads(raw_value)
            else:
                self.logger.warning(f"Unexpected cache value type for {step} - {uri}")
                return None

            payload = self.extract_payload(cache_value)

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
            cache_key = self.generate_cache_key(uri, step, self.env)
            cache_value = self.create_cache_value(step, payload)
            json_value = json.dumps(cache_value)
            self.redis_client.set(cache_key, json_value)
            self.logger.debug(f"Cached data for {step} - {uri}")
            return True

        except Exception as e:
            self.logger.error(f"Error writing to cache for {step} - {uri}: {e}")
            return False

    def delete(self, uri: str, step: str) -> bool:
        """Delete cached data for a URI and step."""
        try:
            cache_key = self.generate_cache_key(uri, step, self.env)
            deleted = self.redis_client.delete(cache_key)

            if deleted:
                self.logger.debug(f"Deleted cache entry for {step} - {uri}")
                return True
            else:
                self.logger.debug(f"No cache entry found to delete for {step} - {uri}")
                return False

        except Exception as e:
            self.logger.error(f"Error deleting from cache for {step} - {uri}: {e}")
            return False

    def ping(self) -> bool:
        """Test Redis connection."""
        try:
            result = (
                self.redis_client.ping()  # pyright: ignore[reportUnknownMemberType]
            )
            return result is True
        except Exception:
            return False

    def close(self) -> None:
        """Close Redis connection pool."""
        try:
            if hasattr(self, "redis_client") and self.redis_client:
                self.redis_client.close()
                self.logger.info("Closed Redis connection pool")
        except Exception as e:
            self.logger.warning(f"Error closing Redis connection: {e}")

    def __enter__(self) -> "RedisCache":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()
