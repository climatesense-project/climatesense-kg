"""Cache interface definition."""

from abc import ABC, abstractmethod
from datetime import UTC, datetime
import hashlib
from typing import Any


class CacheInterface(ABC):
    """Abstract interface for per-step caching."""

    @abstractmethod
    def get(self, uri: str, step: str) -> dict[str, Any] | None:
        """
        Get cached data for a URI and step.

        Args:
            uri: Canonical URI to look up
            step: Step name (e.g., 'enricher.dbpedia_spotlight')

        Returns:
            Cached payload dict or None if not found
        """
        pass

    @abstractmethod
    def set(
        self,
        uri: str,
        step: str,
        payload: dict[str, Any],
    ) -> bool:
        """
        Store data in cache for a URI and step.

        Args:
            uri: URI to cache data for
            step: Step name (e.g., 'enricher.dbpedia_spotlight')
            payload: Step-specific data to cache

        Returns:
            True if successfully cached, False otherwise
        """
        pass

    @abstractmethod
    def delete(self, uri: str, step: str) -> bool:
        """
        Delete cached data for a URI and step.

        Args:
            uri: URI to delete
            step: Step name

        Returns:
            True if deleted, False if not found
        """
        pass

    def generate_cache_key(self, uri: str, step: str, env: str) -> str:
        """
        Generate namespaced cache key.

        Args:
            uri: URI to generate key for
            step: Step name
            env: Environment namespace

        Returns:
            Cache key in format: {env}:climatesense:{step}:{uri_sha256}
        """
        uri_hash = hashlib.sha256(uri.encode()).hexdigest()
        return f"{env}:climatesense:{step}:{uri_hash}"

    def create_cache_value(
        self,
        step: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Create standardized cache value wrapper.

        Args:
            step: Step name
            payload: Step-specific data

        Returns:
            Wrapped cache value with metadata
        """
        value: dict[str, Any] = {
            "step": step,
            "payload": payload,
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }

        return value

    def extract_payload(
        self, cache_value: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        """
        Extract payload from cache value wrapper.

        Args:
            cache_value: Raw cache value or None

        Returns:
            Step-specific payload or None
        """
        if not cache_value:
            return None
        return cache_value.get("payload")
