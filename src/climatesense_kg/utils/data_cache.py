"""Cache utility for storing and retrieving data."""

from dataclasses import dataclass
import gzip
import hashlib
import json
import logging
from pathlib import Path
import shutil
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DataCacheStats:
    total_entries: int
    total_size_bytes: int
    sources: dict[str, dict[str, int]]


class DataCache:
    """File-based cache system."""

    def __init__(self, cache_dir: Path, default_ttl_hours: float = 24.0):
        """Initialize cache.

        Args:
            cache_dir: Directory to store cache files
            default_ttl_hours: Default TTL in hours for cached data
        """
        self.cache_dir = Path(cache_dir)
        self.default_ttl_hours = default_ttl_hours
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._lock = threading.RLock()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _generate_cache_key(self, source_name: str, config: dict[str, Any]) -> str:
        """Generate cache key from source name and config."""
        config_str = json.dumps(config, sort_keys=True, ensure_ascii=True)
        config_hash = hashlib.sha256(config_str.encode()).hexdigest()

        return f"{source_name}_{config_hash}"

    def _get_cache_path(self, cache_key: str) -> Path:
        """Get cache file path for given key."""
        source_name = cache_key.split("_")[0]
        cache_subdir = self.cache_dir / source_name
        cache_subdir.mkdir(exist_ok=True)
        return cache_subdir / f"{cache_key}.gz"

    def _get_metadata_path(self, cache_key: str) -> Path:
        """Get metadata file path for given key."""
        cache_path = self._get_cache_path(cache_key)
        return cache_path.with_suffix(".meta.json")

    def _is_expired(self, cache_key: str, ttl_hours: float) -> bool:
        """Check if cache entry is expired."""
        metadata_path = self._get_metadata_path(cache_key)

        if not metadata_path.exists():
            return True

        try:
            with open(metadata_path, encoding="utf-8") as f:
                metadata = json.load(f)

            cached_time = float(metadata.get("timestamp", 0))
            max_age_seconds = ttl_hours * 3600

            return (time.time() - cached_time) > max_age_seconds

        except Exception as e:
            self.logger.warning(f"Failed to read cache metadata for {cache_key}: {e}")
            return True

    def get(
        self,
        source_name: str,
        config: dict[str, Any],
        ttl_hours: float | None = None,
        ignore_expiry: bool = False,
    ) -> bytes | None:
        """Get cached data if available and not expired.

        Args:
            source_name: Name of the data source
            config: Configuration dict used to generate cache key
            ttl_hours: Cache TTL in hours, uses default if None
            ignore_expiry: If True, ignore expiration and return data if it exists

        Returns:
            Cached data as bytes, or None if not available/expired
        """
        ttl_hours = ttl_hours or self.default_ttl_hours
        cache_key = self._generate_cache_key(source_name, config)
        self.logger.debug(f"Looking for cache key: {cache_key}")

        with self._lock:
            if not ignore_expiry and self._is_expired(cache_key, ttl_hours):
                self.logger.info(f"Cache miss/expired for {source_name}")
                return None

            cache_path = self._get_cache_path(cache_key)

            if not cache_path.exists():
                self.logger.info(f"Cache miss for {source_name}")
                return None

            try:
                with gzip.open(cache_path, "rb") as f:
                    data = f.read()

                self.logger.info(f"Cache hit for {source_name} ({len(data)} bytes)")
                return data

            except Exception as e:
                self.logger.warning(f"Failed to read cache for {source_name}: {e}")
                return None

    def put(self, source_name: str, config: dict[str, Any], data: bytes) -> None:
        """Store data in cache.

        Args:
            source_name: Name of the data source
            config: Configuration dict used to generate cache key
            data: Raw data to cache
        """
        cache_key = self._generate_cache_key(source_name, config)

        with self._lock:
            cache_path = self._get_cache_path(cache_key)
            metadata_path = self._get_metadata_path(cache_key)

            try:
                with gzip.open(cache_path, "wb") as f:
                    f.write(data)

                metadata: dict[str, Any] = {
                    "timestamp": time.time(),
                    "source_name": source_name,
                    "config_hash": cache_key.split("_", 1)[1],
                    "size_bytes": len(data),
                }

                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, indent=2)

                self.logger.info(f"Cached {len(data)} bytes for {source_name}")

            except Exception as e:
                self.logger.error(f"Failed to cache data for {source_name}: {e}")
                # Clean up partial files
                for path in [cache_path, metadata_path]:
                    if path.exists():
                        path.unlink(missing_ok=True)
                raise

    def clear(self, source_name: str | None = None) -> None:
        """Clear cache entries.

        Args:
            source_name: If provided, only clear cache for this source.
                        If None, clear all cache entries.
        """
        with self._lock:
            if source_name:
                source_dir = self.cache_dir / source_name
                if source_dir.exists():
                    shutil.rmtree(source_dir)
                    self.logger.info(f"Cleared cache for {source_name}")
            else:
                shutil.rmtree(self.cache_dir)
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                self.logger.info("Cleared all cache")

    def get_stats(self) -> DataCacheStats:
        """Get cache statistics."""
        with self._lock:
            stats = DataCacheStats(
                total_entries=0,
                total_size_bytes=0,
                sources={},
            )

            for source_dir in self.cache_dir.iterdir():
                if not source_dir.is_dir():
                    continue

                source_name = source_dir.name
                source_entries = 0
                source_size = 0

                for cache_file in source_dir.glob("*.gz"):
                    source_entries += 1
                    source_size += cache_file.stat().st_size

                stats.sources[source_name] = {
                    "entries": source_entries,
                    "size_bytes": source_size,
                }

                stats.total_entries += source_entries
                stats.total_size_bytes += source_size

            return stats
