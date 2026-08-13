"""Cache utility for storing and retrieving data."""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import gzip
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time
from typing import Any, BinaryIO

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

    def _get_cache_path(self, source_name: str, cache_key: str) -> Path:
        """Get cache file path for given key."""
        cache_subdir = self.cache_dir / source_name
        cache_subdir.mkdir(exist_ok=True)
        return cache_subdir / f"{cache_key}.gz"

    def _get_metadata_path(self, source_name: str, cache_key: str) -> Path:
        """Get metadata file path for given key."""
        cache_path = self._get_cache_path(source_name, cache_key)
        return cache_path.with_suffix(".meta.json")

    @contextmanager
    def _cache_file_lock(
        self, source_name: str, cache_key: str, *, exclusive: bool
    ) -> Iterator[None]:
        """Hold a lock shared by every process accessing a cache entry."""
        cache_path = self._get_cache_path(source_name, cache_key)
        lock_path = cache_path.with_suffix(".lock")
        with lock_path.open("a+b") as lock_file:
            operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            fcntl.flock(lock_file.fileno(), operation)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _is_expired(self, source_name: str, cache_key: str, ttl_hours: float) -> bool:
        """Check if cache entry is expired."""
        metadata_path = self._get_metadata_path(source_name, cache_key)

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
        with self.open_stream(
            source_name,
            config,
            ttl_hours,
            ignore_expiry=ignore_expiry,
        ) as stream:
            return stream.read() if stream is not None else None

    @contextmanager
    def open_stream(
        self,
        source_name: str,
        config: dict[str, Any],
        ttl_hours: float | None = None,
        *,
        ignore_expiry: bool = False,
    ) -> Iterator[BinaryIO | None]:
        """Open one cache entry as a decompressed stream under a shared lock."""

        if ttl_hours is None:
            ttl_hours = self.default_ttl_hours
        cache_key = self._generate_cache_key(source_name, config)
        self.logger.debug("Looking for cache key: %s", cache_key)
        with self._lock, self._cache_file_lock(source_name, cache_key, exclusive=False):
            if not ignore_expiry and self._is_expired(
                source_name, cache_key, ttl_hours
            ):
                self.logger.info("Cache miss/expired for %s", source_name)
                yield None
                return
            cache_path = self._get_cache_path(source_name, cache_key)
            if not cache_path.exists():
                self.logger.info("Cache miss for %s", source_name)
                yield None
                return
            try:
                stream = gzip.open(cache_path, "rb")
            except Exception as exc:
                self.logger.warning("Failed to read cache for %s: %s", source_name, exc)
                yield None
                return
            try:
                self.logger.info(
                    "Cache hit for %s (%d compressed bytes)",
                    source_name,
                    cache_path.stat().st_size,
                )
                yield stream
            finally:
                stream.close()

    def put(self, source_name: str, config: dict[str, Any], data: bytes) -> None:
        """Store data in cache.

        Args:
            source_name: Name of the data source
            config: Configuration dict used to generate cache key
            data: Raw data to cache
        """
        cache_key = self._generate_cache_key(source_name, config)

        with self._lock, self._cache_file_lock(source_name, cache_key, exclusive=True):
            cache_path = self._get_cache_path(source_name, cache_key)
            metadata_path = self._get_metadata_path(source_name, cache_key)
            cache_temp_path: Path | None = None
            metadata_temp_path: Path | None = None

            try:
                cache_fd, cache_temp_name = tempfile.mkstemp(
                    dir=cache_path.parent,
                    prefix=f".{cache_path.name}.",
                    suffix=".tmp",
                )
                cache_temp_path = Path(cache_temp_name)
                with os.fdopen(cache_fd, "wb") as raw_cache:
                    with gzip.GzipFile(fileobj=raw_cache, mode="wb") as compressed:
                        compressed.write(data)
                    raw_cache.flush()
                    os.fsync(raw_cache.fileno())

                expected_digest = hashlib.sha256(data).digest()
                with gzip.open(cache_temp_path, "rb") as compressed:
                    cached_digest = hashlib.file_digest(compressed, "sha256").digest()
                if cached_digest != expected_digest:
                    raise OSError("Temporary cache validation failed")

                metadata: dict[str, Any] = {
                    "timestamp": time.time(),
                    "source_name": source_name,
                    "config_hash": cache_key.rsplit("_", 1)[1],
                    "size_bytes": len(data),
                }

                metadata_fd, metadata_temp_name = tempfile.mkstemp(
                    dir=metadata_path.parent,
                    prefix=f".{metadata_path.name}.",
                    suffix=".tmp",
                )
                metadata_temp_path = Path(metadata_temp_name)
                with os.fdopen(metadata_fd, "w", encoding="utf-8") as metadata_file:
                    json.dump(metadata, metadata_file, indent=2)
                    metadata_file.flush()
                    os.fsync(metadata_file.fileno())

                with metadata_temp_path.open(encoding="utf-8") as metadata_file:
                    json.load(metadata_file)

                os.replace(cache_temp_path, cache_path)
                cache_temp_path = None
                os.replace(metadata_temp_path, metadata_path)
                metadata_temp_path = None

                self.logger.info(f"Cached {len(data)} bytes for {source_name}")

            except Exception as e:
                self.logger.error(f"Failed to cache data for {source_name}: {e}")
                raise
            finally:
                for temp_path in (cache_temp_path, metadata_temp_path):
                    if temp_path is not None:
                        temp_path.unlink(missing_ok=True)

    def clear(self, source_name: str | None = None) -> None:
        """Clear cache entries.

        Args:
            source_name: If provided, only clear cache for this source.
                        If None, clear all cache entries.
        """
        with self._lock:
            if source_name is not None:
                source_path = Path(source_name)
                if (
                    not source_name
                    or source_path.is_absolute()
                    or "/" in source_name
                    or "\\" in source_name
                ):
                    raise ValueError(f"Invalid cache source name: {source_name!r}")

                cache_root = self.cache_dir.resolve()
                source_dir = cache_root / source_name
                if source_dir.resolve().parent != cache_root:
                    raise ValueError(f"Invalid cache source name: {source_name!r}")

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
