"""URI cache utility for tracking processed claim review URIs."""

import logging
from pathlib import Path
import threading

logger = logging.getLogger(__name__)


class URICache:
    """URI cache using a text file to track processed claim review URIs."""

    def __init__(self, cache_path: str | Path):
        """
        Initialize URI cache.

        Args:
            cache_path: Path to the cache file
        """
        self.cache_path = Path(cache_path)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._cached_uris: set[str] | None = None
        self._lock = threading.RLock()

    def load_cache(self) -> set[str]:
        """
        Load cached URIs from file.

        Returns:
            Set[str]: Set of previously processed URIs
        """
        with self._lock:
            if self._cached_uris is not None:
                return self._cached_uris

            self._cached_uris = set()

            if not self.cache_path.exists():
                self.logger.info(
                    f"Cache file doesn't exist, starting with empty cache: {self.cache_path}"
                )
                return self._cached_uris

            try:
                with open(self.cache_path, encoding="utf-8") as f:
                    for line in f:
                        uri = line.strip()
                        if uri:  # Skip empty lines
                            self._cached_uris.add(uri)

                self.logger.info(
                    f"Loaded {len(self._cached_uris)} URIs from cache: {self.cache_path}"
                )

            except Exception as e:
                self.logger.error(f"Error loading URI cache: {e}")
                self._cached_uris = set()

            return self._cached_uris

    def is_uri_cached(self, uri: str) -> bool:
        """
        Check if a URI has been processed before.

        Args:
            uri: URI to check

        Returns:
            bool: True if URI is in cache, False otherwise
        """
        with self._lock:
            cached_uris = self.load_cache()
            return uri in cached_uris

    def add_uris(self, uris: list[str]) -> None:
        """
        Add URIs to cache and persist to file.

        Args:
            uris: List of URIs to add to cache
        """
        if not uris:
            return

        with self._lock:
            cached_uris = self.load_cache()
            new_uris = [uri for uri in uris if uri and uri not in cached_uris]
            cached_uris.update(new_uris)

            if not new_uris:
                self.logger.info("No new URIs to add to cache")
                return

            try:
                # Ensure parent directory exists
                self.cache_path.parent.mkdir(parents=True, exist_ok=True)

                # Append new URIs to file
                with open(self.cache_path, "a", encoding="utf-8") as f:
                    for uri in new_uris:
                        f.write(f"{uri}\n")

                self.logger.info(
                    f"Added {len(new_uris)} new URIs to cache: {self.cache_path}"
                )

            except Exception as e:
                self.logger.error(f"Error updating URI cache: {e}")
                raise
