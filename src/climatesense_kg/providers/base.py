"""Base provider class for data fetching."""

from abc import ABC, abstractmethod
import logging
from typing import Any

from ..config.schemas import ProviderConfig


class BaseProvider(ABC):
    """Abstract base class for data providers."""

    def __init__(self, name: str):
        """Initialize provider.

        Args:
            name: Name of the data source
        """
        self.name = name
        self.logger = logging.getLogger(f"provider.{name}")

    @abstractmethod
    def fetch(self, config: ProviderConfig) -> bytes:
        """Fetch raw data from the source.

        Args:
            config: Provider-specific configuration

        Returns:
            Raw data as bytes

        Raises:
            Exception: If fetching fails
        """
        pass

    @abstractmethod
    def get_cache_key_fields(self, config: ProviderConfig) -> dict[str, Any]:
        """Get config fields and values that should be included in cache key.

        Args:
            config: Full provider configuration

        Returns:
            Dict of cache-relevant field names and their values
        """
        pass
