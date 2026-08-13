"""Data manager for orchestrating cache-aware data retrieval and processing."""

from collections.abc import Iterator
import logging
from pathlib import Path

from .config.schemas import DataSourceConfig, ProviderConfig
from .domain import SourceReviewRecord
from .processors import (
    ClaimReviewDataProcessor,
    ClimafactsProcessor,
    ClimateFeverProcessor,
    DbkfProcessor,
    DefactoProcessor,
    DesmogProcessor,
    EuroClimateCheckProcessor,
)
from .processors.base import BaseProcessor
from .provider_registry import PROVIDER_REGISTRATIONS
from .providers.base import BaseProvider
from .utils.data_cache import DataCache

logger = logging.getLogger(__name__)


class DataManager:
    """Central orchestrator for cache-aware data retrieval and processing."""

    def __init__(
        self, cache_dir: Path | str = "cache", default_ttl_hours: float = 24.0
    ):
        """Initialize data manager.

        Args:
            cache_dir: Directory for cache storage
            default_ttl_hours: Default cache TTL in hours
        """
        self.cache = DataCache(Path(cache_dir), default_ttl_hours)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Processor type mapping
        self._processors: dict[str, type[BaseProcessor]] = {
            "claimreviewdata": ClaimReviewDataProcessor,
            "climafacts": ClimafactsProcessor,
            "climate-fever": ClimateFeverProcessor,
            "euroclimatecheck": EuroClimateCheckProcessor,
            "dbkf": DbkfProcessor,
            "defacto": DefactoProcessor,
            "desmog": DesmogProcessor,
        }

    def get_data(
        self, source_config: DataSourceConfig, skip_download: bool = False
    ) -> Iterator[SourceReviewRecord]:
        """Get processed data for a source, using cache when possible.

        Args:
            source_config: DataSourceConfig object containing all source configuration
            skip_download: Skip data downloads and use only cached data

        Yields:
            SourceReviewRecord objects
        """
        source_name = source_config.name
        source_type = source_config.type
        provider_config = source_config.provider
        cache_ttl_hours = source_config.cache_ttl_hours

        if not provider_config:
            raise ValueError(f"Source config for '{source_name}' must have a provider")

        self.logger.info(f"Getting data for source: {source_name}")

        try:
            # 1. Create provider
            provider = self._create_provider(source_name, provider_config)

            # 2. Check cache
            cache_key_config = provider.get_cache_key_fields(provider_config)
            fallback_key_config = provider.get_cache_fallback_key_fields(
                provider_config
            )
            processor = self._create_processor(source_name, source_type)
            with self.cache.open_stream(
                source_name,
                cache_key_config,
                cache_ttl_hours,
                ignore_expiry=skip_download,
            ) as cached:
                if cached is not None:
                    if skip_download:
                        self.logger.info(
                            "Using cached data for %s (--skip-download enabled, "
                            "ignoring expiry)",
                            source_name,
                        )
                    yield from processor.process_stream(cached)
                    return

            if skip_download and fallback_key_config:
                with self.cache.open_stream(
                    source_name,
                    fallback_key_config,
                    cache_ttl_hours,
                    ignore_expiry=True,
                ) as cached:
                    if cached is not None:
                        self.logger.info(
                            "Using fallback cached data for %s (--skip-download enabled)",
                            source_name,
                        )
                        yield from processor.process_stream(cached)
                        return

            if skip_download:
                raise RuntimeError(
                    f"No cached data found for {source_name} and --skip-download is enabled. "
                    "The source cannot be ingested completely."
                )

            self.logger.info("Cache miss for %s, fetching from provider", source_name)
            raw_data = provider.fetch(provider_config)
            self.cache.put(source_name, cache_key_config, raw_data)
            if fallback_key_config and fallback_key_config != cache_key_config:
                self.cache.put(source_name, fallback_key_config, raw_data)
            del raw_data
            with self.cache.open_stream(
                source_name,
                cache_key_config,
                cache_ttl_hours,
                ignore_expiry=True,
            ) as cached:
                if cached is None:  # pragma: no cover - validated by cache.put
                    raise RuntimeError(
                        f"Failed to reopen cached data for {source_name}"
                    )
                yield from processor.process_stream(cached)

        except Exception as e:
            self.logger.error(f"Failed to get data for {source_name}: {e}")
            raise

    def _create_provider(
        self, source_name: str, provider_config: ProviderConfig
    ) -> BaseProvider:
        """Create provider instance from config."""
        provider_type = provider_config.provider_type
        registration = PROVIDER_REGISTRATIONS.get(provider_type)
        if registration is None:
            raise ValueError(f"Unknown provider type: {provider_type}")
        return registration.provider_type(source_name)

    def _create_processor(self, source_name: str, source_type: str) -> BaseProcessor:
        """Create processor instance from source type."""
        if source_type not in self._processors:
            raise ValueError(f"Unknown processor type: {source_type}")

        processor_class = self._processors[source_type]
        return processor_class(source_name)

    def clear_cache(self, source_name: str | None = None) -> None:
        """Clear cache entries.

        Args:
            source_name: If provided, only clear cache for this source.
                        If None, clear all cache.
        """
        self.cache.clear(source_name)
        if source_name:
            self.logger.info(f"Cleared cache for {source_name}")
        else:
            self.logger.info("Cleared all cache")
