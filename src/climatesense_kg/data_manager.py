"""Data manager for orchestrating cache-aware data retrieval and processing."""

from collections.abc import Iterator
import logging
from pathlib import Path

from .config.models import CanonicalClaimReview
from .config.schemas import DataSourceConfig, ProviderConfig
from .processors import (
    ClaimReviewDataProcessor,
    ClimafactsProcessor,
    DbkfProcessor,
    DefactoProcessor,
    DesmogProcessor,
    EuroClimateCheckProcessor,
)
from .processors.base import BaseProcessor
from .providers import (
    FileProvider,
    GitHubProvider,
    GraphQLProvider,
    HttpProvider,
    XWikiProvider,
)
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

        # Provider type mapping
        self._providers: dict[str, type[BaseProvider]] = {
            "file": FileProvider,
            "github": GitHubProvider,
            "graphql": GraphQLProvider,
            "http": HttpProvider,
            "xwiki": XWikiProvider,
        }

        # Processor type mapping
        self._processors: dict[str, type[BaseProcessor]] = {
            "claimreviewdata": ClaimReviewDataProcessor,
            "climafacts": ClimafactsProcessor,
            "euroclimatecheck": EuroClimateCheckProcessor,
            "dbkf": DbkfProcessor,
            "defacto": DefactoProcessor,
            "desmog": DesmogProcessor,
        }

    def get_data(
        self, source_config: DataSourceConfig, skip_download: bool = False
    ) -> Iterator[CanonicalClaimReview]:
        """Get processed data for a source, using cache when possible.

        Args:
            source_config: DataSourceConfig object containing all source configuration
            skip_download: Skip data downloads and use only cached data

        Yields:
            CanonicalClaimReview objects
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
            raw_data = self.cache.get(
                source_name,
                cache_key_config,
                cache_ttl_hours,
                ignore_expiry=skip_download,
            )

            if skip_download and raw_data is not None:
                self.logger.info(
                    f"Using cached data for {source_name} (--skip-download enabled, ignoring expiry)"
                )

            # 3. If cache miss, fetch from provider (unless skip_download is True)
            if raw_data is None:
                if skip_download:
                    self.logger.warning(
                        f"No cached data found for {source_name} and --skip-download is enabled. "
                        f"Skipping download - no data will be processed for this source."
                    )
                    return
                else:
                    self.logger.info(
                        f"Cache miss for {source_name}, fetching from provider"
                    )
                    raw_data = provider.fetch(provider_config)

                    # Store in cache
                    self.cache.put(source_name, cache_key_config, raw_data)

            # 4. Process data
            processor = self._create_processor(source_name, source_type)
            yield from processor.process(raw_data)

        except Exception as e:
            self.logger.error(f"Failed to get data for {source_name}: {e}")
            raise

    def _create_provider(
        self, source_name: str, provider_config: ProviderConfig
    ) -> BaseProvider:
        """Create provider instance from config."""
        provider_type = provider_config.provider_type
        if provider_type not in self._providers:
            raise ValueError(f"Unknown provider type: {provider_type}")

        provider_class = self._providers[provider_type]
        return provider_class(source_name)

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
