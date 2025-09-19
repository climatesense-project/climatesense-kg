"""Base classes for data enrichment."""

from abc import ABC, abstractmethod
import logging
from typing import Any

from ..cache.interface import CacheInterface
from ..config.models import CanonicalClaimReview

logger = logging.getLogger(__name__)


class Enricher(ABC):
    """Abstract base class for data enrichers."""

    def __init__(
        self,
        name: str,
        cache: CacheInterface | None = None,
        **kwargs: Any,
    ):
        self.name = name
        self.config = kwargs
        self.cache = cache
        self.step_name = f"enricher.{name}"
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def enrich(self, items: list[CanonicalClaimReview]) -> list[CanonicalClaimReview]:
        """
        Enrich a list of claim reviews.

        Args:
            items: Single claim review or list of claim reviews

        Returns:
            Enriched single item or list (matches input type)
        """
        uris = [item.uri for item in items if item.uri]
        cached_data = {}
        if self.cache and uris:
            cached_data = self.cache.get_many(uris, self.step_name)

        # Process all items
        results: list[CanonicalClaimReview] = []
        for i, item in enumerate(items):
            try:
                if item.uri and item.uri in cached_data:
                    # Apply cached data
                    enriched = self.apply_cached_data(item, cached_data[item.uri])
                else:
                    # Process item and cache result
                    enriched = self._process_item(item)
                results.append(enriched)

                if (i + 1) % 100 == 0 or (i + 1) == len(items):
                    self.logger.info(
                        f"Enriched {i + 1}/{len(items)} items ({((i + 1) / len(items)) * 100:.1f}%)"
                    )

            except Exception as e:
                self.logger.error(f"Error enriching claim review {item.uri}: {e}")
                results.append(item)

        return results

    @abstractmethod
    def _process_item(self, claim_review: CanonicalClaimReview) -> CanonicalClaimReview:
        """
        Process a single item and cache the result.

        This is the only method enrichers need to implement.

        Args:
            claim_review: The claim review to process

        Returns:
            CanonicalClaimReview: The enriched claim review
        """
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """
        Check if this enricher is available and properly configured.

        Returns:
            bool: True if enricher can be used, False otherwise
        """
        pass

    @abstractmethod
    def apply_cached_data(
        self, claim_review: CanonicalClaimReview, cached_data: dict[str, Any]
    ) -> CanonicalClaimReview:
        """
        Apply cached enrichment data to a claim review.

        Args:
            claim_review: Original claim review
            cached_data: Cached enrichment data

        Returns:
            Enriched claim review with cached data applied
        """
        pass

    def get_cached(self, uri: str) -> dict[str, Any] | None:
        """Get cached enrichment data for a URI."""
        if not self.cache:
            return None
        return self.cache.get(uri, self.step_name)

    def set_cached(self, uri: str, payload: dict[str, Any]) -> bool:
        """Store enrichment data in cache."""
        if not self.cache:
            return False
        return self.cache.set(uri, self.step_name, payload)

    def cache_error(
        self,
        uri: str,
        error_type: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> bool:
        """
        Cache an error for the given URI.

        Args:
            uri: URI to cache error for
            error_type: Type of error
            message: Error description
            data: Optional enricher-specific data

        Returns:
            True if successfully cached, False otherwise
        """
        payload: dict[str, Any] = {
            "success": False,
            "error": {
                "type": error_type,
                "message": message,
            },
            "data": data or {},
        }
        return self.set_cached(uri, payload)

    def cache_success(self, uri: str, data: dict[str, Any]) -> bool:
        """
        Cache successful enrichment data for the given URI.

        Args:
            uri: URI to cache data for
            data: Enricher-specific data to cache

        Returns:
            True if successfully cached, False otherwise
        """
        payload: dict[str, Any] = {"success": True, "data": data}
        return self.set_cached(uri, payload)
