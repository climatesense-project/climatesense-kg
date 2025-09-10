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

    @abstractmethod
    def enrich(self, claim_review: CanonicalClaimReview) -> CanonicalClaimReview:
        """
        Enrich a canonical claim review with additional information.

        Args:
            claim_review: The claim review to enrich

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

    def get_cached(self, uri: str) -> dict[str, Any] | None:
        """
        Get cached enrichment data for a URI.

        Args:
            uri: URI to look up

        Returns:
            Cached payload or None if not found/no cache
        """
        if not self.cache:
            return None

        return self.cache.get(uri, self.step_name)

    def set_cached(self, uri: str, payload: dict[str, Any]) -> bool:
        """
        Store enrichment data in cache.

        Args:
            uri: URI to cache data for
            payload: Enrichment data to cache

        Returns:
            True if successfully cached, False otherwise
        """
        if not self.cache:
            return False

        return self.cache.set(uri, self.step_name, payload)

    def enrich_batch(
        self, claim_reviews: list[CanonicalClaimReview]
    ) -> list[CanonicalClaimReview]:
        """
        Enrich a batch of claim reviews.

        Args:
            claim_reviews: List of claim reviews to enrich

        Returns:
            List[CanonicalClaimReview]: List of enriched claim reviews
        """
        enriched_reviews: list[CanonicalClaimReview] = []

        for claim_review in claim_reviews:
            try:
                enriched = self.enrich(claim_review)
                enriched_reviews.append(enriched)
            except Exception as e:
                self.logger.error(
                    f"Error enriching claim review {claim_review.uri}: {e}"
                )
                # Return original if enrichment fails
                enriched_reviews.append(claim_review)

        return enriched_reviews
