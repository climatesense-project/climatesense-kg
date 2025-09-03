"""Base classes for data enrichment."""

from abc import ABC, abstractmethod
import logging
from typing import Any, NotRequired, TypedDict

from ..config.models import CanonicalClaimReview

logger = logging.getLogger(__name__)


class EnricherMetadata(TypedDict):
    """Metadata about an enricher."""

    name: str
    type: str
    available: bool
    config: dict[str, Any]
    enrichers: NotRequired[list["EnricherMetadata"]]


class Enricher(ABC):
    """Abstract base class for data enrichers."""

    def __init__(self, name: str, **kwargs: Any):
        self.name = name
        self.config = kwargs
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

    def get_metadata(self) -> EnricherMetadata:
        """
        Get metadata about this enricher.

        Returns:
            Dict[str, Any]: Enricher metadata
        """
        return {
            "name": self.name,
            "type": self.__class__.__name__,
            "available": self.is_available(),
            "config": self.config,
        }
