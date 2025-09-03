"""Composite enricher that combines multiple enrichment methods."""

from typing import Any

from ..config.models import CanonicalClaimReview
from .base import Enricher, EnricherMetadata


class CompositeEnricher(Enricher):
    """Enricher that combines multiple enrichment methods."""

    def __init__(self, enrichers: list[Enricher], **kwargs: Any):
        super().__init__("composite", **kwargs)
        self.enrichers = enrichers

    def is_available(self) -> bool:
        """Check if at least one enricher is available."""
        return any(enricher.is_available() for enricher in self.enrichers)

    def enrich(self, claim_review: CanonicalClaimReview) -> CanonicalClaimReview:
        """
        Apply all enrichers sequentially to the claim review.

        Args:
            claim_review: Claim review to enrich

        Returns:
            CanonicalClaimReview: Enriched claim review
        """
        enriched = claim_review

        for enricher in self.enrichers:
            if enricher.is_available():
                try:
                    enriched = enricher.enrich(enriched)
                except Exception as e:
                    self.logger.error(f"Error in enricher {enricher.name}: {e}")
                    continue
            else:
                self.logger.warning(
                    f"Enricher {enricher.name} is not available, skipping"
                )

        return enriched

    def enrich_batch(
        self, claim_reviews: list[CanonicalClaimReview]
    ) -> list[CanonicalClaimReview]:
        """
        Apply all enrichers to a batch of claim reviews.

        Args:
            claim_reviews: List of claim reviews to enrich

        Returns:
            List[CanonicalClaimReview]: List of enriched claim reviews
        """
        enriched_reviews = claim_reviews

        for enricher in self.enrichers:
            if enricher.is_available():
                try:
                    self.logger.info(f"Applying enricher: {enricher.name}")
                    enriched_reviews = enricher.enrich_batch(enriched_reviews)
                except Exception as e:
                    self.logger.error(
                        f"Error in batch enrichment with {enricher.name}: {e}"
                    )
                    continue
            else:
                self.logger.warning(
                    f"Enricher {enricher.name} is not available, skipping"
                )

        return enriched_reviews

    def get_metadata(self) -> EnricherMetadata:
        """Get metadata about all enrichers."""
        metadata = super().get_metadata()
        metadata["enrichers"] = [enricher.get_metadata() for enricher in self.enrichers]
        return metadata
