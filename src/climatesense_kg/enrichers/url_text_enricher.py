"""URL text extraction enrichment for claim reviews."""

import time
from typing import Any

from ..config.models import CanonicalClaimReview
from ..utils.text_processing import fetch_and_extract_text
from .base import Enricher


class URLTextEnricher(Enricher):
    """Enricher that fetches and extracts text content from review URLs."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__("url_text_extractor", **kwargs)

        self.rate_limit_delay = kwargs.get(
            "rate_limit_delay", 0.5
        )  # seconds between requests
        self.timeout = kwargs.get("timeout", 15)
        self.max_retries = kwargs.get("max_retries", 2)

    def is_available(self) -> bool:
        """Check if URL text extraction is available."""
        return True

    def enrich(self, claim_review: CanonicalClaimReview) -> CanonicalClaimReview:
        """
        Enrich claim review with extracted text from review URL.

        Args:
            claim_review: Claim review to enrich

        Returns:
            CanonicalClaimReview: Enriched claim review with URL text
        """
        if not claim_review.review_url:
            self.logger.warning("No review URL available for text extraction")
            return claim_review

        if not claim_review.uri:
            return claim_review

        # Check cache first
        cached_data = self.get_cached(claim_review.uri)
        if cached_data:
            # Apply cached URL text
            claim_review.review_url_text = cached_data.get("review_url_text")
            self.logger.debug(f"Applied cached URL text for {claim_review.uri}")
            return claim_review

        # Extract text from review URL
        extracted_text = self._extract_url_text(claim_review.review_url)

        cache_payload: dict[str, Any] = {}

        if extracted_text and not extracted_text.startswith("Error"):
            claim_review.review_url_text = extracted_text
            self.logger.debug(
                f"Successfully extracted {len(extracted_text)} characters "
                f"from URL: {claim_review.review_url}"
            )

            # Cache the results
            cache_payload = {
                "review_url_text": extracted_text,
                "review_url": claim_review.review_url,  # Store URL for debugging
            }
            self.set_cached(
                claim_review.uri,
                cache_payload,
            )

        else:
            self.logger.warning(
                f"Failed to extract text from URL: {claim_review.review_url}"
            )
            if extracted_text and extracted_text.startswith("Error"):
                claim_review.review_url_text = None  # Don't store error messages

                # Cache the failure to avoid repeated attempts
                cache_payload = {
                    "review_url_text": None,
                    "review_url": claim_review.review_url,
                    "extraction_error": True,
                }
                self.set_cached(
                    claim_review.uri,
                    cache_payload,
                )

        # Rate limiting
        time.sleep(self.rate_limit_delay)

        return claim_review

    def _extract_url_text(self, url: str) -> str | None:
        """
        Extract text from a URL with retry logic.

        Args:
            url: URL to extract text from

        Returns:
            Optional[str]: Extracted text or None if extraction fails
        """
        for attempt in range(self.max_retries + 1):
            try:
                extracted_text = fetch_and_extract_text(url)

                if extracted_text and not extracted_text.startswith("Error"):
                    return extracted_text
                elif attempt < self.max_retries:
                    self.logger.debug(
                        f"Attempt {attempt + 1} failed for URL {url}, retrying..."
                    )
                    time.sleep(1)  # Brief pause before retry
                else:
                    self.logger.warning(
                        f"All {self.max_retries + 1} attempts failed for URL: {url}"
                    )
                    return extracted_text  # Return the error message

            except Exception as e:
                if attempt < self.max_retries:
                    self.logger.debug(
                        f"Exception on attempt {attempt + 1} for URL {url}: {e}"
                    )
                    time.sleep(1)
                else:
                    self.logger.error(f"Error extracting text from URL {url}: {e}")
                    return f"Error extracting text: {e}"

        return None

    def enrich_batch(
        self, claim_reviews: list[CanonicalClaimReview]
    ) -> list[CanonicalClaimReview]:
        """
        Enrich a batch of claim reviews with URL text extraction.

        Args:
            claim_reviews: List of claim reviews to enrich

        Returns:
            List[CanonicalClaimReview]: List of enriched claim reviews
        """
        enriched_reviews: list[CanonicalClaimReview] = []
        total = len(claim_reviews)

        for i, claim_review in enumerate(claim_reviews):
            try:
                enriched = self.enrich(claim_review)
                enriched_reviews.append(enriched)

                if i % 10 == 0:  # Log progress every 10 items
                    self.logger.info(f"URL text extraction progress: {i + 1}/{total}")

            except Exception as e:
                self.logger.error(
                    f"Error enriching claim review {claim_review.uri}: {e}"
                )
                enriched_reviews.append(claim_review)

        self.logger.info(f"Completed URL text extraction for {total} claim reviews")
        return enriched_reviews
