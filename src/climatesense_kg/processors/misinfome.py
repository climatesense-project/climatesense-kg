"""MisInfoMe data processor."""

from collections.abc import Iterator
import json
from typing import Any

from ..config.models import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalRating,
)
from .base import BaseProcessor


class MisinfoMeProcessor(BaseProcessor):
    """Processor for MisInfoMe JSON data."""

    def process(self, raw_data: bytes) -> Iterator[CanonicalClaimReview]:
        """Process MisInfoMe raw data into CanonicalClaimReview objects."""
        try:
            data = json.loads(raw_data.decode("utf-8"))

            for item in data:
                is_valid, errors = self._validate_item(item)
                if not is_valid:
                    self.logger.warning("Skipping invalid item: %s", "; ".join(errors))
                    continue

                try:
                    canonical_review = self._normalize_item(item)
                    yield canonical_review
                except Exception as e:
                    self.logger.warning(f"Failed to normalize item: {e}")
                    continue

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON data: {e}")
        except Exception as e:
            self.logger.error(f"Error processing MisInfoMe data: {e}")

    def _normalize_item(self, item: dict[str, Any]) -> CanonicalClaimReview:
        """Convert MisInfoMe item to CanonicalClaimReview."""
        claim_text_raw = item.get("claim_text", [])
        claim_text = claim_text_raw[0] if claim_text_raw else ""

        claim = CanonicalClaim(text=claim_text, appearances=item.get("appearances", []))

        fact_checker = item.get("fact_checker", {})
        organization = CanonicalOrganization(
            name=fact_checker.get("name", ""),
            website=fact_checker.get("website", ""),
            language=fact_checker.get("language", ""),
        )

        reviews = item.get("reviews", [])
        rating = None
        if reviews and isinstance(reviews[0], dict):
            first_review = reviews[0]
            rating = CanonicalRating(
                label=first_review.get("label", ""),
                original_label=first_review.get("original_label", ""),
            )

        date_published = item.get("date_published")
        if not date_published and reviews:
            date_published = reviews[0].get("date_published")

        return CanonicalClaimReview(
            claim=claim,
            organization=organization,
            review_url=item.get("review_url", ""),
            date_published=str(date_published) if date_published else None,
            language=item.get("language") or fact_checker.get("language"),
            rating=rating,
            source_type="misinfome",
            source_name=self.name,
        )

    def _validate_item(self, item: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate a MisInfoMe item and return validation errors.

        Returns:
            (is_valid, errors)
        """
        is_valid, errors = super()._validate_item(item)
        if not is_valid:
            return False, errors

        if not item.get("claim_text"):
            errors.append("missing claim_text")

        if not item.get("review_url"):
            errors.append("missing review_url")

        reviews: list[dict[str, Any]] = item.get("reviews", [])
        if not reviews:
            errors.append("missing reviews")
        else:
            first_review = reviews[0]
            if not first_review.get("original_label"):
                errors.append("review is missing original_label")

        return not errors, errors
