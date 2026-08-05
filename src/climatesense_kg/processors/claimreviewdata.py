"""ClaimReviewData data processor."""

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


class ClaimReviewDataProcessor(BaseProcessor):
    """Processor for ClaimReviewData JSON data."""

    def process(self, raw_data: bytes) -> Iterator[CanonicalClaimReview]:
        """Process ClaimReviewData raw data into CanonicalClaimReview objects."""
        try:
            data = json.loads(raw_data.decode("utf-8"))
            if not isinstance(data, list):
                raise ValueError("ClaimReviewData payload must be a list")

            for item in data:
                try:
                    is_valid, errors = self._validate_item(item)
                    if not is_valid:
                        review_url = (
                            item.get("review_url", "unknown")
                            if isinstance(item, dict)
                            else "unknown"
                        )
                        self.logger.warning(
                            "Skipping invalid item %s: %s",
                            review_url,
                            "; ".join(errors),
                        )
                        continue

                    yield from self._normalize_item(item)
                except Exception as e:
                    review_url = (
                        item.get("review_url", "unknown")
                        if isinstance(item, dict)
                        else "unknown"
                    )
                    self.logger.warning(
                        "Failed to normalize item %s: %s", review_url, e
                    )
                    continue

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON data: {e}")
        except Exception as e:
            self.logger.error(f"Error processing ClaimReviewData data: {e}")

    def _normalize_item(self, item: dict[str, Any]) -> list[CanonicalClaimReview]:
        """Convert every unambiguous claim/rating pair in one source item."""
        claim_texts: list[str] = item["claim_text"]
        reviews: list[dict[str, Any]] = item["reviews"]
        if len(claim_texts) == len(reviews):
            claim_review_pairs = list(zip(claim_texts, reviews, strict=True))
        elif len(claim_texts) == 1:
            claim_review_pairs = [(claim_texts[0], review) for review in reviews]
        elif len(reviews) == 1:
            claim_review_pairs = [
                (claim_text, reviews[0]) for claim_text in claim_texts
            ]
        else:
            raise ValueError(
                "Ambiguous multi-claim record: claim_text and reviews must have "
                "equal lengths unless one side contains a single value"
            )

        fact_checker = item.get("fact_checker", {})
        organization = CanonicalOrganization(
            name=fact_checker.get("name", ""),
            website=fact_checker.get("website", ""),
            language=fact_checker.get("language", ""),
        )

        canonical_reviews: list[CanonicalClaimReview] = []
        for claim_text, source_review in claim_review_pairs:
            claim = CanonicalClaim(
                text=claim_text, appearances=item.get("appearances", [])
            )
            rating = CanonicalRating(
                label=source_review.get("label", ""),
                original_label=source_review.get("original_label", ""),
            )

            date_published = item.get("date_published") or source_review.get(
                "date_published"
            )
            canonical_reviews.append(
                CanonicalClaimReview(
                    claim=claim,
                    organization=organization,
                    review_url=item.get("review_url", ""),
                    date_published=str(date_published) if date_published else None,
                    language=item.get("language") or fact_checker.get("language"),
                    rating=rating,
                    source_type="claimreviewdata",
                    source_name=self.name,
                )
            )

        return canonical_reviews

    def _validate_item(self, item: Any) -> tuple[bool, list[str]]:
        """Validate a ClaimReviewData item and return validation errors.

        Returns:
            (is_valid, errors)
        """
        if not isinstance(item, dict) or not item:
            return False, ["item must be a non-empty mapping"]

        errors: list[str] = []

        claim_text = item.get("claim_text")
        if (
            not isinstance(claim_text, list)
            or not claim_text
            or not all(isinstance(text, str) and text for text in claim_text)
        ):
            errors.append("claim_text must be a non-empty list of strings")

        if not isinstance(item.get("review_url"), str) or not item["review_url"]:
            errors.append("missing review_url")

        fact_checker = item.get("fact_checker", {})
        if not isinstance(fact_checker, dict):
            errors.append("fact_checker must be a mapping")
        else:
            if (
                not isinstance(fact_checker.get("name"), str)
                or not fact_checker["name"]
            ):
                errors.append("fact_checker is missing name")
            if (
                not isinstance(fact_checker.get("website"), str)
                or not fact_checker["website"]
            ):
                errors.append("fact_checker is missing website")

        appearances = item.get("appearances", [])
        if not isinstance(appearances, list):
            errors.append("appearances must be a list")

        reviews = item.get("reviews")
        if not isinstance(reviews, list) or not reviews:
            errors.append("reviews must be a non-empty list")
        else:
            for review in reviews:
                if not isinstance(review, dict):
                    errors.append("review must be a mapping")
                elif not isinstance(
                    review.get("original_label"), str
                ) or not review.get("original_label"):
                    errors.append("review is missing original_label")

        return not errors, errors
