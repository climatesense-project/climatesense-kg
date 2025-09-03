"""EuroClimateCheck data processor."""

from collections.abc import Iterator
import json
from typing import Any
from urllib.parse import urlparse

from dateutil import parser

from ..config.models import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalRating,
)
from .base import BaseProcessor


class EuroClimateCheckProcessor(BaseProcessor):
    """Processor for EuroClimateCheck data."""

    def process(self, raw_data: bytes) -> Iterator[CanonicalClaimReview]:
        """Process EuroClimateCheck raw data into CanonicalClaimReview objects."""
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
            self.logger.error(f"Error processing EuroClimateCheck data: {e}")

    def _normalize_item(self, item: dict[str, Any]) -> CanonicalClaimReview:
        """Convert EuroClimateCheck item to CanonicalClaimReview."""
        claim_text = item.get("title", "") or item.get("description", "")

        claim = CanonicalClaim(
            text=claim_text,
            appearances=[item.get("url", "")] if item.get("url") else [],
        )

        organization = CanonicalOrganization(
            name=item.get("source", ""),
            website=self._extract_website_from_url(item.get("url", "")),
            language=item.get("language", ""),
        )

        category = item.get("category", "")
        rating = CanonicalRating(
            label=category,
            original_label=category,
        )

        return CanonicalClaimReview(
            claim=claim,
            organization=organization,
            review_url=item.get("url", ""),
            date_published=self._convert_timestamp(item.get("date", "")),
            language=item.get("language", ""),
            rating=rating,
            source_type="euroclimatecheck",
            source_name=self.name,
        )

    def _extract_website_from_url(self, url: str) -> str:
        """Extract website root URL from article URL."""
        if not url:
            return ""

        try:
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            return ""

    def _convert_timestamp(self, timestamp: str) -> str:
        """Convert timestamp to YYYY-MM-DD format, or empty string if invalid."""
        if not timestamp:
            return ""
        try:
            dt = parser.parse(timestamp)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            self.logger.warning(f"Could not parse date: {timestamp}")
            return ""

    def _validate_item(self, item: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate a EuroClimateCheck item and return validation errors."""
        is_valid, errors = super()._validate_item(item)

        if not is_valid:
            return False, errors

        if not item.get("url"):
            errors.append("missing url")

        if not item.get("content") and not item.get("description"):
            errors.append("missing content and description")

        return not errors, errors
