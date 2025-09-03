"""DBKF data processor."""

from collections.abc import Iterator
import json
from typing import Any
from urllib.parse import urlparse

from dateutil import parser

from ..config.models import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
)
from .base import BaseProcessor


class DbkfProcessor(BaseProcessor):
    """Processor for DBKF GraphQL data."""

    def process(self, raw_data: bytes) -> Iterator[CanonicalClaimReview]:
        """Process DBKF raw data into CanonicalClaimReview objects."""
        try:
            data = json.loads(raw_data.decode("utf-8"))

            item_count = 0
            for item in data:
                is_valid, errors = self._validate_item(item)
                if not is_valid:
                    self.logger.warning(
                        "Skipping invalid DBKF item: %s", "; ".join(errors)
                    )
                    continue

                try:
                    canonical_review = self._normalize_item(item)
                    item_count += 1
                    yield canonical_review
                except Exception as e:
                    self.logger.warning(f"Failed to normalize DBKF item: {e}")
                    continue

            self.logger.info(f"Processed {item_count} DBKF items")

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON data: {e}")
        except Exception as e:
            self.logger.error(f"Error processing DBKF data: {e}")

    def _normalize_item(self, item: dict[str, Any]) -> CanonicalClaimReview:
        """Convert DBKF item to CanonicalClaimReview."""
        item_reviewed = item.get("itemReviewed", {})
        claim_text = item_reviewed.get("text", "")

        claim = CanonicalClaim(
            text=claim_text,
            appearances=(
                [item.get("externalUrl", "")] if item.get("externalUrl") else []
            ),
        )

        publisher = item.get("publisher", {})
        organization = CanonicalOrganization(
            name=publisher.get("name", ""),
            website=self._extract_website_from_url(item.get("externalUrl", "")),
            language=self._get_primary_language(item.get("language", [])),
        )

        headline = item.get("headline", "")
        review_body = item.get("reviewBody", "")
        review_text = f"{headline}\n{review_body}".strip()

        return CanonicalClaimReview(
            claim=claim,
            organization=organization,
            review_url=item.get("externalUrl", ""),
            date_published=self._convert_date(item.get("dateCreated", "")),
            language=self._get_primary_language(item.get("language", [])),
            review_text=review_text if review_text else None,
            source_type="dbkf",
            source_name=self.name,
        )

    def _extract_website_from_url(self, url: str) -> str | None:
        """Extract website root URL from article URL."""
        if not url:
            return None

        try:
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            return None

    def _get_primary_language(self, languages: list[str] | str) -> str | None:
        """Get primary language from language list or string."""
        if isinstance(languages, str):
            return languages
        elif languages:
            return languages[0]
        return None

    def _convert_date(self, date_str: str) -> str | None:
        """Convert ISO date string to YYYY-MM-DD format."""
        if not date_str:
            return None

        try:
            dt = parser.parse(date_str)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            self.logger.warning(f"Could not parse date: {date_str}")
            return None

    def _validate_item(self, item: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate a DBKF item and return validation errors."""
        is_valid, errors = super()._validate_item(item)
        if not is_valid:
            return False, errors

        if not item.get("id"):
            errors.append("missing id")

        if not item.get("externalUrl"):
            errors.append("missing externalUrl")

        if not item.get("headline") and not item.get("reviewBody"):
            errors.append("missing headline and reviewBody")

        item_reviewed = item.get("itemReviewed", {})
        if not item_reviewed.get("text"):
            errors.append("itemReviewed missing text")

        return not errors, errors
