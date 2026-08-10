"""DBKF data processor."""

from collections.abc import Iterator
import json
from typing import Any
from urllib.parse import urlparse

from dateutil import parser

from ..domain import CanonicalClaim, OrganizationReference, SourceReviewRecord
from .base import BaseProcessor


class DbkfProcessor(BaseProcessor):
    """Processor for DBKF data."""

    def process(self, raw_data: bytes) -> Iterator[SourceReviewRecord]:
        """Process DBKF raw data into source observations."""
        try:
            data = json.loads(raw_data.decode("utf-8"))
            if not isinstance(data, list):
                raise ValueError("DBKF payload must be a list")

            item_count = 0
            for item in data:
                try:
                    is_valid, errors = self._validate_item(item)
                    if not is_valid:
                        self.logger.warning(
                            "Skipping invalid DBKF item: %s", "; ".join(errors)
                        )
                        continue

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

    def _normalize_item(self, item: dict[str, Any]) -> SourceReviewRecord:
        """Convert a DBKF item to a source observation."""
        item_reviewed = item.get("itemReviewed", {})
        claim_text = item_reviewed.get("text", "")

        claim = CanonicalClaim(
            text=claim_text,
            appearances=(
                [item.get("externalUrl", "")] if item.get("externalUrl") else []
            ),
        )

        publisher = item.get("publisher", {})
        organization_url = self._extract_website_from_url(item.get("externalUrl", ""))
        if not organization_url:
            raise ValueError("DBKF item requires an external organization URL")
        organization = OrganizationReference(
            name=publisher.get("name", ""),
            website=organization_url,
            language=self._get_primary_language(item.get("language", [])),
        )

        headline = item.get("headline", "")
        review_body = item.get("reviewBody", "")
        review_text = f"{headline}\n{review_body}".strip()

        return self._source_record(
            source_type="dbkf",
            claim=claim,
            organization=organization,
            review_url=item.get("externalUrl", ""),
            native_id=str(item["id"]),
            date_published=self._convert_date(item.get("dateCreated", "")),
            language=self._get_primary_language(item.get("language", [])),
            review_text=review_text if review_text else None,
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

    def _validate_item(self, item: Any) -> tuple[bool, list[str]]:
        """Validate a DBKF item and return validation errors."""
        if not isinstance(item, dict) or not item:
            return False, ["item must be a non-empty mapping"]

        errors: list[str] = []

        if not item.get("id"):
            errors.append("missing id")

        if not isinstance(item.get("externalUrl"), str) or not item["externalUrl"]:
            errors.append("missing externalUrl")

        if not item.get("headline") and not item.get("reviewBody"):
            errors.append("missing headline and reviewBody")

        item_reviewed = item.get("itemReviewed", {})
        if not isinstance(item_reviewed, dict):
            errors.append("itemReviewed must be a mapping")
        elif not isinstance(item_reviewed.get("text"), str) or not item_reviewed.get(
            "text"
        ):
            errors.append("itemReviewed missing text")

        publisher = item.get("publisher", {})
        if not isinstance(publisher, dict):
            errors.append("publisher must be a mapping")

        languages = item.get("language", [])
        if not isinstance(languages, str | list) or (
            isinstance(languages, list)
            and not all(isinstance(language, str) for language in languages)
        ):
            errors.append("language must be a string or list of strings")

        return not errors, errors
