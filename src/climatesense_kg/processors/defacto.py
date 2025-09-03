"""DE FACTO data processor."""

from collections.abc import Iterator
import json
import re
from typing import Any

from dateutil import parser

from ..config.models import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
)
from .base import BaseProcessor


class DefactoProcessor(BaseProcessor):
    """Processor for DE FACTO data."""

    def process(self, raw_data: bytes) -> Iterator[CanonicalClaimReview]:
        """Process DE FACTO raw data into CanonicalClaimReview objects."""
        try:
            data = json.loads(raw_data.decode("utf-8"))

            for page_data in data:
                if canonical_review := self._process_page(page_data):
                    yield canonical_review

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON data: {e}")
        except Exception as e:
            self.logger.error(f"Error processing DE FACTO data: {e}")

    def _process_page(self, page_data: dict[str, Any]) -> CanonicalClaimReview | None:
        """Process a single page and return a canonical review."""
        try:
            return self._create_canonical_review(page_data)
        except Exception as e:
            self.logger.warning(
                f"Failed to process page {page_data.get('id', 'unknown')}: {e}"
            )
            return None

    def _create_canonical_review(
        self, page_data: dict[str, Any]
    ) -> CanonicalClaimReview | None:
        """Create a CanonicalClaimReview from page data."""
        try:
            claim_text = self._extract_claim_text(page_data)
            if not claim_text:
                return None

            claim = CanonicalClaim(
                text=claim_text,
                appearances=(
                    [page_data.get("absoluteUrl", "")]
                    if page_data.get("absoluteUrl")
                    else []
                ),
            )

            organization = self._extract_organization(page_data)

            date_published = self._parse_date(page_data.get("created", ""))

            language = page_data.get("language")

            review_text = self._clean_xwiki_content(page_data.get("content", ""))

            return CanonicalClaimReview(
                claim=claim,
                organization=organization,
                review_url=page_data.get("absoluteUrl", ""),
                date_published=date_published,
                language=language,
                review_text=review_text if review_text else None,
                source_type="defacto",
                source_name=self.name,
            )

        except Exception as e:
            self.logger.error(f"Error creating canonical review: {e}")
            return None

    def _extract_claim_text(self, page_data: dict[str, Any]) -> str:
        """Extract the main claim text from the page."""
        return str(page_data.get("title") or page_data.get("rawTitle", ""))

    def _clean_xwiki_content(self, content: str) -> str:
        """Clean XWiki syntax from content to get plain text."""
        if not content:
            return ""

        patterns = [
            (r"\[\[([^>]+)>>[^\]]*\]\]", r"\1"),  # Remove links
            (r"\{\{[^}]*\}\}", ""),  # Remove image syntax
            (r"\(%[^)]*%\)", ""),  # Remove XWiki formatting
            (r"\/\/([^\/]+)\/\/", r"\1"),  # Remove italic
            (r"\*\*([^*]+)\*\*", r"\1"),  # Remove bold
            (r"\s+", " "),  # Clean up extra whitespace
        ]

        for pattern, replacement in patterns:
            content = re.sub(pattern, replacement, content)

        return content.strip()

    def _parse_date(self, date_string: str) -> str | None:
        """Parse date string to YYYY-MM-DD format."""
        if not date_string:
            return None

        try:
            dt = parser.parse(date_string)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            self.logger.warning(f"Could not parse date: {date_string}")
            return None

    def _extract_organization_name_from_page_id(self, page_id: str) -> str | None:
        """Extract organization name from page ID.

        Example: 'xwiki:Medias.20-Minutes.Fact-checks.Something.WebHome' -> '20-Minutes'
        """
        try:
            parts = page_id.split(".")
            if len(parts) >= 3 and parts[0] == "xwiki:Medias":
                return parts[1]
            return None
        except Exception:
            return None

    def _extract_organization(
        self, page_data: dict[str, Any]
    ) -> CanonicalOrganization | None:
        """Extract organization data from page information."""
        page_id = page_data.get("id", "")
        org_name = self._extract_organization_name_from_page_id(page_id)

        if not org_name:
            return None

        org_title = page_data.get("org_title") or page_data.get("org_rawTitle")
        name = org_title if org_title else org_name

        return CanonicalOrganization(name=name)
