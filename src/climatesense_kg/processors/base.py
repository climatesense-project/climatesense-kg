"""Base processor class for data processing."""

from abc import ABC, abstractmethod
from collections.abc import Iterator
import logging
from typing import Any

from ..domain import (
    CanonicalClaim,
    CanonicalPerson,
    CanonicalRating,
    OrganizationReference,
    ReviewDocument,
    SourceReference,
    SourceReviewRecord,
)


class BaseProcessor(ABC):
    """Abstract base class for data processors."""

    def __init__(self, name: str):
        """Initialize processor.

        Args:
            name: Name of the data source
        """
        self.name = name
        self.logger = logging.getLogger(f"processor.{name}")

    @abstractmethod
    def process(self, raw_data: bytes) -> Iterator[SourceReviewRecord]:
        """Process raw data into canonical format.

        Args:
            raw_data: Raw data from provider

        Yields:
            SourceReviewRecord objects
        """
        pass

    def _validate_item(self, item: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate an individual raw item and return any validation errors.

        Args:
            item: Item to validate

        Returns:
            A tuple (is_valid, errors) where `errors` is a list of human
            readable validation error messages. If there are no errors the
            list will be empty.
        """
        if item:
            return True, []
        return False, ["empty item"]

    def _source_record(
        self,
        *,
        source_type: str,
        claim: CanonicalClaim,
        organization: OrganizationReference,
        review_url: str,
        native_id: str | None = None,
        discriminator: str = "",
        date_published: str | None = None,
        language: str | None = None,
        rating: CanonicalRating | None = None,
        review_text: str | None = None,
        description: str | None = None,
        abstract: str | None = None,
        keywords: list[str] | None = None,
        authors: list[CanonicalPerson] | None = None,
        license_url: str | None = None,
    ) -> SourceReviewRecord:
        """Build a source observation without assigning canonical identity."""

        return SourceReviewRecord(
            source=SourceReference.from_observation(
                source_name=self.name,
                source_type=source_type,
                observed_url=review_url,
                claim_text=claim.text,
                native_id=native_id,
                discriminator=discriminator,
            ),
            claim=claim,
            organization=organization,
            document=ReviewDocument(
                observed_url=review_url,
                source_text=review_text,
                description=description,
                abstract=abstract,
            ),
            date_published=date_published,
            language=language,
            rating=rating,
            keywords=keywords or [],
            authors=authors or [],
            license_url=license_url,
        )
