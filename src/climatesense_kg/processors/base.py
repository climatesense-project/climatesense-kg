"""Base processor class for data processing."""

from abc import ABC, abstractmethod
from collections.abc import Iterator
import logging
from typing import Any

from ..config.models import CanonicalClaimReview


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
    def process(self, raw_data: bytes) -> Iterator[CanonicalClaimReview]:
        """Process raw data into canonical format.

        Args:
            raw_data: Raw data from provider

        Yields:
            CanonicalClaimReview objects
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
