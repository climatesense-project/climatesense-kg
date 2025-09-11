"""Canonical data model for the ClimateSense KG Pipeline."""

from dataclasses import dataclass, field
import hashlib
from typing import Any
from urllib.parse import urlparse

from ..utils.text_processing import normalize_text


@dataclass
class CanonicalOrganization:
    """Canonical model for fact-checking organizations."""

    name: str
    website: str | None = None
    language: str | None = None

    @property
    def uri(self) -> str:
        """Generate a unique URI for this organization."""
        identifier = "organization" + str(self.name)
        hash_value = hashlib.sha224(identifier.encode()).hexdigest()
        return f"organization/{hash_value}"


@dataclass
class CanonicalRating:
    """Canonical model for ratings."""

    label: str
    original_label: str | None = None

    @property
    def uri(self) -> str:
        """Generate URI for this rating."""
        label = self.original_label or self.label
        identifier = "rating" + label
        hash_value = hashlib.sha224(identifier.encode()).hexdigest()
        return f"rating/{hash_value}"


@dataclass
class CanonicalClaim:
    """Canonical model for claims."""

    text: str
    appearances: list[str] = field(default_factory=list[str])

    # Enrichment fields
    entities: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    emotion: str | None = None
    sentiment: str | None = None
    political_leaning: str | None = None
    conspiracies: dict[str, list[str]] = field(
        default_factory=lambda: {"mentioned": [], "promoted": []}
    )
    readability_score: float | None = None

    @property
    def normalized_text(self) -> str:
        """Return a normalized version of the claim text."""
        return normalize_text(self.text)

    @property
    def uri(self) -> str:
        """Generate a unique URI for this claim."""
        identifier = "claim" + self.normalized_text
        hash_value = hashlib.sha224(identifier.encode()).hexdigest()
        return f"claim/{hash_value}"


@dataclass
class CanonicalClaimReview:
    """Canonical model for claim reviews."""

    claim: CanonicalClaim
    review_url: str
    organization: CanonicalOrganization | None = None
    date_published: str | None = None
    language: str | None = None

    # Rating
    rating: CanonicalRating | None = None

    # Content analysis
    review_text: str | None = None
    review_url_text: str | None = None
    entities_in_review: list[dict[str, Any]] = field(
        default_factory=list[dict[str, Any]]
    )

    # Metadata
    source_type: str | None = None
    source_name: str | None = None

    @property
    def uri(self) -> str:
        """Generate a unique URI for this claim review."""
        parsed_url = urlparse(self.review_url.lower())
        review_url_for_id_path = parsed_url.path
        if review_url_for_id_path and not review_url_for_id_path.endswith("/"):
            review_url_for_id_path += "/"
        review_url_normalized_for_id = parsed_url.netloc + review_url_for_id_path

        identifier = (
            "claim-review"
            + self.claim.normalized_text
            + (self.rating.label if self.rating else "")
            + review_url_normalized_for_id
            + (self.date_published or "")
        )
        hash_value = hashlib.sha224(identifier.encode()).hexdigest()
        return f"claim-review/{hash_value}"
