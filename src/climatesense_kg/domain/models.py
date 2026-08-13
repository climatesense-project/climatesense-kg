"""Domain entities for source observations, identity, enrichment, and RDF output."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
import hashlib
import json
from typing import Any
from uuid import UUID

from ..utils.ratings import VALID_NORMALIZED_RATINGS
from ..utils.text_processing import (
    normalize_analysis_text,
    normalize_organization_url,
    validate_claim_text,
)


def _digest(namespace: str, *values: str) -> str:
    """Return an unambiguous SHA-256 digest for stable value keys."""

    payload = json.dumps(
        [namespace, *values], ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(frozen=True)
class SourceReference:
    """Stable provenance for one upstream claim/rating observation."""

    source_name: str
    source_type: str
    record_key: str
    native_id: str | None = None

    @classmethod
    def from_observation(
        cls,
        *,
        source_name: str,
        source_type: str,
        observed_url: str,
        claim_text: str,
        native_id: str | None = None,
        discriminator: str = "",
    ) -> SourceReference:
        """Build a deterministic provenance key for one source observation."""

        source_name = source_name.strip()
        source_type = source_type.strip()
        if not source_name or not source_type:
            raise ValueError("Source name and type are required")
        anchor = native_id.strip() if native_id else observed_url.strip()
        if not anchor:
            raise ValueError("A native source identifier or observed URL is required")
        if native_id:
            record_key = _digest(
                "source-review-record-native", source_name, source_type, anchor
            )
        else:
            record_key = _digest(
                "source-review-record",
                source_name,
                source_type,
                anchor,
                claim_text,
                discriminator,
            )
        return cls(
            source_name=source_name,
            source_type=source_type,
            record_key=record_key,
            native_id=native_id.strip() if native_id else None,
        )


@dataclass(frozen=True)
class OrganizationReference:
    """Organization metadata as observed in a source payload."""

    name: str
    website: str
    language: str | None = None

    def __post_init__(self) -> None:
        normalized_website = normalize_organization_url(self.website)
        if not normalized_website:
            raise ValueError(
                f"Organization {self.name!r} requires a valid HTTP(S) website URL"
            )
        object.__setattr__(self, "website", normalized_website)


@dataclass(frozen=True)
class CanonicalOrganization:
    """Resolved organization from the curated catalog."""

    uri: str
    name: str
    website: str
    language: str | None = None

    def __post_init__(self) -> None:
        if not self.uri.startswith(("http://", "https://")):
            raise ValueError("Canonical organization URI must be absolute")
        normalized_website = normalize_organization_url(self.website)
        if not normalized_website:
            raise ValueError(
                f"Organization {self.name!r} requires a valid HTTP(S) website URL"
            )
        object.__setattr__(self, "website", normalized_website)


@dataclass(frozen=True)
class CanonicalPerson:
    """A person credited by a source review."""

    name: str
    website: str | None = None
    role: str | None = None
    source_uri: str | None = None

    @property
    def uri(self) -> str:
        return f"person/{_digest('person', self.name, self.website or '')}"


@dataclass(frozen=True)
class CanonicalRating:
    """Source rating value with an optional normalized ClimateSense concept."""

    label: str
    original_label: str | None = None
    explanation: str | None = None
    rating_value: float | None = None
    best_rating: float | None = None
    worst_rating: float | None = None

    @property
    def normalized_label(self) -> str | None:
        return self.label if self.label in VALID_NORMALIZED_RATINGS else None

    @property
    def fingerprint(self) -> str:
        return _digest(
            "rating",
            self.label,
            self.original_label or "",
            str(self.rating_value) if self.rating_value is not None else "",
            str(self.best_rating) if self.best_rating is not None else "",
            str(self.worst_rating) if self.worst_rating is not None else "",
        )

    def uri_for(self, organization_uri: str) -> str:
        """Return an organization-scoped source-rating URI."""

        return f"rating/{_digest('organization-rating', organization_uri, self.fingerprint)}"

    @property
    def normalized_uri(self) -> str | None:
        if self.normalized_label is None:
            return None
        return f"rating/{self.normalized_label}"


@dataclass(frozen=True)
class EntityPropertyValue:
    """One typed external property value attached to an entity."""

    value: str
    value_type: str
    datatype: str | None = None
    language: str | None = None


@dataclass
class EntityMention:
    """Typed entity mention and provider-owned descriptive properties."""

    uri: str
    source: str
    surface_form: str = ""
    types: list[str] = field(default_factory=list)
    confidence: float | None = None
    support: int | None = None
    offset: int | None = None
    properties: dict[str, list[EntityPropertyValue]] = field(default_factory=dict)


@dataclass
class ClaimAnalysis:
    """Enrichment owned by claim-analysis stages."""

    entities: list[EntityMention] = field(default_factory=list)
    emotion: str | None = None
    sentiment: str | None = None
    political_leaning: str | None = None
    tropes: list[str] = field(default_factory=list)
    persuasion_techniques: list[str] = field(default_factory=list)
    conspiracies: dict[str, list[str]] = field(
        default_factory=lambda: {"mentioned": [], "promoted": []}
    )
    climate_related: bool | None = None
    readability_score: float | None = None


@dataclass
class ReviewAnalysis:
    """Enrichment owned by review-document analysis stages."""

    entities: list[EntityMention] = field(default_factory=list)


@dataclass
class CanonicalClaim:
    """Exact source claim and its canonical text identity."""

    text: str
    headline: str | None = None
    appearances: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    analysis: ClaimAnalysis = field(default_factory=ClaimAnalysis)

    def __post_init__(self) -> None:
        self.text = validate_claim_text(self.text)

    @property
    def analysis_text(self) -> str:
        return normalize_analysis_text(self.text)

    @property
    def uri(self) -> str:
        return f"claim/{_digest('claim', self.text)}"


@dataclass
class ReviewDocument:
    """One source observation of a fact-checking document before resolution."""

    observed_url: str
    final_url: str | None = None
    canonical_url: str | None = None
    extracted_text: str | None = None
    source_text: str | None = None
    description: str | None = None
    abstract: str | None = None
    normalized_text_hash: str | None = None
    word_count: int = 0

    @property
    def preferred_url(self) -> str:
        return self.canonical_url or self.final_url or self.observed_url

    @property
    def content(self) -> str | None:
        return self.extracted_text or self.source_text


@dataclass
class SourceReviewRecord:
    """A source claim/rating observation awaiting identity resolution."""

    source: SourceReference
    claim: CanonicalClaim
    organization: OrganizationReference
    document: ReviewDocument
    date_published: str | None = None
    language: str | None = None
    rating: CanonicalRating | None = None
    keywords: list[str] = field(default_factory=list)
    authors: list[CanonicalPerson] = field(default_factory=list)
    license_url: str | None = None

    @property
    def payload_hash(self) -> str:
        """Fingerprint the source fields consumed by processing stages."""

        return _digest(
            "source-review-payload",
            self.source.record_key,
            self.claim.text,
            self.organization.website,
            self.document.observed_url,
            self.document.source_text or "",
            self.date_published or "",
            self.rating.fingerprint if self.rating else "",
        )


@dataclass
class CanonicalReviewDocument:
    """Identity-resolved document shared by one or more claim reviews."""

    id: UUID
    urls: set[str]
    preferred_url: str
    content: str | None = None
    normalized_text_hash: str | None = None
    word_count: int = 0


@dataclass
class CanonicalClaimReview:
    """Identity-resolved claim-review assertion ready for enrichment and RDF."""

    id: UUID
    claim: CanonicalClaim
    organization: CanonicalOrganization
    document: CanonicalReviewDocument
    source_record_keys: set[str]
    source_names: set[str]
    date_published: str | None = None
    language: str | None = None
    rating: CanonicalRating | None = None
    keywords: list[str] = field(default_factory=list)
    authors: list[CanonicalPerson] = field(default_factory=list)
    license_url: str | None = None
    description: str | None = None
    abstract: str | None = None
    analysis: ReviewAnalysis = field(default_factory=ReviewAnalysis)
    observations: dict[str, SourceReviewRecord] = field(default_factory=dict)

    @property
    def uri(self) -> str:
        return f"claim-review/{self.id}"

    @property
    def key(self) -> str:
        """Canonical persistence key used by semantic enrichment stages."""

        return str(self.id)

    @property
    def review_url(self) -> str:
        return self.document.preferred_url

    @property
    def review_text(self) -> str | None:
        return self.document.content

    def source_graphs(self) -> list[str]:
        if self.observations:
            return sorted(
                {record.source.source_name for record in self.observations.values()}
            )
        return sorted(self.source_names)

    def for_source(self, source_name: str) -> CanonicalClaimReview:
        """Project source-owned metadata for this canonical identity."""

        observations = sorted(
            (
                record
                for record in self.observations.values()
                if record.source.source_name == source_name
            ),
            key=lambda record: record.source.record_key,
        )
        if not observations:
            return self

        selected = max(
            observations,
            key=lambda record: (
                sum(
                    value is not None
                    for value in (
                        record.date_published,
                        record.language,
                        record.rating,
                        record.license_url,
                        record.document.description,
                        record.document.abstract,
                    )
                ),
                len(record.keywords),
                len(record.authors),
                record.source.record_key,
            ),
        )
        claim = replace(selected.claim, analysis=self.claim.analysis)
        keywords = sorted(
            {keyword for record in observations for keyword in record.keywords}
        )
        authors: list[CanonicalPerson] = []
        for record in observations:
            for author in record.authors:
                if author not in authors:
                    authors.append(author)

        return replace(
            self,
            claim=claim,
            date_published=selected.date_published,
            language=selected.language,
            rating=selected.rating,
            keywords=keywords,
            authors=authors,
            license_url=selected.license_url,
            description=max(
                (
                    record.document.description
                    for record in observations
                    if record.document.description
                ),
                key=len,
                default=None,
            ),
            abstract=max(
                (
                    record.document.abstract
                    for record in observations
                    if record.document.abstract
                ),
                key=len,
                default=None,
            ),
            observations={record.source.record_key: record for record in observations},
        )

    def to_debug_dict(self) -> dict[str, Any]:
        """Return concise identity and provenance diagnostics."""

        return {
            "id": str(self.id),
            "claim_uri": self.claim.uri,
            "organization_uri": self.organization.uri,
            "urls": sorted(self.document.urls),
            "sources": sorted(self.source_names),
        }
