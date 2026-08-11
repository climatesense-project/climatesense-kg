"""Persistence-neutral values exchanged by identity resolution and registries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from ..domain import CanonicalOrganization, SourceReviewRecord


@dataclass
class RegisteredDocument:
    """Document identity and matching evidence stored by the registry."""

    id: UUID
    organization_uri: str
    urls: set[str]
    preferred_url: str
    content: str | None
    normalized_text_hash: str | None
    shingles: frozenset[str]
    word_count: int


@dataclass
class RegisteredReview:
    """Canonical review identity and its attached document."""

    id: UUID
    document: RegisteredDocument
    organization_uri: str
    claim_uri: str


@dataclass
class IdentityAssignment:
    """Complete registry assignment returned to the application layer."""

    review: RegisteredReview
    source_record_keys: set[str] = field(default_factory=set)
    source_names: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class IdentityCandidate:
    """Non-deterministic match retained for offline duplicate audits."""

    candidate_review_id: UUID
    similarity: float
    evidence: dict[str, Any]


IdentityBatchRecord = tuple[SourceReviewRecord, CanonicalOrganization]


@dataclass
class IdentityBatchEvidence:
    """Existing identity state relevant to one bounded input batch."""

    assignments_by_source_key: dict[str, IdentityAssignment]
    assignments_by_native_key: dict[tuple[str, str], IdentityAssignment]
    documents: dict[UUID, RegisteredDocument]
    reviews: dict[UUID, RegisteredReview]
    assignments: dict[UUID, IdentityAssignment]
    review_claims: dict[UUID, set[str]]


@dataclass(frozen=True)
class PlannedSourceAssignment:
    """One source observation attached to its planned canonical identity."""

    record: SourceReviewRecord
    assignment: IdentityAssignment


@dataclass(frozen=True)
class PlannedIdentityCandidate:
    """A fuzzy identity candidate associated with its source observation."""

    source_record_key: str
    candidate: IdentityCandidate


@dataclass
class IdentityBatchPlan:
    """Pure identity decisions ready for one atomic repository commit."""

    results: list[IdentityAssignment]
    documents: dict[UUID, RegisteredDocument]
    new_document_ids: set[UUID]
    new_reviews: dict[UUID, RegisteredReview]
    sources: list[PlannedSourceAssignment]
    candidates: list[PlannedIdentityCandidate]
