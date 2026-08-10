"""Persistence-neutral values exchanged by identity resolution and registries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID


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
    rating_fingerprint: str | None


@dataclass
class IdentityAssignment:
    """Complete registry assignment returned to the application layer."""

    review: RegisteredReview
    source_record_keys: set[str] = field(default_factory=set)
    source_names: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class IdentityCandidate:
    """Non-deterministic match that requires adjudication."""

    candidate_review_id: UUID
    similarity: float
    evidence: dict[str, Any]
