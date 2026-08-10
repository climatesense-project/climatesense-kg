"""Identity registry contracts and an in-memory reference implementation."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from threading import RLock
from typing import Protocol
from uuid import UUID

from ..domain import CanonicalOrganization, SourceReviewRecord
from .models import (
    IdentityAssignment,
    IdentityCandidate,
    RegisteredDocument,
    RegisteredReview,
)


class IdentityTransaction(Protocol):
    """Operations available inside one atomic identity-resolution transaction."""

    def lock_scope(self, organization_uri: str) -> None: ...

    def assignment_for_source(self, record_key: str) -> IdentityAssignment | None: ...

    def assignment_for_native_id(
        self, source_name: str, native_id: str
    ) -> IdentityAssignment | None: ...

    def documents_by_evidence(
        self,
        organization_uri: str,
        urls: set[str],
        normalized_text_hash: str | None,
    ) -> list[RegisteredDocument]: ...

    def review_for_document_claim(
        self, document_id: UUID, claim_uri: str
    ) -> RegisteredReview | None: ...

    def reviews_for_claim(
        self, organization_uri: str, claim_uri: str
    ) -> list[RegisteredReview]: ...

    def create_document(
        self,
        document_id: UUID,
        organization: CanonicalOrganization,
        record: SourceReviewRecord,
    ) -> RegisteredDocument: ...

    def create_review(
        self,
        review_id: UUID,
        document: RegisteredDocument,
        organization: CanonicalOrganization,
        record: SourceReviewRecord,
    ) -> RegisteredReview: ...

    def attach_source(
        self,
        record: SourceReviewRecord,
        document: RegisteredDocument,
        review: RegisteredReview,
    ) -> None: ...

    def record_candidate(
        self, source_record_key: str, candidate: IdentityCandidate
    ) -> None: ...

    def assignment(self, review_id: UUID) -> IdentityAssignment: ...


class IdentityRegistry(Protocol):
    """Authoritative persistent boundary for identity resolution."""

    def transaction(self) -> AbstractContextManager[IdentityTransaction]: ...


class InMemoryIdentityRegistry:
    """Thread-safe registry used by domain tests and local deterministic workflows."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._documents: dict[UUID, RegisteredDocument] = {}
        self._reviews: dict[UUID, RegisteredReview] = {}
        self._source_reviews: dict[str, UUID] = {}
        self._source_names: dict[str, str] = {}
        self._native_reviews: dict[tuple[str, str], UUID] = {}
        self._candidates: dict[tuple[str, UUID], IdentityCandidate] = {}

    @contextmanager
    def transaction(self) -> Iterator[IdentityTransaction]:
        with self._lock:
            yield self

    def lock_scope(self, organization_uri: str) -> None:
        del organization_uri

    def assignment_for_source(self, record_key: str) -> IdentityAssignment | None:
        review_id = self._source_reviews.get(record_key)
        return self.assignment(review_id) if review_id else None

    def assignment_for_native_id(
        self, source_name: str, native_id: str
    ) -> IdentityAssignment | None:
        review_id = self._native_reviews.get((source_name, native_id))
        return self.assignment(review_id) if review_id else None

    def documents_by_evidence(
        self,
        organization_uri: str,
        urls: set[str],
        normalized_text_hash: str | None,
    ) -> list[RegisteredDocument]:
        matches = [
            document
            for document in self._documents.values()
            if document.organization_uri == organization_uri
            and (
                bool(document.urls & urls)
                or (
                    normalized_text_hash is not None
                    and document.normalized_text_hash == normalized_text_hash
                )
            )
        ]
        return sorted(matches, key=lambda document: str(document.id))

    def review_for_document_claim(
        self, document_id: UUID, claim_uri: str
    ) -> RegisteredReview | None:
        return next(
            (
                review
                for review in self._reviews.values()
                if review.document.id == document_id and review.claim_uri == claim_uri
            ),
            None,
        )

    def reviews_for_claim(
        self, organization_uri: str, claim_uri: str
    ) -> list[RegisteredReview]:
        return sorted(
            (
                review
                for review in self._reviews.values()
                if review.organization_uri == organization_uri
                and review.claim_uri == claim_uri
            ),
            key=lambda review: str(review.id),
        )

    def create_document(
        self,
        document_id: UUID,
        organization: CanonicalOrganization,
        record: SourceReviewRecord,
    ) -> RegisteredDocument:
        document = RegisteredDocument(
            id=document_id,
            organization_uri=organization.uri,
            urls={
                url
                for url in (
                    record.document.observed_url,
                    record.document.final_url,
                    record.document.canonical_url,
                )
                if url
            },
            preferred_url=record.document.preferred_url,
            content=record.document.content,
            normalized_text_hash=record.document.normalized_text_hash,
            shingles=frozenset(record.document.shingle_signature),
            word_count=record.document.word_count,
        )
        self._documents[document_id] = document
        return document

    def create_review(
        self,
        review_id: UUID,
        document: RegisteredDocument,
        organization: CanonicalOrganization,
        record: SourceReviewRecord,
    ) -> RegisteredReview:
        review = RegisteredReview(
            id=review_id,
            document=document,
            organization_uri=organization.uri,
            claim_uri=record.claim.uri,
            rating_fingerprint=(record.rating.fingerprint if record.rating else None),
        )
        self._reviews[review_id] = review
        return review

    def attach_source(
        self,
        record: SourceReviewRecord,
        document: RegisteredDocument,
        review: RegisteredReview,
    ) -> None:
        self._source_reviews[record.source.record_key] = review.id
        self._source_names[record.source.record_key] = record.source.source_name
        if record.source.native_id:
            self._native_reviews[
                (record.source.source_name, record.source.native_id)
            ] = review.id
        document.urls.update(
            url
            for url in (
                record.document.observed_url,
                record.document.final_url,
                record.document.canonical_url,
            )
            if url
        )
        document.preferred_url = record.document.preferred_url
        if record.document.content:
            document.content = record.document.content
            document.normalized_text_hash = record.document.normalized_text_hash
            document.shingles = frozenset(record.document.shingle_signature)
            document.word_count = record.document.word_count

    def record_candidate(
        self, source_record_key: str, candidate: IdentityCandidate
    ) -> None:
        self._candidates[(source_record_key, candidate.candidate_review_id)] = candidate

    def assignment(self, review_id: UUID) -> IdentityAssignment:
        review = self._reviews[review_id]
        record_keys = {
            record_key
            for record_key, mapped_review_id in self._source_reviews.items()
            if mapped_review_id == review_id
        }
        return IdentityAssignment(
            review=review,
            source_record_keys=record_keys,
            source_names={self._source_names[key] for key in record_keys},
        )

    @property
    def candidates(self) -> list[tuple[str, IdentityCandidate]]:
        """Return recorded candidates for assertions in acceptance tests."""

        return [
            (source_record_key, candidate)
            for (source_record_key, _review_id), candidate in self._candidates.items()
        ]
