"""Batch-oriented identity repository contracts and in-memory adapter."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from copy import deepcopy
from threading import RLock
from typing import Protocol
from uuid import UUID

from ..domain import SourceReviewRecord
from .models import (
    IdentityAssignment,
    IdentityBatchEvidence,
    IdentityBatchPlan,
    IdentityBatchRecord,
    IdentityCandidate,
    RegisteredDocument,
    RegisteredReview,
)


class IdentityRepositoryBatch(Protocol):
    """Set-based persistence operations inside one atomic identity batch."""

    def load_evidence(
        self, records: list[IdentityBatchRecord]
    ) -> IdentityBatchEvidence: ...

    def commit(self, plan: IdentityBatchPlan) -> list[IdentityAssignment]: ...


class IdentityRegistry(Protocol):
    """Authoritative persistent boundary for identity resolution."""

    def batch(
        self, organization_uris: set[str]
    ) -> AbstractContextManager[IdentityRepositoryBatch]: ...


class InMemoryIdentityRegistry:
    """Thread-safe batch repository used by identity acceptance tests."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._documents: dict[UUID, RegisteredDocument] = {}
        self._reviews: dict[UUID, RegisteredReview] = {}
        self._source_reviews: dict[str, UUID] = {}
        self._native_reviews: dict[tuple[str, str], UUID] = {}
        self._source_documents: dict[str, SourceReviewRecord] = {}
        self._source_order: dict[str, int] = {}
        self._next_source_order = 0
        self._candidates: dict[tuple[str, UUID], IdentityCandidate] = {}

    @contextmanager
    def batch(self, organization_uris: set[str]) -> Iterator[IdentityRepositoryBatch]:
        del organization_uris
        with self._lock:
            yield self

    def load_evidence(
        self, records: list[IdentityBatchRecord]
    ) -> IdentityBatchEvidence:
        del records
        documents, reviews, source_documents = deepcopy(
            (self._documents, self._reviews, self._source_documents)
        )
        assignments = {
            review_id: IdentityAssignment(review=review)
            for review_id, review in reviews.items()
        }
        review_claims = {
            review_id: {review.claim_uri} for review_id, review in reviews.items()
        }
        for record_key, review_id in self._source_reviews.items():
            assignment = assignments[review_id]
            source_record = source_documents[record_key]
            assignment.source_record_keys.add(record_key)
            assignment.source_names.add(source_record.source.source_name)
            review_claims[review_id].add(source_record.claim.uri)
        return IdentityBatchEvidence(
            assignments_by_source_key={
                record_key: assignments[review_id]
                for record_key, review_id in self._source_reviews.items()
            },
            assignments_by_native_key={
                native_key: assignments[review_id]
                for native_key, review_id in self._native_reviews.items()
            },
            documents=documents,
            reviews=reviews,
            assignments=assignments,
            review_claims=review_claims,
        )

    def commit(self, plan: IdentityBatchPlan) -> list[IdentityAssignment]:
        committed = deepcopy(plan)
        for document_id, document in committed.documents.items():
            self._documents[document_id] = document
            for review in self._reviews.values():
                if review.document.id == document_id:
                    review.document = document
        for source in committed.sources:
            record = source.record
            review = source.assignment.review
            self._documents[review.document.id] = review.document
            self._reviews[review.id] = review
            self._source_reviews[record.source.record_key] = review.id
            if record.source.native_id is not None:
                self._native_reviews[
                    (record.source.source_name, record.source.native_id)
                ] = review.id
            self._source_documents[record.source.record_key] = record
            self._next_source_order += 1
            self._source_order[record.source.record_key] = self._next_source_order
        for document_id, document in committed.documents.items():
            variants = [
                (record_key, record)
                for record_key, record in self._source_documents.items()
                if record.document.content is not None
                and self._reviews[self._source_reviews[record_key]].document.id
                == document_id
            ]
            if variants:
                _record_key, selected = min(
                    variants,
                    key=lambda item: (
                        -item[1].document.word_count,
                        -self._source_order[item[0]],
                        item[0],
                    ),
                )
                document.content = selected.document.content
                document.normalized_text_hash = selected.document.normalized_text_hash
                document.shingles = frozenset(selected.document.shingle_signature)
                document.word_count = selected.document.word_count
        for planned in committed.candidates:
            candidate = planned.candidate
            self._candidates[
                (planned.source_record_key, candidate.candidate_review_id)
            ] = candidate
        return committed.results

    @property
    def candidates(self) -> list[tuple[str, IdentityCandidate]]:
        """Return recorded candidates for acceptance-test assertions."""

        return [
            (source_record_key, candidate)
            for (source_record_key, _review_id), candidate in self._candidates.items()
        ]
