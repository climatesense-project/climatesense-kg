"""Application service that assigns persistent document and claim-review identity."""

from __future__ import annotations

import logging
import time
from uuid import uuid4

from ..domain import (
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalReviewDocument,
    SourceReviewRecord,
)
from ..utils.progress import format_duration
from .fingerprints import fingerprint_document, shingle_containment
from .models import IdentityAssignment, IdentityCandidate
from .registry import IdentityRegistry, IdentityTransaction

logger = logging.getLogger(__name__)


class IdentityResolver:
    """Resolve source observations to persistent canonical identities."""

    def __init__(
        self,
        registry: IdentityRegistry,
        *,
        similarity_threshold: float = 0.9,
        minimum_similarity_words: int = 50,
        batch_size: int = 500,
        progress_interval_seconds: float = 10.0,
    ) -> None:
        if not 0 <= similarity_threshold <= 1:
            raise ValueError("Similarity threshold must be between zero and one")
        if minimum_similarity_words < 1:
            raise ValueError("Minimum similarity words must be positive")
        if batch_size <= 0:
            raise ValueError("Identity batch size must be positive")
        if progress_interval_seconds < 0:
            raise ValueError("Identity progress interval must be non-negative")
        self.registry = registry
        self.similarity_threshold = similarity_threshold
        self.minimum_similarity_words = minimum_similarity_words
        self.batch_size = batch_size
        self.progress_interval_seconds = progress_interval_seconds

    def resolve(
        self,
        record: SourceReviewRecord,
        organization: CanonicalOrganization,
    ) -> CanonicalClaimReview:
        """Resolve one source observation and return its canonical domain entity."""

        fingerprint_document(record.document)
        with self.registry.transaction() as transaction:
            transaction.lock_scope(organization.uri)
            return self._resolve_in_transaction(transaction, record, organization)

    def resolve_many(
        self,
        records: list[tuple[SourceReviewRecord, CanonicalOrganization]],
    ) -> list[CanonicalClaimReview]:
        """Resolve and merge repeated canonical identities within one pipeline batch."""

        total = len(records)
        started = time.monotonic()
        last_logged_elapsed: float | None = None
        resolved: dict[str, CanonicalClaimReview] = {}

        def log_progress(committed: int, batches: int) -> None:
            nonlocal last_logged_elapsed
            elapsed = max(0.0, time.monotonic() - started)
            should_log = (
                last_logged_elapsed is None
                or committed == total
                or elapsed - last_logged_elapsed >= self.progress_interval_seconds
            )
            if not should_log:
                return
            last_logged_elapsed = elapsed
            rate = committed / elapsed if committed and elapsed > 0 else None
            remaining = max(0, total - committed)
            eta = remaining / rate if rate and remaining else None
            percent = 100.0 if not total else 100 * committed / total
            logger.info(
                "Identity resolution: %d/%d committed (%.1f%%); "
                "canonical=%d, batches=%d; rate=%s; ETA=%s",
                committed,
                total,
                percent,
                len(resolved),
                batches,
                f"{rate:.2f}/s" if rate is not None else "n/a",
                format_duration(eta),
            )

        log_progress(0, 0)
        batches = 0
        for start in range(0, total, self.batch_size):
            batch = records[start : start + self.batch_size]
            for record, _organization in batch:
                fingerprint_document(record.document)
            with self.registry.transaction() as transaction:
                for organization_uri in sorted(
                    {organization.uri for _record, organization in batch}
                ):
                    transaction.lock_scope(organization_uri)
                transaction.prepare(batch)
                for record, organization in batch:
                    current = self._resolve_in_transaction(
                        transaction, record, organization
                    )
                    existing = resolved.get(current.key)
                    if existing is None:
                        resolved[current.key] = current
                    else:
                        self._merge(existing, current)
            batches += 1
            log_progress(min(start + len(batch), total), batches)
        return list(resolved.values())

    def _resolve_in_transaction(
        self,
        transaction: IdentityTransaction,
        record: SourceReviewRecord,
        organization: CanonicalOrganization,
    ) -> CanonicalClaimReview:
        assignment = transaction.assignment_for_source(record.source.record_key)
        if assignment is not None:
            transaction.attach_source(
                record, assignment.review.document, assignment.review
            )
            assignment = transaction.assignment(assignment.review.id)
            return self._to_canonical(record, organization, assignment)

        if record.source.native_id:
            assignment = transaction.assignment_for_native_id(
                record.source.source_name, record.source.native_id
            )
            if assignment is not None:
                transaction.attach_source(
                    record, assignment.review.document, assignment.review
                )
                assignment = transaction.assignment(assignment.review.id)
                return self._to_canonical(record, organization, assignment)

        assignment = self._resolve_new_source(transaction, record, organization)
        return self._to_canonical(record, organization, assignment)

    def _resolve_new_source(
        self,
        transaction: IdentityTransaction,
        record: SourceReviewRecord,
        organization: CanonicalOrganization,
    ) -> IdentityAssignment:
        urls = {
            url
            for url in (
                record.document.observed_url,
                record.document.final_url,
                record.document.canonical_url,
            )
            if url
        }
        documents = transaction.documents_by_evidence(
            organization.uri, urls, record.document.normalized_text_hash
        )
        for document in documents:
            review = transaction.review_for_document_claim(
                document.id, record.claim.uri
            )
            if review is None:
                review = transaction.create_review(
                    uuid4(), document, organization, record
                )
            transaction.attach_source(record, document, review)
            return transaction.assignment(review.id)

        identity_candidates = self._find_fuzzy_candidates(
            transaction, record, organization
        )
        document = transaction.create_document(uuid4(), organization, record)
        review = transaction.create_review(uuid4(), document, organization, record)
        transaction.attach_source(record, document, review)
        seen_candidates: set[str] = set()
        for candidate in identity_candidates:
            candidate_key = str(candidate.candidate_review_id)
            if candidate_key in seen_candidates:
                continue
            seen_candidates.add(candidate_key)
            transaction.record_candidate(record.source.record_key, candidate)
        return transaction.assignment(review.id)

    def _find_fuzzy_candidates(
        self,
        transaction: IdentityTransaction,
        record: SourceReviewRecord,
        organization: CanonicalOrganization,
    ) -> list[IdentityCandidate]:
        if record.document.word_count < self.minimum_similarity_words:
            return []
        record_shingles = frozenset(record.document.shingle_signature)
        candidates: list[IdentityCandidate] = []
        for review in transaction.reviews_for_claim(organization.uri, record.claim.uri):
            if review.document.word_count < self.minimum_similarity_words:
                continue
            similarity = shingle_containment(record_shingles, review.document.shingles)
            if similarity < self.similarity_threshold:
                continue
            candidates.append(
                IdentityCandidate(
                    candidate_review_id=review.id,
                    similarity=similarity,
                    evidence={
                        "kind": "body_similarity",
                        "same_organization": True,
                        "same_claim": True,
                        "left_word_count": record.document.word_count,
                        "right_word_count": review.document.word_count,
                    },
                )
            )
        return candidates

    @staticmethod
    def _to_canonical(
        record: SourceReviewRecord,
        organization: CanonicalOrganization,
        assignment: IdentityAssignment,
    ) -> CanonicalClaimReview:
        registered = assignment.review
        document = registered.document
        return CanonicalClaimReview(
            id=registered.id,
            claim=record.claim,
            organization=organization,
            document=CanonicalReviewDocument(
                id=document.id,
                urls=set(document.urls),
                preferred_url=document.preferred_url,
                content=document.content,
                normalized_text_hash=document.normalized_text_hash,
                shingle_signature=sorted(document.shingles),
                word_count=document.word_count,
            ),
            source_record_keys=set(assignment.source_record_keys),
            source_names=set(assignment.source_names),
            date_published=record.date_published,
            language=record.language,
            rating=record.rating,
            keywords=list(record.keywords),
            authors=list(record.authors),
            license_url=record.license_url,
            description=record.document.description,
            abstract=record.document.abstract,
            observations={record.source.record_key: record},
        )

    @staticmethod
    def _merge(existing: CanonicalClaimReview, current: CanonicalClaimReview) -> None:
        existing.source_record_keys.update(current.source_record_keys)
        existing.source_names.update(current.source_names)
        existing.observations.update(current.observations)
        existing.document.urls.update(current.document.urls)
        existing.document.preferred_url = current.document.preferred_url
        existing.document.content = current.document.content
        existing.document.normalized_text_hash = current.document.normalized_text_hash
        existing.document.shingle_signature = list(current.document.shingle_signature)
        existing.document.word_count = current.document.word_count
        for keyword in current.keywords:
            if keyword not in existing.keywords:
                existing.keywords.append(keyword)
        for author in current.authors:
            if author not in existing.authors:
                existing.authors.append(author)
