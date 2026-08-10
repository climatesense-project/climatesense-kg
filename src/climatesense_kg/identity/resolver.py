"""Application service that assigns persistent document and claim-review identity."""

from __future__ import annotations

from uuid import uuid4

from ..domain import (
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalReviewDocument,
    SourceReviewRecord,
)
from .fingerprints import fingerprint_document, shingle_containment
from .models import IdentityAssignment, IdentityCandidate
from .registry import IdentityRegistry, IdentityTransaction


class IdentityResolver:
    """Resolve source observations without deriving identity from mutable metadata."""

    def __init__(
        self,
        registry: IdentityRegistry,
        *,
        similarity_threshold: float = 0.9,
        minimum_similarity_words: int = 50,
    ) -> None:
        if not 0 <= similarity_threshold <= 1:
            raise ValueError("Similarity threshold must be between zero and one")
        if minimum_similarity_words < 1:
            raise ValueError("Minimum similarity words must be positive")
        self.registry = registry
        self.similarity_threshold = similarity_threshold
        self.minimum_similarity_words = minimum_similarity_words

    def resolve(
        self,
        record: SourceReviewRecord,
        organization: CanonicalOrganization,
    ) -> CanonicalClaimReview:
        """Resolve one source observation and return its canonical domain entity."""

        fingerprint_document(record.document)
        with self.registry.transaction() as transaction:
            transaction.lock_scope(organization.uri)
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

    def resolve_many(
        self,
        records: list[tuple[SourceReviewRecord, CanonicalOrganization]],
    ) -> list[CanonicalClaimReview]:
        """Resolve and merge repeated canonical identities within one pipeline batch."""

        resolved: dict[str, CanonicalClaimReview] = {}
        for record, organization in records:
            current = self.resolve(record, organization)
            existing = resolved.get(current.key)
            if existing is None:
                resolved[current.key] = current
            else:
                self._merge(existing, current)
        return list(resolved.values())

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
