"""Pure batch planner for persistent claim-review identity."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from uuid import UUID, uuid4

from ..domain import SourceReviewRecord
from .models import (
    IdentityAssignment,
    IdentityBatchEvidence,
    IdentityBatchPlan,
    IdentityBatchRecord,
    PlannedSourceAssignment,
    RegisteredDocument,
    RegisteredReview,
)


class IdentityPlanner:
    """Plan identity assignments without performing persistence operations."""

    def plan(
        self,
        records: list[IdentityBatchRecord],
        evidence: IdentityBatchEvidence,
    ) -> IdentityBatchPlan:
        """Resolve a batch against loaded evidence and return persistence changes."""

        assignments_by_source = dict(evidence.assignments_by_source_key)
        assignments_by_native = dict(evidence.assignments_by_native_key)
        assignments = dict(evidence.assignments)
        documents = dict(evidence.documents)
        reviews = dict(evidence.reviews)
        review_claims = {
            review_id: set(claims)
            for review_id, claims in evidence.review_claims.items()
        }
        reviews_by_document_claim: dict[tuple[UUID, str], list[RegisteredReview]] = (
            defaultdict(list)
        )
        for review in reviews.values():
            claims = review_claims.setdefault(review.id, {review.claim_uri})
            claims.add(review.claim_uri)
            for claim_uri in claims:
                reviews_by_document_claim[(review.document.id, claim_uri)].append(
                    review
                )

        results: list[IdentityAssignment] = []
        touched_documents: dict[UUID, RegisteredDocument] = {}
        new_document_ids: set[UUID] = set()
        new_reviews: dict[UUID, RegisteredReview] = {}
        planned_sources: list[PlannedSourceAssignment] = []

        for record, organization in records:
            assignment = assignments_by_source.get(record.source.record_key)
            native_key = (
                (record.source.source_name, record.source.native_id)
                if record.source.native_id is not None
                else None
            )
            if assignment is None and native_key is not None:
                assignment = assignments_by_native.get(native_key)

            if assignment is None:
                matching_documents = self._matching_documents(
                    documents.values(),
                    organization.uri,
                    {
                        url
                        for url in (
                            record.document.observed_url,
                            record.document.final_url,
                            record.document.canonical_url,
                        )
                        if url
                    },
                    record.document.normalized_text_hash,
                )
                if matching_documents:
                    document = matching_documents[0]
                    matching_reviews = reviews_by_document_claim.get(
                        (document.id, record.claim.uri),
                        [],
                    )
                    if matching_reviews:
                        review = min(matching_reviews, key=lambda item: str(item.id))
                        assignment = assignments[review.id]
                    else:
                        assignment = self._new_review_assignment(
                            document,
                            organization.uri,
                            record.claim.uri,
                        )
                        reviews[assignment.review.id] = assignment.review
                        assignments[assignment.review.id] = assignment
                        new_reviews[assignment.review.id] = assignment.review
                        review_claims[assignment.review.id] = {record.claim.uri}
                        reviews_by_document_claim[
                            (document.id, record.claim.uri)
                        ].append(assignment.review)
                else:
                    document = self._new_document(record, organization.uri)
                    documents[document.id] = document
                    new_document_ids.add(document.id)
                    assignment = self._new_review_assignment(
                        document,
                        organization.uri,
                        record.claim.uri,
                    )
                    reviews[assignment.review.id] = assignment.review
                    assignments[assignment.review.id] = assignment
                    new_reviews[assignment.review.id] = assignment.review
                    review_claims[assignment.review.id] = {record.claim.uri}
                    reviews_by_document_claim[(document.id, record.claim.uri)].append(
                        assignment.review
                    )

            known_claims = review_claims.setdefault(
                assignment.review.id, {assignment.review.claim_uri}
            )
            if record.claim.uri not in known_claims:
                known_claims.add(record.claim.uri)
                reviews_by_document_claim[
                    (assignment.review.document.id, record.claim.uri)
                ].append(assignment.review)
            self._attach(record, assignment)
            assignments_by_source[record.source.record_key] = assignment
            if native_key is not None:
                assignments_by_native[native_key] = assignment
            touched_documents[assignment.review.document.id] = (
                assignment.review.document
            )
            planned_sources.append(
                PlannedSourceAssignment(record=record, assignment=assignment)
            )
            results.append(assignment)

        return IdentityBatchPlan(
            results=results,
            documents=touched_documents,
            new_document_ids=new_document_ids,
            new_reviews=new_reviews,
            sources=planned_sources,
        )

    @staticmethod
    def _matching_documents(
        documents: Iterable[RegisteredDocument],
        organization_uri: str,
        urls: set[str],
        normalized_text_hash: str | None,
    ) -> list[RegisteredDocument]:
        return sorted(
            (
                document
                for document in documents
                if document.organization_uri == organization_uri
                and (
                    bool(document.urls & urls)
                    or (
                        normalized_text_hash is not None
                        and document.normalized_text_hash == normalized_text_hash
                    )
                )
            ),
            key=lambda document: str(document.id),
        )

    @staticmethod
    def _new_document(
        record: SourceReviewRecord, organization_uri: str
    ) -> RegisteredDocument:
        document = record.document
        return RegisteredDocument(
            id=uuid4(),
            organization_uri=organization_uri,
            urls={
                url
                for url in (
                    document.observed_url,
                    document.final_url,
                    document.canonical_url,
                )
                if url
            },
            preferred_url=document.preferred_url,
            content=document.content,
            normalized_text_hash=document.normalized_text_hash,
            word_count=document.word_count,
        )

    @staticmethod
    def _new_review_assignment(
        document: RegisteredDocument,
        organization_uri: str,
        claim_uri: str,
    ) -> IdentityAssignment:
        review = RegisteredReview(
            id=uuid4(),
            document=document,
            organization_uri=organization_uri,
            claim_uri=claim_uri,
        )
        return IdentityAssignment(review=review)

    @staticmethod
    def _attach(record: SourceReviewRecord, assignment: IdentityAssignment) -> None:
        source = record.source
        observed_document = record.document
        document = assignment.review.document
        document.urls.update(
            url
            for url in (
                observed_document.observed_url,
                observed_document.final_url,
                observed_document.canonical_url,
            )
            if url
        )
        document.preferred_url = observed_document.preferred_url
        if (
            observed_document.content is not None
            and observed_document.word_count >= document.word_count
        ):
            document.content = observed_document.content
            document.normalized_text_hash = observed_document.normalized_text_hash
            document.word_count = observed_document.word_count
        assignment.source_record_keys.add(source.record_key)
        assignment.source_names.add(source.source_name)
