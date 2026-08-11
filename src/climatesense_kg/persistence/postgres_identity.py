"""PostgreSQL identity registry."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import json
from typing import Any
from uuid import UUID

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ..domain import CanonicalOrganization, SourceReviewRecord
from ..identity import (
    IdentityAssignment,
    IdentityCandidate,
    IdentityTransaction,
    RegisteredDocument,
)
from ..identity.models import RegisteredReview


class PostgresIdentityRegistry:
    """Resolve identity atomically against PostgreSQL."""

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    @contextmanager
    def transaction(self) -> Iterator[IdentityTransaction]:
        with self.pool.connection() as connection:
            with connection.transaction():
                yield PostgresIdentityTransaction(connection)


class PostgresIdentityTransaction:
    """Identity operations bound to one PostgreSQL transaction."""

    def __init__(self, connection: Connection[Any]) -> None:
        self.connection = connection

    def lock_scope(self, organization_uri: str) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (organization_uri,),
            )

    def assignment_for_source(self, record_key: str) -> IdentityAssignment | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT claim_review_id FROM source_review_records WHERE record_key = %s",
                (record_key,),
            )
            row = cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return self.assignment(row[0])

    def assignment_for_native_id(
        self, source_name: str, native_id: str
    ) -> IdentityAssignment | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT claim_review_id
                FROM source_review_records
                WHERE source_name = %s AND native_id = %s
                """,
                (source_name, native_id),
            )
            row = cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return self.assignment(row[0])

    def documents_by_evidence(
        self,
        organization_uri: str,
        urls: set[str],
        normalized_text_hash: str | None,
    ) -> list[RegisteredDocument]:
        url_list = sorted(urls)
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT d.id
                FROM review_documents AS d
                LEFT JOIN source_review_records AS s ON s.document_id = d.id
                WHERE d.organization_uri = %s
                  AND (
                    d.normalized_text_hash = %s
                    OR d.preferred_url = ANY(%s)
                    OR d.final_url = ANY(%s)
                    OR d.canonical_url = ANY(%s)
                    OR s.observed_url = ANY(%s)
                    OR s.final_url = ANY(%s)
                    OR s.canonical_url = ANY(%s)
                  )
                ORDER BY d.id
                """,
                (
                    organization_uri,
                    normalized_text_hash,
                    url_list,
                    url_list,
                    url_list,
                    url_list,
                    url_list,
                    url_list,
                ),
            )
            document_ids = [row[0] for row in cursor.fetchall()]
        return [self._document(document_id) for document_id in document_ids]

    def review_for_document_claim(
        self, document_id: UUID, claim_uri: str
    ) -> RegisteredReview | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT identities.id
                FROM claim_review_identities AS identities
                LEFT JOIN source_review_records AS sources
                  ON sources.claim_review_id = identities.id
                WHERE identities.document_id = %s
                  AND (
                    identities.claim_uri = %s
                    OR sources.claim_uri = %s
                  )
                ORDER BY identities.id
                LIMIT 1
                """,
                (document_id, claim_uri, claim_uri),
            )
            row = cursor.fetchone()
        return self.assignment(row[0]).review if row else None

    def reviews_for_claim(
        self, organization_uri: str, claim_uri: str
    ) -> list[RegisteredReview]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT identities.id
                FROM claim_review_identities AS identities
                LEFT JOIN source_review_records AS sources
                  ON sources.claim_review_id = identities.id
                WHERE identities.organization_uri = %s
                  AND (
                    identities.claim_uri = %s
                    OR sources.claim_uri = %s
                  )
                ORDER BY identities.id
                """,
                (organization_uri, claim_uri, claim_uri),
            )
            review_ids = [row[0] for row in cursor.fetchall()]
        return [self.assignment(review_id).review for review_id in review_ids]

    def create_document(
        self,
        document_id: UUID,
        organization: CanonicalOrganization,
        record: SourceReviewRecord,
    ) -> RegisteredDocument:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO review_documents (
                    id, organization_uri, preferred_url, final_url, canonical_url,
                    extracted_text, normalized_text_hash, shingle_signature, word_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    document_id,
                    organization.uri,
                    record.document.preferred_url,
                    record.document.final_url,
                    record.document.canonical_url,
                    record.document.content,
                    record.document.normalized_text_hash,
                    json.dumps(record.document.shingle_signature),
                    record.document.word_count,
                ),
            )
        return self._document(document_id)

    def create_review(
        self,
        review_id: UUID,
        document: RegisteredDocument,
        organization: CanonicalOrganization,
        record: SourceReviewRecord,
    ) -> RegisteredReview:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO claim_review_identities (
                    id, document_id, organization_uri, claim_uri
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    review_id,
                    document.id,
                    organization.uri,
                    record.claim.uri,
                ),
            )
        return RegisteredReview(
            id=review_id,
            document=document,
            organization_uri=organization.uri,
            claim_uri=record.claim.uri,
        )

    def attach_source(
        self,
        record: SourceReviewRecord,
        document: RegisteredDocument,
        review: RegisteredReview,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO source_review_records (
                    record_key, source_name, source_type, native_id,
                    observed_url, final_url, canonical_url,
                    claim_uri, rating_fingerprint,
                    source_text, extracted_text, normalized_text_hash,
                    shingle_signature, word_count, payload_hash,
                    document_id, claim_review_id
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (record_key) DO UPDATE SET
                    observed_url = EXCLUDED.observed_url,
                    final_url = EXCLUDED.final_url,
                    canonical_url = EXCLUDED.canonical_url,
                    claim_uri = EXCLUDED.claim_uri,
                    rating_fingerprint = EXCLUDED.rating_fingerprint,
                    source_text = EXCLUDED.source_text,
                    extracted_text = EXCLUDED.extracted_text,
                    normalized_text_hash = EXCLUDED.normalized_text_hash,
                    shingle_signature = EXCLUDED.shingle_signature,
                    word_count = EXCLUDED.word_count,
                    payload_hash = EXCLUDED.payload_hash,
                    document_id = EXCLUDED.document_id,
                    claim_review_id = EXCLUDED.claim_review_id,
                    last_seen_at = CURRENT_TIMESTAMP
                """,
                (
                    record.source.record_key,
                    record.source.source_name,
                    record.source.source_type,
                    record.source.native_id,
                    record.document.observed_url,
                    record.document.final_url,
                    record.document.canonical_url,
                    record.claim.uri,
                    record.rating.fingerprint if record.rating else None,
                    record.document.source_text,
                    record.document.extracted_text,
                    record.document.normalized_text_hash,
                    json.dumps(record.document.shingle_signature),
                    record.document.word_count,
                    record.payload_hash,
                    document.id,
                    review.id,
                ),
            )
            cursor.execute(
                """
                WITH selected AS (
                    SELECT
                        COALESCE(extracted_text, source_text) AS content,
                        normalized_text_hash,
                        shingle_signature,
                        word_count
                    FROM source_review_records
                    WHERE document_id = %s
                      AND COALESCE(extracted_text, source_text) IS NOT NULL
                    ORDER BY word_count DESC, last_seen_at DESC, record_key
                    LIMIT 1
                )
                UPDATE review_documents AS document
                SET preferred_url = %s,
                    final_url = COALESCE(%s, document.final_url),
                    canonical_url = COALESCE(%s, document.canonical_url),
                    extracted_text = COALESCE(
                        (SELECT content FROM selected),
                        document.extracted_text
                    ),
                    normalized_text_hash = COALESCE(
                        (SELECT normalized_text_hash FROM selected),
                        document.normalized_text_hash
                    ),
                    shingle_signature = COALESCE(
                        (SELECT shingle_signature FROM selected),
                        document.shingle_signature
                    ),
                    word_count = COALESCE(
                        (SELECT word_count FROM selected),
                        document.word_count
                    ),
                    updated_at = CURRENT_TIMESTAMP
                WHERE document.id = %s
                """,
                (
                    document.id,
                    record.document.preferred_url,
                    record.document.final_url,
                    record.document.canonical_url,
                    document.id,
                ),
            )

    def record_candidate(
        self, source_record_key: str, candidate: IdentityCandidate
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO identity_candidates (
                    source_record_key, candidate_review_id, similarity, evidence
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (source_record_key, candidate_review_id) DO UPDATE SET
                    similarity = EXCLUDED.similarity,
                    evidence = EXCLUDED.evidence
                """,
                (
                    source_record_key,
                    candidate.candidate_review_id,
                    candidate.similarity,
                    json.dumps(candidate.evidence),
                ),
            )

    def assignment(self, review_id: UUID) -> IdentityAssignment:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT id, document_id, organization_uri, claim_uri
                FROM claim_review_identities
                WHERE id = %s
                """,
                (review_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise KeyError(f"Unknown claim review identity: {review_id}")
            cursor.execute(
                """
                SELECT record_key, source_name
                FROM source_review_records
                WHERE claim_review_id = %s
                """,
                (review_id,),
            )
            sources = cursor.fetchall()
        review = RegisteredReview(
            id=row["id"],
            document=self._document(row["document_id"]),
            organization_uri=row["organization_uri"],
            claim_uri=row["claim_uri"],
        )
        return IdentityAssignment(
            review=review,
            source_record_keys={source["record_key"] for source in sources},
            source_names={source["source_name"] for source in sources},
        )

    def _document(self, document_id: UUID) -> RegisteredDocument:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT id, organization_uri, preferred_url, final_url, canonical_url,
                       extracted_text, normalized_text_hash, shingle_signature, word_count
                FROM review_documents
                WHERE id = %s
                """,
                (document_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise KeyError(f"Unknown review document: {document_id}")
            cursor.execute(
                """
                SELECT observed_url, final_url, canonical_url
                FROM source_review_records
                WHERE document_id = %s
                """,
                (document_id,),
            )
            source_rows = cursor.fetchall()
        urls = {
            url
            for url in (
                row["preferred_url"],
                row["final_url"],
                row["canonical_url"],
                *(value for source_row in source_rows for value in source_row.values()),
            )
            if url
        }
        return RegisteredDocument(
            id=row["id"],
            organization_uri=row["organization_uri"],
            urls=urls,
            preferred_url=row["preferred_url"],
            content=row["extracted_text"],
            normalized_text_hash=row["normalized_text_hash"],
            shingles=frozenset(row["shingle_signature"]),
            word_count=row["word_count"],
        )
