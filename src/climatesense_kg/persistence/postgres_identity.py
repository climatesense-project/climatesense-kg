"""PostgreSQL identity registry."""

from __future__ import annotations

from collections import defaultdict
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
        self._prepared_source_keys: set[str] = set()
        self._prepared_native_keys: set[tuple[str, str]] = set()
        self._review_by_source_key: dict[str, UUID] = {}
        self._review_by_native_key: dict[tuple[str, str], UUID] = {}
        self._assignments: dict[UUID, IdentityAssignment] = {}
        self._documents: dict[UUID, RegisteredDocument] = {}
        self._prepared_evidence_keys: set[tuple[str, frozenset[str], str | None]] = (
            set()
        )

    def prepare(
        self,
        records: list[tuple[SourceReviewRecord, CanonicalOrganization]],
    ) -> None:
        """Prefetch assignments and exact document evidence for one batch."""

        source_records = [record for record, _organization in records]
        source_keys = {record.source.record_key for record in source_records}
        native_keys = {
            (record.source.source_name, record.source.native_id)
            for record in source_records
            if record.source.native_id is not None
        }
        self._prepared_source_keys.update(source_keys)
        self._prepared_native_keys.update(native_keys)
        source_names = {source_name for source_name, _native_id in native_keys}
        native_ids = {native_id for _source_name, native_id in native_keys}
        if source_keys:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT record_key, source_name, native_id, claim_review_id
                    FROM source_review_records
                    WHERE claim_review_id IS NOT NULL
                      AND (
                        record_key = ANY(%s::text[])
                        OR (
                            native_id IS NOT NULL
                            AND source_name = ANY(%s::text[])
                            AND native_id = ANY(%s::text[])
                        )
                      )
                    """,
                    (
                        sorted(source_keys),
                        sorted(source_names),
                        sorted(native_ids),
                    ),
                )
                for record_key, source_name, native_id, review_id in cursor.fetchall():
                    self._review_by_source_key[record_key] = review_id
                    if native_id is not None:
                        self._review_by_native_key[(source_name, native_id)] = review_id

        evidence_records = [
            (record, organization)
            for record, organization in records
            if record.source.record_key not in self._review_by_source_key
            and (
                record.source.native_id is None
                or (
                    record.source.source_name,
                    record.source.native_id,
                )
                not in self._review_by_native_key
            )
        ]
        evidence_source_records = [record for record, _organization in evidence_records]
        organization_uris = {
            organization.uri for _record, organization in evidence_records
        }
        urls = {
            url
            for record in evidence_source_records
            for url in (
                record.document.observed_url,
                record.document.final_url,
                record.document.canonical_url,
            )
            if url
        }
        text_hashes = {
            record.document.normalized_text_hash
            for record in evidence_source_records
            if record.document.normalized_text_hash is not None
        }
        self._prepared_evidence_keys.update(
            (
                organization.uri,
                frozenset(
                    url
                    for url in (
                        record.document.observed_url,
                        record.document.final_url,
                        record.document.canonical_url,
                    )
                    if url
                ),
                record.document.normalized_text_hash,
            )
            for record, organization in evidence_records
        )
        if organization_uris and urls:
            self._prepare_evidence_documents(
                organization_uris,
                urls,
                text_hashes,
            )

    def _prepare_evidence_documents(
        self,
        organization_uris: set[str],
        urls: set[str],
        text_hashes: set[str],
    ) -> None:
        urls_by_document: dict[UUID, set[str]] = defaultdict(set)
        rows_by_document: dict[UUID, dict[str, Any]] = {}
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT d.id, d.organization_uri, d.preferred_url,
                       d.final_url, d.canonical_url, d.extracted_text,
                       d.normalized_text_hash, d.shingle_signature, d.word_count,
                       s.observed_url AS source_observed_url,
                       s.final_url AS source_final_url,
                       s.canonical_url AS source_canonical_url
                FROM review_documents AS d
                LEFT JOIN source_review_records AS s ON s.document_id = d.id
                WHERE d.organization_uri = ANY(%s::text[])
                  AND (
                    d.normalized_text_hash = ANY(%s::text[])
                    OR d.preferred_url = ANY(%s::text[])
                    OR d.final_url = ANY(%s::text[])
                    OR d.canonical_url = ANY(%s::text[])
                    OR s.observed_url = ANY(%s::text[])
                    OR s.final_url = ANY(%s::text[])
                    OR s.canonical_url = ANY(%s::text[])
                  )
                """,
                (
                    sorted(organization_uris),
                    sorted(text_hashes),
                    *(sorted(urls) for _index in range(6)),
                ),
            )
            for row in cursor.fetchall():
                document_id = row["id"]
                rows_by_document[document_id] = row
                urls_by_document[document_id].update(
                    url
                    for url in (
                        row["preferred_url"],
                        row["final_url"],
                        row["canonical_url"],
                        row["source_observed_url"],
                        row["source_final_url"],
                        row["source_canonical_url"],
                    )
                    if url
                )
        for document_id, row in rows_by_document.items():
            cached = self._documents.get(document_id)
            if cached is not None:
                cached.urls.update(urls_by_document[document_id])
                continue
            self._documents[document_id] = RegisteredDocument(
                id=document_id,
                organization_uri=row["organization_uri"],
                urls=urls_by_document[document_id],
                preferred_url=row["preferred_url"],
                content=row["extracted_text"],
                normalized_text_hash=row["normalized_text_hash"],
                shingles=frozenset(row["shingle_signature"]),
                word_count=row["word_count"],
            )

    def lock_scope(self, organization_uri: str) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (organization_uri,),
            )

    def assignment_for_source(self, record_key: str) -> IdentityAssignment | None:
        if record_key in self._prepared_source_keys:
            review_id = self._review_by_source_key.get(record_key)
            return self.assignment(review_id) if review_id is not None else None
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
        native_key = (source_name, native_id)
        if native_key in self._prepared_native_keys:
            review_id = self._review_by_native_key.get(native_key)
            return self.assignment(review_id) if review_id is not None else None
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
        evidence_key = (
            organization_uri,
            frozenset(urls),
            normalized_text_hash,
        )
        if evidence_key in self._prepared_evidence_keys:
            return sorted(
                (
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
                ),
                key=lambda document: str(document.id),
            )
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
        review = RegisteredReview(
            id=review_id,
            document=document,
            organization_uri=organization.uri,
            claim_uri=record.claim.uri,
        )
        self._assignments[review_id] = IdentityAssignment(review=review)
        return review

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
                RETURNING preferred_url, final_url, canonical_url,
                          extracted_text, normalized_text_hash,
                          shingle_signature, word_count
                """,
                (
                    document.id,
                    record.document.preferred_url,
                    record.document.final_url,
                    record.document.canonical_url,
                    document.id,
                ),
            )
            document_row = cursor.fetchone()
        if document_row is None:
            raise KeyError(f"Unknown review document: {document.id}")
        document.urls.update(
            url
            for url in (
                record.document.observed_url,
                record.document.final_url,
                record.document.canonical_url,
                document_row[0],
                document_row[1],
                document_row[2],
            )
            if url
        )
        document.preferred_url = document_row[0]
        document.content = document_row[3]
        document.normalized_text_hash = document_row[4]
        document.shingles = frozenset(document_row[5])
        document.word_count = document_row[6]
        assignment = self._assignments.get(review.id)
        if assignment is None:
            assignment = IdentityAssignment(review=review)
            self._assignments[review.id] = assignment
        assignment.source_record_keys.add(record.source.record_key)
        assignment.source_names.add(record.source.source_name)
        self._review_by_source_key[record.source.record_key] = review.id
        if record.source.native_id is not None:
            self._review_by_native_key[
                (record.source.source_name, record.source.native_id)
            ] = review.id

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
        cached = self._assignments.get(review_id)
        if cached is not None:
            return cached
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
        assignment = IdentityAssignment(
            review=review,
            source_record_keys={source["record_key"] for source in sources},
            source_names={source["source_name"] for source in sources},
        )
        self._assignments[review_id] = assignment
        return assignment

    def _document(self, document_id: UUID) -> RegisteredDocument:
        cached = self._documents.get(document_id)
        if cached is not None:
            return cached
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
        document = RegisteredDocument(
            id=row["id"],
            organization_uri=row["organization_uri"],
            urls=urls,
            preferred_url=row["preferred_url"],
            content=row["extracted_text"],
            normalized_text_hash=row["normalized_text_hash"],
            shingles=frozenset(row["shingle_signature"]),
            word_count=row["word_count"],
        )
        self._documents[document_id] = document
        return document
