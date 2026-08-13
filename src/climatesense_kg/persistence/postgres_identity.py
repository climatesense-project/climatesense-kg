"""PostgreSQL persistence for batch identity resolution."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from uuid import UUID

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ..identity.models import (
    IdentityAssignment,
    IdentityBatchEvidence,
    IdentityBatchPlan,
    IdentityBatchRecord,
    PlannedSourceAssignment,
    RegisteredDocument,
    RegisteredReview,
)
from ..identity.registry import IdentityRepositoryBatch


class PostgresIdentityRegistry:
    """Provide atomic, organization-locked identity repository batches."""

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    @contextmanager
    def batch(self, organization_uris: set[str]) -> Iterator[IdentityRepositoryBatch]:
        with self.pool.connection() as connection:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT pg_advisory_xact_lock(hashtextextended(uri, 0))
                        FROM (
                            SELECT unnest(%s::text[]) AS uri
                            ORDER BY uri
                        ) AS scopes
                        """,
                        (sorted(organization_uris),),
                    )
                yield PostgresIdentityBatch(connection)


class PostgresIdentityBatch:
    """Set-based identity reads and writes within one PostgreSQL transaction."""

    def __init__(self, connection: Connection[Any]) -> None:
        self.connection = connection

    def load_evidence(
        self, records: list[IdentityBatchRecord]
    ) -> IdentityBatchEvidence:
        """Load all existing state that can affect decisions for the batch."""

        if not records:
            return self._empty_evidence()

        source_keys = sorted({record.source.record_key for record, _ in records})
        native_keys = sorted(
            {
                (record.source.source_name, record.source.native_id)
                for record, _ in records
                if record.source.native_id is not None
            }
        )
        source_names = [key[0] for key in native_keys]
        native_ids = [key[1] for key in native_keys]

        direct_rows = self._direct_assignments(source_keys, source_names, native_ids)
        direct_review_ids = {
            row["claim_review_id"]
            for row in direct_rows
            if row["claim_review_id"] is not None
        }

        organization_uris = sorted({organization.uri for _, organization in records})
        urls = sorted(
            {
                url
                for record, _ in records
                for url in (
                    record.document.observed_url,
                    record.document.final_url,
                    record.document.canonical_url,
                )
                if url
            }
        )
        text_hashes = sorted(
            {
                record.document.normalized_text_hash
                for record, _ in records
                if record.document.normalized_text_hash is not None
            }
        )
        exact_document_ids = self._exact_document_ids(
            organization_uris, urls, text_hashes
        )

        review_rows = self._review_rows(
            direct_review_ids,
            exact_document_ids,
        )
        document_ids = exact_document_ids | {row["document_id"] for row in review_rows}
        document_rows = self._document_rows(document_ids)
        source_rows = self._source_rows(document_ids)

        documents = self._build_documents(document_rows, source_rows)
        reviews = {
            row["id"]: RegisteredReview(
                id=row["id"],
                document=documents[row["document_id"]],
                organization_uri=row["organization_uri"],
                claim_uri=row["claim_uri"],
            )
            for row in review_rows
        }
        assignments = {
            review_id: IdentityAssignment(review=review)
            for review_id, review in reviews.items()
        }
        review_claims = {
            review_id: {review.claim_uri} for review_id, review in reviews.items()
        }
        for row in source_rows:
            review_id = row["claim_review_id"]
            assignment = assignments.get(review_id)
            if assignment is None:
                continue
            assignment.source_record_keys.add(row["record_key"])
            assignment.source_names.add(row["source_name"])
            review_claims[review_id].add(row["claim_uri"])

        return IdentityBatchEvidence(
            assignments_by_source_key={
                row["record_key"]: assignments[row["claim_review_id"]]
                for row in direct_rows
                if row["claim_review_id"] in assignments
            },
            assignments_by_native_key={
                (row["source_name"], row["native_id"]): assignments[
                    row["claim_review_id"]
                ]
                for row in direct_rows
                if row["native_id"] is not None
                and row["claim_review_id"] in assignments
            },
            documents=documents,
            reviews=reviews,
            assignments=assignments,
            review_claims=review_claims,
        )

    def commit(self, plan: IdentityBatchPlan) -> list[IdentityAssignment]:
        """Persist a complete identity plan using bulk statements."""

        document_urls = self._document_url_metadata(plan)
        new_documents = [
            plan.documents[document_id]
            for document_id in sorted(plan.new_document_ids, key=str)
        ]
        with self.connection.cursor(row_factory=dict_row) as cursor:
            if new_documents:
                cursor.executemany(
                    """
                    INSERT INTO review_documents (
                        id, organization_uri, preferred_url, final_url,
                        canonical_url, extracted_text, normalized_text_hash,
                        word_count
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            document.id,
                            document.organization_uri,
                            document.preferred_url,
                            document_urls[document.id][0],
                            document_urls[document.id][1],
                            document.content,
                            document.normalized_text_hash,
                            document.word_count,
                        )
                        for document in new_documents
                    ],
                )

            if plan.new_reviews:
                cursor.executemany(
                    """
                    INSERT INTO claim_review_identities (
                        id, document_id, organization_uri, claim_uri
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    [
                        (
                            review.id,
                            review.document.id,
                            review.organization_uri,
                            review.claim_uri,
                        )
                        for review in sorted(
                            plan.new_reviews.values(), key=lambda item: str(item.id)
                        )
                    ],
                )

            if plan.sources:
                cursor.executemany(
                    """
                    INSERT INTO source_review_records (
                        record_key, source_name, source_type, native_id,
                        observed_url, final_url, canonical_url,
                        claim_uri, rating_fingerprint,
                        source_text, extracted_text, normalized_text_hash,
                        word_count, payload_hash,
                        document_id, claim_review_id
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
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
                        word_count = EXCLUDED.word_count,
                        payload_hash = EXCLUDED.payload_hash,
                        document_id = EXCLUDED.document_id,
                        claim_review_id = EXCLUDED.claim_review_id,
                        last_seen_at = CURRENT_TIMESTAMP
                    """,
                    [self._source_parameters(source) for source in plan.sources],
                )

            if plan.documents:
                ordered_documents = list(plan.documents.values())
                cursor.execute(
                    """
                    WITH preferred (
                        document_id, preferred_url, final_url, canonical_url
                    ) AS (
                        SELECT * FROM unnest(
                            %s::uuid[], %s::text[], %s::text[], %s::text[]
                        )
                    ),
                    selected AS (
                        SELECT DISTINCT ON (source.document_id)
                               source.document_id,
                               COALESCE(
                                   source.extracted_text, source.source_text
                               ) AS content,
                               source.normalized_text_hash,
                               source.word_count
                        FROM source_review_records AS source
                        WHERE source.document_id = ANY(%s::uuid[])
                          AND COALESCE(
                              source.extracted_text, source.source_text
                          ) IS NOT NULL
                        ORDER BY source.document_id,
                                 source.word_count DESC,
                                 source.last_seen_at DESC,
                                 source.record_key
                    )
                    UPDATE review_documents AS document
                    SET preferred_url = preferred.preferred_url,
                        final_url = COALESCE(
                            preferred.final_url, document.final_url
                        ),
                        canonical_url = COALESCE(
                            preferred.canonical_url, document.canonical_url
                        ),
                        extracted_text = CASE
                            WHEN selected.document_id IS NULL
                            THEN document.extracted_text
                            ELSE selected.content
                        END,
                        normalized_text_hash = CASE
                            WHEN selected.document_id IS NULL
                            THEN document.normalized_text_hash
                            ELSE selected.normalized_text_hash
                        END,
                        word_count = CASE
                            WHEN selected.document_id IS NULL
                            THEN document.word_count
                            ELSE selected.word_count
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    FROM preferred
                    LEFT JOIN selected
                      ON selected.document_id = preferred.document_id
                    WHERE document.id = preferred.document_id
                    RETURNING document.id, document.preferred_url,
                              document.final_url, document.canonical_url,
                              document.extracted_text,
                              document.normalized_text_hash,
                              document.word_count
                    """,
                    (
                        [document.id for document in ordered_documents],
                        [document.preferred_url for document in ordered_documents],
                        [
                            document_urls[document.id][0]
                            for document in ordered_documents
                        ],
                        [
                            document_urls[document.id][1]
                            for document in ordered_documents
                        ],
                        [document.id for document in ordered_documents],
                    ),
                )
                for row in cursor.fetchall():
                    document = plan.documents[row["id"]]
                    document.urls.update(
                        url
                        for url in (
                            row["preferred_url"],
                            row["final_url"],
                            row["canonical_url"],
                        )
                        if url
                    )
                    document.preferred_url = row["preferred_url"]
                    document.content = row["extracted_text"]
                    document.normalized_text_hash = row["normalized_text_hash"]
                    document.word_count = row["word_count"]
        return plan.results

    def _direct_assignments(
        self,
        source_keys: list[str],
        source_names: list[str],
        native_ids: list[str],
    ) -> list[dict[str, Any]]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                WITH requested_native(source_name, native_id) AS (
                    SELECT * FROM unnest(%s::text[], %s::text[])
                )
                SELECT source.record_key, source.source_name,
                       source.native_id, source.claim_review_id
                FROM source_review_records AS source
                WHERE source.record_key = ANY(%s::text[])
                UNION
                SELECT source.record_key, source.source_name,
                       source.native_id, source.claim_review_id
                FROM requested_native AS requested
                JOIN source_review_records AS source
                  ON source.source_name = requested.source_name
                 AND source.native_id = requested.native_id
                """,
                (source_names, native_ids, source_keys),
            )
            return list(cursor.fetchall())

    def _exact_document_ids(
        self,
        organization_uris: list[str],
        urls: list[str],
        text_hashes: list[str],
    ) -> set[UUID]:
        if not urls and not text_hashes:
            return set()
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT document.id
                FROM review_documents AS document
                WHERE document.organization_uri = ANY(%s::text[])
                  AND (
                    document.normalized_text_hash = ANY(%s::text[])
                    OR document.preferred_url = ANY(%s::text[])
                    OR document.final_url = ANY(%s::text[])
                    OR document.canonical_url = ANY(%s::text[])
                  )
                UNION
                SELECT document.id
                FROM review_documents AS document
                JOIN source_review_records AS source
                  ON source.document_id = document.id
                WHERE document.organization_uri = ANY(%s::text[])
                  AND (
                    source.observed_url = ANY(%s::text[])
                    OR source.final_url = ANY(%s::text[])
                    OR source.canonical_url = ANY(%s::text[])
                  )
                """,
                (
                    organization_uris,
                    text_hashes,
                    *(urls for _index in range(3)),
                    organization_uris,
                    *(urls for _index in range(3)),
                ),
            )
            return {row[0] for row in cursor.fetchall()}

    def _claim_review_ids(
        self, organization_uris: list[str], claim_uris: list[str]
    ) -> set[UUID]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                WITH requested(organization_uri, claim_uri) AS (
                    SELECT * FROM unnest(%s::text[], %s::text[])
                )
                SELECT identity.id
                FROM requested
                JOIN claim_review_identities AS identity
                  ON identity.organization_uri = requested.organization_uri
                 AND identity.claim_uri = requested.claim_uri
                UNION
                SELECT identity.id
                FROM requested
                JOIN source_review_records AS source
                  ON source.claim_uri = requested.claim_uri
                JOIN claim_review_identities AS identity
                  ON identity.id = source.claim_review_id
                 AND identity.organization_uri = requested.organization_uri
                """,
                (organization_uris, claim_uris),
            )
            return {row[0] for row in cursor.fetchall()}

    def _review_rows(
        self, review_ids: set[UUID], document_ids: set[UUID]
    ) -> list[dict[str, Any]]:
        if not review_ids and not document_ids:
            return []
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT id, document_id, organization_uri, claim_uri
                FROM claim_review_identities
                WHERE id = ANY(%s::uuid[])
                   OR document_id = ANY(%s::uuid[])
                """,
                (sorted(review_ids, key=str), sorted(document_ids, key=str)),
            )
            return list(cursor.fetchall())

    def _document_rows(self, document_ids: set[UUID]) -> list[dict[str, Any]]:
        if not document_ids:
            return []
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT id, organization_uri, preferred_url,
                       final_url, canonical_url, extracted_text,
                       normalized_text_hash, word_count
                FROM review_documents
                WHERE id = ANY(%s::uuid[])
                """,
                (sorted(document_ids, key=str),),
            )
            return list(cursor.fetchall())

    def _source_rows(self, document_ids: set[UUID]) -> list[dict[str, Any]]:
        if not document_ids:
            return []
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT record_key, source_name, native_id,
                       observed_url, final_url, canonical_url,
                       claim_uri, document_id, claim_review_id
                FROM source_review_records
                WHERE document_id = ANY(%s::uuid[])
                """,
                (sorted(document_ids, key=str),),
            )
            return list(cursor.fetchall())

    @staticmethod
    def _build_documents(
        document_rows: list[dict[str, Any]],
        source_rows: list[dict[str, Any]],
    ) -> dict[UUID, RegisteredDocument]:
        urls_by_document: dict[UUID, set[str]] = defaultdict(set)
        for row in source_rows:
            urls_by_document[row["document_id"]].update(
                url
                for url in (
                    row["observed_url"],
                    row["final_url"],
                    row["canonical_url"],
                )
                if url
            )
        documents: dict[UUID, RegisteredDocument] = {}
        for row in document_rows:
            urls = urls_by_document[row["id"]]
            urls.update(
                url
                for url in (
                    row["preferred_url"],
                    row["final_url"],
                    row["canonical_url"],
                )
                if url
            )
            documents[row["id"]] = RegisteredDocument(
                id=row["id"],
                organization_uri=row["organization_uri"],
                urls=urls,
                preferred_url=row["preferred_url"],
                content=row["extracted_text"],
                normalized_text_hash=row["normalized_text_hash"],
                word_count=row["word_count"],
            )
        return documents

    @staticmethod
    def _document_url_metadata(
        plan: IdentityBatchPlan,
    ) -> dict[UUID, tuple[str | None, str | None]]:
        metadata: dict[UUID, tuple[str | None, str | None]] = {}
        for source in plan.sources:
            document_id = source.assignment.review.document.id
            previous_final, previous_canonical = metadata.get(document_id, (None, None))
            observed = source.record.document
            metadata[document_id] = (
                observed.final_url or previous_final,
                observed.canonical_url or previous_canonical,
            )
        return metadata

    @staticmethod
    def _source_parameters(source: PlannedSourceAssignment) -> tuple[Any, ...]:
        record = source.record
        return (
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
            record.document.word_count,
            record.payload_hash,
            source.assignment.review.document.id,
            source.assignment.review.id,
        )

    @staticmethod
    def _empty_evidence() -> IdentityBatchEvidence:
        return IdentityBatchEvidence(
            assignments_by_source_key={},
            assignments_by_native_key={},
            documents={},
            reviews={},
            assignments={},
            review_claims={},
        )
