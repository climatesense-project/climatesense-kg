"""Transactional staging and application of identity reconciliation plans."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from uuid import UUID

from psycopg import Connection, sql
from psycopg.types.json import Jsonb

from .planner import Document, IdentityPlanner, Merge, ReconciliationPlan, Review


class IdentityRepository:
    """All operations use the caller's single organization transaction."""

    def __init__(self, connection: Connection[Any], organization_uri: str) -> None:
        self.connection = connection
        self.organization_uri = organization_uri

    def load_planner(self) -> IdentityPlanner:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT id, created_at FROM documents WHERE organization_uri = %s",
                (self.organization_uri,),
            )
            documents = [Document(*row) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT id, document_id, claim_uri, created_at FROM claim_reviews
                WHERE organization_uri = %s
                """,
                (self.organization_uri,),
            )
            reviews = [Review(*row) for row in cursor.fetchall()]
            planner = IdentityPlanner(self.organization_uri, documents, reviews)
            cursor.execute(
                """
                SELECT 'url', normalized_url, document_id FROM document_urls
                WHERE organization_uri = %s
                UNION ALL
                SELECT 'text_hash', normalized_text_hash, document_id
                FROM document_text_hashes WHERE organization_uri = %s
                """,
                (self.organization_uri, self.organization_uri),
            )
            while rows := cursor.fetchmany(1000):
                for kind, value, document_id in rows:
                    planner.add_existing_key(kind, value, document_id)
        return planner

    def create_staging(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TEMP TABLE identity_observations (
                    record_key TEXT PRIMARY KEY,
                    preferred_url TEXT NOT NULL,
                    preferred_url_rank SMALLINT NOT NULL,
                    content TEXT,
                    normalized_text_hash TEXT,
                    word_count INTEGER NOT NULL
                ) ON COMMIT DROP;
                CREATE TEMP TABLE identity_urls (
                    record_key TEXT NOT NULL,
                    normalized_url TEXT NOT NULL,
                    url TEXT NOT NULL
                ) ON COMMIT DROP;
                CREATE TEMP TABLE identity_assignments (
                    record_key TEXT PRIMARY KEY,
                    document_id UUID NOT NULL,
                    review_id UUID NOT NULL
                ) ON COMMIT DROP;
                CREATE TEMP TABLE identity_document_map (
                    old_id UUID PRIMARY KEY,
                    survivor_id UUID NOT NULL
                ) ON COMMIT DROP;
                CREATE TEMP TABLE identity_review_map (
                    old_id UUID PRIMARY KEY,
                    survivor_id UUID NOT NULL
                ) ON COMMIT DROP;
                CREATE TEMP TABLE identity_reviews (
                    id UUID PRIMARY KEY,
                    document_id UUID NOT NULL,
                    claim_uri TEXT NOT NULL,
                    is_new BOOLEAN NOT NULL
                ) ON COMMIT DROP;
                """
            )

    def copy(self, table: str, rows: Iterable[tuple[Any, ...]]) -> None:
        with self.connection.cursor() as cursor:
            with cursor.copy(
                sql.SQL("COPY {} FROM STDIN").format(sql.Identifier(table))
            ) as copy:
                for row in rows:
                    copy.write_row(row)

    def apply(self, plan: ReconciliationPlan, run_id: UUID) -> None:
        self.copy("identity_document_map", plan.documents.items())
        self.copy("identity_review_map", plan.reviews.items())
        self.copy(
            "identity_assignments",
            ((key, *assignment) for key, assignment in plan.assignments.items()),
        )
        self.copy(
            "identity_reviews",
            (
                (review_id, *values, review_id in plan.new_reviews)
                for review_id, values in plan.review_documents.items()
            ),
        )
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                ANALYZE identity_observations;
                ANALYZE identity_urls;
                ANALYZE identity_assignments;
                ANALYZE identity_document_map;
                ANALYZE identity_review_map;
                ANALYZE identity_reviews;
                """
            )
        self._validate_references()
        self._consolidate_documents()
        self._record_merges("document", plan.document_merges, run_id)
        self._record_merges("claim_review", plan.review_merges, run_id)
        self._consolidate_reviews()
        self._install_evidence()
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM documents AS document
                USING identity_document_map AS mapping
                WHERE document.id = mapping.old_id
                  AND mapping.old_id <> mapping.survivor_id
                """
            )

    def _validate_references(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT MIN(observation.record_key)
                FROM source_observations AS observation
                JOIN identity_review_map AS mapping
                  ON mapping.old_id = observation.claim_review_id
                WHERE observation.organization_uri <> %s
                """,
                (self.organization_uri,),
            )
            row = cursor.fetchone()
            if row is not None and row[0] is not None:
                raise RuntimeError(f"Observation {row[0]} references a foreign review")
            cursor.execute(
                """
                SELECT MIN(evidence_key) FROM (
                SELECT url.normalized_url AS evidence_key FROM document_urls AS url
                JOIN identity_document_map AS mapping ON mapping.old_id = url.document_id
                WHERE url.organization_uri <> %s
                UNION ALL
                SELECT hash.normalized_text_hash FROM document_text_hashes AS hash
                JOIN identity_document_map AS mapping ON mapping.old_id = hash.document_id
                WHERE hash.organization_uri <> %s
                ) AS invalid_evidence
                """,
                (self.organization_uri, self.organization_uri),
            )
            row = cursor.fetchone()
            if row is not None and row[0] is not None:
                raise RuntimeError(
                    f"Identity evidence {row[0]} references a foreign document"
                )

    def _consolidate_documents(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                WITH candidates AS MATERIALIZED (
                    SELECT mapping.survivor_id AS id, document.preferred_url,
                           document.preferred_url_rank, document.content,
                           document.normalized_text_hash, document.word_count
                    FROM documents AS document
                    JOIN identity_document_map AS mapping ON mapping.old_id = document.id
                    UNION ALL
                    SELECT assignment.document_id, observation.preferred_url,
                           observation.preferred_url_rank, observation.content,
                           observation.normalized_text_hash, observation.word_count
                    FROM identity_observations AS observation
                    JOIN identity_assignments AS assignment USING (record_key)
                ), urls AS (
                    SELECT DISTINCT ON (id) id, preferred_url, preferred_url_rank
                    FROM candidates
                    ORDER BY id, preferred_url_rank, preferred_url COLLATE "C"
                ), texts AS (
                    SELECT DISTINCT ON (id)
                           id, content, normalized_text_hash, word_count
                    FROM candidates WHERE content IS NOT NULL
                    ORDER BY id, word_count DESC,
                             normalized_text_hash COLLATE "C" NULLS LAST,
                             content COLLATE "C"
                )
                INSERT INTO documents (
                    id, organization_uri, preferred_url, preferred_url_rank,
                    content, normalized_text_hash, word_count
                )
                SELECT urls.id, %s, preferred_url, preferred_url_rank,
                       content, normalized_text_hash, COALESCE(word_count, 0)
                FROM urls LEFT JOIN texts USING (id)
                ON CONFLICT (id) DO UPDATE
                SET preferred_url = EXCLUDED.preferred_url,
                    preferred_url_rank = EXCLUDED.preferred_url_rank,
                    content = EXCLUDED.content,
                    normalized_text_hash = EXCLUDED.normalized_text_hash,
                    word_count = EXCLUDED.word_count,
                    updated_at = CURRENT_TIMESTAMP
                WHERE (documents.preferred_url, documents.preferred_url_rank,
                       documents.content, documents.normalized_text_hash, documents.word_count)
                    IS DISTINCT FROM
                      (EXCLUDED.preferred_url, EXCLUDED.preferred_url_rank,
                       EXCLUDED.content, EXCLUDED.normalized_text_hash, EXCLUDED.word_count)
                """,
                (self.organization_uri,),
            )

    def _record_merges(
        self, kind: str, merges: tuple[Merge, ...], run_id: UUID
    ) -> None:
        if kind not in {"document", "claim_review"}:
            raise ValueError(kind)
        mapping = (
            "identity_document_map" if kind == "document" else "identity_review_map"
        )
        table = f"{kind}_merges"
        with self.connection.cursor() as cursor:
            # Keep direct links to the current survivor without rewriting the
            # original decision or its evidence when that survivor is later merged.
            cursor.execute(
                sql.SQL(
                    """
                    UPDATE {history} AS history SET survivor_id = mapping.survivor_id
                    FROM {mapping} AS mapping
                    WHERE history.survivor_id = mapping.old_id
                      AND mapping.old_id <> mapping.survivor_id
                    """
                ).format(history=sql.Identifier(table), mapping=sql.Identifier(mapping))
            )
            cursor.executemany(
                sql.SQL(
                    """
                    INSERT INTO {} (retired_id, survivor_id, merged_into_id, run_id, evidence)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                ).format(sql.Identifier(table)),
                (
                    (
                        merge.retired_id,
                        merge.survivor_id,
                        merge.survivor_id,
                        run_id,
                        Jsonb(merge.evidence),
                    )
                    for merge in merges
                ),
            )

    def _consolidate_reviews(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE source_observations AS observation
                SET claim_review_id = mapping.survivor_id
                FROM identity_review_map AS mapping
                WHERE observation.claim_review_id = mapping.old_id
                  AND mapping.old_id <> mapping.survivor_id;

                DELETE FROM claim_reviews AS review
                USING identity_review_map AS mapping
                WHERE review.id = mapping.old_id AND mapping.old_id <> mapping.survivor_id;

                UPDATE claim_reviews AS review
                SET document_id = planned.document_id, updated_at = CURRENT_TIMESTAMP
                FROM identity_reviews AS planned
                WHERE review.id = planned.id AND review.document_id <> planned.document_id;
                """
            )
            cursor.execute(
                """
                INSERT INTO claim_reviews (id, document_id, organization_uri, claim_uri)
                SELECT id, document_id, %s, claim_uri FROM identity_reviews WHERE is_new
                """,
                (self.organization_uri,),
            )
            cursor.execute(
                """
                UPDATE source_observations AS observation
                SET claim_review_id = assignment.review_id
                FROM identity_assignments AS assignment
                WHERE observation.record_key = assignment.record_key
                  AND observation.claim_review_id IS DISTINCT FROM assignment.review_id
                """
            )

    def _install_evidence(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE document_urls AS url SET document_id = mapping.survivor_id
                FROM identity_document_map AS mapping
                WHERE url.document_id = mapping.old_id AND mapping.old_id <> mapping.survivor_id;
                UPDATE document_text_hashes AS hash SET document_id = mapping.survivor_id
                FROM identity_document_map AS mapping
                WHERE hash.document_id = mapping.old_id AND mapping.old_id <> mapping.survivor_id;
                """
            )
            cursor.execute(
                """
                INSERT INTO document_urls (organization_uri, normalized_url, url, document_id)
                SELECT DISTINCT ON (url.normalized_url)
                       %s, url.normalized_url, url.url, assignment.document_id
                FROM identity_urls AS url
                JOIN identity_assignments AS assignment USING (record_key)
                WHERE NOT EXISTS (
                    SELECT 1 FROM document_urls AS stored
                    WHERE stored.organization_uri = %s AND stored.normalized_url = url.normalized_url
                )
                ORDER BY url.normalized_url, url.url COLLATE "C"
                """,
                (self.organization_uri, self.organization_uri),
            )
            cursor.execute(
                """
                INSERT INTO document_text_hashes (organization_uri, normalized_text_hash, document_id)
                SELECT DISTINCT ON (observation.normalized_text_hash)
                       %s, observation.normalized_text_hash, assignment.document_id
                FROM identity_observations AS observation
                JOIN identity_assignments AS assignment USING (record_key)
                WHERE observation.normalized_text_hash IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM document_text_hashes AS stored
                    WHERE stored.organization_uri = %s
                      AND stored.normalized_text_hash = observation.normalized_text_hash
                )
                ORDER BY observation.normalized_text_hash
                """,
                (self.organization_uri, self.organization_uri),
            )
            # Do not silently accept an alias/hash omitted or assigned inconsistently
            # by a defective plan. Aggregate the full check: LIMIT 1 encourages
            # quadratic nested loops when the expected result is no violations.
            cursor.execute(
                """
                SELECT MIN(evidence_key) FROM (
                SELECT url.normalized_url AS evidence_key FROM identity_urls AS url
                JOIN identity_assignments AS assignment USING (record_key)
                LEFT JOIN document_urls AS stored
                  ON stored.organization_uri = %s AND stored.normalized_url = url.normalized_url
                WHERE stored.document_id IS DISTINCT FROM assignment.document_id
                UNION ALL
                SELECT observation.normalized_text_hash FROM identity_observations AS observation
                JOIN identity_assignments AS assignment USING (record_key)
                LEFT JOIN document_text_hashes AS stored
                  ON stored.organization_uri = %s
                 AND stored.normalized_text_hash = observation.normalized_text_hash
                WHERE observation.normalized_text_hash IS NOT NULL
                  AND stored.document_id IS DISTINCT FROM assignment.document_id
                ) AS invalid_evidence
                """,
                (self.organization_uri, self.organization_uri),
            )
            row = cursor.fetchone()
            if row is not None and row[0] is not None:
                raise RuntimeError(
                    f"Reconciliation left inconsistent identity evidence: {row[0]}"
                )
