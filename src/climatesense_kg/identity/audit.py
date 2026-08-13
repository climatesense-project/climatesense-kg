"""Bounded exact near-duplicate auditing outside online identity assignment."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations
import json
import logging
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .fingerprints import shingle_containment, text_shingles

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DuplicateAuditReport:
    """Counts from one complete exact duplicate-candidate audit."""

    groups: int
    candidate_pairs: int
    eligible_pairs: int
    matches: int


class DuplicateAuditor:
    """Compare only same-organization, same-claim review groups exactly."""

    def __init__(
        self,
        pool: ConnectionPool,
        *,
        similarity_threshold: float = 0.9,
        minimum_similarity_words: int = 50,
        group_batch_size: int = 100,
    ) -> None:
        if not 0 <= similarity_threshold <= 1:
            raise ValueError("Similarity threshold must be between zero and one")
        if minimum_similarity_words < 1:
            raise ValueError("Minimum similarity words must be positive")
        if group_batch_size < 1:
            raise ValueError("Audit group batch size must be positive")
        self.pool = pool
        self.similarity_threshold = similarity_threshold
        self.minimum_similarity_words = minimum_similarity_words
        self.group_batch_size = group_batch_size

    def run(self) -> DuplicateAuditReport:
        """Atomically replace candidate evidence using bounded comparisons."""

        groups = 0
        candidate_pairs = 0
        eligible_pairs = 0
        matches = 0
        with self.pool.connection() as connection:
            with connection.transaction():
                with connection.cursor() as setup_cursor:
                    setup_cursor.execute(
                        """
                        CREATE TEMP TABLE duplicate_audit_candidates (
                            source_record_key TEXT NOT NULL,
                            candidate_review_id UUID NOT NULL,
                            similarity DOUBLE PRECISION NOT NULL,
                            evidence JSONB NOT NULL,
                            PRIMARY KEY (source_record_key, candidate_review_id)
                        ) ON COMMIT DROP
                        """
                    )
                with connection.cursor(name="duplicate_audit_groups") as cursor:
                    cursor.execute(
                        """
                        SELECT identity.organization_uri, source.claim_uri
                        FROM source_review_records AS source
                        JOIN claim_review_identities AS identity
                          ON identity.id = source.claim_review_id
                        JOIN review_documents AS document
                          ON document.id = identity.document_id
                        WHERE document.extracted_text IS NOT NULL
                        GROUP BY identity.organization_uri, source.claim_uri
                        HAVING COUNT(DISTINCT identity.id) > 1
                        ORDER BY identity.organization_uri, source.claim_uri
                        """
                    )
                    while rows := cursor.fetchmany(self.group_batch_size):
                        batch_groups = [(row[0], row[1]) for row in rows]
                        batch = self._load_groups(connection, batch_groups)
                        planned: list[tuple[str, object, float, str]] = []
                        for group in batch_groups:
                            group_pairs, group_eligible, group_matches = (
                                self._compare_group(batch[group])
                            )
                            groups += 1
                            candidate_pairs += group_pairs
                            eligible_pairs += group_eligible
                            matches += len(group_matches)
                            planned.extend(group_matches)
                        self._store_candidates(connection, planned)
                        logger.info(
                            "Duplicate audit: %d groups, %d pairs, %d matches",
                            groups,
                            candidate_pairs,
                            matches,
                        )
                with connection.cursor() as publish_cursor:
                    publish_cursor.execute("DELETE FROM identity_candidates")
                    publish_cursor.execute(
                        """
                        INSERT INTO identity_candidates (
                            source_record_key, candidate_review_id,
                            similarity, evidence
                        )
                        SELECT source_record_key, candidate_review_id,
                               similarity, evidence
                        FROM duplicate_audit_candidates
                        """
                    )
        return DuplicateAuditReport(
            groups=groups,
            candidate_pairs=candidate_pairs,
            eligible_pairs=eligible_pairs,
            matches=matches,
        )

    def _compare_group(
        self, reviews: list[dict[str, Any]]
    ) -> tuple[int, int, list[tuple[str, object, float, str]]]:
        features = {row["id"]: text_shingles(row["extracted_text"]) for row in reviews}
        candidate_pairs = 0
        eligible_pairs = 0
        matches: list[tuple[str, object, float, str]] = []
        for left, right in combinations(reviews, 2):
            candidate_pairs += 1
            if (
                min(left["word_count"], right["word_count"])
                < self.minimum_similarity_words
            ):
                continue
            eligible_pairs += 1
            similarity = shingle_containment(
                features[left["id"]],
                features[right["id"]],
            )
            if similarity < self.similarity_threshold:
                continue
            matches.append(
                (
                    right["record_key"],
                    left["id"],
                    similarity,
                    json.dumps(
                        {
                            "kind": "body_similarity",
                            "same_organization": True,
                            "same_claim": True,
                            "left_word_count": left["word_count"],
                            "right_word_count": right["word_count"],
                        }
                    ),
                )
            )
        return candidate_pairs, eligible_pairs, matches

    @staticmethod
    def _load_groups(
        connection: Any, groups: list[tuple[str, str]]
    ) -> dict[tuple[str, str], list[dict[str, Any]]]:
        by_group: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                WITH requested(organization_uri, claim_uri) AS (
                    SELECT * FROM unnest(%s::text[], %s::text[])
                )
                SELECT requested.organization_uri, requested.claim_uri,
                       identity.id, document.extracted_text,
                       document.word_count, MIN(source.record_key) AS record_key
                FROM requested
                JOIN claim_review_identities AS identity
                  ON identity.organization_uri = requested.organization_uri
                JOIN source_review_records AS source
                  ON source.claim_review_id = identity.id
                 AND source.claim_uri = requested.claim_uri
                JOIN review_documents AS document
                  ON document.id = identity.document_id
                WHERE document.extracted_text IS NOT NULL
                GROUP BY requested.organization_uri, requested.claim_uri,
                         identity.id, document.extracted_text,
                         document.word_count
                ORDER BY requested.organization_uri, requested.claim_uri,
                         identity.id
                """,
                ([group[0] for group in groups], [group[1] for group in groups]),
            )
            for row in cursor.fetchall():
                by_group[(row["organization_uri"], row["claim_uri"])].append(row)
        return by_group

    @staticmethod
    def _store_candidates(
        connection: Any, candidates: list[tuple[str, object, float, str]]
    ) -> None:
        if not candidates:
            return
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO duplicate_audit_candidates (
                    source_record_key, candidate_review_id,
                    similarity, evidence
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (
                    source_record_key, candidate_review_id
                ) DO UPDATE SET
                    similarity = EXCLUDED.similarity,
                    evidence = EXCLUDED.evidence
                """,
                candidates,
            )
