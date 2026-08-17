"""Bounded canonical review projections from authoritative database state."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterator
import time
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .domain import (
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalPerson,
    CanonicalReviewDocument,
    OrganizationReference,
    SourceReviewRecord,
    source_record_from_payload,
)

OrganizationResolver = Callable[[OrganizationReference], CanonicalOrganization | None]


class ReviewProjectionReader:
    """Construct canonical review batches without retaining the corpus."""

    def __init__(
        self,
        pool: ConnectionPool,
        resolve_organization: OrganizationResolver,
    ) -> None:
        self.pool = pool
        self.resolve_organization = resolve_organization

    def count(self) -> int:
        with self.pool.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(DISTINCT claim_review_id)
                FROM source_observations
                WHERE active AND claim_review_id IS NOT NULL
                """
            )
            row = cursor.fetchone()
        return int(row[0]) if row else 0

    def iter_batches(self, *, batch_size: int) -> Iterator[list[CanonicalClaimReview]]:
        if batch_size <= 0:
            raise ValueError("Projection batch size must be positive")
        cursor_name = f"projection_{int(time.time() * 1_000_000)}"
        with self.pool.connection() as connection:
            with (
                connection.transaction(),
                connection.cursor(name=cursor_name) as cursor,
            ):
                cursor.execute(
                    """
                    SELECT DISTINCT claim_review_id
                    FROM source_observations
                    WHERE active AND claim_review_id IS NOT NULL
                    ORDER BY claim_review_id
                    """
                )
                while rows := cursor.fetchmany(batch_size):
                    yield self._load([row[0] for row in rows])

    def _load(self, review_ids: list[UUID]) -> list[CanonicalClaimReview]:
        with self.pool.connection() as connection:
            reviews = self._review_rows(connection, review_ids)
            document_ids = [row["document_id"] for row in reviews]
            urls = self._document_urls(connection, document_ids)
            observations = self._observation_rows(connection, review_ids)

        records_by_review: dict[UUID, list[SourceReviewRecord]] = defaultdict(list)
        for row in observations:
            records_by_review[row["claim_review_id"]].append(
                source_record_from_payload(row["payload"])
            )

        projected: list[CanonicalClaimReview] = []
        for row in reviews:
            records = records_by_review[row["id"]]
            if not records:
                continue
            organization = self.resolve_organization(records[0].organization)
            if organization is None:
                raise RuntimeError(
                    "Active observation references an organization missing from "
                    f"the curated catalog: {records[0].organization.website}"
                )
            if organization.uri != row["organization_uri"]:
                raise RuntimeError(
                    f"Organization URI changed for review {row['id']}: "
                    f"{row['organization_uri']} -> {organization.uri}"
                )
            selected = next(
                (record for record in records if record.claim.uri == row["claim_uri"]),
                records[0],
            )
            keywords = sorted(
                {keyword for record in records for keyword in record.keywords}
            )
            authors: list[CanonicalPerson] = []
            for record in records:
                for author in record.authors:
                    if author not in authors:
                        authors.append(author)
            projected.append(
                CanonicalClaimReview(
                    id=row["id"],
                    claim=selected.claim,
                    organization=organization,
                    document=CanonicalReviewDocument(
                        id=row["document_id"],
                        urls=urls[row["document_id"]],
                        preferred_url=row["preferred_url"],
                        content=row["content"],
                        normalized_text_hash=row["normalized_text_hash"],
                        word_count=row["word_count"],
                    ),
                    source_record_keys={record.source.record_key for record in records},
                    source_names={record.source.source_name for record in records},
                    date_published=selected.date_published,
                    language=selected.language,
                    rating=selected.rating,
                    keywords=keywords,
                    authors=authors,
                    license_url=selected.license_url,
                    description=selected.document.description,
                    abstract=selected.document.abstract,
                    observations={
                        record.source.record_key: record for record in records
                    },
                )
            )
        return projected

    @staticmethod
    def _review_rows(connection: Any, review_ids: list[UUID]) -> list[dict[str, Any]]:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT review.id, review.document_id,
                       review.organization_uri, review.claim_uri,
                       document.preferred_url, document.content,
                       document.normalized_text_hash, document.word_count
                FROM claim_reviews AS review
                JOIN documents AS document ON document.id = review.document_id
                WHERE review.id = ANY(%s::uuid[])
                ORDER BY review.id
                """,
                (review_ids,),
            )
            return list(cursor.fetchall())

    @staticmethod
    def _document_urls(
        connection: Any,
        document_ids: list[UUID],
    ) -> dict[UUID, set[str]]:
        urls: dict[UUID, set[str]] = defaultdict(set)
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT document_id, url
                FROM document_urls
                WHERE document_id = ANY(%s::uuid[])
                """,
                (document_ids,),
            )
            for row in cursor.fetchall():
                urls[row["document_id"]].add(row["url"])
        return urls

    @staticmethod
    def _observation_rows(
        connection: Any,
        review_ids: list[UUID],
    ) -> list[dict[str, Any]]:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT claim_review_id, payload
                FROM source_observations
                WHERE active
                  AND claim_review_id = ANY(%s::uuid[])
                ORDER BY source_name, record_key
                """,
                (review_ids,),
            )
            return list(cursor.fetchall())
