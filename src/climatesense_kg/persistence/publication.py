"""Bounded reconstruction of canonical reviews for one ingestion snapshot."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterator
from typing import Any, Protocol
from uuid import UUID

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ..domain import (
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalPerson,
    CanonicalReviewDocument,
    OrganizationReference,
    SourceReviewRecord,
    source_record_from_payload,
)

OrganizationResolver = Callable[[OrganizationReference], CanonicalOrganization | None]


class PublicationReader(Protocol):
    """Read current-run canonical reviews without retaining the full corpus."""

    def iter_batches(
        self,
        run_id: UUID,
        *,
        batch_size: int,
        resolve_organization: OrganizationResolver,
    ) -> Iterator[list[CanonicalClaimReview]]: ...

    def count(self, run_id: UUID) -> int: ...


class PostgresPublicationReader:
    """Reconstruct canonical reviews from run observations and identity state."""

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    def count(self, run_id: UUID) -> int:
        """Count current-run canonical reviews for progress reporting."""

        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT source.claim_review_id)
                    FROM ingestion_records AS ingestion
                    JOIN source_review_records AS source
                      ON source.record_key = ingestion.record_key
                    WHERE ingestion.run_id = %s
                      AND source.claim_review_id IS NOT NULL
                    """,
                    (run_id,),
                )
                row = cursor.fetchone()
        return int(row[0]) if row else 0

    def iter_batches(
        self,
        run_id: UUID,
        *,
        batch_size: int,
        resolve_organization: OrganizationResolver,
    ) -> Iterator[list[CanonicalClaimReview]]:
        if batch_size <= 0:
            raise ValueError("Publication batch size must be positive")
        cursor_name = f"publication_{run_id.hex}"
        with self.pool.connection() as connection:
            with connection.cursor(name=cursor_name) as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT source.claim_review_id
                    FROM ingestion_records AS ingestion
                    JOIN source_review_records AS source
                      ON source.record_key = ingestion.record_key
                    WHERE ingestion.run_id = %s
                      AND source.claim_review_id IS NOT NULL
                    ORDER BY source.claim_review_id
                    """,
                    (run_id,),
                )
                while rows := cursor.fetchmany(batch_size):
                    review_ids = [row[0] for row in rows]
                    yield self._load_batch(
                        run_id,
                        review_ids,
                        resolve_organization=resolve_organization,
                    )

    def _load_batch(
        self,
        run_id: UUID,
        review_ids: list[UUID],
        *,
        resolve_organization: OrganizationResolver,
    ) -> list[CanonicalClaimReview]:
        with self.pool.connection() as connection:
            identity_rows = self._identity_rows(connection, review_ids)
            document_ids = [row["document_id"] for row in identity_rows]
            urls = self._document_urls(connection, document_ids)
            observation_rows = self._observation_rows(connection, run_id, review_ids)

        records_by_review: dict[UUID, list[SourceReviewRecord]] = defaultdict(list)
        for row in observation_rows:
            records_by_review[row["claim_review_id"]].append(
                source_record_from_payload(row["payload"])
            )

        reviews: list[CanonicalClaimReview] = []
        for row in identity_rows:
            records = records_by_review[row["id"]]
            if not records:
                continue
            organization = resolve_organization(records[0].organization)
            if organization is None:
                raise RuntimeError(
                    "Current-run observation references an organization missing "
                    f"from the curated catalog: {records[0].organization.website}"
                )
            if organization.uri != row["organization_uri"]:
                raise RuntimeError(
                    "Curated organization URI changed for persisted identity "
                    f"{row['id']}: {row['organization_uri']} -> {organization.uri}"
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
            reviews.append(
                CanonicalClaimReview(
                    id=row["id"],
                    claim=selected.claim,
                    organization=organization,
                    document=CanonicalReviewDocument(
                        id=row["document_id"],
                        urls=urls[row["document_id"]],
                        preferred_url=row["preferred_url"],
                        content=row["extracted_text"],
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
        return reviews

    @staticmethod
    def _identity_rows(connection: Any, review_ids: list[UUID]) -> list[dict[str, Any]]:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT identity.id, identity.document_id,
                       identity.organization_uri, identity.claim_uri,
                       document.preferred_url, document.final_url,
                       document.canonical_url, document.extracted_text,
                       document.normalized_text_hash, document.word_count
                FROM claim_review_identities AS identity
                JOIN review_documents AS document
                  ON document.id = identity.document_id
                WHERE identity.id = ANY(%s::uuid[])
                ORDER BY identity.id
                """,
                (review_ids,),
            )
            return list(cursor.fetchall())

    @staticmethod
    def _document_urls(
        connection: Any, document_ids: list[UUID]
    ) -> dict[UUID, set[str]]:
        urls: dict[UUID, set[str]] = defaultdict(set)
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT document.id, document.preferred_url,
                       document.final_url, document.canonical_url,
                       source.observed_url, source.final_url AS source_final_url,
                       source.canonical_url AS source_canonical_url
                FROM review_documents AS document
                LEFT JOIN source_review_records AS source
                  ON source.document_id = document.id
                WHERE document.id = ANY(%s::uuid[])
                """,
                (document_ids,),
            )
            for row in cursor.fetchall():
                urls[row["id"]].update(
                    value
                    for value in (
                        row["preferred_url"],
                        row["final_url"],
                        row["canonical_url"],
                        row["observed_url"],
                        row["source_final_url"],
                        row["source_canonical_url"],
                    )
                    if value
                )
        return urls

    @staticmethod
    def _observation_rows(
        connection: Any,
        run_id: UUID,
        review_ids: list[UUID],
    ) -> list[dict[str, Any]]:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT source.claim_review_id, ingestion.payload
                FROM ingestion_records AS ingestion
                JOIN source_review_records AS source
                  ON source.record_key = ingestion.record_key
                WHERE ingestion.run_id = %s
                  AND source.claim_review_id = ANY(%s::uuid[])
                ORDER BY ingestion.source_name, ingestion.position
                """,
                (run_id, review_ids),
            )
            return list(cursor.fetchall())
