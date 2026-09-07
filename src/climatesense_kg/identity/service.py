"""Discover exact identity evidence and reconcile each organization atomically."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ..utils.progress import ProgressLogger
from ..utils.text_processing import normalize_document_url
from .fingerprints import fingerprint_text
from .planner import IdentityPlanner, Observation
from .repository import IdentityRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IdentitySummary:
    observations: int
    documents_created: int
    reviews_created: int
    documents_merged: int = 0
    reviews_merged: int = 0


class IdentityService:
    """Maintain canonical identities as exact evidence accumulates across runs.

    The caller holds Database's pipeline writer lock throughout this service.
    Discovery finishes before writes begin, so assignments read from a server
    cursor cannot become stale during reconciliation. One organization is the
    transaction boundary; batch size only controls transfer of article bodies.
    """

    def __init__(
        self,
        pool: ConnectionPool,
        *,
        batch_size: int = 500,
        progress_interval_seconds: float = 10.0,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("Identity batch size must be positive")
        if progress_interval_seconds < 0:
            raise ValueError("Identity progress interval must be non-negative")
        self.pool = pool
        self.batch_size = batch_size
        self.progress_interval_seconds = progress_interval_seconds

    def run(self, run_id: UUID) -> IdentitySummary:
        with self.pool.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT status FROM pipeline_runs WHERE id = %s", (run_id,))
            if cursor.fetchone() != ("running",):
                raise RuntimeError(
                    f"Identity reconciliation requires an active run: {run_id}"
                )
            cursor.execute(
                """
                SELECT organization_uri, COUNT(*) FROM source_observations
                WHERE active GROUP BY organization_uri ORDER BY organization_uri
                """
            )
            organizations = cursor.fetchall()
        progress = ProgressLogger(
            logger,
            "Identity resolution",
            sum(count for _organization, count in organizations),
            interval_seconds=self.progress_interval_seconds,
        )
        processed = documents_created = reviews_created = 0
        documents_merged = reviews_merged = 0
        for organization, count in organizations:
            with self.pool.connection() as connection, connection.transaction():
                repository = IdentityRepository(connection, organization)
                planner = repository.load_planner()
                repository.create_staging()
                self._discover(repository, planner, progress, processed)
                plan = planner.plan()
                del planner
                repository.apply(plan, run_id)
            processed += count
            documents_created += len(plan.new_documents)
            reviews_created += len(plan.new_reviews)
            documents_merged += len(plan.document_merges)
            reviews_merged += len(plan.review_merges)
            if plan.document_merges or plan.review_merges:
                logger.info(
                    "Identity reconciled %s: document_merges=%s; review_merges=%s; run=%s",
                    organization,
                    [
                        (str(item.retired_id), str(item.survivor_id))
                        for item in plan.document_merges
                    ],
                    [
                        (str(item.retired_id), str(item.survivor_id))
                        for item in plan.review_merges
                    ],
                    run_id,
                )
            progress.update(
                processed,
                {
                    "documents_created": documents_created,
                    "reviews_created": reviews_created,
                    "documents_merged": documents_merged,
                    "reviews_merged": reviews_merged,
                },
            )
        progress.update(processed, force=True)
        return IdentitySummary(
            processed,
            documents_created,
            reviews_created,
            documents_merged,
            reviews_merged,
        )

    def _discover(
        self,
        repository: IdentityRepository,
        planner: IdentityPlanner,
        progress: ProgressLogger,
        processed: int,
    ) -> None:
        with repository.connection.cursor(
            name="identity_observations", row_factory=dict_row
        ) as cursor:
            cursor.execute(
                """
                SELECT observation.record_key, observation.claim_uri,
                       observation.claim_review_id, observation.observed_url,
                       observation.payload #>> '{document,source_text}' AS source_text,
                       extraction.final_url, extraction.canonical_url,
                       extraction.content AS extracted_text,
                       extraction.normalized_text_hash, extraction.word_count
                FROM source_observations AS observation
                LEFT JOIN document_extractions AS extraction
                  ON extraction.document_key = observation.document_key
                 AND extraction.status = 'success'
                WHERE observation.active AND observation.organization_uri = %s
                ORDER BY observation.record_key
                """,
                (repository.organization_uri,),
            )
            while rows := cursor.fetchmany(self.batch_size):
                staged: list[tuple[Any, ...]] = []
                aliases: list[tuple[str, str, str]] = []
                for row in rows:
                    observation, content_row, urls = self._observation(row)
                    planner.observe(observation)
                    staged.append(content_row)
                    aliases.extend(urls)
                repository.copy("identity_observations", staged)
                repository.copy("identity_urls", aliases)
                processed += len(rows)
                progress.update(processed)

    @staticmethod
    def _observation(
        row: dict[str, Any],
    ) -> tuple[Observation, tuple[Any, ...], list[tuple[str, str, str]]]:
        content = row["extracted_text"]
        if content is None:
            content = row["source_text"]
        if content is not None and not isinstance(content, str):
            raise RuntimeError(f"Invalid document text for {row['record_key']}")
        if row["normalized_text_hash"] is not None:
            text_hash = row["normalized_text_hash"]
            word_count = row["word_count"]
        else:
            fingerprint = fingerprint_text(content)
            text_hash, word_count = (
                fingerprint.normalized_text_hash,
                fingerprint.word_count,
            )
        urls: list[tuple[int, str, str]] = []
        seen: set[str] = set()
        for rank, raw in enumerate(
            (row["canonical_url"], row["final_url"], row["observed_url"])
        ):
            if raw is None:
                continue
            normalized = normalize_document_url(raw) or raw
            if normalized not in seen:
                urls.append((rank, normalized, raw))
                seen.add(normalized)
        if not urls:
            raise RuntimeError(f"Observation {row['record_key']} has no document URL")
        rank, _normalized, preferred_url = min(urls)
        key = row["record_key"]
        return (
            Observation(
                key,
                row["claim_uri"],
                row["claim_review_id"],
                tuple(normalized for _rank, normalized, _raw in urls),
                text_hash,
            ),
            (key, preferred_url, rank, content, text_hash, word_count),
            [(key, normalized, raw) for _rank, normalized, raw in urls],
        )
