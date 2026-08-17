"""Bounded enrichment execution backed by current PostgreSQL results."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime
import logging
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .enrichers import Enricher
from .enrichers.base import EnrichmentSubject
from .processing import ProcessingResult, ResultStatus, StageSummary
from .projection import ReviewProjectionReader
from .utils.progress import ProgressLogger

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StoredEnrichment:
    version: str
    input_hash: str
    config_hash: str
    result: ProcessingResult


class EnrichmentService:
    """Run enabled enrichers without retaining canonical review projections."""

    def __init__(
        self,
        pool: ConnectionPool,
        reader: ReviewProjectionReader,
        enrichers: list[Enricher],
        *,
        batch_size: int = 500,
        progress_interval_seconds: float = 10.0,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("Enrichment batch size must be positive")
        if progress_interval_seconds < 0:
            raise ValueError("Enrichment progress interval must be non-negative")
        self.pool = pool
        self.reader = reader
        self.enrichers = enrichers
        self.batch_size = batch_size
        self.progress_interval_seconds = progress_interval_seconds
        self._availability: dict[str, bool] = {}

    def run(self, *, offline: bool = False, force: bool = False) -> list[StageSummary]:
        """Process all canonical reviews in bounded batches."""

        self._availability.clear()
        if not self.enrichers:
            return []
        total = self.reader.count()
        progress = ProgressLogger(
            logger,
            "Enrichment overall",
            total,
            interval_seconds=self.progress_interval_seconds,
            rate_window_size=5,
        )
        combined = {
            enricher.name: StageSummary(enricher.name) for enricher in self.enrichers
        }
        processed = 0
        for items in self.reader.iter_batches(batch_size=self.batch_size):
            batch_start = processed + 1
            batch_end = processed + len(items)
            for enricher in self.enrichers:
                combined[enricher.name].merge(
                    self._process_stage(
                        enricher,
                        items,
                        offline=offline,
                        force=force,
                        batch_start=batch_start,
                        batch_end=batch_end,
                        total_reviews=total,
                    )
                )
            processed += len(items)
            progress.update(processed)
        progress.update(processed, force=True)
        for summary in combined.values():
            logger.info(
                "Enrichment finished: %s; eligible=%d, cached=%d, "
                "succeeded=%d, retryable=%d, permanent=%d, missing=%d",
                summary.name,
                summary.eligible,
                summary.cached,
                summary.succeeded,
                summary.retryable_failures,
                summary.permanent_failures,
                summary.missing,
            )
        return list(combined.values())

    def apply_stored(self, items: list[Any]) -> None:
        """Apply current successful results to one export batch."""

        for enricher in self.enrichers:
            subjects = enricher.subjects(items)
            stored = self._load(enricher.name, [subject.key for subject in subjects])
            for subject in subjects:
                current = stored.get(subject.key)
                if current is not None and self._is_current(enricher, subject, current):
                    if current.result.succeeded:
                        enricher.apply(subject, current.result.payload)

    def _process_stage(
        self,
        enricher: Enricher,
        items: list[Any],
        *,
        offline: bool,
        force: bool,
        batch_start: int = 1,
        batch_end: int | None = None,
        total_reviews: int | None = None,
    ) -> StageSummary:
        batch_end = batch_end if batch_end is not None else len(items)
        total_reviews = total_reviews if total_reviews is not None else len(items)
        subjects = enricher.subjects(items)
        summary = StageSummary(enricher.name, eligible=len(subjects))
        stored = (
            {}
            if force
            else self._load(
                enricher.name,
                [subject.key for subject in subjects],
            )
        )
        pending: list[EnrichmentSubject] = []
        now = datetime.now(UTC)
        for subject in subjects:
            current = stored.get(subject.key)
            if current is None or not self._is_current(enricher, subject, current):
                pending.append(subject)
                continue
            result = current.result
            if result.succeeded:
                summary.cached += 1
                enricher.apply(subject, result.payload)
            elif result.permanent:
                summary.permanent_failures += 1
                summary.missing += 1
            elif result.deferred(now):
                summary.retryable_failures += 1
                summary.missing += 1
            else:
                pending.append(subject)

        logger.info(
            "Enrichment [%s]: reviews %d-%d/%d; eligible=%d, cached=%d, "
            "pending=%d, retryable=%d, permanent=%d",
            enricher.name,
            batch_start,
            batch_end,
            total_reviews,
            summary.eligible,
            summary.cached,
            len(pending),
            summary.retryable_failures,
            summary.permanent_failures,
        )
        if not pending:
            return summary
        if offline:
            summary.missing += len(pending)
            return summary

        available = self._availability.get(enricher.availability_key)
        if available is None:
            try:
                available = enricher.is_available()
            except Exception as exc:
                logger.warning("Enricher %s healthcheck failed: %s", enricher.name, exc)
                available = False
            self._availability[enricher.availability_key] = available
        summary.available = available
        if not available:
            summary.missing += len(pending)
            return summary

        self._compute_pending(
            enricher,
            pending,
            summary,
            batch_start=batch_start,
            batch_end=batch_end,
        )
        return summary

    def _compute_pending(
        self,
        enricher: Enricher,
        pending: list[EnrichmentSubject],
        summary: StageSummary,
        *,
        batch_start: int,
        batch_end: int,
    ) -> None:
        """Compute and checkpoint independent work units with bounded concurrency."""

        work_units = [
            pending[start : start + enricher.batch_size]
            for start in range(0, len(pending), enricher.batch_size)
        ]
        progress = ProgressLogger(
            logger,
            f"Enrichment [{enricher.name}] batch {batch_start}-{batch_end}",
            len(pending),
            interval_seconds=self.progress_interval_seconds,
        )
        completed = 0
        max_workers = min(enricher.max_workers, len(work_units))
        executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"enrichment-{enricher.name}",
        )
        futures: dict[Future[list[ProcessingResult]], list[EnrichmentSubject]] = {}
        remaining = iter(work_units)

        def submit_next() -> bool:
            try:
                unit = next(remaining)
            except StopIteration:
                return False
            future = executor.submit(self._compute_work_unit, enricher, unit)
            futures[future] = unit
            return True

        try:
            for _worker in range(max_workers):
                submit_next()
            while futures:
                complete, _ = wait(futures, return_when=FIRST_COMPLETED)
                for future in complete:
                    unit = futures.pop(future)
                    computed = future.result()
                    stored = list(zip(unit, computed, strict=True))
                    self._store(enricher, stored)
                    for subject, result in stored:
                        self._record_result(enricher, subject, result, summary)
                    completed += len(unit)
                    progress.update(
                        completed,
                        {
                            "succeeded": summary.succeeded,
                            "retryable": summary.retryable_failures,
                            "permanent": summary.permanent_failures,
                        },
                    )
                    submit_next()
        finally:
            for future in futures:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)
        progress.update(
            completed,
            {
                "succeeded": summary.succeeded,
                "retryable": summary.retryable_failures,
                "permanent": summary.permanent_failures,
            },
            force=True,
        )

    @staticmethod
    def _compute_work_unit(
        enricher: Enricher,
        subjects: list[EnrichmentSubject],
    ) -> list[ProcessingResult]:
        try:
            computed = enricher.compute_batch(subjects)
            if len(computed) != len(subjects):
                raise ValueError(
                    f"Enricher {enricher.name} returned {len(computed)} results "
                    f"for {len(subjects)} subjects"
                )
            return computed
        except Exception as exc:
            logger.error("Enricher %s work unit failed: %s", enricher.name, exc)
            return [
                ProcessingResult.retryable(
                    {"error_type": "stage_error", "error": str(exc)}
                )
                for _subject in subjects
            ]

    @staticmethod
    def _record_result(
        enricher: Enricher,
        subject: EnrichmentSubject,
        result: ProcessingResult,
        summary: StageSummary,
    ) -> None:
        if result.succeeded:
            summary.succeeded += 1
            enricher.apply(subject, result.payload)
        elif result.permanent:
            summary.permanent_failures += 1
            summary.missing += 1
        else:
            summary.retryable_failures += 1
            summary.missing += 1

    @staticmethod
    def _is_current(
        enricher: Enricher,
        subject: EnrichmentSubject,
        stored: StoredEnrichment,
    ) -> bool:
        return (
            stored.version == enricher.version
            and stored.input_hash == subject.input_hash
            and stored.config_hash == enricher.config_hash
        )

    def _load(
        self,
        enricher: str,
        subject_keys: list[str],
    ) -> dict[str, StoredEnrichment]:
        if not subject_keys:
            return {}
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    SELECT subject_key, enricher_version, input_hash, config_hash,
                           status, retry_at, payload
                    FROM enrichment_results
                    WHERE enricher = %s
                      AND subject_key = ANY(%s::text[])
                    """,
                    (enricher, subject_keys),
                )
                rows = cursor.fetchall()
        return {
            row["subject_key"]: StoredEnrichment(
                version=row["enricher_version"],
                input_hash=row["input_hash"],
                config_hash=row["config_hash"],
                result=ProcessingResult(
                    ResultStatus(row["status"]),
                    row["payload"],
                    row["retry_at"],
                ),
            )
            for row in rows
        }

    def _store(
        self,
        enricher: Enricher,
        results: list[tuple[EnrichmentSubject, ProcessingResult]],
    ) -> None:
        if not results:
            return
        with self.pool.connection() as connection:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO enrichment_results (
                        enricher, subject_key, enricher_version,
                        input_hash, config_hash, status, retry_at, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (enricher, subject_key) DO UPDATE
                    SET enricher_version = EXCLUDED.enricher_version,
                        input_hash = EXCLUDED.input_hash,
                        config_hash = EXCLUDED.config_hash,
                        status = EXCLUDED.status,
                        retry_at = EXCLUDED.retry_at,
                        payload = EXCLUDED.payload,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    [
                        (
                            enricher.name,
                            subject.key,
                            enricher.version,
                            subject.input_hash,
                            enricher.config_hash,
                            result.status.value,
                            result.retry_at,
                            Jsonb(result.payload),
                        )
                        for subject, result in results
                    ],
                )


def clear_processing_results(pool: ConnectionPool) -> int:
    """Delete recomputable enrichment and extraction results."""

    with pool.connection() as connection:
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute("DELETE FROM enrichment_results")
            enrichment_count = cursor.rowcount
            cursor.execute("DELETE FROM document_extractions")
            extraction_count = cursor.rowcount
    return enrichment_count + extraction_count
