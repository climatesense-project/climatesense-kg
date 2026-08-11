"""Fetch review documents before assigning canonical identity."""

from __future__ import annotations

from collections import defaultdict
import logging
import time
from typing import Any
from urllib.parse import urlparse

from ..domain import SourceReviewRecord
from ..persistence import StageResult, StageResultKey, StageResultStore
from ..utils.text_processing import (
    TextExtractionResult,
    fetch_and_extract_text,
    redact_url_credentials,
)
from .persisted import StageExecutionReport, StageProgress, execute_persisted_stage

logger = logging.getLogger(__name__)


class DocumentExtractor:
    """Populate document evidence using versioned, input-aware stage state."""

    name = "document.extract"
    version = "1"

    def __init__(
        self,
        store: StageResultStore,
        *,
        rate_limit_delay: float = 0.5,
        timeout: int = 15,
        max_retries: int = 2,
        checkpoint_size: int = 25,
        progress_interval_seconds: float = 10.0,
    ) -> None:
        if rate_limit_delay < 0:
            raise ValueError("rate_limit_delay must be non-negative")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if checkpoint_size <= 0:
            raise ValueError("checkpoint_size must be greater than zero")
        if progress_interval_seconds < 0:
            raise ValueError("progress_interval_seconds must be non-negative")
        self.store = store
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.checkpoint_size = checkpoint_size
        self.progress_interval_seconds = progress_interval_seconds

    def extract(
        self, record: SourceReviewRecord, *, force: bool = False
    ) -> SourceReviewRecord:
        """Fetch one HTTP document or restore the exact stage result."""

        self.extract_many([record], force=force)
        return record

    def extract_many(
        self, records: list[SourceReviewRecord], *, force: bool = False
    ) -> StageExecutionReport:
        """Restore HTTP documents, checkpoint new results, and report progress."""

        records_by_key: dict[StageResultKey, list[SourceReviewRecord]] = defaultdict(
            list
        )
        for record in records:
            if urlparse(record.document.observed_url).scheme in {"http", "https"}:
                records_by_key[self._key(record)].append(record)

        last_logged_elapsed: float | None = None

        def log_progress(progress: StageProgress) -> None:
            nonlocal last_logged_elapsed
            should_log = (
                last_logged_elapsed is None
                or progress.remaining_subjects == 0
                or progress.elapsed_seconds - last_logged_elapsed
                >= self.progress_interval_seconds
            )
            if not should_log:
                return
            last_logged_elapsed = progress.elapsed_seconds
            rate = progress.computation_rate
            rate_text = f"{rate:.2f}/s" if rate is not None else "n/a"
            eta_text = self._format_duration(progress.eta_seconds)
            logger.info(
                "Document extraction: %d/%d processed (%.1f%%); "
                "restored=%d, stored_failures=%d, fetched=%d, failed=%d; "
                "rate=%s; ETA=%s",
                progress.processed_subjects,
                progress.eligible_subjects,
                progress.percent_complete,
                progress.stored_successes,
                progress.stored_failures,
                progress.computed_successes,
                progress.computed_failures,
                rate_text,
                eta_text,
            )

        def apply_group(
            matching_records: list[SourceReviewRecord], payload: dict[str, Any]
        ) -> None:
            for record in matching_records:
                self._apply(record, payload)

        return execute_persisted_stage(
            stage_name=self.name,
            subjects=dict(records_by_key),
            store=self.store,
            compute_many=lambda groups: [self._compute(group[0]) for group in groups],
            apply_result=apply_group,
            force=force,
            stage_logger=logger,
            compute_batch_size=1,
            checkpoint_size=self.checkpoint_size,
            progress_callback=log_progress,
        )

    def _compute(self, record: SourceReviewRecord) -> StageResult:
        url = record.document.observed_url
        result = self._fetch(url)
        if result.success:
            return StageResult(
                success=True,
                payload={
                    "content": result.content,
                    "final_url": result.final_url,
                    "canonical_url": result.canonical_url,
                },
            )
        logger.warning("Document extraction failed for %s", url)
        return StageResult(
            success=False,
            payload={
                "url": redact_url_credentials(url),
                "error_type": (
                    result.error_type.value if result.error_type else "unknown"
                ),
                "error_message": result.error_message,
            },
        )

    def _key(self, record: SourceReviewRecord) -> StageResultKey:
        return StageResultKey.build(
            subject_key=record.source.record_key,
            stage_name=self.name,
            stage_version=self.version,
            input_value={"url": record.document.observed_url},
            config_value={},
        )

    def _fetch(self, url: str) -> TextExtractionResult:
        result = TextExtractionResult(success=False, error_message="No attempt made")
        for attempt in range(self.max_retries + 1):
            result = fetch_and_extract_text(url, timeout=self.timeout)
            if result.success:
                break
            if not result.error_type or not result.error_type.is_retryable:
                break
            if attempt < self.max_retries:
                time.sleep(min(2**attempt, 2))
        time.sleep(self.rate_limit_delay)
        return result

    @staticmethod
    def _apply(record: SourceReviewRecord, payload: dict[str, Any]) -> None:
        content = payload.get("content")
        final_url = payload.get("final_url")
        canonical_url = payload.get("canonical_url")
        record.document.extracted_text = content if isinstance(content, str) else None
        record.document.final_url = final_url if isinstance(final_url, str) else None
        record.document.canonical_url = (
            canonical_url if isinstance(canonical_url, str) else None
        )

    @staticmethod
    def _format_duration(seconds: float | None) -> str:
        if seconds is None:
            return "n/a"
        rounded = max(0, round(seconds))
        minutes, remaining_seconds = divmod(rounded, 60)
        hours, remaining_minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}h {remaining_minutes}m"
        if minutes:
            return f"{minutes}m {remaining_seconds}s"
        return f"{remaining_seconds}s"
