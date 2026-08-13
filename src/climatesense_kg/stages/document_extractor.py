"""Fetch review documents before assigning canonical identity."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
import logging
import time
from typing import Any
from urllib.parse import urlparse

from ..domain import SourceReviewRecord
from ..persistence import StageResult, StageResultKey, StageResultStore
from ..utils.text_processing import (
    ExtractionErrorType,
    TextExtractionResult,
    fetch_and_extract_text,
    normalize_document_url,
    redact_url_credentials,
)
from .persisted import (
    StageExecutionPolicy,
    StageExecutionReport,
    StageProgressLogger,
    execute_persisted_stage,
)

logger = logging.getLogger(__name__)


class DocumentFailureCategory(StrEnum):
    """Operational categories that determine extraction retry behavior."""

    TRANSIENT = "transient"
    ACCESS_BLOCKED = "access_blocked"
    DNS = "dns"
    UNEXTRACTABLE_CONTENT = "unextractable_content"
    PERMANENT = "permanent"


@dataclass(frozen=True)
class DocumentRetryPolicy:
    """Cooldowns applied after document-extraction failures."""

    transient_delay: timedelta = timedelta(hours=1)
    blocked_delay: timedelta = timedelta(days=30)
    dns_delay: timedelta = timedelta(days=7)
    content_delay: timedelta = timedelta(days=30)

    def __post_init__(self) -> None:
        if any(
            delay < timedelta(0)
            for delay in (
                self.transient_delay,
                self.blocked_delay,
                self.dns_delay,
                self.content_delay,
            )
        ):
            raise ValueError("Document extraction retry delays must be non-negative")

    def retry_at(
        self,
        category: DocumentFailureCategory,
        server_retry_at: datetime | None = None,
    ) -> datetime:
        """Return the next eligible retry time for a non-permanent failure."""

        if server_retry_at is not None:
            return server_retry_at
        delays = {
            DocumentFailureCategory.TRANSIENT: self.transient_delay,
            DocumentFailureCategory.ACCESS_BLOCKED: self.blocked_delay,
            DocumentFailureCategory.DNS: self.dns_delay,
            DocumentFailureCategory.UNEXTRACTABLE_CONTENT: self.content_delay,
        }
        return datetime.now(UTC) + delays[category]


@dataclass
class _HostCooldown:
    """Run-local state for a host that rejected document extraction."""

    retry_at: datetime
    deferred_documents: int = 0


class DocumentExtractor:
    """Populate document evidence using versioned, input-aware stage state."""

    name = "document.extract"
    version = "1"

    def __init__(
        self,
        store: StageResultStore,
        *,
        max_workers: int = 32,
        rate_limit_delay: float = 0.5,
        timeout: int = 15,
        max_retries: int = 2,
        retry_policy: DocumentRetryPolicy | None = None,
        checkpoint_size: int = 25,
        progress_interval_seconds: float = 10.0,
    ) -> None:
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than zero")
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
        self.max_workers = max_workers
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_policy = retry_policy or DocumentRetryPolicy()
        self.checkpoint_size = checkpoint_size
        self.progress_interval_seconds = progress_interval_seconds
        self._host_cooldowns: dict[str, _HostCooldown] = {}

    def start_run(self) -> None:
        """Reset host cooldowns before one complete extraction pass."""

        self._host_cooldowns.clear()

    def extract(
        self, record: SourceReviewRecord, *, force: bool = False
    ) -> SourceReviewRecord:
        """Fetch one HTTP document or restore the exact stage result."""

        self.extract_many([record], force=force)
        return record

    def extract_many(
        self,
        records: list[SourceReviewRecord],
        *,
        force: bool = False,
        stored_only: bool = False,
        report_progress: bool = True,
    ) -> StageExecutionReport:
        """Restore HTTP documents and optionally fetch and checkpoint missing ones."""

        records_by_url: dict[str, list[SourceReviewRecord]] = defaultdict(list)
        for record in records:
            if urlparse(record.document.observed_url).scheme in {"http", "https"}:
                records_by_url[self._url_key(record)].append(record)

        subjects = {
            self._key(matching_records[0]): matching_records
            for matching_records in records_by_url.values()
        }
        subjects = dict(self._interleave_hosts(subjects.items()))
        duplicate_records = sum(len(group) - 1 for group in subjects.values())
        if duplicate_records:
            logger.debug(
                "Document extraction: grouped %d duplicate records into %d "
                "unique document URLs",
                duplicate_records,
                len(subjects),
            )

        def apply_group(
            matching_records: list[SourceReviewRecord], payload: dict[str, Any]
        ) -> None:
            for record in matching_records:
                self._apply(record, payload)

        with ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="document-extraction",
        ) as executor:
            report = execute_persisted_stage(
                stage_name=self.name,
                subjects=subjects,
                store=self.store,
                compute_many=lambda groups: self._compute_many(
                    groups,
                    executor,
                    self._host_cooldowns,
                ),
                apply_result=apply_group,
                policy=(
                    StageExecutionPolicy.STORED_ONLY
                    if stored_only
                    else StageExecutionPolicy.COMPUTE
                ),
                force=force if not stored_only else False,
                stage_logger=logger,
                compute_batch_size=(
                    1 if self.max_workers == 1 else self.max_workers * 2
                ),
                checkpoint_size=self.checkpoint_size,
                progress_callback=(
                    StageProgressLogger(
                        logger,
                        label="Document extraction",
                        interval_seconds=self.progress_interval_seconds,
                        success_label="fetched",
                        failure_label="failed",
                        include_stage_name=False,
                    )
                    if report_progress
                    else None
                ),
            )
        deferred_documents = sum(
            cooldown.deferred_documents for cooldown in self._host_cooldowns.values()
        )
        if deferred_documents:
            logger.info(
                "Document extraction: deferred %d unattempted documents across "
                "%d rate-limited hosts",
                deferred_documents,
                len(self._host_cooldowns),
            )
        return report

    def _compute_many(
        self,
        groups: list[list[SourceReviewRecord]],
        executor: ThreadPoolExecutor,
        host_cooldowns: dict[str, _HostCooldown],
    ) -> list[StageResult]:
        """Extract concurrently, serializing and cooling down individual hosts."""

        by_host: dict[str, deque[tuple[int, list[SourceReviewRecord]]]] = defaultdict(
            deque
        )
        for index, group in enumerate(groups):
            by_host[self._hostname(group[0])].append((index, group))
        results: list[StageResult | None] = [None] * len(groups)
        futures: dict[Future[StageResult], tuple[int, str]] = {}

        def defer_queued(host: str) -> None:
            cooldown = host_cooldowns[host]
            while by_host[host]:
                index, group = by_host[host].popleft()
                results[index] = self._host_cooldown_result(
                    group[0],
                    host=host,
                    retry_at=cooldown.retry_at,
                )
                cooldown.deferred_documents += 1

        ready_hosts: deque[str] = deque()
        for host in by_host:
            if host in host_cooldowns:
                defer_queued(host)
            else:
                ready_hosts.append(host)

        def submit(host: str) -> None:
            index, group = by_host[host].popleft()
            future = executor.submit(self._compute, group[0])
            futures[future] = (index, host)

        try:
            while ready_hosts and len(futures) < self.max_workers:
                submit(ready_hosts.popleft())
            while futures:
                completed, _pending = wait(futures, return_when=FIRST_COMPLETED)
                for future in completed:
                    index, host = futures.pop(future)
                    result = future.result()
                    results[index] = result
                    if result.payload.get("http_status") == 429:
                        retry_at = result.retry_at or self.retry_policy.retry_at(
                            DocumentFailureCategory.TRANSIENT
                        )
                        host_cooldowns[host] = _HostCooldown(retry_at=retry_at)
                        logger.warning(
                            "Document extraction paused for host %s until %s after "
                            "HTTP 429",
                            host,
                            retry_at.isoformat(),
                        )
                        defer_queued(host)
                    elif by_host[host]:
                        ready_hosts.append(host)
                while ready_hosts and len(futures) < self.max_workers:
                    submit(ready_hosts.popleft())
        finally:
            for future in futures:
                future.cancel()

        if any(result is None for result in results):  # pragma: no cover
            raise RuntimeError("Document extraction batch did not complete")
        return [result for result in results if result is not None]

    @staticmethod
    def _host_cooldown_result(
        record: SourceReviewRecord,
        *,
        host: str,
        retry_at: datetime,
    ) -> StageResult:
        """Defer a document without requesting a host already returning 429."""

        return StageResult.retryable_failure(
            {
                "url": redact_url_credentials(record.document.observed_url),
                "error_type": "host_cooldown",
                "error_message": (
                    f"Request deferred because {host} returned HTTP 429 earlier "
                    "in this extraction run"
                ),
                "failure_category": DocumentFailureCategory.TRANSIENT.value,
                "http_status": 429,
                "request_attempted": False,
            },
            retry_at=retry_at,
        )

    @classmethod
    def _interleave_hosts(
        cls,
        subjects: Iterable[tuple[StageResultKey, list[SourceReviewRecord]]],
    ) -> list[tuple[StageResultKey, list[SourceReviewRecord]]]:
        """Round-robin documents by host so each worker batch spans sites."""

        by_host: dict[str, deque[tuple[StageResultKey, list[SourceReviewRecord]]]] = (
            defaultdict(deque)
        )
        for key, matching_records in subjects:
            by_host[cls._hostname(matching_records[0])].append((key, matching_records))

        hosts = deque(by_host)
        ordered: list[tuple[StageResultKey, list[SourceReviewRecord]]] = []
        while hosts:
            host = hosts.popleft()
            queue = by_host[host]
            ordered.append(queue.popleft())
            if queue:
                hosts.append(host)
        return ordered

    @staticmethod
    def _hostname(record: SourceReviewRecord) -> str:
        return urlparse(record.document.observed_url).hostname or ""

    @staticmethod
    def _url_key(record: SourceReviewRecord) -> str:
        observed_url = record.document.observed_url
        return normalize_document_url(observed_url) or observed_url

    def _compute(self, record: SourceReviewRecord) -> StageResult:
        url = record.document.observed_url
        result = self._fetch(url)
        if result.success:
            return StageResult.succeeded(
                {
                    "content": result.content,
                    "final_url": result.final_url,
                    "canonical_url": result.canonical_url,
                },
            )
        logger.warning("Document extraction failed for %s", url)
        payload: dict[str, Any] = {
            "url": redact_url_credentials(url),
            "error_type": (result.error_type.value if result.error_type else "unknown"),
            "error_message": result.error_message,
            "request_attempted": True,
        }
        if result.http_status is not None:
            payload["http_status"] = result.http_status
        return self._classify_failure(result, payload)

    def _classify_failure(
        self,
        result: TextExtractionResult,
        payload: dict[str, Any],
    ) -> StageResult:
        """Map extraction evidence to durable retry behavior."""

        error_type = result.error_type or ExtractionErrorType.UNKNOWN
        if error_type in {
            ExtractionErrorType.INVALID_INPUT,
            ExtractionErrorType.INVALID_URL,
            ExtractionErrorType.RESPONSE_TOO_LARGE,
            ExtractionErrorType.UNSUPPORTED_CONTENT,
        }:
            payload["failure_category"] = DocumentFailureCategory.PERMANENT.value
            return StageResult.permanent_failure(payload)

        if error_type is ExtractionErrorType.HTTP_ERROR:
            status = result.http_status
            if status in {401, 403}:
                return self._deferred_failure(
                    payload,
                    category=DocumentFailureCategory.ACCESS_BLOCKED,
                )
            if status in {404, 410} or (
                status is not None
                and 400 <= status < 500
                and status not in {408, 425, 429}
            ):
                payload["failure_category"] = DocumentFailureCategory.PERMANENT.value
                return StageResult.permanent_failure(payload)
            return self._deferred_failure(
                payload,
                category=DocumentFailureCategory.TRANSIENT,
                retry_at=result.retry_at,
            )

        if error_type is ExtractionErrorType.ACCESS_CHALLENGE:
            return self._deferred_failure(
                payload,
                category=DocumentFailureCategory.ACCESS_BLOCKED,
            )
        if error_type is ExtractionErrorType.DNS:
            return self._deferred_failure(
                payload,
                category=DocumentFailureCategory.DNS,
            )
        if error_type is ExtractionErrorType.EXTRACTION_FAILED:
            return self._deferred_failure(
                payload,
                category=DocumentFailureCategory.UNEXTRACTABLE_CONTENT,
            )
        return self._deferred_failure(
            payload,
            category=DocumentFailureCategory.TRANSIENT,
            retry_at=result.retry_at,
        )

    def _deferred_failure(
        self,
        payload: dict[str, Any],
        *,
        category: DocumentFailureCategory,
        retry_at: datetime | None = None,
    ) -> StageResult:
        payload["failure_category"] = category.value
        scheduled_retry = self.retry_policy.retry_at(category, retry_at)
        return StageResult.retryable_failure(payload, retry_at=scheduled_retry)

    def _key(self, record: SourceReviewRecord) -> StageResultKey:
        url = self._url_key(record)
        return StageResultKey.build(
            subject_key=url,
            stage_name=self.name,
            stage_version=self.version,
            input_value={"url": url},
            config_value={},
        )

    def _fetch(self, url: str) -> TextExtractionResult:
        result = TextExtractionResult(success=False, error_message="No attempt made")
        for attempt in range(self.max_retries + 1):
            result = fetch_and_extract_text(url, timeout=self.timeout)
            if result.success:
                break
            if not result.retryable_immediately:
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
