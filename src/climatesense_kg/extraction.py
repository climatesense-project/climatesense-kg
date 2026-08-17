"""Bounded document extraction directly against authoritative database state."""

from __future__ import annotations

from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
import logging
import time
from typing import Any
from urllib.parse import urlparse

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .identity.fingerprints import fingerprint_text
from .processing import ProcessingResult, ResultStatus, StageSummary, stable_hash
from .utils.progress import ProgressLogger
from .utils.text_processing import (
    ExtractionErrorType,
    TextExtractionResult,
    fetch_and_extract_text,
    redact_url_credentials,
)

logger = logging.getLogger(__name__)


class FailureCategory(StrEnum):
    TRANSIENT = "transient"
    ACCESS_BLOCKED = "access_blocked"
    DNS = "dns"
    UNEXTRACTABLE_CONTENT = "unextractable_content"
    PERMANENT = "permanent"


@dataclass(frozen=True)
class RetryPolicy:
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
            raise ValueError("Extraction retry delays must be non-negative")

    def next_retry(
        self,
        category: FailureCategory,
        server_retry_at: datetime | None = None,
    ) -> datetime:
        if server_retry_at is not None:
            return server_retry_at
        delays = {
            FailureCategory.TRANSIENT: self.transient_delay,
            FailureCategory.ACCESS_BLOCKED: self.blocked_delay,
            FailureCategory.DNS: self.dns_delay,
            FailureCategory.UNEXTRACTABLE_CONTENT: self.content_delay,
        }
        return datetime.now(UTC) + delays[category]


@dataclass(frozen=True)
class DocumentTarget:
    key: str
    url: str


@dataclass(frozen=True)
class StoredExtraction:
    version: str
    input_hash: str
    config_hash: str
    result: ProcessingResult


@dataclass
class _HostCooldown:
    retry_at: datetime
    deferred: int = 0


class DocumentExtractionService:
    """Extract each active normalized document URL at most once per need."""

    name = "document.extract"
    version = "1"

    def __init__(
        self,
        pool: ConnectionPool,
        *,
        batch_size: int = 500,
        max_workers: int = 32,
        rate_limit_delay: float = 0.5,
        timeout: int = 15,
        max_retries: int = 2,
        retry_policy: RetryPolicy | None = None,
        progress_interval_seconds: float = 10.0,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("Extraction batch size must be positive")
        if max_workers <= 0:
            raise ValueError("Extraction worker count must be positive")
        if rate_limit_delay < 0:
            raise ValueError("Extraction rate limit must be non-negative")
        if timeout <= 0:
            raise ValueError("Extraction timeout must be positive")
        if max_retries < 0:
            raise ValueError("Extraction retries must be non-negative")
        self.pool = pool
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_policy = retry_policy or RetryPolicy()
        self.progress_interval_seconds = progress_interval_seconds
        self.input_config_hash = stable_hash({})
        self._host_cooldowns: dict[str, _HostCooldown] = {}

    def run(self, *, offline: bool = False, force: bool = False) -> StageSummary:
        """Restore current results and optionally fetch missing or due URLs."""

        self._host_cooldowns.clear()
        total = self._target_count()
        summary = StageSummary(
            self.name, eligible=total, available=None if offline else True
        )
        progress = ProgressLogger(
            logger,
            "Document extraction",
            total,
            interval_seconds=self.progress_interval_seconds,
        )
        processed = 0
        cursor_name = f"document_extraction_{int(time.time() * 1_000_000)}"
        with self.pool.connection() as connection:
            with (
                connection.transaction(),
                connection.cursor(name=cursor_name, row_factory=dict_row) as cursor,
            ):
                cursor.execute(
                    """
                    SELECT document_key, MIN(observed_url) AS requested_url
                    FROM source_observations
                    WHERE active
                      AND (
                        observed_url LIKE 'http://%'
                        OR observed_url LIKE 'https://%'
                      )
                    GROUP BY document_key
                    -- A lexical URL order groups each batch by hostname and can
                    -- leave all but one worker idle. A stable hash distributes
                    -- hosts while retaining deterministic traversal.
                    ORDER BY hashtextextended(document_key, 0), document_key
                    """
                )
                while rows := cursor.fetchmany(self.batch_size):
                    targets = [
                        DocumentTarget(row["document_key"], row["requested_url"])
                        for row in rows
                    ]
                    summary.merge(
                        self._process_batch(targets, offline=offline, force=force)
                    )
                    processed += len(targets)
                    progress.update(
                        processed,
                        {
                            "cached": summary.cached,
                            "fetched": summary.succeeded,
                            "retryable": summary.retryable_failures,
                            "permanent": summary.permanent_failures,
                        },
                    )
        progress.update(
            processed,
            {
                "cached": summary.cached,
                "fetched": summary.succeeded,
                "retryable": summary.retryable_failures,
                "permanent": summary.permanent_failures,
            },
            force=True,
        )
        deferred = sum(item.deferred for item in self._host_cooldowns.values())
        if deferred:
            logger.info(
                "Document extraction deferred %d URLs across %d throttled hosts",
                deferred,
                len(self._host_cooldowns),
            )
        return summary

    def _target_count(self) -> int:
        with self.pool.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(DISTINCT document_key)
                FROM source_observations
                WHERE active
                  AND (
                    observed_url LIKE 'http://%'
                    OR observed_url LIKE 'https://%'
                  )
                """
            )
            row = cursor.fetchone()
        return int(row[0]) if row else 0

    def _process_batch(
        self,
        targets: list[DocumentTarget],
        *,
        offline: bool,
        force: bool,
    ) -> StageSummary:
        summary = StageSummary(self.name, available=None if offline else True)
        stored = {} if force else self._load([target.key for target in targets])
        pending: list[DocumentTarget] = []
        now = datetime.now(UTC)
        for target in targets:
            current = stored.get(target.key)
            expected_input = stable_hash({"url": target.key})
            if current is None or not self._is_current(current, expected_input):
                pending.append(target)
                continue
            result = current.result
            if result.succeeded:
                summary.cached += 1
            elif result.permanent:
                summary.permanent_failures += 1
                summary.missing += 1
            elif result.deferred(now):
                summary.retryable_failures += 1
                summary.missing += 1
            else:
                pending.append(target)

        if not pending:
            return summary
        if offline:
            summary.missing += len(pending)
            return summary

        with ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="document-extraction",
        ) as executor:
            computed = self._compute_many(pending, executor)
        self._store(list(zip(pending, computed, strict=True)))
        for result in computed:
            if result.succeeded:
                summary.succeeded += 1
            elif result.permanent:
                summary.permanent_failures += 1
                summary.missing += 1
            else:
                summary.retryable_failures += 1
                summary.missing += 1
        return summary

    def _load(self, keys: list[str]) -> dict[str, StoredExtraction]:
        if not keys:
            return {}
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    SELECT document_key, extractor_version, input_hash, config_hash,
                           status, retry_at, final_url, canonical_url, content,
                           normalized_text_hash, word_count, failure_category,
                           http_status, error_type, error_message, request_attempted
                    FROM document_extractions
                    WHERE document_key = ANY(%s::text[])
                    """,
                    (keys,),
                )
                rows = cursor.fetchall()
        loaded: dict[str, StoredExtraction] = {}
        for row in rows:
            status = ResultStatus(row["status"])
            payload = (
                {
                    "content": row["content"],
                    "final_url": row["final_url"],
                    "canonical_url": row["canonical_url"],
                    "normalized_text_hash": row["normalized_text_hash"],
                    "word_count": row["word_count"],
                }
                if status is ResultStatus.SUCCESS
                else {
                    "failure_category": row["failure_category"],
                    "http_status": row["http_status"],
                    "error_type": row["error_type"],
                    "error_message": row["error_message"],
                    "request_attempted": row["request_attempted"],
                }
            )
            loaded[row["document_key"]] = StoredExtraction(
                version=row["extractor_version"],
                input_hash=row["input_hash"],
                config_hash=row["config_hash"],
                result=ProcessingResult(status, payload, row["retry_at"]),
            )
        return loaded

    def _is_current(self, stored: StoredExtraction, input_hash: str) -> bool:
        return (
            stored.version == self.version
            and stored.input_hash == input_hash
            and stored.config_hash == self.input_config_hash
        )

    def _store(
        self,
        computed: list[tuple[DocumentTarget, ProcessingResult]],
    ) -> None:
        if not computed:
            return
        rows: list[tuple[Any, ...]] = []
        for target, result in computed:
            payload = result.payload
            content = payload.get("content") if result.succeeded else None
            fingerprint = fingerprint_text(
                content if isinstance(content, str) else None
            )
            rows.append(
                (
                    target.key,
                    target.url,
                    payload.get("final_url") if result.succeeded else None,
                    payload.get("canonical_url") if result.succeeded else None,
                    content,
                    fingerprint.normalized_text_hash,
                    fingerprint.word_count,
                    self.version,
                    stable_hash({"url": target.key}),
                    self.input_config_hash,
                    result.status.value,
                    result.retry_at,
                    payload.get("failure_category"),
                    payload.get("http_status"),
                    payload.get("error_type"),
                    payload.get("error_message"),
                    bool(payload.get("request_attempted", result.succeeded)),
                )
            )
        with self.pool.connection() as connection:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO document_extractions (
                        document_key, requested_url, final_url, canonical_url,
                        content, normalized_text_hash, word_count,
                        extractor_version, input_hash, config_hash, status,
                        retry_at, failure_category, http_status, error_type,
                        error_message, request_attempted
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (document_key) DO UPDATE
                    SET requested_url = EXCLUDED.requested_url,
                        final_url = EXCLUDED.final_url,
                        canonical_url = EXCLUDED.canonical_url,
                        content = EXCLUDED.content,
                        normalized_text_hash = EXCLUDED.normalized_text_hash,
                        word_count = EXCLUDED.word_count,
                        extractor_version = EXCLUDED.extractor_version,
                        input_hash = EXCLUDED.input_hash,
                        config_hash = EXCLUDED.config_hash,
                        status = EXCLUDED.status,
                        retry_at = EXCLUDED.retry_at,
                        failure_category = EXCLUDED.failure_category,
                        http_status = EXCLUDED.http_status,
                        error_type = EXCLUDED.error_type,
                        error_message = EXCLUDED.error_message,
                        request_attempted = EXCLUDED.request_attempted,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    rows,
                )

    def _compute_many(
        self,
        targets: list[DocumentTarget],
        executor: ThreadPoolExecutor,
    ) -> list[ProcessingResult]:
        by_host: dict[str, deque[tuple[int, DocumentTarget]]] = defaultdict(deque)
        for index, target in enumerate(targets):
            by_host[urlparse(target.url).hostname or ""].append((index, target))
        results: list[ProcessingResult | None] = [None] * len(targets)
        futures: dict[Future[ProcessingResult], tuple[int, str]] = {}

        def defer_host(host: str) -> None:
            cooldown = self._host_cooldowns[host]
            while by_host[host]:
                index, target = by_host[host].popleft()
                results[index] = self._cooldown_result(
                    target,
                    host=host,
                    retry_at=cooldown.retry_at,
                )
                cooldown.deferred += 1

        ready = deque[str]()
        for host in by_host:
            if host in self._host_cooldowns:
                defer_host(host)
            else:
                ready.append(host)

        def submit(host: str) -> None:
            index, target = by_host[host].popleft()
            futures[executor.submit(self._compute, target)] = (index, host)

        try:
            while ready and len(futures) < self.max_workers:
                submit(ready.popleft())
            while futures:
                complete, _ = wait(futures, return_when=FIRST_COMPLETED)
                for future in complete:
                    index, host = futures.pop(future)
                    result = future.result()
                    results[index] = result
                    if result.payload.get("http_status") == 429:
                        retry_at = result.retry_at or self.retry_policy.next_retry(
                            FailureCategory.TRANSIENT
                        )
                        self._host_cooldowns[host] = _HostCooldown(retry_at)
                        logger.warning(
                            "Document extraction paused for %s until %s after HTTP 429",
                            host,
                            retry_at.isoformat(),
                        )
                        defer_host(host)
                    elif by_host[host]:
                        ready.append(host)
                while ready and len(futures) < self.max_workers:
                    submit(ready.popleft())
        finally:
            for future in futures:
                future.cancel()
        if any(result is None for result in results):
            raise RuntimeError("Document extraction batch did not complete")
        return [result for result in results if result is not None]

    def _compute(self, target: DocumentTarget) -> ProcessingResult:
        fetched = self._fetch(target.url)
        if fetched.success:
            return ProcessingResult.success(
                {
                    "content": fetched.content,
                    "final_url": fetched.final_url,
                    "canonical_url": fetched.canonical_url,
                }
            )
        payload: dict[str, Any] = {
            "url": redact_url_credentials(target.url),
            "error_type": fetched.error_type.value if fetched.error_type else "unknown",
            "error_message": fetched.error_message,
            "request_attempted": True,
        }
        if fetched.http_status is not None:
            payload["http_status"] = fetched.http_status
        return self._classify_failure(fetched, payload)

    def _fetch(self, url: str) -> TextExtractionResult:
        result = TextExtractionResult(False, error_message="No attempt made")
        for attempt in range(self.max_retries + 1):
            result = fetch_and_extract_text(url, timeout=self.timeout)
            if result.success or not result.retryable_immediately:
                break
            if attempt < self.max_retries:
                time.sleep(min(2**attempt, 2))
        time.sleep(self.rate_limit_delay)
        return result

    def _classify_failure(
        self,
        result: TextExtractionResult,
        payload: dict[str, Any],
    ) -> ProcessingResult:
        error_type = result.error_type or ExtractionErrorType.UNKNOWN
        if error_type in {
            ExtractionErrorType.INVALID_INPUT,
            ExtractionErrorType.INVALID_URL,
            ExtractionErrorType.RESPONSE_TOO_LARGE,
            ExtractionErrorType.UNSUPPORTED_CONTENT,
        }:
            payload["failure_category"] = FailureCategory.PERMANENT.value
            return ProcessingResult.permanent_failure(payload)
        if error_type is ExtractionErrorType.HTTP_ERROR:
            status = result.http_status
            if status in {401, 403}:
                return self._retryable(payload, FailureCategory.ACCESS_BLOCKED)
            if status in {404, 410} or (
                status is not None
                and 400 <= status < 500
                and status not in {408, 425, 429}
            ):
                payload["failure_category"] = FailureCategory.PERMANENT.value
                return ProcessingResult.permanent_failure(payload)
            return self._retryable(
                payload,
                FailureCategory.TRANSIENT,
                retry_at=result.retry_at,
            )
        categories = {
            ExtractionErrorType.ACCESS_CHALLENGE: FailureCategory.ACCESS_BLOCKED,
            ExtractionErrorType.DNS: FailureCategory.DNS,
            ExtractionErrorType.EXTRACTION_FAILED: FailureCategory.UNEXTRACTABLE_CONTENT,
        }
        return self._retryable(
            payload,
            categories.get(error_type, FailureCategory.TRANSIENT),
            retry_at=result.retry_at,
        )

    def _retryable(
        self,
        payload: dict[str, Any],
        category: FailureCategory,
        *,
        retry_at: datetime | None = None,
    ) -> ProcessingResult:
        payload["failure_category"] = category.value
        return ProcessingResult.retryable(
            payload,
            retry_at=self.retry_policy.next_retry(category, retry_at),
        )

    @staticmethod
    def _cooldown_result(
        target: DocumentTarget,
        *,
        host: str,
        retry_at: datetime,
    ) -> ProcessingResult:
        return ProcessingResult.retryable(
            {
                "url": redact_url_credentials(target.url),
                "error_type": "host_cooldown",
                "error_message": (
                    f"Request deferred because {host} returned HTTP 429 earlier "
                    "in this extraction run"
                ),
                "failure_category": FailureCategory.TRANSIENT.value,
                "http_status": 429,
                "request_attempted": False,
            },
            retry_at=retry_at,
        )
