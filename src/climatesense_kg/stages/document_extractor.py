"""Fetch review documents before assigning canonical identity."""

from __future__ import annotations

import logging
import time
from urllib.parse import urlparse

from ..domain import SourceReviewRecord
from ..persistence import StageResult, StageResultKey, StageResultStore
from ..utils.text_processing import (
    TextExtractionResult,
    fetch_and_extract_text,
    redact_url_credentials,
)

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
    ) -> None:
        if rate_limit_delay < 0:
            raise ValueError("rate_limit_delay must be non-negative")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        self.store = store
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self.max_retries = max_retries

    def extract(
        self, record: SourceReviewRecord, *, force: bool = False
    ) -> SourceReviewRecord:
        """Fetch one HTTP document or restore the exact stage result."""

        return self.extract_many([record], force=force)[0]

    def extract_many(
        self, records: list[SourceReviewRecord], *, force: bool = False
    ) -> list[SourceReviewRecord]:
        """Restore state in bulk, then fetch only unresolved HTTP documents."""

        keyed_records = [
            (record, self._key(record))
            for record in records
            if urlparse(record.document.observed_url).scheme in {"http", "https"}
        ]
        stored_results = (
            {}
            if force
            else self.store.get_many([key for _record, key in keyed_records])
        )
        pending: list[tuple[SourceReviewRecord, StageResultKey]] = []
        for record, key in keyed_records:
            stored = stored_results.get(key)
            if stored is None or not stored.success:
                pending.append((record, key))
            else:
                self._apply(record, stored.payload)

        new_results: dict[StageResultKey, StageResult] = {}
        for record, key in pending:
            url = record.document.observed_url
            result = self._fetch(url)
            if result.success:
                payload = {
                    "content": result.content,
                    "final_url": result.final_url,
                    "canonical_url": result.canonical_url,
                }
                self._apply(record, payload)
                new_results[key] = StageResult(success=True, payload=payload)
            else:
                new_results[key] = StageResult(
                    success=False,
                    payload={
                        "url": redact_url_credentials(url),
                        "error_type": (
                            result.error_type.value if result.error_type else "unknown"
                        ),
                        "error_message": result.error_message,
                    },
                )
                logger.warning("Document extraction failed for %s", url)
        self.store.put_many(new_results)
        return records

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
    def _apply(record: SourceReviewRecord, payload: dict[str, object]) -> None:
        content = payload.get("content")
        final_url = payload.get("final_url")
        canonical_url = payload.get("canonical_url")
        record.document.extracted_text = content if isinstance(content, str) else None
        record.document.final_url = final_url if isinstance(final_url, str) else None
        record.document.canonical_url = (
            canonical_url if isinstance(canonical_url, str) else None
        )
