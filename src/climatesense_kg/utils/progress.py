"""Shared progress reporting for long-running pipeline operations."""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
import logging
import time

from .memory import format_process_rss


def format_duration(seconds: float | None) -> str:
    """Format a duration for compact progress logs."""

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


class ProgressLogger:
    """Emit consistent rate-limited progress without retaining subject state."""

    def __init__(
        self,
        logger: logging.Logger,
        label: str,
        total: int,
        *,
        interval_seconds: float = 10.0,
        rate_window_size: int | None = None,
    ) -> None:
        if total < 0:
            raise ValueError("Progress total must be non-negative")
        if interval_seconds < 0:
            raise ValueError("Progress interval must be non-negative")
        if rate_window_size is not None and rate_window_size < 1:
            raise ValueError("Progress rate window must be positive")
        self.logger = logger
        self.label = label
        self.total = total
        self.interval_seconds = interval_seconds
        self.rate_window_size = rate_window_size
        self.started = time.monotonic()
        self.last_logged: float | None = None
        self.last_processed: int | None = None
        self._sampled_at = self.started
        self._sampled_processed = 0
        self._rate_samples: deque[tuple[int, float]] | None = (
            deque(maxlen=rate_window_size) if rate_window_size is not None else None
        )

    def update(
        self,
        processed: int,
        counters: Mapping[str, int] | None = None,
        *,
        force: bool = False,
    ) -> None:
        now = time.monotonic()
        if force and self.last_processed == processed:
            return
        if self._rate_samples is not None and processed > self._sampled_processed:
            self._rate_samples.append(
                (processed - self._sampled_processed, now - self._sampled_at)
            )
            self._sampled_processed = processed
            self._sampled_at = now
        if not (
            force
            or self.last_logged is None
            or processed >= self.total
            or now - self.last_logged >= self.interval_seconds
        ):
            return
        self.last_logged = now
        self.last_processed = processed
        elapsed = max(0.0, now - self.started)
        rate = processed / elapsed if processed and elapsed > 0 else None
        if self._rate_samples:
            window_processed = sum(sample[0] for sample in self._rate_samples)
            window_elapsed = sum(sample[1] for sample in self._rate_samples)
            if window_processed and window_elapsed > 0:
                rate = window_processed / window_elapsed
        remaining = max(0, self.total - processed)
        eta = remaining / rate if rate and remaining else None
        details = ", ".join(
            f"{name}={value}" for name, value in (counters or {}).items()
        )
        if details:
            details = f"; {details}"
        percent = 100.0 if not self.total else 100 * processed / self.total
        self.logger.info(
            "%s: %d/%d (%.1f%%)%s; rate=%s; ETA=%s; RSS=%s",
            self.label,
            processed,
            self.total,
            percent,
            details,
            f"{rate:.2f}/s" if rate is not None else "n/a",
            format_duration(eta),
            format_process_rss(),
        )
