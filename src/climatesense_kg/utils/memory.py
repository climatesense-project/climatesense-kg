"""Lightweight process-memory diagnostics without optional dependencies."""

from __future__ import annotations

import os
from pathlib import Path
import resource
import sys


def process_rss_bytes() -> int:
    """Return current RSS on Linux and peak RSS on other supported platforms."""

    statm = Path("/proc/self/statm")
    if statm.exists():
        fields = statm.read_text(encoding="ascii").split()
        if len(fields) >= 2:
            return int(fields[1]) * os.sysconf("SC_PAGE_SIZE")
    maximum = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(maximum if sys.platform == "darwin" else maximum * 1024)


def format_bytes(value: int) -> str:
    """Format a byte count for compact operational logs."""

    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TiB"


def format_process_rss() -> str:
    """Return a compact process RSS label."""

    return format_bytes(process_rss_bytes())
