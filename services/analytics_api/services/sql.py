"""Helpers for running parameterised SQL analytics queries."""

from __future__ import annotations

from pathlib import Path
import time
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import DateTime, Integer, String

_QUERY_CACHE: dict[str, str] = {}
_RESULT_CACHE: dict[str, tuple[list[dict[str, Any]], float]] = {}
_BASE_DIR = Path(__file__).resolve().parent.parent / "queries"

CACHE_TTL = 86400  # 24 hours


def _load_query(namespace: str, filename: str) -> str:
    """Load and cache a query file from disk."""
    cache_key = f"{namespace}:{filename}"
    if cache_key not in _QUERY_CACHE:
        query_path = _BASE_DIR / namespace / filename
        if not query_path.exists():
            raise FileNotFoundError(f"Query file not found: {query_path}")
        _QUERY_CACHE[cache_key] = query_path.read_text(encoding="utf-8")
    return _QUERY_CACHE[cache_key]


async def run_query(
    session: AsyncSession,
    namespace: str,
    filename: str,
    params: dict[str, Any] | None = None,
    use_cache: bool = True,
) -> list[dict[str, Any]]:
    """Execute a query and return rows as plain dictionaries."""
    cache_key = f"{namespace}:{filename}"

    if use_cache and cache_key in _RESULT_CACHE:
        cached_result, cached_time = _RESULT_CACHE[cache_key]
        if time.time() - cached_time < CACHE_TTL:
            return cached_result

    raw_sql = _load_query(namespace, filename)
    stmt = text(raw_sql)

    type_hints: dict[str, Any] = {
        "step": String(),
        "from_ts": DateTime(timezone=True),
        "to_ts": DateTime(timezone=True),
        "limit": Integer(),
    }

    if params:
        for key, sa_type in type_hints.items():
            if key in params:
                stmt = stmt.bindparams(bindparam(key, type_=sa_type))

    result = await session.execute(stmt, params or {})
    rows = [dict(row) for row in result.mappings().all()]

    if use_cache:
        _RESULT_CACHE[cache_key] = (rows, time.time())

    return rows


def clear_cache() -> int:
    entries_removed = len(_RESULT_CACHE)
    _RESULT_CACHE.clear()
    return entries_removed
