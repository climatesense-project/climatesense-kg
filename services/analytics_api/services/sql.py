"""Helpers for running parameterised SQL analytics queries."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import DateTime, Integer, String

_QUERY_CACHE: dict[str, str] = {}
_BASE_DIR = Path(__file__).resolve().parent.parent / "queries"


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
) -> list[dict[str, Any]]:
    """Execute a query and return rows as plain dictionaries."""

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
    return [dict(row) for row in result.mappings().all()]
