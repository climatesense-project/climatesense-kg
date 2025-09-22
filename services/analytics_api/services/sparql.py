"""Lightweight SPARQL query execution helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import requests

from ..config import settings

_QUERY_CACHE: dict[str, str] = {}
_BASE_DIR = Path(__file__).resolve().parent.parent / "queries"


def _load_query(namespace: str, filename: str) -> str:
    cache_key = f"{namespace}:{filename}"
    if cache_key not in _QUERY_CACHE:
        query_path = _BASE_DIR / namespace / filename
        if not query_path.exists():
            raise FileNotFoundError(f"SPARQL query file not found: {query_path}")
        _QUERY_CACHE[cache_key] = query_path.read_text(encoding="utf-8")
    return _QUERY_CACHE[cache_key]


def _binding_to_python(binding: dict[str, Any]) -> Any:
    value = binding.get("value")
    datatype = binding.get("datatype")
    if datatype in {
        "http://www.w3.org/2001/XMLSchema#integer",
        "http://www.w3.org/2001/XMLSchema#decimal",
    }:
        try:
            if value is not None:
                if datatype.endswith("integer"):
                    return int(value)
                return float(value)
            return value
        except (TypeError, ValueError):
            return value
    return value


def sparql_select(namespace: str, filename: str) -> list[dict[str, Any]]:
    """Execute a SELECT SPARQL query and normalize bindings."""

    query = _load_query(namespace, filename)
    auth = None
    if settings.virtuoso_user and settings.virtuoso_password:
        auth = (settings.virtuoso_user, settings.virtuoso_password)

    response = requests.post(
        settings.virtuoso_endpoint,
        data={"query": query},
        headers={"Accept": "application/sparql-results+json"},
        auth=auth,
        timeout=settings.sparql_timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()

    results: list[dict[str, Any]] = []
    for row in payload.get("results", {}).get("bindings", []):
        parsed_row = {key: _binding_to_python(value) for key, value in row.items()}
        results.append(parsed_row)
    return results
