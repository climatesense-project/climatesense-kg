"""Tests for backend-neutral SPARQL requests."""

import asyncio
import importlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
import requests


class StubResponse:
    """Minimal JSON response for a SPARQL SELECT request."""

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return {
            "results": {
                "bindings": [
                    {
                        "tripleCount": {
                            "type": "literal",
                            "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                            "value": "12",
                        }
                    }
                ]
            }
        }


class CountingResponse(StubResponse):
    def __init__(self, value: int) -> None:
        self.value = value

    def json(self) -> dict[str, Any]:
        payload = super().json()
        payload["results"]["bindings"][0]["tripleCount"]["value"] = str(self.value)
        return payload


def test_sends_standard_sparql_query_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name, value in {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "test",
        "POSTGRES_USER": "test",
        "POSTGRES_PASSWORD": "test",
    }.items():
        monkeypatch.setenv(name, value)

    sparql = importlib.import_module("services.analytics_api.services.sparql")
    monkeypatch.setattr(
        sparql,
        "settings",
        SimpleNamespace(
            sparql_endpoint="https://qlever.test",
            sparql_user=None,
            sparql_password=None,
            sparql_timeout_seconds=15,
        ),
    )
    captured: dict[str, Any] = {}

    def fake_post(url: str, **kwargs: Any) -> StubResponse:
        captured["url"] = url
        captured.update(kwargs)
        return StubResponse()

    monkeypatch.setattr(requests, "post", fake_post)

    result = sparql.sparql_select("kg", "triple_volume.rq", use_cache=False)

    assert result == [{"tripleCount": 12}]
    assert captured["url"] == "https://qlever.test"
    assert "SELECT ?graph" in captured["data"]
    assert captured["headers"] == {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/sparql-query",
    }
    assert captured["auth"] is None
    assert captured["timeout"] == 15


def test_expired_sparql_result_is_refreshed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sparql = importlib.import_module("services.analytics_api.services.sparql")
    sparql.clear_cache()
    monkeypatch.setattr(
        sparql,
        "settings",
        SimpleNamespace(
            sparql_endpoint="https://qlever.test",
            sparql_user=None,
            sparql_password=None,
            sparql_timeout_seconds=15,
            result_cache_ttl_seconds=10,
        ),
    )
    responses = iter([CountingResponse(1), CountingResponse(2)])
    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: next(responses))

    first = sparql.sparql_select("kg", "triple_volume.rq")
    cache_key = "kg:triple_volume.rq"
    inserted_at, rows = sparql._RESULT_CACHE[cache_key]
    sparql._RESULT_CACHE[cache_key] = (inserted_at - 11, rows)
    refreshed = sparql.sparql_select("kg", "triple_volume.rq")

    assert first == [{"tripleCount": 1}]
    assert refreshed == [{"tripleCount": 2}]


def test_async_sparql_select_offloads_blocking_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sparql = importlib.import_module("services.analytics_api.services.sparql")
    to_thread = AsyncMock(return_value=[{"count": 1}])
    monkeypatch.setattr(sparql.asyncio, "to_thread", to_thread)

    result = asyncio.run(sparql.sparql_select_async("kg", "core_counts.rq"))

    assert result == [{"count": 1}]
    to_thread.assert_awaited_once_with(
        sparql.sparql_select, "kg", "core_counts.rq", True
    )
