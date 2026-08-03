"""Tests for backend-neutral SPARQL requests."""

import importlib
from types import SimpleNamespace
from typing import Any

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
