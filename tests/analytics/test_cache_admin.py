"""Tests for analytics cache administration controls."""

import importlib
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

from fastapi import HTTPException
import pytest


@pytest.fixture
def cache_router(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    for name, value in {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "test",
        "POSTGRES_USER": "test",
        "POSTGRES_PASSWORD": "test",
    }.items():
        monkeypatch.setenv(name, value)
    module = importlib.import_module("services.analytics_api.routers.cache")
    credential = "admin-token"
    monkeypatch.setattr(
        module,
        "settings",
        SimpleNamespace(admin_token=credential, admin_rate_limit_seconds=10),
    )
    module._LAST_ADMIN_REQUESTS.clear()
    return module


def _request(authorization: str) -> Mock:
    return Mock(
        headers={"Authorization": authorization},
        client=SimpleNamespace(host="127.0.0.1"),
    )


def test_cache_mutations_require_admin_bearer_token(cache_router) -> None:
    with pytest.raises(HTTPException) as exc_info:
        cache_router._require_admin(_request("Bearer wrong-token"))

    assert isinstance(exc_info.value, HTTPException)
    assert exc_info.value.status_code == 401


def test_cache_mutations_are_rate_limited(cache_router) -> None:
    request = _request("Bearer admin-token")
    cache_router._require_admin(request)

    with pytest.raises(HTTPException) as exc_info:
        cache_router._require_admin(request)

    assert isinstance(exc_info.value, HTTPException)
    assert exc_info.value.status_code == 429
