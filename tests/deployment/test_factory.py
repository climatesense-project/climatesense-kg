"""Tests for deployment handler construction."""

import pytest

from climatesense_kg.config.schemas import DeploymentConfig
from climatesense_kg.deployment.factory import create_deployment_handler
from climatesense_kg.deployment.virtuoso import VirtuosoDeploymentHandler


def test_returns_none_when_deployment_is_disabled() -> None:
    assert create_deployment_handler(DeploymentConfig()) is None


def test_creates_virtuoso_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VIRTUOSO_HOST", "virtuoso.test")
    monkeypatch.setenv("VIRTUOSO_PASSWORD", "test-password")
    monkeypatch.setenv("ISQL_SERVICE_TOKEN", "test-token")

    handler = create_deployment_handler(DeploymentConfig(backend="virtuoso"))

    assert isinstance(handler, VirtuosoDeploymentHandler)
    assert handler.host == "virtuoso.test"
    assert handler.isql_service_token == "test-token"  # noqa: S105


def test_virtuoso_requires_database_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VIRTUOSO_PASSWORD", raising=False)
    monkeypatch.setenv("ISQL_SERVICE_TOKEN", "test-token")

    with pytest.raises(ValueError, match="VIRTUOSO_PASSWORD"):
        create_deployment_handler(DeploymentConfig(backend="virtuoso"))
