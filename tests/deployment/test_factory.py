"""Tests for deployment handler construction."""

import pytest

from climatesense_kg.config.schemas import DeploymentConfig
from climatesense_kg.deployment.factory import create_deployment_handler
from climatesense_kg.deployment.qlever import QLeverDeploymentHandler
from climatesense_kg.deployment.virtuoso import VirtuosoDeploymentHandler


def test_returns_none_when_deployment_is_disabled() -> None:
    assert create_deployment_handler(DeploymentConfig()) is None


def test_creates_virtuoso_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VIRTUOSO_HOST", "virtuoso.test")

    handler = create_deployment_handler(DeploymentConfig(backend="virtuoso"))

    assert isinstance(handler, VirtuosoDeploymentHandler)
    assert handler.host == "virtuoso.test"


def test_creates_qlever_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("QLEVER_ENDPOINT", "https://qlever.test/")
    monkeypatch.setenv("QLEVER_ACCESS_TOKEN", "secret")
    monkeypatch.setenv("QLEVER_UPLOAD_TIMEOUT_SECONDS", "42")

    handler = create_deployment_handler(DeploymentConfig(backend="qlever"))

    assert isinstance(handler, QLeverDeploymentHandler)
    assert handler.endpoint == "https://qlever.test"
    assert handler.timeout == 42


def test_qlever_requires_access_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QLEVER_ACCESS_TOKEN", raising=False)

    with pytest.raises(ValueError, match="QLEVER_ACCESS_TOKEN"):
        create_deployment_handler(DeploymentConfig(backend="qlever"))
