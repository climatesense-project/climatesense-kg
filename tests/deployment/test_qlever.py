"""Tests for QLever Graph Store uploads."""

import logging
from pathlib import Path
from typing import Any

import pytest
import requests

from climatesense_kg.deployment.qlever import QLeverDeploymentHandler

TEST_CREDENTIAL = "test-credential"


class StubResponse:
    """Minimal requests response used by the upload tests."""

    def __init__(self, status_code: int = 200, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


@pytest.mark.parametrize(
    ("suffix", "content_type"),
    [(".nt", "application/n-triples"), (".ttl", "text/turtle")],
)
def test_streams_supported_rdf_to_named_graph(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    suffix: str,
    content_type: str,
) -> None:
    rdf_path = tmp_path / f"source{suffix}"
    rdf_path.write_bytes(b"<s> <p> <o> .\n")
    captured: dict[str, Any] = {}

    def fake_post(url: str, **kwargs: Any) -> StubResponse:
        captured["url"] = url
        captured.update(kwargs)
        captured["body"] = kwargs["data"].read()
        captured["stream_was_open"] = not kwargs["data"].closed
        return StubResponse(status_code=201)

    monkeypatch.setattr(requests, "post", fake_post)
    handler = QLeverDeploymentHandler(
        endpoint="https://qlever.test/",
        access_token=TEST_CREDENTIAL,
        graph_template="https://data.test/graph/{SOURCE}",
        timeout=30,
    )

    assert handler.deploy(rdf_path, "source") is True
    assert captured["url"] == "https://qlever.test"
    assert captured["params"] == {"graph": "https://data.test/graph/source"}
    assert captured["headers"] == {
        "Authorization": f"Bearer {TEST_CREDENTIAL}",
        "Content-Type": content_type,
    }
    assert captured["body"] == b"<s> <p> <o> .\n"
    assert captured["stream_was_open"] is True
    assert captured["timeout"] == 30


def test_replace_uses_graph_store_put(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rdf_path = tmp_path / "organizations.ttl"
    rdf_path.write_text("<s> <p> <o> .\n", encoding="utf-8")
    methods: list[str] = []

    def fake_put(*args: Any, **kwargs: Any) -> StubResponse:
        methods.append("PUT")
        return StubResponse(status_code=204)

    def fake_post(*args: Any, **kwargs: Any) -> StubResponse:
        methods.append("POST")
        return StubResponse(status_code=500)

    monkeypatch.setattr(requests, "put", fake_put)
    monkeypatch.setattr(requests, "post", fake_post)
    handler = QLeverDeploymentHandler(
        endpoint="https://qlever.test",
        access_token=TEST_CREDENTIAL,
        graph_template="http://data.test/graph/{SOURCE}",
    )

    assert handler.deploy(rdf_path, "organizations", replace=True) is True
    assert methods == ["PUT"]


def test_rejects_unsupported_format(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rdf_path = tmp_path / "source.rdf"
    rdf_path.write_text("data", encoding="utf-8")
    called = False

    def fake_post(*args: Any, **kwargs: Any) -> StubResponse:
        nonlocal called
        called = True
        return StubResponse()

    monkeypatch.setattr(requests, "post", fake_post)
    handler = QLeverDeploymentHandler(
        endpoint="https://qlever.test",
        access_token=TEST_CREDENTIAL,
        graph_template="https://data.test/graph/{SOURCE}",
    )

    assert handler.deploy(rdf_path, "source") is False
    assert called is False


def test_rejects_missing_file(tmp_path: Path) -> None:
    handler = QLeverDeploymentHandler(
        endpoint="https://qlever.test",
        access_token=TEST_CREDENTIAL,
        graph_template="https://data.test/graph/{SOURCE}",
    )

    assert handler.deploy(tmp_path / "missing.nt", "source") is False


def test_redacts_token_from_http_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rdf_path = tmp_path / "source.nt"
    rdf_path.write_text("<s> <p> <o> .\n", encoding="utf-8")
    credential = TEST_CREDENTIAL

    monkeypatch.setattr(
        requests,
        "post",
        lambda *args, **kwargs: StubResponse(401, f"invalid {credential}"),
    )
    handler = QLeverDeploymentHandler(
        endpoint="https://qlever.test",
        access_token=credential,
        graph_template="https://data.test/graph/{SOURCE}",
    )

    with caplog.at_level(logging.ERROR):
        assert handler.deploy(rdf_path, "source") is False

    assert credential not in caplog.text
    assert "invalid ***" in caplog.text


def test_handles_upload_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rdf_path = tmp_path / "source.nt"
    rdf_path.write_text("<s> <p> <o> .\n", encoding="utf-8")

    def raise_timeout(*args: Any, **kwargs: Any) -> StubResponse:
        raise requests.Timeout("timed out")

    monkeypatch.setattr(requests, "post", raise_timeout)
    handler = QLeverDeploymentHandler(
        endpoint="https://qlever.test",
        access_token=TEST_CREDENTIAL,
        graph_template="https://data.test/graph/{SOURCE}",
        timeout=1,
    )

    assert handler.deploy(rdf_path, "source") is False
