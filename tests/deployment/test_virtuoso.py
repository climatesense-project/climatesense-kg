"""Tests for Virtuoso deployments."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from climatesense_kg.deployment.virtuoso import VirtuosoDeploymentHandler


def test_load_only_clears_its_own_load_list_entry(monkeypatch) -> None:
    """Concurrent deployments must not delete each other's queued files."""
    handler = VirtuosoDeploymentHandler(
        host="virtuoso",
        port=1111,
        user="dba",
        password="test-password",  # noqa: S106
        graph_template="https://example.test/graph/{SOURCE}",
        isql_service_url="http://isql-service:8080",
        isql_service_token="test-token",  # noqa: S106
    )
    executed_commands: list[tuple[str, int]] = []

    def execute(command: str, timeout: int = 300) -> bool:
        executed_commands.append((command, timeout))
        return True

    monkeypatch.setattr(handler, "_execute_sql", execute)

    assert handler._load_rdf_file(
        Path("/database/data/reviews.nt"),
        "https://example.test/graph/reviews",
    )

    assert executed_commands == [
        (
            "delete from DB.DBA.LOAD_LIST where LL_FILE = '/database/data/reviews.nt'",
            300,
        ),
        (
            "ld_dir('/database/data', 'reviews.nt', "
            "'https://example.test/graph/reviews')",
            300,
        ),
        ("rdf_loader_run()", 7200),
        ("checkpoint", 300),
    ]


def test_replace_clears_named_graph_before_loading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rdf_path = tmp_path / "organizations.ttl"
    rdf_path.write_text("<s> <p> <o> .\n", encoding="utf-8")
    handler = VirtuosoDeploymentHandler(
        host="virtuoso",
        port=1111,
        user="dba",
        password="test-password",  # noqa: S106
        graph_template="http://data.test/graph/{SOURCE}",
        isql_service_url="http://isql-service:8080",
        isql_service_token="test-token",  # noqa: S106
    )
    operations: list[tuple[str, str]] = []

    monkeypatch.setattr(
        handler,
        "_execute_sql",
        lambda command, timeout=300: not operations.append(("sql", command)),
    )
    monkeypatch.setattr(
        handler,
        "_load_rdf_file",
        lambda path, graph: not operations.append(("load", f"{path}|{graph}")),
    )

    assert handler.deploy(rdf_path, "organizations", replace=True) is True
    assert operations == [
        (
            "sql",
            "SPARQL CLEAR SILENT GRAPH <http://data.test/graph/organizations>",
        ),
        (
            "load",
            f"{rdf_path}|http://data.test/graph/organizations",
        ),
    ]


@patch("climatesense_kg.deployment.virtuoso.requests.post")
def test_sql_requests_authenticate_to_isql_service(mock_post: Mock) -> None:
    """The DBA proxy must receive the configured bearer token."""
    mock_post.return_value = Mock(status_code=200)
    handler = VirtuosoDeploymentHandler(
        host="virtuoso",
        port=1111,
        user="dba",
        password="test-password",  # noqa: S106
        graph_template="https://example.test/graph/{SOURCE}",
        isql_service_url="http://isql-service:8080",
        isql_service_token="test-token",  # noqa: S106
    )

    assert handler._execute_sql("checkpoint")

    mock_post.assert_called_once_with(
        "http://isql-service:8080/sql",
        json={"query": "checkpoint"},
        headers={"Authorization": "Bearer test-token"},
        timeout=310,
    )


def test_loader_escapes_quotes_in_paths_and_graph_uris(monkeypatch) -> None:
    handler = VirtuosoDeploymentHandler(
        host="virtuoso",
        port=1111,
        user="dba",
        password="test-password",  # noqa: S106
        graph_template="https://example.test/graph/{SOURCE}",
        isql_service_url="http://isql-service:8080",
        isql_service_token="test-token",  # noqa: S106
    )
    commands: list[str] = []
    monkeypatch.setattr(
        handler,
        "_execute_sql",
        lambda command, timeout=300: not commands.append(command),
    )

    assert handler._load_rdf_file(
        Path("/database/data/team's/review's.nt"),
        "https://example.test/graph/team's",
    )

    assert "team''s/review''s.nt" in commands[0]
    assert "'/database/data/team''s', 'review''s.nt'" in commands[1]
    assert "https://example.test/graph/team''s" in commands[1]


@pytest.mark.parametrize(
    ("file_path", "graph_uri"),
    [
        (Path("/database/data/bad\nname.nt"), "https://example.test/graph/source"),
        (Path("/database/data/source.nt"), "not a graph URI"),
    ],
)
def test_loader_rejects_invalid_path_and_graph_values(
    file_path: Path, graph_uri: str
) -> None:
    with pytest.raises(ValueError):
        VirtuosoDeploymentHandler._validate_loader_inputs(file_path, graph_uri)
