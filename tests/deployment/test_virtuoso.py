"""Tests for Virtuoso deployments."""

from pathlib import Path
from unittest.mock import Mock, patch

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
