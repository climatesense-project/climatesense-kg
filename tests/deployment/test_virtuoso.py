"""Tests for Virtuoso deployments."""

from pathlib import Path

from climatesense_kg.deployment.virtuoso import VirtuosoDeploymentHandler


def test_load_only_clears_its_own_load_list_entry(monkeypatch) -> None:
    """Concurrent deployments must not delete each other's queued files."""
    handler = VirtuosoDeploymentHandler(
        host="virtuoso",
        port=1111,
        user="dba",
        password="",
        graph_template="https://example.test/graph/{SOURCE}",
        isql_service_url="http://isql-service:8080",
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
