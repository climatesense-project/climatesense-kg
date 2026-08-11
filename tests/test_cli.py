"""Tests for command-line workflows."""

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from src.climatesense_kg.cli import run_flush_stage_results, run_redeploy
from src.climatesense_kg.config.graphs import (
    DBPEDIA_ENRICHER_SOURCE_NAME,
    GRAPH_CATALOG_PATH,
    GRAPH_CATALOG_SOURCE_NAME,
)
from src.climatesense_kg.config.organizations import (
    ORGANIZATION_CATALOG_PATH,
    ORGANIZATION_SOURCE_NAME,
)


def test_redeploy_preserves_underscored_source_name(tmp_path: Path) -> None:
    rdf_file = tmp_path / "climate_feedback_2026-08-04_120000.ttl"
    rdf_file.write_text("@prefix ex: <https://example.test/> .", encoding="utf-8")
    config = SimpleNamespace(
        logging=SimpleNamespace(level="INFO"),
        deployment=SimpleNamespace(
            graph_template="https://example.test/graph/{SOURCE}"
        ),
        data_sources=[
            SimpleNamespace(name="climate"),
            SimpleNamespace(name="climate_feedback"),
        ],
    )
    handler = Mock()
    handler.deploy.return_value = True
    args = Namespace(config="config.yaml", debug=False, rdf_dir=str(tmp_path))

    with (
        patch("src.climatesense_kg.config.load_config", return_value=config),
        patch(
            "src.climatesense_kg.deployment.factory.create_deployment_handler",
            return_value=handler,
        ),
        patch("src.climatesense_kg.utils.logging.setup_logging"),
    ):
        exit_code = run_redeploy(args)

    assert exit_code == 0
    assert handler.deploy.call_args_list == [
        call(
            GRAPH_CATALOG_PATH,
            GRAPH_CATALOG_SOURCE_NAME,
            replace=True,
        ),
        call(
            ORGANIZATION_CATALOG_PATH,
            ORGANIZATION_SOURCE_NAME,
            replace=True,
        ),
        call(rdf_file, "climate_feedback", replace=True),
    ]


def test_redeploy_routes_enrichment_artifact_as_full_snapshot(
    tmp_path: Path,
) -> None:
    rdf_file = tmp_path / "dbpedia-enricher_2026-08-07_120000.ttl"
    rdf_file.write_text("@prefix ex: <https://example.test/> .", encoding="utf-8")
    config = SimpleNamespace(
        logging=SimpleNamespace(level="INFO"),
        deployment=SimpleNamespace(
            graph_template="https://example.test/graph/{SOURCE}"
        ),
        data_sources=[SimpleNamespace(name="claimreviewdata")],
    )
    handler = Mock()
    handler.deploy.return_value = True
    args = Namespace(config="config.yaml", debug=False, rdf_dir=str(tmp_path))

    with (
        patch("src.climatesense_kg.config.load_config", return_value=config),
        patch(
            "src.climatesense_kg.deployment.factory.create_deployment_handler",
            return_value=handler,
        ),
        patch("src.climatesense_kg.utils.logging.setup_logging"),
    ):
        exit_code = run_redeploy(args)

    assert exit_code == 0
    assert handler.deploy.call_args_list[-1] == call(
        rdf_file, DBPEDIA_ENRICHER_SOURCE_NAME, replace=True
    )


def test_redeploy_rejects_multiple_snapshots_for_one_graph(
    tmp_path: Path,
) -> None:
    for timestamp in ("120000", "130000"):
        (tmp_path / f"dbpedia-enricher_2026-08-07_{timestamp}.ttl").write_text(
            "@prefix ex: <https://example.test/> .", encoding="utf-8"
        )
    config = SimpleNamespace(
        logging=SimpleNamespace(level="INFO"),
        deployment=SimpleNamespace(
            graph_template="https://example.test/graph/{SOURCE}"
        ),
        data_sources=[SimpleNamespace(name="claimreviewdata")],
    )
    handler = Mock()
    args = Namespace(config="config.yaml", debug=False, rdf_dir=str(tmp_path))

    with (
        patch("src.climatesense_kg.config.load_config", return_value=config),
        patch(
            "src.climatesense_kg.deployment.factory.create_deployment_handler",
            return_value=handler,
        ),
        patch("src.climatesense_kg.utils.logging.setup_logging"),
    ):
        exit_code = run_redeploy(args)

    assert exit_code == 1
    handler.deploy.assert_not_called()


def test_flush_stage_results_requires_explicit_confirmation() -> None:
    assert run_flush_stage_results(Namespace(yes=False)) == 1


def test_flush_stage_results_uses_only_stage_store() -> None:
    database = Mock()
    database.__enter__ = Mock(return_value=database)
    database.__exit__ = Mock(return_value=None)
    store = Mock()
    store.clear.return_value = 7

    with (
        patch(
            "src.climatesense_kg.persistence.PostgresDatabase.from_environment",
            return_value=database,
        ),
        patch(
            "src.climatesense_kg.persistence.PostgresStageResultStore",
            return_value=store,
        ),
    ):
        exit_code = run_flush_stage_results(Namespace(yes=True))

    assert exit_code == 0
    store.clear.assert_called_once_with()
