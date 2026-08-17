"""Tests for command-line workflows."""

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from src.climatesense_kg.cli import (
    create_parser,
    run_duplicate_audit,
    run_flush_processing_results,
    run_pipeline,
    run_redeploy,
)
from src.climatesense_kg.config.graphs import (
    DBPEDIA_ENRICHER_SOURCE_NAME,
    GRAPH_CATALOG_PATH,
    GRAPH_CATALOG_SOURCE_NAME,
)
from src.climatesense_kg.config.organizations import (
    ORGANIZATION_CATALOG_PATH,
    ORGANIZATION_SOURCE_NAME,
)


def test_run_parser_accepts_skip_extraction() -> None:
    args = create_parser().parse_args(
        ["run", "--config", "config/local.yaml", "--skip-extraction"]
    )

    assert args.skip_extraction is True


def test_duplicate_audit_parser_requires_configuration() -> None:
    args = create_parser().parse_args(
        ["audit-duplicates", "--config", "config/local.yaml"]
    )

    assert args.command == "audit-duplicates"
    assert args.config == "config/local.yaml"


def test_duplicate_audit_uses_its_own_configuration(capsys) -> None:
    config = SimpleNamespace(
        logging=SimpleNamespace(level="INFO"),
        duplicate_audit=SimpleNamespace(
            similarity_threshold=0.95,
            minimum_similarity_words=75,
            group_batch_size=20,
        ),
    )
    database = Mock()
    database.__enter__ = Mock(return_value=database)
    database.__exit__ = Mock(return_value=None)
    auditor = Mock()
    auditor.run.return_value = SimpleNamespace(
        groups=4,
        candidate_pairs=7,
        eligible_pairs=5,
        matches=2,
    )

    with (
        patch("src.climatesense_kg.config.load_config", return_value=config),
        patch(
            "src.climatesense_kg.database.Database.from_environment",
            return_value=database,
        ),
        patch(
            "src.climatesense_kg.identity.DuplicateAuditor",
            return_value=auditor,
        ) as auditor_type,
        patch("src.climatesense_kg.utils.logging.setup_logging"),
    ):
        exit_code = run_duplicate_audit(Namespace(config="config.yaml"))

    assert exit_code == 0
    auditor_type.assert_called_once_with(
        database.pool,
        similarity_threshold=0.95,
        minimum_similarity_words=75,
        group_batch_size=20,
    )
    assert "matches=2" in capsys.readouterr().out


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


def test_flush_processing_results_requires_explicit_confirmation() -> None:
    assert run_flush_processing_results(Namespace(yes=False)) == 1


def test_flush_processing_results_deletes_only_recomputable_results() -> None:
    database = Mock()
    database.__enter__ = Mock(return_value=database)
    database.__exit__ = Mock(return_value=None)
    with (
        patch(
            "src.climatesense_kg.database.Database.from_environment",
            return_value=database,
        ),
        patch(
            "src.climatesense_kg.enrichment.clear_processing_results",
            return_value=7,
        ) as clear,
    ):
        exit_code = run_flush_processing_results(Namespace(yes=True))

    assert exit_code == 0
    clear.assert_called_once_with(database.pool)


def test_pipeline_interrupt_reports_preserved_results(capsys) -> None:
    config = SimpleNamespace(logging=SimpleNamespace(level="INFO"))
    pipeline = Mock()
    pipeline.__enter__ = Mock(return_value=pipeline)
    pipeline.__exit__ = Mock(return_value=None)
    pipeline.run.side_effect = KeyboardInterrupt

    with (
        patch("src.climatesense_kg.config.load_config", return_value=config),
        patch("src.climatesense_kg.pipeline.Pipeline", return_value=pipeline),
    ):
        exit_code = run_pipeline(Namespace(config="config.yaml", debug=False))

    assert exit_code == 130
    assert "committed database results were preserved" in capsys.readouterr().err
