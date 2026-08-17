"""Tests for command-line workflows."""

from argparse import Namespace
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.climatesense_kg.cli import (
    create_parser,
    run_duplicate_audit,
    run_flush_processing_results,
    run_pipeline,
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
