"""Tests for command-line workflows."""

from argparse import Namespace
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.climatesense_kg.cli import (
    create_parser,
    run_flush_processing_results,
    run_pipeline,
)


def test_run_parser_accepts_skip_extraction() -> None:
    args = create_parser().parse_args(
        ["run", "--config", "config/local.yaml", "--skip-extraction"]
    )

    assert args.skip_extraction is True


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
