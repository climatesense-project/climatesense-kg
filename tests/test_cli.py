"""Tests for command-line workflows."""

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.climatesense_kg.cli import run_redeploy


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
    handler.deploy.assert_called_once_with(rdf_file, "climate_feedback")
