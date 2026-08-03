"""Tests for deployment configuration loading."""

from pathlib import Path

import pytest

from climatesense_kg.config import load_config


def _write_config(path: Path, deployment: str) -> Path:
    path.write_text(f"deployment:\n{deployment}", encoding="utf-8")
    return path


def test_loads_backend_deployment_config(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "config.yaml",
        "  backend: qlever\n  graph_template: https://example.test/graph/{SOURCE}\n",
    )

    config = load_config(config_path)

    assert config.deployment.backend == "qlever"
    assert config.deployment.graph_template == ("https://example.test/graph/{SOURCE}")


def test_rejects_backend_specific_deployment_section(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "config.yaml",
        "  virtuoso:\n    enabled: true\n",
    )

    with pytest.raises(ValueError, match="Failed to parse configuration"):
        load_config(config_path)


def test_rejects_unknown_deployment_backend(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "config.yaml",
        "  backend: unsupported\n",
    )

    with pytest.raises(ValueError, match="Failed to parse configuration"):
        load_config(config_path)
