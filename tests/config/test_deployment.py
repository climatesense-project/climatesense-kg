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
        "  backend: virtuoso\n  graph_template: https://example.test/graph/{SOURCE}\n",
    )

    config = load_config(config_path)

    assert config.deployment.backend == "virtuoso"
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


def test_rejects_non_ntriples_gz_output_extension(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "output:\n  output_path: output/graph.ttl\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"must use the \.nt\.gz extension"):
        load_config(config_path)


def test_accepts_ntriples_gz_output_extension(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "output:\n  output_path: output/graph.nt.gz\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.output.output_path == "output/graph.nt.gz"


def test_rejects_obsolete_output_format_option(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "output:\n  format: nt\n  output_path: output/graph.nt.gz\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Failed to parse configuration"):
        load_config(config_path)


def test_dbpedia_properties_require_spotlight(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "enrichment:\n"
        "  dbpedia_entity_properties:\n"
        "    enabled: true\n"
        "output:\n"
        "  output_path: output/graph.nt.gz\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="require DBpedia Spotlight"):
        load_config(config_path)


@pytest.mark.parametrize(
    "section",
    [
        "  dbpedia_spotlight:\n    max_workers: 0\n",
        "  cimple:\n    max_workers: 0\n",
    ],
)
def test_enrichment_worker_counts_must_be_positive(
    tmp_path: Path,
    section: str,
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"enrichment:\n{section}output:\n  output_path: output/graph.nt.gz\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="max_workers must be positive"):
        load_config(config_path)
