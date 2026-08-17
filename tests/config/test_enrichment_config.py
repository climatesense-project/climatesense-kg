"""Tests for enrichment configuration validation."""

from pathlib import Path

import pytest

from climatesense_kg.config import load_config


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
    tmp_path: Path, section: str
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"enrichment:\n{section}output:\n  output_path: output/graph.nt.gz\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="max_workers must be positive"):
        load_config(config_path)
