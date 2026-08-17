"""Tests for RDF output configuration and retention."""

from pathlib import Path

import pytest

from climatesense_kg.config import load_config
from climatesense_kg.config.schemas import (
    OutputConfig,
    SnapshotRetentionConfig,
)


def test_output_retention_defaults_to_disabled() -> None:
    assert OutputConfig().retention.keep_latest == 0


def test_output_retention_rejects_negative_keep_latest() -> None:
    with pytest.raises(ValueError, match="keep_latest must be non-negative"):
        OutputConfig(retention=SnapshotRetentionConfig(keep_latest=-1))


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


def test_rejects_unknown_output_keys(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "output:\n  bogus_key: true\n  output_path: output/graph.nt.gz\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Failed to parse configuration"):
        load_config(config_path)
