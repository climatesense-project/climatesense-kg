"""Tests for RDF output retention configuration."""

import pytest

from climatesense_kg.config.schemas import (
    OutputConfig,
    SnapshotRetentionConfig,
)


def test_output_retention_defaults_to_disabled() -> None:
    assert OutputConfig().retention.keep_latest == 0


def test_output_retention_accepts_positive_keep_latest() -> None:
    config = OutputConfig(retention=SnapshotRetentionConfig(keep_latest=3))

    assert config.retention.keep_latest == 3


def test_output_retention_rejects_negative_keep_latest() -> None:
    with pytest.raises(ValueError, match="keep_latest must be non-negative"):
        OutputConfig(retention=SnapshotRetentionConfig(keep_latest=-1))
