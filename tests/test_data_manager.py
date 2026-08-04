"""Tests for cache-aware data retrieval."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from climatesense_kg.config.schemas import DataSourceConfig, ProviderConfig
from climatesense_kg.data_manager import DataManager


def test_file_cache_can_be_recovered_after_source_is_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_path = tmp_path / "source.json"
    source_path.write_bytes(b"cached source data")
    source = DataSourceConfig(
        name="file-source",
        type="claimreviewdata",
        provider=ProviderConfig(provider_type="file", file_path=source_path),
    )
    manager = DataManager(cache_dir=tmp_path / "cache")
    processor = Mock()
    processor.process.side_effect = lambda data: iter([data])
    monkeypatch.setattr(manager, "_create_processor", Mock(return_value=processor))

    assert list(manager.get_data(source)) == [b"cached source data"]
    source_path.unlink()

    assert list(manager.get_data(source, skip_download=True)) == [b"cached source data"]
