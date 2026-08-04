"""Tests for the file-based data cache."""

from pathlib import Path
import threading
from unittest.mock import patch

import pytest
from src.climatesense_kg.utils.data_cache import DataCache


def test_clear_rejects_paths_outside_cache_root(tmp_path: Path) -> None:
    """Cache clearing must not accept absolute or traversing source paths."""
    cache = DataCache(tmp_path / "cache")
    victim = tmp_path / "victim"
    victim.mkdir()
    sentinel = victim / "sentinel"
    sentinel.write_text("must survive", encoding="utf-8")

    for source_name in (str(victim), f"../{victim.name}"):
        with pytest.raises(ValueError, match="Invalid cache source name"):
            cache.clear(source_name)
        assert sentinel.exists()


@pytest.mark.parametrize(
    "source_name", ["", ".", "..", "nested/source", "nested\\source"]
)
def test_clear_rejects_invalid_source_names(tmp_path: Path, source_name: str) -> None:
    """A source name must identify one non-empty path component."""
    cache = DataCache(tmp_path / "cache")

    with pytest.raises(ValueError, match="Invalid cache source name"):
        cache.clear(source_name)


def test_clear_rejects_symlink_outside_cache_root(tmp_path: Path) -> None:
    """A source symlink must not resolve outside the cache root."""
    cache_dir = tmp_path / "cache"
    cache = DataCache(cache_dir)
    victim = tmp_path / "victim"
    victim.mkdir()
    (cache_dir / "source").symlink_to(victim, target_is_directory=True)

    with pytest.raises(ValueError, match="Invalid cache source name"):
        cache.clear("source")

    assert victim.exists()


def test_clear_removes_valid_source_directory(tmp_path: Path) -> None:
    """A direct cache source directory remains clearable."""
    cache_dir = tmp_path / "cache"
    cache = DataCache(cache_dir)
    source_dir = cache_dir / "source"
    source_dir.mkdir()
    (source_dir / "entry.gz").write_bytes(b"cache data")

    cache.clear("source")

    assert not source_dir.exists()


def test_failed_overwrite_preserves_previous_cache_entry(tmp_path: Path) -> None:
    """A replacement must be complete before it can replace valid cache data."""
    cache = DataCache(tmp_path / "cache")
    config = {"url": "https://example.test/data"}
    cache.put("source", config, b"previous data")

    with (
        patch(
            "src.climatesense_kg.utils.data_cache.json.dump",
            side_effect=OSError("disk full"),
        ),
        pytest.raises(OSError, match="disk full"),
    ):
        cache.put("source", config, b"replacement data")

    assert cache.get("source", config, ignore_expiry=True) == b"previous data"


def test_separate_cache_instances_serialize_writes(tmp_path: Path) -> None:
    """The cache lock must coordinate instances, not only threads on one instance."""
    cache_dir = tmp_path / "cache"
    first = DataCache(cache_dir)
    second = DataCache(cache_dir)
    config = {"url": "https://example.test/data"}
    cache_key = first._generate_cache_key("source", config)
    finished = threading.Event()

    def write_from_second_instance() -> None:
        second.put("source", config, b"new data")
        finished.set()

    with first._cache_file_lock(cache_key, exclusive=True):
        writer = threading.Thread(target=write_from_second_instance)
        writer.start()
        assert not finished.wait(timeout=0.05)

    writer.join(timeout=1)
    assert finished.is_set()
    assert first.get("source", config, ignore_expiry=True) == b"new data"
