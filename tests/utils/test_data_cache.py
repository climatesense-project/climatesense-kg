"""Tests for the file-based data cache."""

from pathlib import Path

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
