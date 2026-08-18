"""Tests for RDF snapshot retention pruning."""

from pathlib import Path

import pytest

from climatesense_kg.export import retain_latest_snapshots


def _make_snapshot_dirs(root: Path, names: list[str]) -> list[Path]:
    dirs = []
    for name in names:
        snapshot = root / name
        snapshot.mkdir(parents=True)
        (snapshot / "desmog.nt.gz").write_bytes(b"<s> <p> <o> .\n")
        dirs.append(snapshot)
    return dirs


def test_retention_disabled_when_keep_latest_zero(tmp_path: Path) -> None:
    dirs = _make_snapshot_dirs(
        tmp_path, ["2026-08-13_000000", "2026-08-14_000000", "2026-08-15_000000"]
    )

    removed = retain_latest_snapshots(tmp_path, 0)

    assert removed == ()
    assert all(path.is_dir() for path in dirs)


def test_retention_keeps_newest_and_deletes_oldest(tmp_path: Path) -> None:
    dirs = _make_snapshot_dirs(
        tmp_path,
        [
            "2026-08-11_000000",
            "2026-08-12_000000",
            "2026-08-13_000000",
            "2026-08-14_000000",
            "2026-08-15_000000",
        ],
    )

    removed = retain_latest_snapshots(tmp_path, 3)

    assert set(removed) == set(dirs[:2])
    assert not dirs[0].exists()
    assert not dirs[1].exists()
    assert all(path.is_dir() for path in dirs[2:])


def test_retention_protects_dir_with_scratch_files(tmp_path: Path) -> None:
    dirs = _make_snapshot_dirs(tmp_path, ["2026-08-14_000000", "2026-08-15_000000"])
    (dirs[0] / ".desmog.nt.gz.lock").touch()

    removed = retain_latest_snapshots(tmp_path, 1)

    assert removed == ()
    assert all(path.is_dir() for path in dirs)


def test_retention_ignores_non_directory_entries(tmp_path: Path) -> None:
    dirs = _make_snapshot_dirs(tmp_path, ["2026-08-14_000000", "2026-08-15_000000"])
    catalog = tmp_path / "graphs.ttl"
    catalog.write_text("<s> <p> <o> .\n", encoding="utf-8")

    removed = retain_latest_snapshots(tmp_path, 1)

    assert removed == (dirs[0],)
    assert not dirs[0].exists()
    assert catalog.is_file()


def test_retention_raises_for_missing_root(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="not a directory"):
        retain_latest_snapshots(tmp_path / "missing", 3)
