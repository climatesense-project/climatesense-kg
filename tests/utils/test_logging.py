"""Tests for logging utilities."""

import pytest
from src.climatesense_kg.utils.logging import parse_file_size


@pytest.mark.parametrize(
    ("size", "expected_bytes"),
    [
        ("50B", 50),
        ("50KB", 50 * 1024),
        ("50MB", 50 * 1024**2),
        ("1.5GB", int(1.5 * 1024**3)),
        ("2TB", 2 * 1024**4),
    ],
)
def test_parse_file_size_matches_longest_suffix_first(
    size: str, expected_bytes: int
) -> None:
    assert parse_file_size(size) == expected_bytes
