"""Tests for bounded GitHub asset downloads."""

import io
from unittest.mock import Mock, patch
import zipfile

import pytest

from climatesense_kg.providers.github import GitHubAsset, GitHubProvider


def test_streamed_asset_is_aborted_at_download_limit() -> None:
    response = Mock(
        headers={},
        iter_content=Mock(return_value=[b"1234", b"5678"]),
    )
    provider = GitHubProvider("github")
    asset = GitHubAsset("data.zip", "https://api.github.test/asset", 8)

    with (
        patch("climatesense_kg.providers.github.requests.get", return_value=response),
        pytest.raises(ValueError, match="5-byte download limit"),
    ):
        provider._download_asset(
            asset, timeout=10, max_bytes=5, spool_threshold_bytes=2
        )

    response.close.assert_called_once_with()


def test_oversized_zip_member_is_rejected_before_expansion() -> None:
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("data.json", b"x" * 100)

    with pytest.raises(ValueError, match=r"Expanded ZIP member.*10-byte limit"):
        GitHubProvider("github")._extract_from_zip(
            archive.getvalue(),
            "data.json",
            max_uncompressed_bytes=10,
            max_compressed_bytes=1000,
        )
