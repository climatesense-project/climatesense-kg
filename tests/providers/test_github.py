"""Tests for bounded GitHub asset downloads."""

import gzip
import io
from typing import Any
from unittest.mock import Mock, patch
import zipfile

import pytest

from climatesense_kg.config.schemas import GitHubProviderConfig
from climatesense_kg.providers.github import GitHubAsset, GitHubProvider


def _api_response(payload: Any) -> Mock:
    return Mock(raise_for_status=Mock(), json=Mock(return_value=payload))


def _asset_response(chunks: list[bytes]) -> Mock:
    return Mock(
        headers={}, raise_for_status=Mock(), iter_content=Mock(return_value=chunks)
    )


def test_tag_pattern_fetch_selects_the_newest_matching_release() -> None:
    software_release = {
        "tag_name": "v2.6.0",
        "assets": [
            {"name": "pkg-2.6.0.whl", "url": "https://api.github.test/whl", "size": 1}
        ],
    }
    data_release = {
        "tag_name": "data-2026.10.05",
        "assets": [
            {
                "name": "kg.ttl",
                "url": "https://api.github.test/ttl",
                "size": 4,
            }
        ],
    }
    listing = _api_response([software_release, data_release])
    download = _asset_response([b"kg!!"])
    api = Mock(side_effect=[listing, download])
    config = GitHubProviderConfig(
        provider_type="github",
        repository="example/split-releases",
        mode="release",
        asset_pattern="kg.ttl",
        tag_pattern="data-*",
        timeout=30,
    )

    with patch("climatesense_kg.providers.github.requests.get", api):
        assert GitHubProvider("github").fetch(config) == b"kg!!"

    list_url = api.call_args_list[0].args[0]
    asset_url = api.call_args_list[1].args[0]
    assert list_url == "https://api.github.com/repos/example/split-releases/releases"
    assert asset_url == "https://api.github.test/ttl"


def test_tag_pattern_fetch_skips_newer_releases_outside_the_pattern() -> None:
    software_release = {
        "tag_name": "v2.6.0",
        "assets": [
            {"name": "pkg-2.6.0.whl", "url": "https://api.github.test/whl", "size": 1}
        ],
    }
    stale_monthly_release = {
        "tag_name": "data-2025.09.01",
        "assets": [
            {
                "name": "kg.ttl",
                "url": "https://api.github.test/stale-ttl",
                "size": 4,
            }
        ],
    }
    current_release = {
        "tag_name": "data-2026.10.05",
        "assets": [
            {
                "name": "kg.ttl",
                "url": "https://api.github.test/ttl",
                "size": 4,
            }
        ],
    }
    listing = _api_response([software_release, stale_monthly_release, current_release])
    download = _asset_response([b"kg!!"])
    api = Mock(side_effect=[listing, download])
    config = GitHubProviderConfig(
        provider_type="github",
        repository="example/split-releases",
        mode="release",
        asset_pattern="kg.ttl",
        tag_pattern="data-2026.*",
        timeout=30,
    )

    with patch("climatesense_kg.providers.github.requests.get", api):
        assert GitHubProvider("github").fetch(config) == b"kg!!"

    assert api.call_args_list[1].args[0] == "https://api.github.test/ttl"


def test_tag_pattern_fetch_fails_loudly_without_a_match() -> None:
    listing = _api_response(
        [
            {
                "tag_name": "v2.6.0",
                "assets": [
                    {
                        "name": "pkg-2.6.0.whl",
                        "url": "https://api.github.test/whl",
                        "size": 1,
                    }
                ],
            }
        ]
    )
    config = GitHubProviderConfig(
        provider_type="github",
        repository="example/split-releases",
        mode="release",
        asset_pattern="kg.ttl",
        tag_pattern="data-*",
        timeout=30,
    )

    with (
        patch(
            "climatesense_kg.providers.github.requests.get",
            Mock(return_value=listing),
        ),
        pytest.raises(RuntimeError, match=r"tag pattern 'data-\*'"),
    ):
        GitHubProvider("github").fetch(config)


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


def test_gzip_asset_is_expanded() -> None:
    compressed = gzip.compress(b'[{"id": "review-1"}]')

    assert (
        GitHubProvider("github")._extract_from_gzip(
            compressed,
            max_uncompressed_bytes=100,
            max_compressed_bytes=100,
        )
        == b'[{"id": "review-1"}]'
    )


def test_oversized_gzip_asset_is_rejected_during_expansion() -> None:
    compressed = gzip.compress(b"x" * 100)

    with pytest.raises(ValueError, match=r"Expanded gzip asset.*10-byte limit"):
        GitHubProvider("github")._extract_from_gzip(
            compressed,
            max_uncompressed_bytes=10,
            max_compressed_bytes=1000,
        )
