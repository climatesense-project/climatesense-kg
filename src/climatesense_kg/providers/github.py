"""GitHub provider for fetching release assets or repository files."""

import base64
from dataclasses import dataclass
import gzip
import io
import os
import tempfile
from typing import Any, cast
import zipfile

import requests
from tqdm import tqdm

from .. import USER_AGENT
from ..config.schemas import GitHubProviderConfig
from ..utils.logging import parse_file_size
from .base import BaseProvider


@dataclass
class GitHubAsset:
    name: str
    url: str
    size: int


class GitHubProvider(BaseProvider[GitHubProviderConfig]):
    """Provider for fetching data from GitHub releases or repositories."""

    def __init__(self, name: str):
        """Initialize GitHub provider.

        Args:
            name: Name of the data source
            github_token: GitHub token for authentication
        """
        super().__init__(name)
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.api_base = "https://api.github.com"

    def fetch(self, config: GitHubProviderConfig) -> bytes:
        """Fetch data from GitHub based on configured mode."""
        repository = config.repository
        if not repository:
            raise ValueError("GitHubProvider requires 'repository' in config")

        mode = getattr(config, "mode", "release")

        if mode == "repository":
            path = config.repository_path
            if not path:
                raise ValueError(
                    "GitHubProvider configured for repository mode requires 'repository_path'"
                )

            ref = config.repository_ref or "main"
            self.logger.info(f"Fetching repository file {path} from {repository}@{ref}")
            return self._fetch_repository_file(
                repository=repository,
                path=path,
                ref=ref,
                timeout=config.timeout,
            )

        if mode != "release":
            raise ValueError(f"Unsupported GitHub provider mode: {mode}")

        return self._fetch_latest_release_asset(config)

    def _fetch_latest_release_asset(self, config: GitHubProviderConfig) -> bytes:
        repository = config.repository
        asset_pattern = config.asset_pattern
        extract_file = config.extract_file

        self.logger.info(f"Fetching latest release from {repository}")

        release = self._get_latest_release(repository, timeout=config.timeout)
        if not release:
            raise RuntimeError(f"No release found for {repository}")

        raw_assets = release.get("assets", [])
        if not raw_assets:
            raise RuntimeError(f"No assets found in release for {repository}")

        assets: list[GitHubAsset] = [
            GitHubAsset(name=asset["name"], url=asset["url"], size=asset["size"])
            for asset in raw_assets
        ]

        target_assets = self._filter_assets(assets, asset_pattern)
        if not target_assets:
            raise RuntimeError(
                f"No assets matching pattern '{asset_pattern}' in {repository}"
            )

        asset = target_assets[0]
        max_download_bytes = parse_file_size(config.max_download_size)
        self.logger.info(f"Downloading asset: {asset.name} ({asset.size} bytes)")
        if asset.size > max_download_bytes:
            raise ValueError(
                f"GitHub asset {asset.name!r} exceeds the configured "
                f"{max_download_bytes}-byte download limit"
            )

        asset_data = self._download_asset(
            asset,
            timeout=max(config.timeout, 300),
            max_bytes=max_download_bytes,
            spool_threshold_bytes=config.download_spool_threshold_bytes,
        )

        if asset.name.endswith(".zip") and extract_file:
            return self._extract_from_zip(
                asset_data,
                extract_file,
                max_uncompressed_bytes=parse_file_size(config.max_extract_size),
                max_compressed_bytes=max_download_bytes,
            )
        if asset.name.endswith(".gz"):
            return self._extract_from_gzip(
                asset_data,
                max_uncompressed_bytes=parse_file_size(config.max_extract_size),
                max_compressed_bytes=max_download_bytes,
            )
        return asset_data

    def _get_latest_release(
        self, repository: str, timeout: int
    ) -> dict[str, Any] | None:
        """Get latest release from repository."""
        url = f"{self.api_base}/repos/{repository}/releases/latest"

        try:
            response = requests.get(
                url,
                headers=self._build_headers(accept="application/vnd.github.v3+json"),
                timeout=timeout,
            )
            response.raise_for_status()
            result = response.json()
            if isinstance(result, dict):
                return cast(dict[str, Any], result)
            return None
        except Exception as e:
            self.logger.error(f"Failed to get latest release from {repository}: {e}")
            return None

    def _filter_assets(
        self, assets: list[GitHubAsset], pattern: str
    ) -> list[GitHubAsset]:
        """Filter assets by pattern."""
        if pattern.startswith("*."):
            extension = pattern[1:]  # Remove *
            return [asset for asset in assets if asset.name.endswith(extension)]
        else:
            return [asset for asset in assets if asset.name == pattern]

    def _download_asset(
        self,
        asset: GitHubAsset,
        timeout: int,
        max_bytes: int,
        spool_threshold_bytes: int,
    ) -> bytes:
        """Download asset data."""
        response = requests.get(
            asset.url,
            headers=self._build_headers("application/octet-stream"),
            timeout=timeout,
            stream=True,
        )
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        if total_size > max_bytes:
            response.close()
            raise ValueError(
                f"GitHub asset {asset.name!r} exceeds the configured "
                f"{max_bytes}-byte download limit"
            )

        bytes_read = 0
        try:
            with (
                tqdm(
                    total=total_size, unit="B", unit_scale=True, desc=asset.name
                ) as pbar,
                tempfile.SpooledTemporaryFile(
                    max_size=spool_threshold_bytes, mode="w+b"
                ) as buffer,
            ):
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    bytes_read += len(chunk)
                    if bytes_read > max_bytes:
                        raise ValueError(
                            f"GitHub asset {asset.name!r} exceeds the configured "
                            f"{max_bytes}-byte download limit"
                        )
                    buffer.write(chunk)
                    pbar.update(len(chunk))
                buffer.seek(0)
                return buffer.read()
        finally:
            response.close()

    def _extract_from_zip(
        self,
        zip_data: bytes,
        extract_file: str,
        *,
        max_uncompressed_bytes: int,
        max_compressed_bytes: int,
    ) -> bytes:
        """Extract specific file from zip data."""
        with zipfile.ZipFile(io.BytesIO(zip_data), "r") as zip_ref:
            # Handle wildcard patterns
            if extract_file.startswith("*."):
                extension = extract_file[1:]  # Remove *
                matching_files_by_ext = [
                    f for f in zip_ref.namelist() if f.endswith(extension)
                ]
                if not matching_files_by_ext:
                    raise ValueError(f"No {extension} files found in zip")
                extract_file = matching_files_by_ext[0]  # Use first match

            # Find the file (may be in subdirectory)
            matching_files: list[str] = []
            for file_path in zip_ref.namelist():
                if file_path.endswith("/" + extract_file) or file_path == extract_file:
                    matching_files.append(file_path)

            if not matching_files:
                raise ValueError(f"File '{extract_file}' not found in zip")

            # Use first match
            target_file = matching_files[0]
            self.logger.info(f"Extracting {target_file} from zip")
            target_info = zip_ref.getinfo(target_file)
            if target_info.compress_size > max_compressed_bytes:
                raise ValueError(
                    f"Compressed ZIP member {target_file!r} exceeds the configured "
                    f"{max_compressed_bytes}-byte limit"
                )
            if target_info.file_size > max_uncompressed_bytes:
                raise ValueError(
                    f"Expanded ZIP member {target_file!r} exceeds the configured "
                    f"{max_uncompressed_bytes}-byte limit"
                )

            with zip_ref.open(target_file) as f:
                chunks: list[bytes] = []
                bytes_read = 0
                while chunk := f.read(min(1024 * 1024, max_uncompressed_bytes + 1)):
                    bytes_read += len(chunk)
                    if bytes_read > max_uncompressed_bytes:
                        raise ValueError(
                            f"Expanded ZIP member {target_file!r} exceeds the "
                            f"configured {max_uncompressed_bytes}-byte limit"
                        )
                    chunks.append(chunk)
                return b"".join(chunks)

    def _extract_from_gzip(
        self,
        gzip_data: bytes,
        *,
        max_uncompressed_bytes: int,
        max_compressed_bytes: int,
    ) -> bytes:
        """Expand a gzip release asset within configured size limits."""
        if len(gzip_data) > max_compressed_bytes:
            raise ValueError(
                "Compressed gzip asset exceeds the configured "
                f"{max_compressed_bytes}-byte limit"
            )

        chunks: list[bytes] = []
        bytes_read = 0
        with gzip.GzipFile(fileobj=io.BytesIO(gzip_data)) as gzip_file:
            while chunk := gzip_file.read(min(1024 * 1024, max_uncompressed_bytes + 1)):
                bytes_read += len(chunk)
                if bytes_read > max_uncompressed_bytes:
                    raise ValueError(
                        "Expanded gzip asset exceeds the configured "
                        f"{max_uncompressed_bytes}-byte limit"
                    )
                chunks.append(chunk)
        return b"".join(chunks)

    def get_cache_key_fields(self, config: GitHubProviderConfig) -> dict[str, Any]:
        """Repository and asset pattern affect cache."""
        fields: dict[str, Any] = {
            "repository": config.repository,
            "mode": getattr(config, "mode", "release"),
        }

        if fields["mode"] == "release":
            fields["asset_pattern"] = config.asset_pattern
            fields["extract_file"] = config.extract_file
        else:
            fields["repository_path"] = config.repository_path
            fields["repository_ref"] = config.repository_ref

        return fields

    def _build_headers(self, accept: str) -> dict[str, str]:
        """Build headers for GitHub API requests."""
        headers = {
            "Accept": accept,
            "User-Agent": USER_AGENT,
        }

        if self.github_token:
            headers["Authorization"] = f"token {self.github_token}"

        return headers

    def _fetch_repository_file(
        self, repository: str, path: str, ref: str, timeout: int
    ) -> bytes:
        """Fetch a file directly from a GitHub repository."""
        url = f"{self.api_base}/repos/{repository}/contents/{path}"
        params: dict[str, str] = {}
        if ref:
            params["ref"] = ref

        response = requests.get(
            url,
            headers=self._build_headers("application/vnd.github.v3+json"),
            params=params or None,
            timeout=timeout,
        )
        response.raise_for_status()

        payload = response.json()
        if payload.get("type") != "file":
            raise ValueError(
                f"Path '{path}' in {repository}@{ref} is not a file (type={payload.get('type')})"
            )

        content = payload.get("content")
        encoding = payload.get("encoding")
        if content and encoding == "base64":
            return base64.b64decode(content)

        blob_sha = payload.get("sha")
        if blob_sha:
            return self._download_blob(repository, blob_sha, timeout)

        download_url = payload.get("download_url")
        if download_url:
            return self._download_raw(download_url, timeout)

        raise RuntimeError(
            f"Unable to retrieve file content for {repository}/{path} at {ref}"
        )

    def _download_blob(self, repository: str, sha: str, timeout: int) -> bytes:
        """Download a blob from the GitHub Git data API."""
        url = f"{self.api_base}/repos/{repository}/git/blobs/{sha}"
        response = requests.get(
            url,
            headers=self._build_headers("application/vnd.github.raw"),
            timeout=timeout,
        )
        response.raise_for_status()
        return response.content

    def _download_raw(self, url: str, timeout: int) -> bytes:
        """Download raw file content using provided URL."""
        response = requests.get(
            url,
            headers=self._build_headers("application/octet-stream"),
            timeout=timeout,
        )
        response.raise_for_status()
        return response.content
