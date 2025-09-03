"""GitHub provider for fetching release data."""

from dataclasses import dataclass
import io
import os
from typing import Any, cast
import zipfile

import requests
from tqdm import tqdm

from ..config.schemas import ProviderConfig
from .base import BaseProvider


@dataclass
class GitHubAsset:
    name: str
    url: str
    size: int


class GitHubProvider(BaseProvider):
    """Provider for fetching data from GitHub releases."""

    def __init__(self, name: str):
        """Initialize GitHub provider.

        Args:
            name: Name of the data source
            github_token: GitHub token for authentication
        """
        super().__init__(name)
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.api_base = "https://api.github.com"

    def fetch(self, config: ProviderConfig) -> bytes:
        """Fetch data from GitHub release.

        Args:
            config: Must contain 'repository', optionally 'asset_pattern', 'extract_file'

        Returns:
            Raw data as bytes (JSON or extracted file content)
        """
        repository = config.repository
        if not repository:
            raise ValueError("GitHubProvider requires 'repository' in config")

        asset_pattern = config.asset_pattern
        extract_file = config.extract_file

        self.logger.info(f"Fetching latest release from {repository}")

        release = self._get_latest_release(repository)
        if not release:
            raise RuntimeError(f"No release found for {repository}")

        raw_assets = release.get("assets", [])
        if not raw_assets:
            raise RuntimeError(f"No assets found in release for {repository}")

        # Convert raw dict assets to GitHubAsset objects
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
        self.logger.info(f"Downloading asset: {asset.name} ({asset.size} bytes)")

        asset_data = self._download_asset(asset)

        if asset.name.endswith(".zip") and extract_file:
            return self._extract_from_zip(asset_data, extract_file)
        else:
            return asset_data

    def _get_latest_release(self, repository: str) -> dict[str, Any] | None:
        """Get latest release from repository."""
        url = f"{self.api_base}/repos/{repository}/releases/latest"

        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "ClimateSense-Pipeline/2.0",
        }

        if self.github_token:
            headers["Authorization"] = f"token {self.github_token}"

        try:
            response = requests.get(url, headers=headers, timeout=30)
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

    def _download_asset(self, asset: GitHubAsset) -> bytes:
        """Download asset data."""
        headers = {
            "Accept": "application/octet-stream",
            "User-Agent": "ClimateSense-Pipeline/1.0 (+https://github.com/climatesense-project)",
        }

        if self.github_token:
            headers["Authorization"] = f"token {self.github_token}"

        response = requests.get(asset.url, headers=headers, timeout=300, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        with tqdm(total=total_size, unit="B", unit_scale=True, desc=asset.name) as pbar:
            buffer = io.BytesIO()
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                buffer.write(chunk)
                pbar.update(len(chunk))

        return buffer.getvalue()

    def _extract_from_zip(self, zip_data: bytes, extract_file: str) -> bytes:
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

            with zip_ref.open(target_file) as f:
                return f.read()

    def get_cache_key_fields(self, config: ProviderConfig) -> dict[str, Any]:
        """Repository and asset pattern affect cache."""
        return {
            "repository": config.repository,
            "asset_pattern": config.asset_pattern,
            "extract_file": config.extract_file,
        }
