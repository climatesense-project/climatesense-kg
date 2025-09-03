"""File provider for reading local files."""

from pathlib import Path
from typing import Any

from ..config.schemas import ProviderConfig
from .base import BaseProvider


class FileProvider(BaseProvider):
    """Provider for reading data from local files."""

    def fetch(self, config: ProviderConfig) -> bytes:
        """Fetch data from local file.

        Args:
            config: Must contain 'file_path' key

        Returns:
            File content as bytes
        """
        file_path = config.file_path
        if not file_path:
            raise ValueError("FileProvider requires 'file_path' in config")

        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        self.logger.info(f"Reading file: {path}")

        with open(path, "rb") as f:
            data = f.read()

        self.logger.info(f"Read {len(data)} bytes from {path}")
        return data

    def get_cache_key_fields(self, config: ProviderConfig) -> dict[str, Any]:
        """File path and modification time affect cache."""
        file_path = config.file_path
        cache_fields = {"file_path": str(file_path) if file_path else None}

        if file_path and Path(file_path).exists():
            cache_fields["file_mtime"] = str(Path(file_path).stat().st_mtime)

        return cache_fields
