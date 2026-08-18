"""File provider for reading local files."""

from pathlib import Path
from typing import Any

from ..config.schemas import FileProviderConfig
from .base import BaseProvider


class FileProvider(BaseProvider[FileProviderConfig]):
    """Provider for reading data from local files."""

    def fetch(self, config: FileProviderConfig) -> bytes:
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

    def get_cache_key_fields(self, config: FileProviderConfig) -> dict[str, Any]:
        """Use the configured path as the stable file-cache identity."""
        file_path = config.file_path
        return {"file_path": str(file_path)}
