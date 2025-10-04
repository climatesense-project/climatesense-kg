"""HTTP provider for fetching data from static URLs."""

from typing import Any

import requests

from ..config.schemas import ProviderConfig
from .base import BaseProvider


class HttpProvider(BaseProvider):
    """Provider that downloads data from a configured HTTP(S) endpoint."""

    def fetch(self, config: ProviderConfig) -> bytes:
        url = config.url
        if not url:
            raise ValueError("HttpProvider requires 'url' in configuration")

        timeout = max(int(config.timeout), 1)

        self.logger.info("Downloading data from %s", url)
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        self.logger.info("Fetched %s bytes from %s", len(response.content), url)
        return response.content

    def get_cache_key_fields(self, config: ProviderConfig) -> dict[str, Any]:
        return {"url": config.url}
