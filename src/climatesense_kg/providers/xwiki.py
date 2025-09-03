"""REST API provider for scraping web APIs."""

import json
import time
from typing import Any, TypedDict
from urllib.parse import quote

import defusedxml.ElementTree as ET
import requests

from ..config.schemas import ProviderConfig
from .base import BaseProvider


class PageSummary(TypedDict, total=False):
    """Summary information for a page."""

    id: str
    title: str
    rawTitle: str
    absoluteUrl: str
    pageApiUrl: str


class PageDetails(TypedDict, total=False):
    """Detailed information for a page."""

    content: str
    created: str
    language: str


class XWikiProvider(BaseProvider):
    """Provider for fetching data from REST APIs."""

    def fetch(self, config: ProviderConfig) -> bytes:
        """Fetch data from REST API.

        Args:
            config: Must contain 'base_url' and 'tags', optionally other params

        Returns:
            All fetched data as JSON bytes
        """
        base_url = config.base_url
        tags = config.tags
        rate_limit_delay = config.rate_limit_delay
        timeout = config.timeout

        self.logger.info(f"Fetching data from REST API: {base_url}")

        session = requests.Session()

        all_pages_data: dict[str, PageSummary] = {}  # {page_id: page_data}

        # Fetch pages for each tag
        for tag in tags:
            try:
                self.logger.info(f"Fetching pages for tag: {tag}")
                pages = self._fetch_pages_for_tag(session, base_url, tag, timeout)

                # Deduplicate pages by ID
                for page in pages:
                    page_id = page.get("id")
                    if page_id and page_id not in all_pages_data:
                        all_pages_data[page_id] = page

                time.sleep(rate_limit_delay)

            except Exception as e:
                self.logger.error(f"Failed to fetch pages for tag '{tag}': {e}")
                continue

        self.logger.info(f"Found {len(all_pages_data)} unique pages across all tags")

        # Fetch detailed data for each page
        all_page_details: list[dict[str, Any]] = []
        for page_data in all_pages_data.values():
            try:
                time.sleep(rate_limit_delay)
                page_details = self._fetch_page_details(session, page_data, timeout)
                if page_details:
                    # Merge basic page data with details
                    combined_data = {**page_data, **page_details}
                    all_page_details.append(combined_data)

            except Exception as e:
                self.logger.warning(
                    f"Failed to fetch details for page {page_data.get('id')}: {e}"
                )
                continue

        self.logger.info(f"Fetched details for {len(all_page_details)} pages")

        # Return all data as JSON bytes
        return json.dumps(all_page_details, ensure_ascii=False).encode("utf-8")

    def _fetch_pages_for_tag(
        self, session: requests.Session, base_url: str, tag: str, timeout: int
    ) -> list[PageSummary]:
        """Fetch all pages for a specific tag."""
        url = f"{base_url}/rest/wikis/xwiki/tags/{quote(tag)}"

        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()

            root = ET.fromstring(response.content)

            pages: list[PageSummary] = []
            ns = {"xwiki": "http://www.xwiki.org"}

            for page_summary in root.findall(".//xwiki:pageSummary", ns):
                page_info = self._extract_page_summary_info(page_summary, ns)
                if page_info:
                    pages.append(page_info)

            self.logger.info(f"Found {len(pages)} pages for tag '{tag}'")
            return pages

        except Exception as e:
            self.logger.error(f"Failed to fetch pages for tag '{tag}': {e}")
            return []

    def _extract_page_summary_info(
        self, page_summary: Any, ns: dict[str, str]
    ) -> PageSummary | None:
        """Extract relevant information from a pageSummary XML element."""
        try:
            link_elem = page_summary.find(
                ".//xwiki:link[@rel='http://www.xwiki.org/rel/page']", ns
            )
            if link_elem is None:
                return None

            page_link = link_elem.get("href")
            if not page_link:
                return None

            page_info: PageSummary = {"pageApiUrl": page_link}

            # Extract individual fields
            id_elem = page_summary.find(".//xwiki:id", ns)
            page_info["id"] = self._get_element_text(id_elem)

            title_elem = page_summary.find(".//xwiki:title", ns)
            page_info["title"] = self._get_element_text(title_elem)

            raw_title_elem = page_summary.find(".//xwiki:rawTitle", ns)
            page_info["rawTitle"] = self._get_element_text(raw_title_elem)

            absolute_url_elem = page_summary.find(".//xwiki:xwikiAbsoluteUrl", ns)
            page_info["absoluteUrl"] = self._get_element_text(absolute_url_elem)

            return page_info

        except Exception as e:
            self.logger.warning(f"Error extracting page summary info: {e}")
            return None

    def _get_element_text(self, element: Any) -> str:
        """Safely get text content from an XML element."""
        return element.text.strip() if element is not None and element.text else ""

    def _fetch_page_details(
        self, session: requests.Session, page_data: PageSummary, timeout: int
    ) -> PageDetails | None:
        """Fetch detailed page information."""
        page_api_url = page_data.get("pageApiUrl")
        if not page_api_url:
            return None

        try:
            response = session.get(page_api_url, timeout=timeout)
            response.raise_for_status()

            root = ET.fromstring(response.content)

            ns = {"xwiki": "http://www.xwiki.org"}

            page_details: PageDetails = {}

            content_element = root.find(".//xwiki:content", ns)
            page_details["content"] = self._get_element_text(content_element)

            created_element = root.find(".//xwiki:created", ns)
            page_details["created"] = self._get_element_text(created_element)

            language_element = root.find(".//xwiki:language", ns)
            page_details["language"] = self._get_element_text(language_element)

            return page_details

        except Exception as e:
            self.logger.error(f"Failed to fetch page details from {page_api_url}: {e}")
            return None

    def get_cache_key_fields(self, config: ProviderConfig) -> dict[str, Any]:
        """Base URL and tags affect cache."""
        return {
            "base_url": config.base_url,
            "tags": config.tags,
        }
