"""REST API provider for scraping web APIs."""

import json
import time
from typing import Any, TypedDict
from urllib.parse import quote, urlsplit, urlunsplit

import defusedxml.ElementTree as ET
import requests

from ..config.schemas import XWikiProviderConfig
from ..utils.text_processing import (
    _fetch_public_url,
    redact_url_credentials,
    sanitize_url,
)
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


class XWikiProvider(BaseProvider[XWikiProviderConfig]):
    """Provider for fetching data from REST APIs."""

    def fetch(self, config: XWikiProviderConfig) -> bytes:
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
        successful_tag_requests = 0

        # Fetch pages for each tag
        for tag in tags:
            try:
                self.logger.info(f"Fetching pages for tag: {tag}")
                pages = self._fetch_pages_for_tag(session, base_url, tag, timeout)
                successful_tag_requests += 1

                # Deduplicate pages by ID
                for page in pages:
                    page_id = page.get("id")
                    if page_id and page_id not in all_pages_data:
                        all_pages_data[page_id] = page

                time.sleep(rate_limit_delay)

            except Exception as e:
                self.logger.error(f"Failed to fetch pages for tag '{tag}': {e}")
                continue

        if tags and successful_tag_requests == 0:
            raise RuntimeError("All XWiki tag requests failed")

        self.logger.info(f"Found {len(all_pages_data)} unique pages across all tags")

        # Fetch detailed data for each page
        all_page_details: list[dict[str, Any]] = []
        organization_site_cache: dict[str, str | None] = {}
        for page_data in all_pages_data.values():
            try:
                time.sleep(rate_limit_delay)
                page_details = self._fetch_page_details(base_url, page_data, timeout)
                if page_details:
                    organization_url = self._fetch_organization_site(
                        base_url,
                        page_data,
                        timeout,
                        organization_site_cache,
                    )
                    if not organization_url:
                        self.logger.warning(
                            "Skipping XWiki page without an organization site: %s",
                            page_data.get("id", "unknown"),
                        )
                        continue

                    # Merge basic page data with details
                    combined_data = {
                        **page_data,
                        **page_details,
                        "organization_url": organization_url,
                    }
                    all_page_details.append(combined_data)

            except Exception as e:
                self.logger.warning(
                    f"Failed to fetch details for page {page_data.get('id')}: {e}"
                )
                continue

        self.logger.info(f"Fetched details for {len(all_page_details)} pages")

        if all_pages_data and not all_page_details:
            raise RuntimeError(
                "All XWiki page-detail requests failed or lacked organization sites"
            )

        # Return all data as JSON bytes
        return json.dumps(all_page_details, ensure_ascii=False).encode("utf-8")

    def _fetch_pages_for_tag(
        self, session: requests.Session, base_url: str, tag: str, timeout: int
    ) -> list[PageSummary]:
        """Fetch all pages for a specific tag."""
        url = f"{base_url}/rest/wikis/xwiki/tags/{quote(tag)}"

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

    @staticmethod
    def _media_site_property_url(page_api_url: str) -> str | None:
        """Build the MediaClass.site property URL for a DeFacto fact-check page."""

        parsed = urlsplit(page_api_url)
        parts = parsed.path.strip("/").split("/")
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or len(parts) < 7
            or parts[:2] != ["rest", "wikis"]
            or parts[3:6] != ["spaces", "Medias", "spaces"]
        ):
            return None

        wiki = parts[2]
        media_space = parts[6]
        property_path = (
            f"/rest/wikis/{wiki}/spaces/Medias/spaces/{media_space}"
            "/pages/WebHome/objects/XWiki.DeFacto.Media.MediaClass/0"
            "/properties/site"
        )
        return urlunsplit((parsed.scheme, parsed.netloc, property_path, "", ""))

    def _fetch_organization_site(
        self,
        base_url: str,
        page_data: PageSummary,
        timeout: int,
        cache: dict[str, str | None],
    ) -> str | None:
        """Fetch and cache the public website for a page's media organization."""

        page_api_url = page_data.get("pageApiUrl", "")
        property_url = self._media_site_property_url(page_api_url)
        if not property_url:
            self.logger.error(
                "Could not derive media site property from XWiki page URL: %s",
                redact_url_credentials(page_api_url),
            )
            return None

        if property_url in cache:
            return cache[property_url]

        try:
            response = _fetch_public_url(
                property_url,
                headers={"Accept": "application/json"},
                timeout=timeout,
                allowed_origin=base_url,
            )
            response.raise_for_status()
            property_data = response.json()
            value = (
                property_data.get("value") if isinstance(property_data, dict) else None
            )
            organization_url = sanitize_url(value) if isinstance(value, str) else None
            if not organization_url:
                raise ValueError("MediaClass.site is missing a valid HTTP(S) URL")
            cache[property_url] = organization_url
            return organization_url
        except Exception as exc:
            self.logger.error(
                "Failed to fetch organization site from %s: %s",
                redact_url_credentials(property_url),
                exc,
            )
            cache[property_url] = None
            return None

    def _get_element_text(self, element: Any) -> str:
        """Safely get text content from an XML element."""
        return element.text.strip() if element is not None and element.text else ""

    def _fetch_page_details(
        self, base_url: str, page_data: PageSummary, timeout: int
    ) -> PageDetails | None:
        """Fetch detailed page information."""
        page_api_url = page_data.get("pageApiUrl")
        if not page_api_url:
            return None

        try:
            response = _fetch_public_url(
                page_api_url,
                headers={"Accept": "application/xml"},
                timeout=timeout,
                allowed_origin=base_url,
            )
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
            self.logger.error(
                "Failed to fetch page details from %s: %s",
                redact_url_credentials(page_api_url),
                e,
            )
            return None

    def get_cache_key_fields(self, config: XWikiProviderConfig) -> dict[str, Any]:
        """Base URL and tags affect cache."""
        return {
            "base_url": config.base_url,
            "tags": config.tags,
        }
