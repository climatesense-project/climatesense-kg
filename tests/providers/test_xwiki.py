"""Tests for the XWiki provider."""

from unittest.mock import Mock, patch

import pytest
from src.climatesense_kg.config.schemas import XWikiProviderConfig
from src.climatesense_kg.providers.xwiki import XWikiProvider


class TestXWikiProvider:
    """Tests for safe XWiki page-detail fetching."""

    @patch("src.climatesense_kg.providers.xwiki._fetch_public_url")
    def test_detail_requests_are_restricted_to_configured_origin(
        self, mock_fetch: Mock
    ) -> None:
        """Response-controlled detail URLs must use the safe origin constraint."""
        response = Mock(
            content=(
                b'<page xmlns="http://www.xwiki.org">'
                b"<content>Fact check</content>"
                b"<created>2026-08-04</created>"
                b"<language>en</language>"
                b"</page>"
            )
        )
        mock_fetch.return_value = response
        provider = XWikiProvider("xwiki")

        details = provider._fetch_page_details(
            "https://wiki.example",
            {"pageApiUrl": "https://wiki.example/rest/pages/1"},
            timeout=10,
        )

        assert details == {
            "content": "Fact check",
            "created": "2026-08-04",
            "language": "en",
        }
        mock_fetch.assert_called_once_with(
            "https://wiki.example/rest/pages/1",
            headers={"Accept": "application/xml"},
            timeout=10,
            allowed_origin="https://wiki.example",
        )
        response.raise_for_status.assert_called_once_with()

    @patch("src.climatesense_kg.providers.xwiki._fetch_public_url")
    def test_media_site_is_derived_and_cached(self, mock_fetch: Mock) -> None:
        response = Mock()
        response.json.return_value = {
            "name": "site",
            "value": "https://www.lessurligneurs.eu/",
        }
        mock_fetch.return_value = response
        provider = XWikiProvider("xwiki")
        page_data = {
            "pageApiUrl": (
                "https://defacto-observatoire.fr/rest/wikis/xwiki/spaces/Medias/"
                "spaces/Les-Surligneurs/spaces/Fact-checks/spaces/Example/"
                "pages/WebHome"
            )
        }
        cache: dict[str, str | None] = {}

        first = provider._fetch_organization_site(
            "https://defacto-observatoire.fr", page_data, 10, cache
        )
        second = provider._fetch_organization_site(
            "https://defacto-observatoire.fr", page_data, 10, cache
        )

        assert first == "https://www.lessurligneurs.eu/"
        assert second == first
        mock_fetch.assert_called_once_with(
            "https://defacto-observatoire.fr/rest/wikis/xwiki/spaces/Medias/"
            "spaces/Les-Surligneurs/pages/WebHome/objects/"
            "XWiki.DeFacto.Media.MediaClass/0/properties/site",
            headers={"Accept": "application/json"},
            timeout=10,
            allowed_origin="https://defacto-observatoire.fr",
        )
        response.raise_for_status.assert_called_once_with()

    def test_complete_tag_failure_is_raised(self) -> None:
        provider = XWikiProvider("xwiki")
        config = XWikiProviderConfig(
            provider_type="xwiki",
            base_url="https://wiki.example",
            tags=["climate", "energy"],
            rate_limit_delay=0,
        )

        with (
            patch.object(
                provider,
                "_fetch_pages_for_tag",
                side_effect=RuntimeError("unavailable"),
            ),
            pytest.raises(RuntimeError, match="All XWiki tag requests failed"),
        ):
            provider.fetch(config)

    def test_complete_detail_failure_is_raised(self) -> None:
        provider = XWikiProvider("xwiki")
        config = XWikiProviderConfig(
            provider_type="xwiki",
            base_url="https://wiki.example",
            tags=["climate"],
            rate_limit_delay=0,
        )

        with (
            patch.object(
                provider,
                "_fetch_pages_for_tag",
                return_value=[
                    {
                        "id": "page-1",
                        "pageApiUrl": "https://wiki.example/rest/pages/1",
                    }
                ],
            ),
            patch.object(provider, "_fetch_page_details", return_value=None),
            pytest.raises(RuntimeError, match="All XWiki page-detail requests failed"),
        ):
            provider.fetch(config)
