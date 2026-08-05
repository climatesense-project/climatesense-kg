"""Tests for text processing utilities."""

from unittest.mock import Mock, patch

import pytest
import requests
from src.climatesense_kg.utils.text_processing import (
    ExtractionErrorType,
    TextExtractionResult,
    _fetch_public_url,
    _request_url_at_address,
    _UnsafeURLError,
    fetch_and_extract_text,
    normalize_organization_url,
    normalize_text,
    sanitize_url,
)


class TestExtractionErrorType:
    """Test ExtractionErrorType enum."""

    def test_is_retryable_true(self) -> None:
        """Test retryable error types."""
        retryable = [
            ExtractionErrorType.TIMEOUT,
            ExtractionErrorType.CONNECTION,
            ExtractionErrorType.REQUEST_ERROR,
            ExtractionErrorType.DOWNLOAD_FAILED,
            ExtractionErrorType.UNKNOWN,
        ]
        for error_type in retryable:
            assert error_type.is_retryable is True

    def test_is_retryable_false(self) -> None:
        """Test non-retryable error types."""
        non_retryable = [
            ExtractionErrorType.INVALID_INPUT,
            ExtractionErrorType.INVALID_URL,
            ExtractionErrorType.HTTP_ERROR,
            ExtractionErrorType.EXTRACTION_FAILED,
        ]
        for error_type in non_retryable:
            assert error_type.is_retryable is False


class TestNormalizeText:
    """Test normalize_text function."""

    def test_html_entities(self) -> None:
        """Test HTML entity normalization."""
        text = "This &amp; that"
        result = normalize_text(text)
        assert result == "This & that"

    def test_non_breaking_spaces(self) -> None:
        """Test non-breaking space normalization."""
        text = "Hello\xa0world"
        result = normalize_text(text)
        assert result == "Hello world"

    def test_url_removal(self) -> None:
        """Test URL removal."""
        text = "Check this out https://example.com for more info"
        result = normalize_text(text)
        assert result == "Check this out for more info"

    def test_whitespace_normalization(self) -> None:
        """Test whitespace normalization."""
        text = "  Multiple   \t\n  spaces  "
        result = normalize_text(text)
        assert result == "Multiple spaces"

    def test_html_unescape(self) -> None:
        """Test HTML unescaping."""
        text = "&lt;div&gt;Hello&lt;/div&gt;"
        result = normalize_text(text)
        assert result == "<div>Hello</div>"

    def test_empty_string(self) -> None:
        """Test empty string handling."""
        result = normalize_text("")
        assert result == ""

    def test_combined_normalization(self) -> None:
        """Test combined text normalization."""
        text = "  &amp; Check\xa0this https://example.com &lt;tag&gt;  \n\t  "
        result = normalize_text(text)
        assert result == "& Check this <tag>"


class TestNormalizeOrganizationUrl:
    """Test normalize_organization_url function."""

    def test_all_variants_produce_same_result(self) -> None:
        """Different URL forms for the same site should produce identical output."""
        expected = "https://stopfake.org"
        variants = [
            "http://www.stopfake.org",
            "https://www.stopfake.org",
            "https://www.stopfake.org/",
            "https://www.stopfake.org/en/about-us/",
            "http://www.stopfake.org/en/about-us/",
        ]
        for variant in variants:
            assert normalize_organization_url(variant) == expected, (
                f"Failed for {variant}"
            )

    def test_preserves_non_standard_port(self) -> None:
        """Non-standard ports should be preserved."""
        assert (
            normalize_organization_url("https://example.com:8080/path")
            == "https://example.com:8080"
        )

    def test_strips_default_ports(self) -> None:
        """Default ports (80, 443) should be stripped."""
        assert (
            normalize_organization_url("https://example.com:443/path")
            == "https://example.com"
        )
        assert (
            normalize_organization_url("http://example.com:80/path")
            == "https://example.com"
        )

    def test_strips_query_and_fragment(self) -> None:
        """Query strings and fragments should be stripped."""
        assert (
            normalize_organization_url("https://example.com/page?lang=en#section")
            == "https://example.com"
        )

    def test_adds_scheme_if_missing(self) -> None:
        """URLs without scheme should get https:// prepended."""
        assert normalize_organization_url("example.com") == "https://example.com"

    def test_none_input(self) -> None:
        """None input should return None."""
        assert normalize_organization_url(None) is None

    def test_empty_string(self) -> None:
        """Empty string should return None."""
        assert normalize_organization_url("") is None

    def test_whitespace_only(self) -> None:
        """Whitespace-only string should return None."""
        assert normalize_organization_url("  ") is None

    def test_invalid_scheme(self) -> None:
        """Non-HTTP(S) schemes should return None."""
        assert normalize_organization_url("ftp://example.com") is None

    def test_unicode_hostname(self) -> None:
        """Unicode hostnames should be converted to punycode."""
        assert (
            normalize_organization_url("https://mañana.com/path")
            == "https://xn--maana-pta.com"
        )


class TestSanitizeUrl:
    """Test sanitize_url function."""

    def test_valid_https_url(self) -> None:
        """Test valid HTTPS URL."""
        url = "https://example.com/path"
        result = sanitize_url(url)
        assert result == "https://example.com/path"

    def test_valid_http_url(self) -> None:
        """Test valid HTTP URL."""
        url = "http://example.com/path"
        result = sanitize_url(url)
        assert result == "http://example.com/path"

    def test_auto_https_prefix(self) -> None:
        """Test automatic HTTPS prefixing."""
        url = "example.com/path"
        result = sanitize_url(url)
        assert result == "https://example.com/path"

    def test_special_characters_encoding(self) -> None:
        """Test special character encoding in URL."""
        url = "https://example.com/path with spaces?query=hello world"
        result = sanitize_url(url)
        assert result == "https://example.com/path%20with%20spaces?query=hello%20world"

    def test_preserves_existing_percent_escapes(self) -> None:
        url = "https://example.com/already%20encoded?q=also%2Fencoded"

        assert sanitize_url(url) == url

    def test_encodes_invalid_percent_escapes(self) -> None:
        assert (
            sanitize_url("https://example.com/invalid%escape")
            == "https://example.com/invalid%25escape"
        )

    def test_empty_url(self) -> None:
        """Test empty URL handling."""
        result = sanitize_url("")
        assert result is None

    def test_invalid_scheme(self) -> None:
        """Test invalid URL scheme is rejected."""
        url = "ftp://example.com"
        result = sanitize_url(url)
        assert result is None

    def test_no_netloc(self) -> None:
        """Test URL with no netloc."""
        url = "https://"
        result = sanitize_url(url)
        assert result is None

    def test_netloc_with_whitespace(self) -> None:
        """Test URL whose netloc contains whitespace characters."""
        url = "http://foo bar.com/path"
        result = sanitize_url(url)
        assert result is None

    def test_invalid_port(self) -> None:
        """Test URL containing an invalid port component."""
        url = "http://example.com:abc/path"
        result = sanitize_url(url)
        assert result is None

    def test_unicode_hostname(self) -> None:
        """Test URL with unicode hostname gets converted to punycode."""
        url = "https://mañana.com/path"
        result = sanitize_url(url)
        assert result == "https://xn--maana-pta.com/path"

    def test_strips_url_credentials(self) -> None:
        assert (
            sanitize_url("https://user:secret@example.com/private")
            == "https://example.com/private"
        )

    def test_malformed_url_exception(self) -> None:
        """Test malformed URL that raises exception."""
        with patch("src.climatesense_kg.utils.text_processing.urlparse") as mock_parse:
            mock_parse.side_effect = ValueError("Invalid URL")
            result = sanitize_url("malformed-url")
            assert result is None


class TestFetchAndExtractText:
    """Test fetch_and_extract_text function."""

    def test_empty_url(self) -> None:
        """Test empty URL input."""
        result = fetch_and_extract_text("")
        assert result.success is False
        assert result.error_type == ExtractionErrorType.INVALID_INPUT
        assert result.error_message == "Empty URL provided"

    @patch("src.climatesense_kg.utils.text_processing._request_url_at_address")
    @patch("socket.getaddrinfo")
    def test_rejects_link_local_address_before_request(
        self, mock_getaddrinfo: Mock, mock_request: Mock
    ) -> None:
        """Link-local metadata addresses must never reach the HTTP client."""
        mock_getaddrinfo.return_value = [
            (2, 1, 6, "", ("169.254.169.254", 80)),
        ]

        result = fetch_and_extract_text("http://169.254.169.254/latest/meta-data/")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.INVALID_URL
        mock_request.assert_not_called()

    @patch("src.climatesense_kg.utils.text_processing._request_url_at_address")
    @patch("socket.getaddrinfo")
    def test_rejects_credentials_before_request(
        self, mock_getaddrinfo: Mock, mock_request: Mock
    ) -> None:
        with pytest.raises(_UnsafeURLError, match="credentials"):
            _fetch_public_url(
                "https://user:secret@example.com/private",
                {"Accept": "text/html"},
                timeout=10,
            )

        mock_getaddrinfo.assert_not_called()
        mock_request.assert_not_called()

    @patch("src.climatesense_kg.utils.text_processing._request_url_at_address")
    @patch("socket.getaddrinfo")
    def test_rejects_hostname_resolving_to_private_address(
        self, mock_getaddrinfo: Mock, mock_request: Mock
    ) -> None:
        """Hostnames resolving to private addresses must not be requested."""
        mock_getaddrinfo.return_value = [
            (2, 1, 6, "", ("10.0.0.8", 443)),
        ]

        result = fetch_and_extract_text("https://reviews.example/article")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.INVALID_URL
        mock_request.assert_not_called()

    @patch("src.climatesense_kg.utils.text_processing._request_url_at_address")
    @patch("socket.getaddrinfo")
    def test_rejects_redirect_to_link_local_address(
        self, mock_getaddrinfo: Mock, mock_request: Mock
    ) -> None:
        """Every redirect target must pass the public-address check."""
        mock_getaddrinfo.side_effect = [
            [(2, 1, 6, "", ("93.184.216.34", 443))],
            [(2, 1, 6, "", ("169.254.169.254", 80))],
        ]
        mock_request.return_value = Mock(
            status_code=302,
            headers={"Location": "http://169.254.169.254/latest/meta-data/"},
        )

        result = fetch_and_extract_text("https://reviews.example/article")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.INVALID_URL
        mock_request.assert_called_once()

    @patch("src.climatesense_kg.utils.text_processing._request_url_at_address")
    @patch("socket.getaddrinfo")
    def test_rejects_url_outside_allowed_origin(
        self, mock_getaddrinfo: Mock, mock_request: Mock
    ) -> None:
        """An origin-constrained request must reject response-selected hosts."""
        with pytest.raises(_UnsafeURLError, match="configured origin"):
            _fetch_public_url(
                "https://attacker.example/rest/pages/1",
                {"Accept": "application/xml"},
                timeout=10,
                allowed_origin="https://wiki.example",
            )

        mock_getaddrinfo.assert_not_called()
        mock_request.assert_not_called()

    @patch("src.climatesense_kg.utils.text_processing._request_url_at_address")
    @patch("socket.getaddrinfo")
    def test_rejects_redirect_outside_allowed_origin(
        self, mock_getaddrinfo: Mock, mock_request: Mock
    ) -> None:
        """Every redirect must remain on the configured origin."""
        mock_getaddrinfo.return_value = [
            (2, 1, 6, "", ("93.184.216.34", 443)),
        ]
        mock_request.return_value = Mock(
            status_code=302,
            headers={"Location": "https://attacker.example/private"},
        )

        with pytest.raises(_UnsafeURLError, match="configured origin"):
            _fetch_public_url(
                "https://wiki.example/rest/pages/1",
                {"Accept": "application/xml"},
                timeout=10,
                allowed_origin="https://wiki.example",
            )

        mock_request.assert_called_once()

    @patch("src.climatesense_kg.utils.text_processing.requests.Session")
    def test_request_is_pinned_and_redirects_are_disabled(
        self, mock_session_factory: Mock
    ) -> None:
        """The request must connect to the validated IP without auto-redirecting."""
        mock_session = mock_session_factory.return_value
        mock_response = Mock()
        mock_session.get.return_value = mock_response

        result = _request_url_at_address(
            "https://reviews.example/article?id=1",
            "93.184.216.34",
            {"Accept": "text/html"},
            10,
        )

        assert result is mock_response
        mock_session.get.assert_called_once_with(
            "https://93.184.216.34:443/article?id=1",
            headers={"Accept": "text/html", "Host": "reviews.example"},
            timeout=10,
            allow_redirects=False,
            stream=True,
        )
        assert mock_session.trust_env is False
        adapter = mock_session.mount.call_args.args[1]
        assert adapter._hostname == "reviews.example"
        mock_session.close.assert_called_once_with()

    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_invalid_url(self, mock_sanitize: Mock) -> None:
        """Test invalid URL input."""
        mock_sanitize.return_value = None
        result = fetch_and_extract_text("invalid-url")
        assert result.success is False
        assert result.error_type == ExtractionErrorType.INVALID_URL
        assert result.error_message == "Invalid URL format"

    @patch("src.climatesense_kg.utils.text_processing._fetch_public_url")
    @patch("src.climatesense_kg.utils.text_processing.trafilatura")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_successful_extraction(
        self, mock_sanitize: Mock, mock_trafilatura: Mock, mock_fetch: Mock
    ) -> None:
        """Test successful text extraction with trafilatura."""
        mock_sanitize.return_value = "https://example.com"
        mock_response = Mock(headers={"Content-Type": "text/html"}, encoding="utf-8")
        mock_response.iter_content.return_value = [b"<html>content</html>"]
        mock_fetch.return_value = mock_response
        mock_trafilatura.extract.return_value = "Extracted text content"

        result = fetch_and_extract_text("https://example.com")

        assert result.success is True
        assert "Extracted text content" in result.content
        assert result.error_type is None
        mock_response.raise_for_status.assert_called_once_with()

    @patch("src.climatesense_kg.utils.text_processing._fetch_public_url")
    @patch("src.climatesense_kg.utils.text_processing.trafilatura")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_trafilatura_fallback_to_requests(
        self, mock_sanitize: Mock, mock_trafilatura: Mock, mock_fetch: Mock
    ) -> None:
        """Test fallback to requests when trafilatura fails."""
        mock_sanitize.return_value = "https://example.com"

        mock_response = Mock(headers={"Content-Type": "text/html"}, encoding="utf-8")
        mock_response.iter_content.return_value = [b"<html>content</html>"]
        mock_fetch.return_value = mock_response
        mock_trafilatura.extract.return_value = "Extracted text"

        result = fetch_and_extract_text("https://example.com")

        assert result.success is True
        mock_fetch.assert_called_once()

    @patch("src.climatesense_kg.utils.text_processing._fetch_public_url")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_timeout_error(self, mock_sanitize: Mock, mock_fetch: Mock) -> None:
        """Test timeout error handling."""
        mock_sanitize.return_value = "https://example.com"
        mock_fetch.side_effect = requests.Timeout("Timeout")

        result = fetch_and_extract_text("https://example.com")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.TIMEOUT

    @patch("src.climatesense_kg.utils.text_processing._fetch_public_url")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_forwards_configured_timeout(
        self, mock_sanitize: Mock, mock_fetch: Mock
    ) -> None:
        mock_sanitize.return_value = "https://example.com"
        mock_fetch.return_value = Mock(
            headers={"Content-Type": "text/html"},
            encoding="utf-8",
            iter_content=Mock(return_value=[]),
        )

        fetch_and_extract_text("https://example.com", timeout=37)

        assert mock_fetch.call_args.kwargs["timeout"] == 37

    @patch("src.climatesense_kg.utils.text_processing._fetch_public_url")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_connection_error(self, mock_sanitize: Mock, mock_fetch: Mock) -> None:
        """Test connection error handling."""
        mock_sanitize.return_value = "https://example.com"
        mock_fetch.side_effect = requests.ConnectionError("Connection failed")

        result = fetch_and_extract_text("https://example.com")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.CONNECTION

    @patch("src.climatesense_kg.utils.text_processing._fetch_public_url")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_http_error(self, mock_sanitize: Mock, mock_fetch: Mock) -> None:
        """Test HTTP error handling."""
        mock_sanitize.return_value = "https://example.com"

        http_error = requests.HTTPError("404 Not Found")
        mock_response = Mock()
        mock_response.status_code = 404
        http_error.response = mock_response
        mock_fetch.side_effect = http_error

        result = fetch_and_extract_text("https://example.com")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.HTTP_ERROR
        assert "404" in result.error_message

    @patch("src.climatesense_kg.utils.text_processing._fetch_public_url")
    @patch("src.climatesense_kg.utils.text_processing.trafilatura")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_no_text_extracted(
        self, mock_sanitize: Mock, mock_trafilatura: Mock, mock_fetch: Mock
    ) -> None:
        """Test when no text content is extracted."""
        mock_sanitize.return_value = "https://example.com"
        mock_fetch.return_value = Mock(
            headers={"Content-Type": "text/html"},
            encoding="utf-8",
            iter_content=Mock(return_value=[b"<html>content</html>"]),
        )
        mock_trafilatura.extract.return_value = None

        result = fetch_and_extract_text("https://example.com")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.EXTRACTION_FAILED

    @patch("src.climatesense_kg.utils.text_processing._fetch_public_url")
    @patch("src.climatesense_kg.utils.text_processing.trafilatura")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_no_content_downloaded(
        self, mock_sanitize: Mock, mock_trafilatura: Mock, mock_fetch: Mock
    ) -> None:
        """Test when no content is downloaded."""
        mock_sanitize.return_value = "https://example.com"
        mock_fetch.return_value = Mock(
            headers={"Content-Type": "text/html"},
            encoding="utf-8",
            iter_content=Mock(return_value=[]),
        )
        mock_trafilatura.extract.return_value = None

        result = fetch_and_extract_text("https://example.com")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.DOWNLOAD_FAILED

    @patch("src.climatesense_kg.utils.text_processing._fetch_public_url")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_rejects_non_text_response(
        self, mock_sanitize: Mock, mock_fetch: Mock
    ) -> None:
        mock_sanitize.return_value = "https://example.com/file"
        mock_fetch.return_value = Mock(
            headers={"Content-Type": "application/octet-stream"},
            encoding=None,
        )

        result = fetch_and_extract_text("https://example.com/file")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.DOWNLOAD_FAILED
        mock_fetch.return_value.iter_content.assert_not_called()

    @patch("src.climatesense_kg.utils.text_processing._fetch_public_url")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_aborts_response_that_exceeds_size_limit(
        self, mock_sanitize: Mock, mock_fetch: Mock
    ) -> None:
        mock_sanitize.return_value = "https://example.com/large"
        mock_fetch.return_value = Mock(
            headers={"Content-Type": "text/html"},
            encoding="utf-8",
            iter_content=Mock(return_value=[b"x" * (5 * 1024 * 1024 + 1)]),
        )

        result = fetch_and_extract_text("https://example.com/large")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.DOWNLOAD_FAILED
        assert "download limit" in result.error_message


class TestTextExtractionResult:
    """Test TextExtractionResult dataclass."""

    def test_successful_result(self) -> None:
        """Test successful extraction result."""
        result = TextExtractionResult(success=True, content="extracted text")
        assert result.success is True
        assert result.content == "extracted text"
        assert result.error_message == ""
        assert result.error_type is None

    def test_error_result(self) -> None:
        """Test error extraction result."""
        result = TextExtractionResult(
            success=False,
            error_message="Test error",
            error_type=ExtractionErrorType.TIMEOUT,
        )
        assert result.success is False
        assert result.content == ""
        assert result.error_message == "Test error"
        assert result.error_type == ExtractionErrorType.TIMEOUT
