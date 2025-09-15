"""Tests for text processing utilities."""

from unittest.mock import Mock, patch

import requests
from src.climatesense_kg.utils.text_processing import (
    ExtractionErrorType,
    TextExtractionResult,
    fetch_and_extract_text,
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
            ExtractionErrorType.UNEXPECTED,
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
        """Test non-breaking space removal."""
        text = "Hello\xa0world"
        result = normalize_text(text)
        assert result == "Helloworld"

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
        assert result == "& Checkthis <tag>"


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

    def test_empty_url(self) -> None:
        """Test empty URL handling."""
        result = sanitize_url("")
        assert result is None

    def test_invalid_scheme(self) -> None:
        """Test invalid URL scheme - ftp URLs get https prefix added."""
        url = "ftp://example.com"
        result = sanitize_url(url)
        assert result == "https://ftp://example.com"

    def test_no_netloc(self) -> None:
        """Test URL with no netloc."""
        url = "https://"
        result = sanitize_url(url)
        assert result is None

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

    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_invalid_url(self, mock_sanitize: Mock) -> None:
        """Test invalid URL input."""
        mock_sanitize.return_value = None
        result = fetch_and_extract_text("invalid-url")
        assert result.success is False
        assert result.error_type == ExtractionErrorType.INVALID_URL
        assert result.error_message == "Invalid URL format"

    @patch("src.climatesense_kg.utils.text_processing.trafilatura")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_successful_extraction(
        self, mock_sanitize: Mock, mock_trafilatura: Mock
    ) -> None:
        """Test successful text extraction with trafilatura."""
        mock_sanitize.return_value = "https://example.com"
        mock_trafilatura.fetch_url.return_value = "<html>content</html>"
        mock_trafilatura.extract.return_value = "Extracted text content"

        result = fetch_and_extract_text("https://example.com")

        assert result.success is True
        assert "Extracted text content" in result.content
        assert result.error_type is None

    @patch("src.climatesense_kg.utils.text_processing.requests")
    @patch("src.climatesense_kg.utils.text_processing.trafilatura")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_trafilatura_fallback_to_requests(
        self, mock_sanitize: Mock, mock_trafilatura: Mock, mock_requests: Mock
    ) -> None:
        """Test fallback to requests when trafilatura fails."""
        mock_sanitize.return_value = "https://example.com"
        mock_trafilatura.fetch_url.return_value = None

        mock_response = Mock()
        mock_response.text = "<html>content</html>"
        mock_requests.get.return_value = mock_response
        mock_trafilatura.extract.return_value = "Extracted text"

        result = fetch_and_extract_text("https://example.com")

        assert result.success is True
        mock_requests.get.assert_called_once()

    @patch("src.climatesense_kg.utils.text_processing.requests.get")
    @patch("src.climatesense_kg.utils.text_processing.trafilatura")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_timeout_error(
        self, mock_sanitize: Mock, mock_trafilatura: Mock, mock_get: Mock
    ) -> None:
        """Test timeout error handling."""
        mock_sanitize.return_value = "https://example.com"
        mock_trafilatura.fetch_url.return_value = None
        mock_get.side_effect = requests.Timeout("Timeout")

        result = fetch_and_extract_text("https://example.com")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.TIMEOUT

    @patch("src.climatesense_kg.utils.text_processing.requests.get")
    @patch("src.climatesense_kg.utils.text_processing.trafilatura")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_connection_error(
        self, mock_sanitize: Mock, mock_trafilatura: Mock, mock_get: Mock
    ) -> None:
        """Test connection error handling."""
        mock_sanitize.return_value = "https://example.com"
        mock_trafilatura.fetch_url.return_value = None
        mock_get.side_effect = requests.ConnectionError("Connection failed")

        result = fetch_and_extract_text("https://example.com")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.CONNECTION

    @patch("src.climatesense_kg.utils.text_processing.requests.get")
    @patch("src.climatesense_kg.utils.text_processing.trafilatura")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_http_error(
        self, mock_sanitize: Mock, mock_trafilatura: Mock, mock_get: Mock
    ) -> None:
        """Test HTTP error handling."""
        mock_sanitize.return_value = "https://example.com"
        mock_trafilatura.fetch_url.return_value = None

        http_error = requests.HTTPError("404 Not Found")
        mock_response = Mock()
        mock_response.status_code = 404
        http_error.response = mock_response
        mock_get.side_effect = http_error

        result = fetch_and_extract_text("https://example.com")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.HTTP_ERROR
        assert "404" in result.error_message

    @patch("src.climatesense_kg.utils.text_processing.trafilatura")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_no_text_extracted(
        self, mock_sanitize: Mock, mock_trafilatura: Mock
    ) -> None:
        """Test when no text content is extracted."""
        mock_sanitize.return_value = "https://example.com"
        mock_trafilatura.fetch_url.return_value = "<html>content</html>"
        mock_trafilatura.extract.return_value = None

        result = fetch_and_extract_text("https://example.com")

        assert result.success is False
        assert result.error_type == ExtractionErrorType.EXTRACTION_FAILED

    @patch("src.climatesense_kg.utils.text_processing.trafilatura")
    @patch("src.climatesense_kg.utils.text_processing.sanitize_url")
    def test_no_content_downloaded(
        self, mock_sanitize: Mock, mock_trafilatura: Mock
    ) -> None:
        """Test when no content is downloaded."""
        mock_sanitize.return_value = "https://example.com"
        mock_trafilatura.fetch_url.return_value = None

        with patch(
            "src.climatesense_kg.utils.text_processing.requests"
        ) as mock_requests:
            mock_response = Mock()
            mock_response.text = ""
            mock_requests.get.return_value = mock_response
            mock_trafilatura.extract.return_value = None

            result = fetch_and_extract_text("https://example.com")

            assert result.success is False
            assert result.error_type == ExtractionErrorType.DOWNLOAD_FAILED


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
