"""Text processing utilities."""

from dataclasses import dataclass
from enum import Enum
import html
import logging
import re
from urllib.parse import quote, urlparse, urlunparse

import requests
import trafilatura  # pyright: ignore[reportMissingTypeStubs]

logger = logging.getLogger(__name__)

_URL_PATTERN = re.compile(r"http\S+")


class ExtractionErrorType(Enum):
    """Error types for text extraction operations."""

    INVALID_INPUT = "invalid_input"
    INVALID_URL = "invalid_url"
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    HTTP_ERROR = "http"
    REQUEST_ERROR = "request"
    DOWNLOAD_FAILED = "download_failed"
    EXTRACTION_FAILED = "extraction_failed"
    UNKNOWN = "unknown"

    @property
    def is_retryable(self) -> bool:
        """Return True if this error type should be retried."""
        retryable_types = {
            ExtractionErrorType.TIMEOUT,
            ExtractionErrorType.CONNECTION,
            ExtractionErrorType.REQUEST_ERROR,
            ExtractionErrorType.DOWNLOAD_FAILED,
            ExtractionErrorType.UNKNOWN,
        }
        return self in retryable_types


@dataclass
class TextExtractionResult:
    """Result of text extraction operation."""

    success: bool
    content: str = ""
    error_message: str = ""
    error_type: ExtractionErrorType | None = None


def normalize_text(text: str) -> str:
    """
    Normalize text for consistent processing.

    Args:
        text: Raw text to normalize

    Returns:
        str: Normalized text
    """
    # Normalize HTML entities and special characters
    text = text.replace("&amp;", "&")
    text = text.replace("\xa0", "")  # Remove non-breaking spaces
    text = _URL_PATTERN.sub("", text)  # Remove URLs
    text = html.unescape(text)  # Unescape HTML entities
    text = " ".join(text.split())  # Normalize whitespace

    return text


def sanitize_url(url: str) -> str | None:
    """Sanitize a URL and ensure it is safe for RDF serialization."""

    if not url:
        return None

    candidate = url.strip()
    if not candidate:
        return None

    has_scheme = bool(re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", candidate))
    to_parse = candidate if has_scheme else f"https://{candidate}"

    try:
        parsed = urlparse(to_parse)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning(f"Failed to parse URL '{candidate}': {exc}")
        return None

    if parsed.scheme not in ("http", "https"):
        logger.debug(f"Invalid scheme '{parsed.scheme}' in URL: {candidate}")
        return None

    if not parsed.netloc:
        logger.debug(f"No netloc found in URL: {candidate}")
        return None

    hostname = parsed.hostname
    if not hostname:
        logger.debug(f"Hostname missing in URL: {candidate}")
        return None

    if any(ch.isspace() for ch in hostname):
        logger.debug(f"Whitespace found in hostname for URL: {candidate}")
        return None

    if any(ch in '"<>' for ch in hostname):
        logger.debug(f"Invalid character in hostname for URL: {candidate}")
        return None

    try:
        hostname_ascii = hostname.encode("idna").decode("ascii")
    except UnicodeError:
        logger.debug(f"Hostname contains invalid characters: {hostname}")
        return None

    netloc = hostname_ascii
    try:
        port = parsed.port
    except ValueError:
        logger.debug(f"Invalid port in URL: {candidate}")
        return None

    if port is not None:
        netloc = f"{netloc}:{port}"

    if parsed.username:
        userinfo = quote(parsed.username, safe="")
        if parsed.password:
            userinfo += f":{quote(parsed.password, safe='')}"
        netloc = f"{userinfo}@{netloc}"

    path = quote(parsed.path, safe="/")
    query = quote(parsed.query, safe="=&?")
    fragment = quote(parsed.fragment, safe="")

    sanitized = urlunparse(
        (
            parsed.scheme,
            netloc,
            path,
            parsed.params,
            query,
            fragment,
        )
    )

    return sanitized if sanitized else None


def fetch_and_extract_text(url: str) -> TextExtractionResult:
    """
    Fetch and extract main text content from a URL using trafilatura.

    This function attempts to fetch web content and extract the main text
    using trafilatura's content extraction capabilities.

    Args:
        url: URL to fetch and extract text from

    Returns:
        TextExtractionResult: Result containing extracted text or error information
    """
    if not url:
        return TextExtractionResult(
            success=False,
            error_message="Empty URL provided",
            error_type=ExtractionErrorType.INVALID_INPUT,
        )

    sanitized_url = sanitize_url(url)
    if not sanitized_url:
        logger.warning("Invalid URL provided for text extraction")
        return TextExtractionResult(
            success=False,
            error_message="Invalid URL format",
            error_type=ExtractionErrorType.INVALID_URL,
        )

    try:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.7",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "Sec-CH-UA": '"Chromium";v="139", "Not=A?Brand";v="24", "Google Chrome";v="139"',
            "Accept-Encoding": "gzip, deflate, br",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "DNT": "1",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
        }
        response = requests.get(sanitized_url, headers=headers, timeout=10)
        response.raise_for_status()
        downloaded = response.text

        if downloaded:
            main_text: str | None = trafilatura.extract(  # pyright: ignore[reportUnknownMemberType]
                downloaded
            )
            if main_text:
                normalized_text: str = normalize_text(main_text)
                return TextExtractionResult(success=True, content=normalized_text)
            else:
                logger.warning(f"No text content extracted from URL: {sanitized_url}")
                return TextExtractionResult(
                    success=False,
                    error_message="No text content found",
                    error_type=ExtractionErrorType.EXTRACTION_FAILED,
                )

        logger.warning(f"No content downloaded from URL: {sanitized_url}")
        return TextExtractionResult(
            success=False,
            error_message="No content downloaded",
            error_type=ExtractionErrorType.DOWNLOAD_FAILED,
        )

    except requests.Timeout as e:
        logger.error(f"Timeout fetching URL {sanitized_url}: {e}")
        return TextExtractionResult(
            success=False, error_message=str(e), error_type=ExtractionErrorType.TIMEOUT
        )
    except requests.ConnectionError as e:
        logger.error(f"Connection error for URL {sanitized_url}: {e}")
        return TextExtractionResult(
            success=False,
            error_message=str(e),
            error_type=ExtractionErrorType.CONNECTION,
        )
    except requests.HTTPError as e:
        logger.error(f"HTTP error for URL {sanitized_url}: {e}")
        status_code = e.response.status_code if e.response else "unknown"
        return TextExtractionResult(
            success=False,
            error_message=f"HTTP {status_code}: {e}",
            error_type=ExtractionErrorType.HTTP_ERROR,
        )
    except requests.RequestException as e:
        logger.error(f"Request error for URL {sanitized_url}: {e}")
        return TextExtractionResult(
            success=False,
            error_message=str(e),
            error_type=ExtractionErrorType.REQUEST_ERROR,
        )
    except Exception as e:
        logger.error(f"Unexpected error extracting text from URL {sanitized_url}: {e}")
        return TextExtractionResult(
            success=False, error_message=str(e), error_type=ExtractionErrorType.UNKNOWN
        )
