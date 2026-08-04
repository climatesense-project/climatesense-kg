"""Text processing utilities."""

from dataclasses import dataclass
from enum import Enum
import html
from ipaddress import ip_address
import logging
import re
import socket
from typing import Any
from urllib.parse import quote, urljoin, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
import trafilatura  # pyright: ignore[reportMissingTypeStubs]

logger = logging.getLogger(__name__)

_URL_PATTERN = re.compile(r"http\S+")
_SURROGATE_PATTERN = re.compile(r"[\ud800-\udfff]")
_REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}
_MAX_REDIRECTS = 5
_MAX_RESPONSE_BYTES = 5 * 1024 * 1024
_ALLOWED_TEXT_CONTENT_TYPES = {
    "text/html",
    "application/xhtml+xml",
    "text/plain",
}
_INVALID_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")


class _UnsafeURLError(ValueError):
    """Raised when a URL could reach a non-public network address."""


class _ResponseTooLargeError(ValueError):
    """Raised when a response exceeds the bounded extraction size."""


class _PinnedHTTPSAdapter(HTTPAdapter):
    """Verify TLS for the original host while connecting to a resolved IP."""

    def __init__(self, hostname: str) -> None:
        self._hostname = hostname
        super().__init__()

    def init_poolmanager(
        self,
        connections: int,
        maxsize: int,
        block: bool = False,
        **pool_kwargs: Any,
    ) -> None:
        pool_kwargs["assert_hostname"] = self._hostname
        pool_kwargs["server_hostname"] = self._hostname
        super().init_poolmanager(connections, maxsize, block, **pool_kwargs)


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
    # JSON may contain escaped, unpaired UTF-16 surrogates. Convert any valid
    # surrogate pair to its Unicode code point and replace lone surrogates.
    if _SURROGATE_PATTERN.search(text):
        text = text.encode("utf-16", errors="surrogatepass").decode(
            "utf-16", errors="replace"
        )

    # Normalize HTML entities and special characters
    text = text.replace("&amp;", "&")
    text = text.replace("\xa0", "")  # Remove non-breaking spaces
    text = _URL_PATTERN.sub("", text)  # Remove URLs
    text = html.unescape(text)  # Unescape HTML entities
    text = " ".join(text.split())  # Normalize whitespace

    return text


def normalize_organization_url(url: str | None) -> str | None:
    """Normalize a URL to its canonical root form for organization deduplication.

    Reduces any URL variant to a single canonical form so that different
    representations of the same website map to one organization:
      - Strips path, query, and fragment (keeps only scheme + authority)
      - Normalizes scheme to https
      - Lowercases the hostname
      - Removes default ports (80, 443)
      - Strips trailing slashes

    Examples:
        http://www.stopfake.org           → https://www.stopfake.org
        https://www.stopfake.org/         → https://www.stopfake.org
        https://www.stopfake.org/en/about → https://www.stopfake.org
    """
    if not url:
        return None

    candidate = url.strip()
    if not candidate:
        return None

    has_scheme = bool(re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", candidate))
    to_parse = candidate if has_scheme else f"https://{candidate}"

    try:
        parsed = urlparse(to_parse)
    except Exception:
        return None

    if parsed.scheme not in ("http", "https"):
        return None

    hostname = parsed.hostname
    if not hostname:
        return None

    try:
        hostname = hostname.encode("idna").decode("ascii")
    except UnicodeError:
        return None

    port = None
    try:
        port = parsed.port
    except ValueError:
        return None

    netloc = hostname
    if port and port not in (80, 443):
        netloc = f"{hostname}:{port}"

    return f"https://{netloc}"


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
    except Exception as exc:  # pragma: no cover
        logger.warning(
            "Failed to parse URL '%s': %s", _redact_url_credentials(candidate), exc
        )
        return None

    if parsed.scheme not in ("http", "https"):
        logger.debug("Invalid URL scheme: %s", parsed.scheme)
        return None

    if not parsed.netloc:
        logger.debug("No network location found in URL")
        return None

    hostname = parsed.hostname
    if not hostname:
        logger.debug("Hostname missing in URL")
        return None

    if any(ch.isspace() for ch in hostname):
        logger.debug("Whitespace found in URL hostname")
        return None

    if any(ch in '"<>' for ch in hostname):
        logger.debug("Invalid character in URL hostname")
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
        logger.debug("Invalid port in URL")
        return None

    if port is not None:
        netloc = f"{netloc}:{port}"

    path = _quote_url_component(parsed.path, safe="/")
    query = _quote_url_component(parsed.query, safe="=&?")
    fragment = _quote_url_component(parsed.fragment, safe="")

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


def _quote_url_component(value: str, *, safe: str) -> str:
    """Quote a URL component while preserving only valid existing escapes."""
    normalized = _INVALID_PERCENT_ESCAPE.sub("%25", value)
    return quote(normalized, safe=f"{safe}%")


def _redact_url_credentials(url: str) -> str:
    """Remove URL userinfo before including a URL in diagnostics."""
    try:
        parsed = urlparse(url)
        if "@" not in parsed.netloc:
            return url
        return urlunparse(parsed._replace(netloc=parsed.netloc.rpartition("@")[2]))
    except Exception:
        return re.sub(r"(?<=//)[^/@\s]+@", "***@", url)


def _resolve_public_address(hostname: str, port: int) -> str:
    """Resolve a host once and return an address that is safe to connect to."""
    try:
        address_info = socket.getaddrinfo(
            hostname,
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror as exc:
        raise requests.ConnectionError(f"Unable to resolve host: {hostname}") from exc

    addresses: list[str] = []
    for (
        family,
        _socket_type,
        _protocol,
        _canonical_name,
        socket_address,
    ) in address_info:
        if family not in {socket.AF_INET, socket.AF_INET6}:
            continue
        address = socket_address[0]
        if not isinstance(address, str):
            continue
        if address not in addresses:
            addresses.append(address)

    if not addresses:
        raise requests.ConnectionError(f"No IP addresses found for host: {hostname}")

    for address in addresses:
        if not ip_address(address).is_global:
            raise _UnsafeURLError("URL resolves to a non-public network address")

    return addresses[0]


def _request_url_at_address(
    url: str,
    address: str,
    headers: dict[str, str],
    timeout: float,
) -> requests.Response:
    """Request a URL through its already validated address without re-resolving it."""
    parsed = urlparse(url)
    hostname = parsed.hostname
    if not hostname:
        raise _UnsafeURLError("Redirect URL has no hostname")

    default_port = 443 if parsed.scheme == "https" else 80
    port = parsed.port or default_port
    address_netloc = f"[{address}]" if ip_address(address).version == 6 else address

    pinned_url = urlunparse(parsed._replace(netloc=f"{address_netloc}:{port}"))

    host_header = f"[{hostname}]" if ":" in hostname else hostname
    if parsed.port is not None and parsed.port != default_port:
        host_header = f"{host_header}:{parsed.port}"

    session = requests.Session()
    session.trust_env = False
    if parsed.scheme == "https":
        session.mount("https://", _PinnedHTTPSAdapter(hostname))

    try:
        return session.get(
            pinned_url,
            headers={**headers, "Host": host_header},
            timeout=timeout,
            allow_redirects=False,
            stream=True,
        )
    finally:
        session.close()


def _fetch_public_url(
    url: str,
    headers: dict[str, str],
    timeout: float,
    allowed_origin: str | None = None,
) -> requests.Response:
    """Fetch a public HTTP(S) URL while validating and pinning every hop."""
    current_url = url
    required_origin = _url_origin(allowed_origin) if allowed_origin else None
    if allowed_origin and required_origin is None:
        raise _UnsafeURLError("Configured origin must use HTTP or HTTPS")

    for redirect_count in range(_MAX_REDIRECTS + 1):
        parsed = urlparse(current_url)
        hostname = parsed.hostname
        if parsed.scheme not in {"http", "https"} or not hostname:
            raise _UnsafeURLError("Redirect URL must use HTTP or HTTPS")
        if parsed.username is not None or parsed.password is not None:
            raise _UnsafeURLError("URL credentials are not allowed")
        if required_origin is not None and _url_origin(current_url) != required_origin:
            raise _UnsafeURLError("URL is outside the configured origin")

        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        address = _resolve_public_address(hostname, port)
        response = _request_url_at_address(
            current_url,
            address,
            headers,
            timeout,
        )

        location = response.headers.get("Location")
        if response.status_code not in _REDIRECT_STATUS_CODES or not location:
            return response

        if redirect_count == _MAX_REDIRECTS:
            response.close()
            raise requests.TooManyRedirects(
                f"Exceeded {_MAX_REDIRECTS} redirects",
                response=response,
            )

        redirected_url = sanitize_url(urljoin(current_url, location))
        response.close()
        if not redirected_url:
            raise _UnsafeURLError("Redirect target is not a valid HTTP(S) URL")
        current_url = redirected_url

    raise requests.TooManyRedirects(f"Exceeded {_MAX_REDIRECTS} redirects")


def _url_origin(url: str) -> tuple[str, str, int] | None:
    """Return a canonical HTTP(S) origin tuple for exact comparisons."""
    parsed = urlparse(url)
    hostname = parsed.hostname
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not hostname:
        return None

    try:
        port = parsed.port
    except ValueError:
        return None

    default_port = 443 if scheme == "https" else 80
    return scheme, hostname.rstrip(".").lower(), port or default_port


def _read_bounded_text_response(
    response: requests.Response, max_bytes: int = _MAX_RESPONSE_BYTES
) -> str:
    """Read an allowed textual response without exceeding the byte limit."""
    content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip()
    if content_type.lower() not in _ALLOWED_TEXT_CONTENT_TYPES:
        raise ValueError(
            f"Unsupported response content type: {content_type or '<none>'}"
        )

    content_length = response.headers.get("Content-Length")
    if content_length:
        try:
            declared_length = int(content_length)
        except ValueError as exc:
            raise ValueError("Invalid response Content-Length") from exc
        if declared_length > max_bytes:
            raise _ResponseTooLargeError(
                f"Response exceeds the {max_bytes}-byte download limit"
            )

    chunks: list[bytes] = []
    bytes_read = 0
    for chunk in response.iter_content(chunk_size=64 * 1024):
        if not chunk:
            continue
        bytes_read += len(chunk)
        if bytes_read > max_bytes:
            raise _ResponseTooLargeError(
                f"Response exceeds the {max_bytes}-byte download limit"
            )
        chunks.append(chunk)

    encoding = response.encoding or "utf-8"
    return b"".join(chunks).decode(encoding, errors="replace")


def fetch_and_extract_text(url: str, timeout: float = 10) -> TextExtractionResult:
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
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "DNT": "1",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
        }
        response = _fetch_public_url(sanitized_url, headers=headers, timeout=timeout)
        try:
            response.raise_for_status()
            downloaded = _read_bounded_text_response(response)
        finally:
            response.close()

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

    except _UnsafeURLError as e:
        logger.warning("Rejected unsafe URL during text extraction: %s", e)
        return TextExtractionResult(
            success=False,
            error_message=str(e),
            error_type=ExtractionErrorType.INVALID_URL,
        )
    except (_ResponseTooLargeError, ValueError) as e:
        logger.warning("Rejected URL response during text extraction: %s", e)
        return TextExtractionResult(
            success=False,
            error_message=str(e),
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
