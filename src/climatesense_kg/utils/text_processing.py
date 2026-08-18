"""Text processing utilities."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from enum import Enum
import html
from html.parser import HTMLParser
from ipaddress import ip_address
import logging
import re
import socket
from typing import Any
from urllib.parse import quote, urljoin, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
import trafilatura  # pyright: ignore[reportMissingTypeStubs]

from .. import USER_AGENT

logger = logging.getLogger(__name__)

_URL_PATTERN = re.compile(r"https?://\S+", flags=re.IGNORECASE)
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

_DEFAULT_DOCUMENT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.1",
    "User-Agent": USER_AGENT,
}

# AFP's edge rejects generic HTTP clients, but accepts a coherent browser
# navigation request. Keep this compatibility profile limited to AFP-owned
# hosts instead of impersonating a browser for every extracted document.
_AFP_DOCUMENT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.7",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    ),
    "Sec-CH-UA": ('"Chromium";v="139", "Not=A?Brand";v="24", "Google Chrome";v="139"'),
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
    "Upgrade-Insecure-Requests": "1",
}


class _UnsafeURLError(ValueError):
    """Raised when a URL could reach a non-public network address."""


class _ResponseTooLargeError(ValueError):
    """Raised when a response exceeds the bounded extraction size."""


class _UnsupportedContentError(ValueError):
    """Raised when a response cannot contain supported review text."""


class _DNSResolutionError(requests.ConnectionError):
    """Raised when a public hostname cannot be resolved."""


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
    DNS = "dns"
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    HTTP_ERROR = "http"
    ACCESS_CHALLENGE = "access_challenge"
    REQUEST_ERROR = "request"
    DOWNLOAD_FAILED = "download_failed"
    RESPONSE_TOO_LARGE = "response_too_large"
    UNSUPPORTED_CONTENT = "unsupported_content"
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
    final_url: str | None = None
    canonical_url: str | None = None
    http_status: int | None = None
    retry_at: datetime | None = None

    @property
    def retryable_immediately(self) -> bool:
        """Return whether another request is useful during the current run."""

        if self.error_type is ExtractionErrorType.HTTP_ERROR:
            return self.http_status == 408 or bool(
                self.http_status is not None and self.http_status >= 500
            )
        return bool(self.error_type and self.error_type.is_retryable)


class _CanonicalLinkParser(HTMLParser):
    """Capture the first HTML canonical link without constructing a DOM."""

    def __init__(self) -> None:
        super().__init__()
        self.href: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.href is not None or tag.casefold() != "link":
            return
        values = {name.casefold(): value for name, value in attrs}
        relations = (values.get("rel") or "").casefold().split()
        href = values.get("href")
        if "canonical" in relations and href:
            self.href = href.strip()


def _extract_canonical_url(document: str, final_url: str) -> str | None:
    parser = _CanonicalLinkParser()
    try:
        parser.feed(document)
    except Exception:
        return None
    if not parser.href:
        return None
    return sanitize_url(urljoin(final_url, parser.href))


def canonicalize_text(text: str) -> str:
    """Canonicalize text without discarding identity-bearing content.

    Args:
        text: Raw text to normalize

    Returns:
        str: Canonicalized text
    """
    # JSON may contain escaped, unpaired UTF-16 surrogates. Convert any valid
    # surrogate pair to its Unicode code point and replace lone surrogates.
    if _SURROGATE_PATTERN.search(text):
        text = text.encode("utf-16", errors="surrogatepass").decode(
            "utf-16", errors="replace"
        )

    # Normalize HTML entities and whitespace while preserving URLs and other
    # content that can distinguish one claim from another.
    text = text.replace("&amp;", "&")
    text = text.replace("\xa0", " ")  # Normalize non-breaking spaces
    text = html.unescape(text)  # Unescape HTML entities
    text = " ".join(text.split())  # Normalize whitespace

    return text


def normalize_analysis_text(text: str) -> str:
    """Normalize text for NLP analysis, where URLs carry little meaning."""

    text = canonicalize_text(text)
    text = _URL_PATTERN.sub("", text)
    return " ".join(text.split())


def validate_claim_text(text: str) -> str:
    """Return canonical claim text or raise when it is not meaningful."""

    canonical_text = canonicalize_text(text)
    if not canonical_text:
        raise ValueError("claim text is empty after canonicalization")
    if _URL_PATTERN.fullmatch(canonical_text):
        raise ValueError("claim text contains only a URL")
    if not any(character.isalnum() for character in canonical_text):
        raise ValueError("claim text does not contain meaningful content")

    return canonical_text


def normalize_organization_url(url: str | None) -> str | None:
    """Normalize a URL to its canonical root form for organization deduplication.

    Reduces any URL variant to a single canonical form so that different
    representations of the same website map to one organization:
      - Strips path, query, and fragment (keeps only scheme + authority)
      - Normalizes scheme to https
      - Lowercases the hostname and removes a leading ``www.``
      - Removes default ports (80, 443)
      - Strips trailing slashes

    Examples:
        http://www.stopfake.org           → https://stopfake.org
        https://www.stopfake.org/         → https://stopfake.org
        https://www.stopfake.org/en/about → https://stopfake.org
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
    if any(ch.isspace() or ch in '"<>' for ch in hostname):
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

    if hostname.startswith("www."):
        hostname = hostname[4:]

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
            "Failed to parse URL '%s': %s", redact_url_credentials(candidate), exc
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


def normalize_document_url(url: str) -> str | None:
    """Return the normalized HTTP resource identity used by the pipeline."""

    sanitized = sanitize_url(url)
    if sanitized is None:
        return None
    parsed = urlparse(sanitized)
    hostname = parsed.hostname
    if hostname is None:  # pragma: no cover - guaranteed by sanitize_url
        return None
    try:
        port = parsed.port
    except ValueError:  # pragma: no cover - guaranteed by sanitize_url
        return None
    default_port = (parsed.scheme == "http" and port == 80) or (
        parsed.scheme == "https" and port == 443
    )
    netloc = hostname if port is None or default_port else f"{hostname}:{port}"
    return urlunparse(
        parsed._replace(
            netloc=netloc,
            path=parsed.path or "/",
            fragment="",
        )
    )


def _quote_url_component(value: str, *, safe: str) -> str:
    """Quote a URL component while preserving only valid existing escapes."""
    normalized = _INVALID_PERCENT_ESCAPE.sub("%25", value)
    return quote(normalized, safe=f"{safe}%")


def redact_url_credentials(url: str) -> str:
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
        raise _DNSResolutionError(f"Unable to resolve host: {hostname}") from exc

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
        raise _DNSResolutionError(f"No IP addresses found for host: {hostname}")

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
    headers: dict[str, str] | None = None,
    timeout: float = 10,
    allowed_origin: str | None = None,
    *,
    header_provider: Callable[[str], dict[str, str]] | None = None,
) -> requests.Response:
    """Fetch a public HTTP(S) URL while validating and pinning every hop."""
    if (headers is None) == (header_provider is None):
        raise ValueError("Provide exactly one of headers or header_provider")

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
        request_headers = (
            header_provider(current_url) if header_provider is not None else headers
        )
        if request_headers is None:  # Guard for static type narrowing.
            raise ValueError("Request headers are required")
        response = _request_url_at_address(
            current_url,
            address,
            request_headers,
            timeout,
        )

        location = response.headers.get("Location")
        if response.status_code not in _REDIRECT_STATUS_CODES or not location:
            response.url = current_url
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


def _document_headers_for_url(url: str) -> dict[str, str]:
    """Return the request profile appropriate for a document URL."""
    hostname = (urlparse(url).hostname or "").rstrip(".").casefold()
    profile = (
        _AFP_DOCUMENT_HEADERS
        if hostname == "afp.com" or hostname.endswith(".afp.com")
        else _DEFAULT_DOCUMENT_HEADERS
    )
    return dict(profile)


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
        raise _UnsupportedContentError(
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


def _parse_retry_after(value: object) -> datetime | None:
    """Parse an HTTP Retry-After value into an absolute UTC timestamp."""

    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.strip()
    now = datetime.now(UTC)
    if candidate.isdigit():
        return now + timedelta(seconds=int(candidate))
    try:
        parsed = parsedate_to_datetime(candidate)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return max(now, parsed.astimezone(UTC))


def _looks_like_access_challenge(document: str, response_url: str) -> bool:
    """Conservatively identify bot challenges returned with HTTP 200."""

    lowered = document[:200_000].casefold()
    url_path = urlparse(response_url).path.casefold()
    strong_markers = (
        "cf-chl-",
        "/cdn-cgi/challenge-platform/",
        'id="challenge-form"',
        "id='challenge-form'",
    )
    if "/cdn-cgi/challenge-platform/" in url_path or any(
        marker in lowered for marker in strong_markers
    ):
        return True

    challenge_markers = (
        "verify you are human",
        "checking your browser",
        "enable javascript and cookies",
        "g-recaptcha",
        "hcaptcha",
        "cf-turnstile",
        "attention required! | cloudflare",
    )
    return sum(marker in lowered for marker in challenge_markers) >= 2


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
        response = _fetch_public_url(
            sanitized_url,
            timeout=timeout,
            header_provider=_document_headers_for_url,
        )
        try:
            response.raise_for_status()
            downloaded = _read_bounded_text_response(response)
            response_url = response.url
            final_url = (
                sanitize_url(response_url)
                if isinstance(response_url, str)
                else sanitized_url
            ) or sanitized_url
        finally:
            response.close()

        if downloaded:
            canonical_url = _extract_canonical_url(downloaded, final_url)
            if _looks_like_access_challenge(downloaded, final_url):
                logger.warning("Access challenge returned for URL: %s", sanitized_url)
                return TextExtractionResult(
                    success=False,
                    error_message="Website returned an access challenge",
                    error_type=ExtractionErrorType.ACCESS_CHALLENGE,
                    final_url=final_url,
                )
            main_text: str | None = trafilatura.extract(  # pyright: ignore[reportUnknownMemberType]
                downloaded
            )
            if main_text:
                analysis_text = normalize_analysis_text(main_text)
                return TextExtractionResult(
                    success=True,
                    content=analysis_text,
                    final_url=final_url,
                    canonical_url=canonical_url,
                )
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
    except _ResponseTooLargeError as e:
        logger.warning("Rejected oversized URL response: %s", e)
        return TextExtractionResult(
            success=False,
            error_message=str(e),
            error_type=ExtractionErrorType.RESPONSE_TOO_LARGE,
        )
    except _UnsupportedContentError as e:
        logger.warning("Rejected unsupported URL response: %s", e)
        return TextExtractionResult(
            success=False,
            error_message=str(e),
            error_type=ExtractionErrorType.UNSUPPORTED_CONTENT,
        )
    except ValueError as e:
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
    except _DNSResolutionError as e:
        logger.error("DNS error for URL %s: %s", sanitized_url, e)
        return TextExtractionResult(
            success=False,
            error_message=str(e),
            error_type=ExtractionErrorType.DNS,
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
        status_code = e.response.status_code if e.response is not None else None
        retry_at = (
            _parse_retry_after(e.response.headers.get("Retry-After"))
            if e.response is not None
            else None
        )
        return TextExtractionResult(
            success=False,
            error_message=f"HTTP {status_code or 'unknown'}: {e}",
            error_type=ExtractionErrorType.HTTP_ERROR,
            http_status=status_code,
            retry_at=retry_at,
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
