"""Text processing utilities."""

import html
import logging
import re
from urllib.parse import quote, urlparse, urlunparse

import requests
import trafilatura  # pyright: ignore[reportMissingTypeStubs]

logger = logging.getLogger(__name__)


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
    text = re.sub(r"http\S+", "", text)  # Remove URLs
    text = html.unescape(text)  # Unescape HTML entities
    text = " ".join(text.split())  # Normalize whitespace

    return text


def sanitize_url(url: str) -> str | None:
    """
    Sanitize URL by properly encoding invalid URI characters.

    Args:
        url: URL to sanitize

    Returns:
        str | None: Sanitized URL or None if invalid
    """
    if not url:
        return None

    if "://" not in url:
        url = "https://" + url

    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return None
        if not parsed.netloc:
            return None

        path = quote(parsed.path, safe="/")
        query = quote(parsed.query, safe="=&?")
        fragment = quote(parsed.fragment, safe="")

        sanitized = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                path,
                parsed.params,
                query,
                fragment,
            )
        )
        return sanitized
    except Exception:
        return None


def fetch_and_extract_text(url: str) -> str:
    """
    Fetch and extract main text content from a URL using trafilatura.

    This function attempts to fetch web content and extract the main text
    using trafilatura's content extraction capabilities. It includes fallback
    mechanisms for better reliability.

    Args:
        url: URL to fetch and extract text from

    Returns:
        str: Extracted main text content, or error message if extraction fails
    """
    if not url:
        return ""

    sanitized_url = sanitize_url(url)
    if not sanitized_url:
        logger.warning("Invalid URL provided for text extraction")
        return "Error: Invalid URL"

    try:
        # Attempt with trafilatura's default fetch
        downloaded = trafilatura.fetch_url(sanitized_url)

        if downloaded is None:
            # Fallback with a common user-agent
            headers = {
                "Accept-Language": "en-US,en;q=0.6'",
                "User-Agent": "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
                "Sec-CH-UA": '"Not_A Brand";v="8", "Chromium";v="108", "Microsoft Edge";v="108"',
            }
            response = requests.get(sanitized_url, headers=headers, timeout=15)
            response.raise_for_status()
            downloaded = response.text

        if downloaded:
            main_text = trafilatura.extract(  # pyright: ignore[reportUnknownMemberType]
                downloaded
            )
            if main_text:
                return normalize_text(main_text)
            else:
                logger.warning(f"No text content extracted from URL: {sanitized_url}")
                return ""

        logger.warning(f"No content downloaded from URL: {sanitized_url}")
        return ""

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch URL {sanitized_url}: {e}")
        return f"Error fetching URL: {e}"
    except Exception as e:
        logger.error(
            f"Error extracting text from URL {sanitized_url} with Trafilatura: {e}"
        )
        return f"Error extracting text: {e}"
