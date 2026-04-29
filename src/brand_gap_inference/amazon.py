from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
from html.parser import HTMLParser
import re
from urllib.parse import urlparse

from .browser_capture import BrowserCaptureRunner, NodePlaywrightAmazonCaptureRunner
from .connectors import RawSourceRecord
from .http_client import HttpFetcher, UrllibHttpFetcher

AMAZON_HOST_PATTERN = re.compile(r"(^|\.)amazon\.[a-z.]+$", re.IGNORECASE)
ASIN_PATTERNS = (
    re.compile(r"/dp/([A-Z0-9]{10})(?:[/?]|$)", re.IGNORECASE),
    re.compile(r"/gp/product/([A-Z0-9]{10})(?:[/?]|$)", re.IGNORECASE),
    re.compile(r"/product/([A-Z0-9]{10})(?:[/?]|$)", re.IGNORECASE),
)
BOT_CHECK_TOKENS = ("captcha", "robot check", "automated access", "sorry")
DEFAULT_HEADERS = {
    "User-Agent": "BrandGapInference/0.1 (+https://example.invalid)",
    "Accept-Language": "en-US,en;q=0.9",
}


class _TitleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._inside_title = False
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "title":
            self._inside_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._inside_title = False

    def handle_data(self, data: str) -> None:
        if self._inside_title:
            self._parts.append(data)

    @property
    def title(self) -> str | None:
        value = "".join(self._parts).strip()
        return value or None


@dataclass(frozen=True)
class AmazonProductConnector:
    product_url: str
    fetcher: HttpFetcher = field(default_factory=UrllibHttpFetcher)
    captured_at: str | None = None
    source_name: str = "amazon"

    def fetch_snapshot(self) -> list[RawSourceRecord]:
        asin = extract_amazon_asin(self.product_url)
        if asin is None:
            raise ValueError("unable to extract a valid ASIN from the provided Amazon URL")

        captured_at = self.captured_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        canonical_url = canonicalize_amazon_product_url(self.product_url)
        response = self.fetcher.fetch(canonical_url, headers=DEFAULT_HEADERS, timeout_seconds=30)
        snapshot_id = build_snapshot_id(self.source_name, asin, captured_at)

        payload = {
            "asin": asin,
            "acquisition_method": "http",
            "original_url": self.product_url,
            "canonical_url": canonical_url,
            "final_url": response.final_url,
            "status_code": response.status_code,
            "page_title": extract_html_title(response.body),
            "is_robot_check": detect_robot_check(response.body),
            "content_sha256": hashlib.sha256(response.body.encode("utf-8")).hexdigest(),
            "headers": response.headers,
            "html": response.body,
        }

        return [
            RawSourceRecord(
                record_id=asin,
                source=self.source_name,
                snapshot_id=snapshot_id,
                captured_at=captured_at,
                payload=payload,
                cursor=canonical_url,
            )
        ]


@dataclass(frozen=True)
class AmazonBrowserProductConnector:
    product_url: str
    capture_runner: BrowserCaptureRunner = field(default_factory=NodePlaywrightAmazonCaptureRunner)
    captured_at: str | None = None
    source_name: str = "amazon"

    def fetch_snapshot(self) -> list[RawSourceRecord]:
        asin = extract_amazon_asin(self.product_url)
        if asin is None:
            raise ValueError("unable to extract a valid ASIN from the provided Amazon URL")

        captured_at = self.captured_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        canonical_url = canonicalize_amazon_product_url(self.product_url)
        capture = self.capture_runner.capture(canonical_url, timeout_seconds=45)
        snapshot_id = build_snapshot_id(self.source_name, asin, captured_at)

        payload = {
            "asin": asin,
            "acquisition_method": "browser_playwright",
            "browser_engine": "chromium",
            "original_url": self.product_url,
            "canonical_url": canonical_url,
            "final_url": capture.final_url,
            "status_code": capture.status_code,
            "page_title": capture.page_title,
            "is_robot_check": capture.is_robot_check,
            "content_sha256": hashlib.sha256(capture.html.encode("utf-8")).hexdigest(),
            "headers": {},
            "html": capture.html,
            "capture_diagnostics": capture.capture_diagnostics,
        }

        return [
            RawSourceRecord(
                record_id=asin,
                source=self.source_name,
                snapshot_id=snapshot_id,
                captured_at=captured_at,
                payload=payload,
                cursor=canonical_url,
            )
        ]


def extract_amazon_asin(url: str) -> str | None:
    parsed = urlparse(url)
    if not _is_amazon_host(parsed.netloc):
        return None

    for pattern in ASIN_PATTERNS:
        match = pattern.search(parsed.path)
        if match:
            return match.group(1).upper()
    return None


def canonicalize_amazon_product_url(url: str) -> str:
    parsed = urlparse(url)
    if not _is_amazon_host(parsed.netloc):
        raise ValueError("expected an Amazon product URL")

    asin = extract_amazon_asin(url)
    if asin is None:
        raise ValueError("unable to extract a valid ASIN from the provided Amazon URL")

    host = parsed.netloc.lower()
    return f"{parsed.scheme or 'https'}://{host}/dp/{asin}"


def detect_robot_check(html: str) -> bool:
    lowered = html.lower()
    title = (extract_html_title(html) or "").lower()

    if "robot check" in title:
        return True

    explicit_markers = (
        "captchacharacters",
        "enter the characters you see below",
        "type the characters you see in this image",
        "sorry, we just need to make sure you're not a robot",
        "to discuss automated access to amazon data please contact",
    )
    return any(marker in lowered for marker in explicit_markers)


def extract_html_title(html: str) -> str | None:
    parser = _TitleParser()
    parser.feed(html)
    return parser.title


def build_snapshot_id(source_name: str, asin: str, captured_at: str) -> str:
    safe_timestamp = captured_at.replace(":", "-")
    return f"{source_name}-{asin}-{safe_timestamp}"


def _is_amazon_host(host: str) -> bool:
    normalized = host.lower().strip()
    return bool(normalized and AMAZON_HOST_PATTERN.search(normalized))
