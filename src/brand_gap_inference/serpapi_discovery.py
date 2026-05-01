from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
import json
import os
import re
from urllib.parse import urlencode
import urllib.error
import urllib.request

from .connectors import RawSourceRecord

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"
QUERY_SLUG_RE = re.compile(r"[^a-z0-9]+")


class SerpApiError(RuntimeError):
    pass


class SerpApiClient:
    def search_amazon_products(self, keyword: str) -> dict:
        raise NotImplementedError


@dataclass(frozen=True)
class UrllibSerpApiClient(SerpApiClient):
    api_key: str | None = None
    endpoint: str = SERPAPI_ENDPOINT
    amazon_domain: str = "amazon.com"
    language: str = "en_US"

    def search_amazon_products(self, keyword: str) -> dict:
        resolved_api_key = (self.api_key or os.environ.get("SERPAPI_API_KEY") or "").strip()
        if not resolved_api_key:
            raise SerpApiError("SERPAPI_API_KEY is required for live discovery")

        params = {
            "engine": "amazon",
            "amazon_domain": self.amazon_domain,
            "language": self.language,
            "k": keyword,
            "api_key": resolved_api_key,
        }
        url = f"{self.endpoint}?{urlencode(params)}"
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "BrandGapInference/0.1 (+https://example.invalid)",
            },
        )

        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", "ignore")
            raise SerpApiError(f"serpapi http error {error.code}: {body[:400]}") from error
        except Exception as error:
            raise SerpApiError(str(error)) from error

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as error:
            raise SerpApiError(f"serpapi returned invalid json: {error}") from error

        if not isinstance(payload, dict):
            raise SerpApiError("serpapi returned non-object json payload")
        return payload


@dataclass(frozen=True)
class SerpApiDiscoveryConnector:
    keyword: str
    client: SerpApiClient = field(default_factory=UrllibSerpApiClient)
    captured_at: str | None = None
    source_name: str = "amazon_api_discovery"
    provider_name: str = "serpapi"

    def fetch_snapshot(self) -> list[RawSourceRecord]:
        query = self.keyword.strip()
        if not query:
            raise ValueError("keyword must not be empty")

        captured_at = self.captured_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        provider_response = self.client.search_amazon_products(query)
        record_id = build_discovery_record_id(self.provider_name, query)
        snapshot_id = build_discovery_snapshot_id(self.source_name, record_id, captured_at)

        payload = {
            "provider": self.provider_name,
            "query": query,
            "requested_at": captured_at,
            "provider_request_metadata": {
                "endpoint": SERPAPI_ENDPOINT,
                "engine": "amazon",
                "amazon_domain": "amazon.com",
                "language": "en_US",
            },
            "provider_response": provider_response,
            "result_count": _estimate_result_count(provider_response),
        }

        return [
            RawSourceRecord(
                record_id=record_id,
                source=self.source_name,
                snapshot_id=snapshot_id,
                captured_at=captured_at,
                payload=payload,
                cursor=query,
            )
        ]


def build_discovery_record_id(provider: str, keyword: str) -> str:
    normalized_keyword = keyword.strip().lower()
    slug = QUERY_SLUG_RE.sub("-", normalized_keyword).strip("-")[:24] or "query"
    digest = hashlib.sha1(normalized_keyword.encode("utf-8")).hexdigest()[:10]
    return f"{provider}-{slug}-{digest}"


def build_discovery_snapshot_id(source_name: str, record_id: str, captured_at: str) -> str:
    safe_timestamp = captured_at.replace(":", "-")
    return f"{source_name}-{record_id}-{safe_timestamp}"


def _estimate_result_count(provider_response: dict) -> int:
    count = 0
    for key in ("organic_results", "products"):
        value = provider_response.get(key)
        if isinstance(value, list):
            count += len(value)
    return count
