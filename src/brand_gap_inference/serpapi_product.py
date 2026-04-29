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
ASIN_RE = re.compile(r"^[A-Z0-9]{10}$", re.IGNORECASE)


class SerpApiProductError(RuntimeError):
    pass


class SerpApiProductClient:
    def fetch_amazon_product(self, asin: str) -> dict:
        raise NotImplementedError


@dataclass(frozen=True)
class UrllibSerpApiProductClient(SerpApiProductClient):
    api_key: str | None = None
    endpoint: str = SERPAPI_ENDPOINT
    amazon_domain: str = "amazon.com"
    language: str = "en_US"

    def fetch_amazon_product(self, asin: str) -> dict:
        resolved_api_key = (self.api_key or os.environ.get("SERPAPI_API_KEY") or "").strip()
        if not resolved_api_key:
            raise SerpApiProductError("SERPAPI_API_KEY is required for live product detail collection")

        params = {
            "engine": "amazon_product",
            "amazon_domain": self.amazon_domain,
            "language": self.language,
            "asin": asin,
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
            raise SerpApiProductError(f"serpapi product http error {error.code}: {body[:400]}") from error
        except Exception as error:
            raise SerpApiProductError(str(error)) from error

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as error:
            raise SerpApiProductError(f"serpapi product returned invalid json: {error}") from error

        if not isinstance(payload, dict):
            raise SerpApiProductError("serpapi product returned non-object json payload")
        return payload


@dataclass(frozen=True)
class SerpApiProductConnector:
    asins: list[str]
    client: SerpApiProductClient = field(default_factory=UrllibSerpApiProductClient)
    captured_at: str | None = None
    source_name: str = "amazon_api_product"
    provider_name: str = "serpapi"

    def fetch_snapshot(self) -> list[RawSourceRecord]:
        normalized_asins = normalize_asins(self.asins)
        if not normalized_asins:
            raise ValueError("at least one valid ASIN is required for product detail collection")

        captured_at = self.captured_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        snapshot_id = build_product_snapshot_id(self.source_name, normalized_asins, captured_at)
        records: list[RawSourceRecord] = []

        for asin in normalized_asins:
            provider_response = self.client.fetch_amazon_product(asin)
            payload = {
                "provider": self.provider_name,
                "asin": asin,
                "requested_at": captured_at,
                "provider_request_metadata": {
                    "endpoint": SERPAPI_ENDPOINT,
                    "engine": "amazon_product",
                    "amazon_domain": "amazon.com",
                    "language": "en_US",
                },
                "provider_response": provider_response,
            }
            records.append(
                RawSourceRecord(
                    record_id=build_product_record_id(self.provider_name, asin),
                    source=self.source_name,
                    snapshot_id=snapshot_id,
                    captured_at=captured_at,
                    payload=payload,
                    cursor=asin,
                )
            )

        return records


def normalize_asins(asins: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw_asin in asins:
        asin = str(raw_asin).strip().upper()
        if not ASIN_RE.match(asin) or asin in seen:
            continue
        seen.add(asin)
        normalized.append(asin)
    return normalized


def build_product_record_id(provider: str, asin: str) -> str:
    return f"{provider}-amazon-product-{asin.upper()}"


def build_product_snapshot_id(source_name: str, asins: list[str], captured_at: str) -> str:
    digest = hashlib.sha1(",".join(asins).encode("utf-8")).hexdigest()[:10]
    safe_timestamp = captured_at.replace(":", "-")
    return f"{source_name}-{digest}-{safe_timestamp}"
