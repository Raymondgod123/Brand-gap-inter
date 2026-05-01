from __future__ import annotations

from dataclasses import dataclass
import json
import re
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from .amazon import canonicalize_amazon_product_url, extract_amazon_asin
from .connectors import RawSourceRecord
from .contracts import assert_valid
from .raw_store import SourceSnapshotManifest


@dataclass(frozen=True)
class DiscoveryIssue:
    code: str
    message: str
    severity: str


@dataclass(frozen=True)
class DiscoveryRecordResult:
    discovery_id: str
    snapshot_id: str
    source: str
    provider: str
    query: str
    rank: int
    status: str
    title: str | None
    product_url: str | None
    asin: str | None
    price: float | None
    currency: str | None
    rating: float | None
    review_count: int | None
    sponsored: bool | None
    provider_metadata: dict[str, object]
    raw_payload_uri: str
    warnings: list[str]
    issues: list[DiscoveryIssue]

    def to_dict(self) -> dict:
        payload = {
            "discovery_id": self.discovery_id,
            "snapshot_id": self.snapshot_id,
            "source": self.source,
            "provider": self.provider,
            "query": self.query,
            "rank": self.rank,
            "status": self.status,
            "title": self.title,
            "product_url": self.product_url,
            "asin": self.asin,
            "price": self.price,
            "currency": self.currency,
            "rating": self.rating,
            "review_count": self.review_count,
            "sponsored": self.sponsored,
            "provider_metadata": self.provider_metadata,
            "raw_payload_uri": self.raw_payload_uri,
            "warnings": self.warnings,
            "issues": [
                {"code": issue.code, "message": issue.message, "severity": issue.severity}
                for issue in self.issues
            ],
        }
        assert_valid("discovery_result_record", payload)
        return payload

    def to_report_dict(self) -> dict:
        payload = {
            "discovery_id": self.discovery_id,
            "rank": self.rank,
            "status": self.status,
            "warning_count": len(self.warnings),
            "issue_count": len(self.issues),
        }
        if self.title is not None:
            payload["title"] = self.title
        if self.product_url is not None:
            payload["product_url"] = self.product_url
        if self.asin is not None:
            payload["asin"] = self.asin
        return payload


@dataclass(frozen=True)
class DiscoverySummary:
    run_status: str
    total_candidates: int
    valid_candidates: int
    invalid_candidates: int
    warning_records: int


@dataclass(frozen=True)
class DiscoveryBatchResult:
    snapshot_id: str
    source: str
    provider: str
    query: str
    summary: DiscoverySummary
    records: list[DiscoveryRecordResult]

    def to_report_dict(self) -> dict:
        payload = {
            "snapshot_id": self.snapshot_id,
            "source": self.source,
            "provider": self.provider,
            "query": self.query,
            "run_status": self.summary.run_status,
            "total_candidates": self.summary.total_candidates,
            "valid_candidates": self.summary.valid_candidates,
            "invalid_candidates": self.summary.invalid_candidates,
            "warning_records": self.summary.warning_records,
            "records": [record.to_report_dict() for record in self.records],
        }
        assert_valid("discovery_batch_report", payload)
        return payload


class DiscoveryExtractor:
    def extract_snapshot(
        self,
        manifest: SourceSnapshotManifest,
        records: list[RawSourceRecord],
    ) -> DiscoveryBatchResult:
        extracted_records: list[DiscoveryRecordResult] = []
        provider = ""
        query = ""

        for record in records:
            payload = record.payload
            provider = str(payload.get("provider") or provider or "unknown")
            query = str(payload.get("query") or query or "").strip()
            raw_payload_uri = f"{manifest.storage_uri}/{record.record_id}.json"
            provider_response = payload.get("provider_response")
            if not isinstance(provider_response, dict):
                extracted_records.append(
                    self._invalid_record(
                        snapshot_id=manifest.snapshot_id,
                        source=record.source,
                        provider=provider,
                        query=query or "unknown query",
                        rank=1,
                        raw_payload_uri=raw_payload_uri,
                        issue_code="provider_response_missing",
                        issue_message="provider_response is missing or not an object",
                    )
                )
                continue

            result_entries = _collect_result_entries(provider_response)
            if not result_entries:
                extracted_records.append(
                    self._invalid_record(
                        snapshot_id=manifest.snapshot_id,
                        source=record.source,
                        provider=provider,
                        query=query or "unknown query",
                        rank=1,
                        raw_payload_uri=raw_payload_uri,
                        issue_code="no_candidates_found",
                        issue_message="provider response did not contain any discovery candidates",
                    )
                )
                continue

            for rank, entry in enumerate(result_entries, start=1):
                extracted_records.append(
                    self._extract_entry(
                        snapshot_id=manifest.snapshot_id,
                        source=record.source,
                        provider=provider,
                        query=query or "unknown query",
                        rank=rank,
                        entry=entry,
                        raw_payload_uri=f"{raw_payload_uri}#{entry['result_set']}[{entry['index']}]",
                    )
                )

        valid_count = sum(1 for record in extracted_records if record.status == "valid")
        invalid_count = sum(1 for record in extracted_records if record.status == "invalid")
        warning_records = sum(1 for record in extracted_records if record.warnings)
        run_status = _compute_run_status(valid_count, invalid_count, len(extracted_records))

        return DiscoveryBatchResult(
            snapshot_id=manifest.snapshot_id,
            source=manifest.source,
            provider=provider or "unknown",
            query=query or "unknown query",
            summary=DiscoverySummary(
                run_status=run_status,
                total_candidates=len(extracted_records),
                valid_candidates=valid_count,
                invalid_candidates=invalid_count,
                warning_records=warning_records,
            ),
            records=extracted_records,
        )

    def _extract_entry(
        self,
        *,
        snapshot_id: str,
        source: str,
        provider: str,
        query: str,
        rank: int,
        entry: dict[str, object],
        raw_payload_uri: str,
    ) -> DiscoveryRecordResult:
        warnings: list[str] = []
        issues: list[DiscoveryIssue] = []

        raw_title = entry.get("title")
        title = str(raw_title).strip() if isinstance(raw_title, str) and raw_title.strip() else None

        raw_link = entry.get("link_clean") or entry.get("link")
        product_url = _normalize_product_url(raw_link)
        asin = None
        if isinstance(entry.get("asin"), str) and entry.get("asin", "").strip():
            asin = str(entry["asin"]).strip().upper()
        elif product_url:
            asin = extract_amazon_asin(product_url)
            if asin is not None:
                warnings.append("asin inferred from amazon product_url")

        if title is None:
            issues.append(DiscoveryIssue("missing_title", "candidate is missing title", "error"))
        if product_url is None:
            issues.append(
                DiscoveryIssue(
                    "missing_product_url",
                    "candidate is missing a valid Amazon product_url",
                    "error",
                )
            )
        if asin is None:
            warnings.append("asin missing from provider response")

        price = _coerce_optional_float(entry.get("extracted_price"))
        if price is None:
            warnings.append("price missing from provider response")
        currency = _coerce_optional_string(entry.get("currency"))
        if currency is None:
            warnings.append("currency missing from provider response")
        rating = _coerce_optional_float(entry.get("rating"))
        if rating is None:
            warnings.append("rating missing from provider response")
        review_count = _coerce_optional_int(entry.get("reviews"))
        if review_count is None:
            warnings.append("review_count missing from provider response")

        sponsored = _coerce_optional_bool(entry.get("sponsored"))
        provider_metadata = {
            "result_set": entry.get("result_set"),
            "raw_position": entry.get("position"),
            "thumbnail": entry.get("thumbnail"),
            "badges": entry.get("badges"),
            "tags": entry.get("tags"),
        }

        discovery_id = _build_discovery_id(snapshot_id, rank, asin, product_url)
        status = "invalid" if issues else "valid"
        return DiscoveryRecordResult(
            discovery_id=discovery_id,
            snapshot_id=snapshot_id,
            source=source,
            provider=provider,
            query=query,
            rank=rank,
            status=status,
            title=title,
            product_url=product_url,
            asin=asin,
            price=price,
            currency=currency,
            rating=rating,
            review_count=review_count,
            sponsored=sponsored,
            provider_metadata=provider_metadata,
            raw_payload_uri=raw_payload_uri,
            warnings=warnings,
            issues=issues,
        )

    def _invalid_record(
        self,
        *,
        snapshot_id: str,
        source: str,
        provider: str,
        query: str,
        rank: int,
        raw_payload_uri: str,
        issue_code: str,
        issue_message: str,
    ) -> DiscoveryRecordResult:
        discovery_id = _build_discovery_id(snapshot_id, rank, None, raw_payload_uri)
        return DiscoveryRecordResult(
            discovery_id=discovery_id,
            snapshot_id=snapshot_id,
            source=source,
            provider=provider,
            query=query,
            rank=rank,
            status="invalid",
            title=None,
            product_url=None,
            asin=None,
            price=None,
            currency=None,
            rating=None,
            review_count=None,
            sponsored=None,
            provider_metadata={},
            raw_payload_uri=raw_payload_uri,
            warnings=[],
            issues=[DiscoveryIssue(issue_code, issue_message, "error")],
        )


def write_discovery_artifacts(output_dir: Path, result: DiscoveryBatchResult) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    records_path = output_dir / "discovery_records.json"
    report_path = output_dir / "discovery_report.json"

    records_path.write_text(
        json.dumps([record.to_dict() for record in result.records], indent=2),
        encoding="utf-8",
    )
    report_path.write_text(json.dumps(result.to_report_dict(), indent=2), encoding="utf-8")
    return {
        "discovery_records": str(records_path),
        "discovery_report": str(report_path),
    }


def _collect_result_entries(provider_response: dict) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for result_set in ("organic_results", "products"):
        values = provider_response.get(result_set)
        if not isinstance(values, list):
            continue
        for index, item in enumerate(values):
            if not isinstance(item, dict):
                continue
            entry = dict(item)
            entry["result_set"] = result_set
            entry["index"] = index
            entries.append(entry)
    return entries


def _normalize_product_url(value: object) -> str | None:
    text = _coerce_optional_string(value)
    if text is None:
        return None
    parsed = urlparse(text)
    if not parsed.netloc or "amazon." not in parsed.netloc.lower():
        return None
    asin = extract_amazon_asin(text)
    if asin is not None:
        try:
            return canonicalize_amazon_product_url(text)
        except ValueError:
            return text
    cleaned = parsed._replace(query="", fragment="")
    normalized = urlunparse(cleaned)
    return normalized or None


def _build_discovery_id(snapshot_id: str, rank: int, asin: str | None, product_url: str | None) -> str:
    seed = asin or product_url or f"{snapshot_id}-{rank}"
    digest = re.sub(r"[^a-zA-Z0-9]+", "-", seed).strip("-")[:32] or f"candidate-{rank}"
    return f"{snapshot_id}-rank-{rank:02d}-{digest}"


def _coerce_optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_optional_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _coerce_optional_string(value)
    if text is None:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", text)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _coerce_optional_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = _coerce_optional_string(value)
    if text is None:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if not digits:
        return None
    return int(digits)


def _coerce_optional_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _compute_run_status(valid_count: int, invalid_count: int, total_count: int) -> str:
    if total_count == 0 or valid_count == 0:
        return "failed"
    if invalid_count > 0:
        return "partial_success"
    return "success"
