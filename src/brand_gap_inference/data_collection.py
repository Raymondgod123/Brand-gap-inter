from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import re
from pathlib import Path

from .candidate_selection import CandidateSelectionResult, select_candidates
from .connectors import RawSourceRecord
from .contracts import assert_valid
from .discover_products import run_discovery, run_discovery_from_snapshot
from .ingestion import IngestionService
from .product_intelligence import ProductIntelligenceMerger, write_product_intelligence_artifacts
from .raw_store import FilesystemRawStore, SourceSnapshotManifest
from .serpapi_discovery import SerpApiDiscoveryConnector
from .serpapi_product import SerpApiProductClient, SerpApiProductConnector, UrllibSerpApiProductClient


@dataclass(frozen=True)
class ProductDetailIssue:
    code: str
    message: str
    severity: str


@dataclass(frozen=True)
class ProductDetailRecordResult:
    detail_id: str
    snapshot_id: str
    source: str
    provider: str
    asin: str
    status: str
    title: str | None
    brand: str | None
    product_url: str | None
    price: float | None
    currency: str | None
    rating: float | None
    review_count: int | None
    availability: str | None
    media_assets: dict[str, object]
    promotional_content: list[dict[str, object]]
    description_bullets: list[str]
    provider_metadata: dict[str, object]
    raw_payload_uri: str
    warnings: list[str]
    issues: list[ProductDetailIssue]

    def to_dict(self) -> dict:
        payload = {
            "detail_id": self.detail_id,
            "snapshot_id": self.snapshot_id,
            "source": self.source,
            "provider": self.provider,
            "asin": self.asin,
            "status": self.status,
            "title": self.title,
            "brand": self.brand,
            "product_url": self.product_url,
            "price": self.price,
            "currency": self.currency,
            "rating": self.rating,
            "review_count": self.review_count,
            "availability": self.availability,
            "media_assets": self.media_assets,
            "promotional_content": self.promotional_content,
            "description_bullets": self.description_bullets,
            "provider_metadata": self.provider_metadata,
            "raw_payload_uri": self.raw_payload_uri,
            "warnings": self.warnings,
            "issues": [
                {"code": issue.code, "message": issue.message, "severity": issue.severity}
                for issue in self.issues
            ],
        }
        assert_valid("product_detail_record", payload)
        return payload


@dataclass(frozen=True)
class ProductDetailBatchResult:
    snapshot_id: str
    source: str
    provider: str
    records: list[ProductDetailRecordResult]

    @property
    def valid_records(self) -> int:
        return sum(1 for record in self.records if record.status == "valid")

    @property
    def invalid_records(self) -> int:
        return sum(1 for record in self.records if record.status == "invalid")


@dataclass(frozen=True)
class DataCollectionRunResult:
    run_id: str
    status: str
    output_dir: Path
    artifacts: dict[str, str]
    report: dict[str, object]


class ProductDetailExtractor:
    def extract_snapshot(
        self,
        manifest: SourceSnapshotManifest,
        records: list[RawSourceRecord],
    ) -> ProductDetailBatchResult:
        extracted_records = [self._extract_record(manifest, record) for record in records]
        provider = extracted_records[0].provider if extracted_records else "unknown"
        return ProductDetailBatchResult(
            snapshot_id=manifest.snapshot_id,
            source=manifest.source,
            provider=provider,
            records=extracted_records,
        )

    def _extract_record(
        self,
        manifest: SourceSnapshotManifest,
        record: RawSourceRecord,
    ) -> ProductDetailRecordResult:
        payload = record.payload
        provider = str(payload.get("provider") or "unknown")
        asin = str(payload.get("asin") or record.cursor or record.record_id).strip().upper()
        raw_payload_uri = f"{manifest.storage_uri}/{record.record_id}.json"
        provider_response = payload.get("provider_response")
        issues: list[ProductDetailIssue] = []
        warnings: list[str] = []

        if not isinstance(provider_response, dict):
            return self._invalid_record(
                manifest=manifest,
                record=record,
                provider=provider,
                asin=asin,
                raw_payload_uri=raw_payload_uri,
                issue_code="provider_response_missing",
                issue_message="provider_response is missing or not an object",
            )

        product_results = provider_response.get("product_results")
        if not isinstance(product_results, dict):
            return self._invalid_record(
                manifest=manifest,
                record=record,
                provider=provider,
                asin=asin,
                raw_payload_uri=raw_payload_uri,
                issue_code="product_results_missing",
                issue_message="product_results is missing or not an object",
            )

        title = _coerce_optional_string(product_results.get("title"))
        if title is None:
            issues.append(ProductDetailIssue("missing_title", "product detail is missing title", "error"))

        brand = _coerce_optional_string(product_results.get("brand"))
        if brand is None:
            warnings.append("brand missing from product detail response")

        product_url = _coerce_optional_string(
            product_results.get("product_link")
            or product_results.get("link")
            or product_results.get("url")
        )
        if product_url is None:
            product_url = f"https://www.amazon.com/dp/{asin}"
            warnings.append("product_url inferred from ASIN")

        offer_state = _extract_offer_state(product_results, provider_response)
        price = _safe_primary_offer_price(product_results, offer_state, warnings)

        currency = _coerce_optional_string(product_results.get("currency"))
        if currency is None:
            warnings.append("currency missing from product detail response")

        rating = _coerce_optional_float(product_results.get("rating"))
        if rating is None:
            warnings.append("rating missing from product detail response")

        review_count = _coerce_optional_int(product_results.get("reviews") or product_results.get("ratings"))
        if review_count is None:
            warnings.append("review_count missing from product detail response")

        availability = _safe_availability(product_results, offer_state)
        if availability is None:
            warnings.append("availability missing from product detail response")

        media_assets = _extract_media_assets(product_results, provider_response)
        if not media_assets["gallery_images"]:
            warnings.append("packaging/gallery images missing from product detail response")

        promotional_content = _extract_promotional_content(provider_response)
        if not promotional_content:
            warnings.append("promotional content missing from product detail response")

        description_bullets = _extract_description_bullets(provider_response)
        if not description_bullets:
            warnings.append("description bullets missing from product detail response")

        provider_metadata = {
            "sections_present": sorted(provider_response.keys()),
            "purchase_option_count": _list_length(provider_response.get("purchase_options")),
            "other_seller_count": _list_length(provider_response.get("other_sellers")),
            "offer_state": offer_state,
        }
        status = "invalid" if issues else "valid"

        return ProductDetailRecordResult(
            detail_id=f"{manifest.snapshot_id}-{asin}",
            snapshot_id=manifest.snapshot_id,
            source=record.source,
            provider=provider,
            asin=asin,
            status=status,
            title=title,
            brand=brand,
            product_url=product_url,
            price=price,
            currency=currency,
            rating=rating,
            review_count=review_count,
            availability=availability,
            media_assets=media_assets,
            promotional_content=promotional_content,
            description_bullets=description_bullets,
            provider_metadata=provider_metadata,
            raw_payload_uri=f"{raw_payload_uri}#product_results",
            warnings=warnings,
            issues=issues,
        )

    def _invalid_record(
        self,
        *,
        manifest: SourceSnapshotManifest,
        record: RawSourceRecord,
        provider: str,
        asin: str,
        raw_payload_uri: str,
        issue_code: str,
        issue_message: str,
    ) -> ProductDetailRecordResult:
        return ProductDetailRecordResult(
            detail_id=f"{manifest.snapshot_id}-{asin}",
            snapshot_id=manifest.snapshot_id,
            source=record.source,
            provider=provider,
            asin=asin,
            status="invalid",
            title=None,
            brand=None,
            product_url=None,
            price=None,
            currency=None,
            rating=None,
            review_count=None,
            availability=None,
            media_assets={},
            promotional_content=[],
            description_bullets=[],
            provider_metadata={},
            raw_payload_uri=raw_payload_uri,
            warnings=[],
            issues=[ProductDetailIssue(issue_code, issue_message, "error")],
        )


def run_data_collection(
    *,
    keyword: str | None = None,
    discovery_snapshot_id: str | None = None,
    store_dir: Path,
    output_dir: Path | None = None,
    max_products: int = 5,
    detail_mode: str = "serpapi_product",
    discovery_connector: SerpApiDiscoveryConnector | None = None,
    product_client: SerpApiProductClient | None = None,
    captured_at: str | None = None,
) -> DataCollectionRunResult:
    if (keyword is None) == (discovery_snapshot_id is None):
        raise ValueError("provide exactly one of keyword or discovery_snapshot_id")
    if max_products < 0:
        raise ValueError("max_products must be zero or greater")
    if detail_mode not in {"serpapi_product", "none"}:
        raise ValueError("detail_mode must be serpapi_product or none")

    generated_at = captured_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    mode = "live" if keyword is not None else "replay"
    run_id = f"data-collection-{_timestamp_slug(generated_at)}"
    resolved_output_dir = output_dir or Path("artifacts") / run_id
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    discovery_output_dir = resolved_output_dir / "discovery"
    if keyword is not None:
        discovery_result = run_discovery(
            keyword=keyword,
            store_dir=store_dir,
            output_dir=discovery_output_dir,
            connector=discovery_connector,
            captured_at=generated_at,
        )
    else:
        discovery_result = run_discovery_from_snapshot(
            source="amazon_api_discovery",
            snapshot_id=str(discovery_snapshot_id),
            store_dir=store_dir,
            output_dir=discovery_output_dir,
        )

    discovery_records = _load_json_list(Path(discovery_result.artifacts["discovery_records"]))
    selection_result = evaluate_collection_candidates(
        discovery_records,
        query=discovery_result.query,
        max_products=max_products,
    )
    selected_candidates = selection_result.selected_candidates
    detail_records: list[dict[str, object]] = []

    detail_summary: dict[str, object]
    artifacts = {
        "discovery_records": discovery_result.artifacts["discovery_records"],
        "discovery_report": discovery_result.artifacts["discovery_report"],
        "discovery_bundle_manifest": discovery_result.artifacts["bundle_manifest"],
    }
    artifacts["selection_report"] = _write_selection_report(resolved_output_dir, selection_result)

    if detail_mode == "none":
        detail_summary = {
            "enabled": False,
            "source": None,
            "snapshot_id": None,
            "selected_count": len(selected_candidates),
            "collected_count": 0,
            "valid_records": 0,
            "invalid_records": 0,
            "skipped_reason": "detail_mode none",
            "artifacts": {},
        }
    else:
        detail_result, detail_artifacts = _collect_product_details(
            selected_candidates=selected_candidates,
            store_dir=store_dir,
            output_dir=resolved_output_dir / "details",
            captured_at=generated_at,
            product_client=product_client,
        )
        artifacts.update(detail_artifacts)
        if "product_detail_records" in detail_artifacts:
            detail_records = _load_json_list(Path(detail_artifacts["product_detail_records"]))
        detail_summary = {
            "enabled": True,
            "source": "amazon_api_product",
            "snapshot_id": detail_result.snapshot_id if detail_result is not None else None,
            "selected_count": len(selected_candidates),
            "collected_count": len(detail_result.records) if detail_result is not None else 0,
            "valid_records": detail_result.valid_records if detail_result is not None else 0,
            "invalid_records": detail_result.invalid_records if detail_result is not None else 0,
            "skipped_reason": None if detail_result is not None else "no selected candidates with ASIN",
            "artifacts": detail_artifacts,
        }

    status = _collection_status(
        selected_count=len(selected_candidates),
        detail_mode=detail_mode,
        detail_summary=detail_summary,
    )
    report = {
        "run_id": run_id,
        "generated_at": generated_at,
        "mode": mode,
        "keyword": discovery_result.query,
        "status": status,
        "max_products": max_products,
        "discovery": {
            "snapshot_id": discovery_result.snapshot_id,
            "run_status": discovery_result.summary["run_status"],
            "total_candidates": discovery_result.summary["total_candidates"],
            "valid_candidates": discovery_result.summary["valid_candidates"],
            "invalid_candidates": discovery_result.summary["invalid_candidates"],
            "warning_records": discovery_result.summary["warning_records"],
            "artifacts": discovery_result.artifacts,
        },
        "selection": {
            "selector_version": selection_result.to_report_dict()["selector_version"],
            "query_family": selection_result.context.family_name,
            "focus_terms": sorted(selection_result.context.focus_terms),
            "preferred_pool_count": sum(1 for decision in selection_result.all_decisions if decision.is_preferred),
            "filtered_out_count": selection_result.filtered_out_count,
            "backfill_count": selection_result.backfill_count,
        },
        "selected_candidates": selected_candidates,
        "detail": detail_summary,
        "artifacts": artifacts,
    }
    assert_valid("data_collection_report", report)

    report_path = resolved_output_dir / "data_collection_report.json"
    manifest_path = resolved_output_dir / "data_collection_bundle_manifest.json"
    selected_path = resolved_output_dir / "selected_candidates.json"
    selected_path.write_text(json.dumps(selected_candidates, indent=2), encoding="utf-8")

    if detail_records:
        product_intelligence_result = ProductIntelligenceMerger().merge_collection(
            run_id=run_id,
            collection_report=report,
            discovery_records=discovery_records,
            detail_records=detail_records,
        )
        artifacts.update(
            write_product_intelligence_artifacts(
                resolved_output_dir / "product_intelligence",
                product_intelligence_result,
            )
        )

    report["artifacts"] = artifacts
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    manifest_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "status": status,
                "generated_at": generated_at,
                "artifacts": {
                    **artifacts,
                    "selected_candidates": str(selected_path),
                    "data_collection_report": str(report_path),
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    final_artifacts = {
        **artifacts,
        "selected_candidates": str(selected_path),
        "data_collection_report": str(report_path),
        "data_collection_bundle_manifest": str(manifest_path),
    }
    return DataCollectionRunResult(
        run_id=run_id,
        status=status,
        output_dir=resolved_output_dir,
        artifacts=final_artifacts,
        report=report,
    )


def select_collection_candidates(records: list[dict[str, object]], *, max_products: int) -> list[dict[str, object]]:
    return evaluate_collection_candidates(records, query="", max_products=max_products).selected_candidates


def evaluate_collection_candidates(
    records: list[dict[str, object]],
    *,
    query: str,
    max_products: int,
) -> CandidateSelectionResult:
    return select_candidates(records, query=query, max_products=max_products)


def write_product_detail_artifacts(output_dir: Path, result: ProductDetailBatchResult) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    records_path = output_dir / "product_detail_records.json"
    report_path = output_dir / "product_detail_report.json"
    records_path.write_text(
        json.dumps([record.to_dict() for record in result.records], indent=2),
        encoding="utf-8",
    )
    report_path.write_text(
        json.dumps(
            {
                "snapshot_id": result.snapshot_id,
                "source": result.source,
                "provider": result.provider,
                "total_records": len(result.records),
                "valid_records": result.valid_records,
                "invalid_records": result.invalid_records,
                "records": [
                    {
                        "detail_id": record.detail_id,
                        "asin": record.asin,
                        "status": record.status,
                        "title": record.title,
                        "warning_count": len(record.warnings),
                        "issue_count": len(record.issues),
                    }
                    for record in result.records
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "product_detail_records": str(records_path),
        "product_detail_report": str(report_path),
    }


def _write_selection_report(output_dir: Path, selection_result: CandidateSelectionResult) -> str:
    report_path = output_dir / "selection_report.json"
    report_path.write_text(
        json.dumps(selection_result.to_report_dict(), indent=2),
        encoding="utf-8",
    )
    return str(report_path)


def _collect_product_details(
    *,
    selected_candidates: list[dict[str, object]],
    store_dir: Path,
    output_dir: Path,
    captured_at: str,
    product_client: SerpApiProductClient | None,
) -> tuple[ProductDetailBatchResult | None, dict[str, str]]:
    asins = [str(candidate["asin"]) for candidate in selected_candidates if candidate.get("asin")]
    if not asins:
        return None, {}

    connector = SerpApiProductConnector(
        asins=asins,
        client=product_client or UrllibSerpApiProductClient(),
        captured_at=captured_at,
    )
    service = IngestionService(FilesystemRawStore(store_dir))
    ingest_result = service.ingest(connector)
    detail_result = ProductDetailExtractor().extract_snapshot(ingest_result.manifest, ingest_result.records)
    return detail_result, write_product_detail_artifacts(output_dir, detail_result)


def _collection_status(
    *,
    selected_count: int,
    detail_mode: str,
    detail_summary: dict[str, object],
) -> str:
    if selected_count == 0:
        return "failed"
    if detail_mode == "none":
        return "success"
    valid_records = int(detail_summary["valid_records"])
    collected_count = int(detail_summary["collected_count"])
    if valid_records == selected_count and collected_count == selected_count:
        return "success"
    if valid_records > 0:
        return "partial_success"
    return "failed"


def _load_json_list(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"expected list JSON payload in {path}")
    return [item for item in payload if isinstance(item, dict)]


def _timestamp_slug(timestamp: str) -> str:
    return timestamp.replace(":", "-").replace(".", "-")


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


def _extract_offer_state(product_results: dict[str, object], provider_response: dict[str, object]) -> dict[str, object]:
    purchase_options = provider_response.get("purchase_options")
    single_offer = _single_offer_payload(purchase_options)
    offer_text = " ".join(
        text
        for text in [
            _coerce_optional_string(product_results.get("availability")),
            _coerce_optional_string(product_results.get("stock")),
            _stringify_for_signal(product_results.get("delivery")),
            _stringify_for_signal(product_results.get("buybox_winner")),
            _stringify_for_signal(product_results.get("buying_options")),
            _stringify_for_signal(provider_response.get("buying_options")),
            _stringify_for_signal(provider_response.get("other_sellers")),
            _stringify_for_signal(purchase_options),
        ]
        if text
    ).lower()
    single_offer_text = _stringify_for_signal(single_offer).lower()
    no_featured_offer = any(
        marker in offer_text
        for marker in (
            "no featured offer",
            "no featured offers",
            "featured offer unavailable",
            "available from these sellers",
            "see all buying options",
            "buying options",
        )
    )
    unavailable = any(
        marker in offer_text
        for marker in (
            "currently unavailable",
            "temporarily out of stock",
            "out of stock",
            "not available",
            "unavailable",
        )
    )
    in_stock_signal = any(marker in offer_text for marker in ("in stock", "ships from", "sold by"))
    single_offer_price = _coerce_optional_float(
        single_offer.get("extracted_price") or single_offer.get("price")
        if isinstance(single_offer, dict)
        else None
    )
    single_offer_stock = (
        _coerce_optional_string(single_offer.get("availability") or single_offer.get("stock"))
        if isinstance(single_offer, dict)
        else None
    )
    single_offer_safe = bool(
        isinstance(single_offer, dict)
        and single_offer
        and not any(marker in single_offer_text for marker in ("currently unavailable", "out of stock", "no featured offer"))
    )
    product_price = _coerce_optional_float(product_results.get("extracted_price") or product_results.get("price"))
    return {
        "has_single_offer": isinstance(single_offer, dict) and bool(single_offer),
        "single_offer_price_present": single_offer_price is not None,
        "single_offer_price": single_offer_price,
        "single_offer_stock": single_offer_stock,
        "product_price_present": product_price is not None,
        "no_featured_offer_signal": no_featured_offer,
        "unavailable_signal": unavailable,
        "in_stock_signal": in_stock_signal,
        "primary_offer_safe": not no_featured_offer and not unavailable and (single_offer_safe or in_stock_signal),
        "price_fallback_blocked": no_featured_offer or unavailable,
    }


def _safe_primary_offer_price(
    product_results: dict[str, object],
    offer_state: dict[str, object],
    warnings: list[str],
) -> float | None:
    if offer_state.get("price_fallback_blocked"):
        warnings.append("price blocked because Amazon detail source did not expose a safe primary offer")
        return None

    single_offer_price = offer_state.get("single_offer_price")
    if isinstance(single_offer_price, (int, float)) and not isinstance(single_offer_price, bool):
        return float(single_offer_price)

    price = _coerce_optional_float(product_results.get("extracted_price") or product_results.get("price"))
    if price is None:
        warnings.append("price missing from product detail response")
        return None
    if not offer_state.get("primary_offer_safe"):
        warnings.append("price accepted from product detail without strong primary-offer status signal")
    return price


def _safe_availability(product_results: dict[str, object], offer_state: dict[str, object]) -> str | None:
    if offer_state.get("unavailable_signal"):
        return "Currently unavailable"
    if offer_state.get("no_featured_offer_signal"):
        return "No featured offer"
    return (
        _coerce_optional_string(product_results.get("availability"))
        or _coerce_optional_string(product_results.get("stock"))
        or _coerce_optional_string(offer_state.get("single_offer_stock"))
    )


def _single_offer_payload(purchase_options: object) -> dict[str, object]:
    if isinstance(purchase_options, dict):
        single_offer = purchase_options.get("single_offer")
        if isinstance(single_offer, dict):
            return single_offer
    return {}


def _stringify_for_signal(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    if isinstance(value, list):
        return " ".join(_stringify_for_signal(item) for item in value)
    if isinstance(value, dict):
        return " ".join(f"{key} {_stringify_for_signal(item)}" for key, item in value.items())
    return str(value)


def _list_length(value: object) -> int:
    return len(value) if isinstance(value, list) else 0


def _extract_media_assets(product_results: dict[str, object], provider_response: dict[str, object]) -> dict[str, object]:
    thumbnail = _coerce_optional_string(product_results.get("thumbnail"))
    gallery_images = _coerce_string_list(product_results.get("thumbnails"))
    promotional_images = [
        str(item["image"])
        for item in _extract_promotional_content(provider_response)
        if item.get("image")
    ]
    videos = []
    raw_videos = provider_response.get("videos")
    if isinstance(raw_videos, list):
        for item in raw_videos:
            if not isinstance(item, dict):
                continue
            videos.append(
                {
                    "title": _coerce_optional_string(item.get("title")),
                    "link": _coerce_optional_string(item.get("link")),
                    "thumbnail": _coerce_optional_string(item.get("thumbnail")),
                    "duration": _coerce_optional_string(item.get("duration")),
                }
            )
    return {
        "primary_image": thumbnail,
        "gallery_images": gallery_images,
        "promotional_images": promotional_images,
        "videos": videos,
    }


def _extract_promotional_content(provider_response: dict[str, object]) -> list[dict[str, object]]:
    raw_blocks = provider_response.get("product_description")
    if not isinstance(raw_blocks, list):
        return []
    blocks: list[dict[str, object]] = []
    for item in raw_blocks:
        if not isinstance(item, dict):
            continue
        block = {
            "position": _coerce_optional_int(item.get("position")),
            "title": _coerce_optional_string(item.get("title")),
            "image": _coerce_optional_string(item.get("image")),
        }
        if block["title"] is not None or block["image"] is not None:
            blocks.append(block)
    return blocks


def _extract_description_bullets(provider_response: dict[str, object]) -> list[str]:
    return _coerce_string_list(provider_response.get("about_item"))


def _coerce_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for item in value:
        text = _coerce_optional_string(item)
        if text is not None:
            items.append(text)
    return items
