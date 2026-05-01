from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re

from .contracts import assert_valid

TOKEN_RE = re.compile(r"[a-z0-9]+")

CONTENT_FAMILY_KEYWORDS = {
    "candy": {
        "candy",
        "candies",
        "gummy",
        "gummies",
        "hard",
        "chewy",
        "chocolate",
        "caramel",
        "caramels",
        "mint",
        "patties",
        "licorice",
        "lollipop",
        "lollipops",
        "taffy",
        "drops",
    },
    "syrup": {
        "syrup",
        "bottle",
        "pour",
        "desserts",
        "milk",
        "ice",
        "cream",
    },
    "beverage": {
        "drink",
        "drinks",
        "energy",
        "caffeine",
        "coffee",
        "tea",
        "workout",
    },
    "sweetener": {
        "sweetener",
        "sweeteners",
        "stevia",
        "monk",
        "fruit",
        "allulose",
        "erythritol",
        "granulated",
        "canister",
        "packets",
        "packet",
    },
}

SEVERE_FAMILY_MISMATCHES = {
    ("candy", "syrup"),
    ("candy", "beverage"),
    ("candy", "sweetener"),
    ("sweetener", "candy"),
    ("sweetener", "beverage"),
    ("sweetener", "syrup"),
}


@dataclass(frozen=True)
class ProductIntelligenceIssue:
    code: str
    message: str
    severity: str


@dataclass(frozen=True)
class ProductIntelligenceRecord:
    product_id: str
    asin: str
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
    discovery_rank: int | None
    sponsored: bool | None
    source_snapshots: dict[str, object]
    field_provenance: dict[str, object]
    warnings: list[str]
    issues: list[ProductIntelligenceIssue]

    def to_dict(self) -> dict:
        payload = {
            "product_id": self.product_id,
            "asin": self.asin,
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
            "discovery_rank": self.discovery_rank,
            "sponsored": self.sponsored,
            "source_snapshots": self.source_snapshots,
            "field_provenance": self.field_provenance,
            "warnings": self.warnings,
            "issues": [
                {"code": issue.code, "message": issue.message, "severity": issue.severity}
                for issue in self.issues
            ],
        }
        assert_valid("product_intelligence_record", payload)
        return payload


@dataclass(frozen=True)
class ProductIntelligenceBatchResult:
    run_id: str
    records: list[ProductIntelligenceRecord]

    @property
    def complete_products(self) -> int:
        return sum(1 for record in self.records if not record.issues)

    @property
    def warning_products(self) -> int:
        return sum(1 for record in self.records if record.warnings)

    @property
    def issue_products(self) -> int:
        return sum(1 for record in self.records if record.issues)

    @property
    def status(self) -> str:
        if not self.records or self.complete_products == 0:
            return "failed"
        if self.issue_products > 0:
            return "partial_success"
        return "success"

    def to_report_dict(self) -> dict:
        payload = {
            "run_id": self.run_id,
            "status": self.status,
            "total_products": len(self.records),
            "complete_products": self.complete_products,
            "warning_products": self.warning_products,
            "issue_products": self.issue_products,
            "records": [
                {
                    "product_id": record.product_id,
                    "asin": record.asin,
                    "title": record.title,
                    "brand": record.brand,
                    "price": record.price,
                    "currency": record.currency,
                    "rating": record.rating,
                    "review_count": record.review_count,
                    "availability": record.availability,
                    "media_asset_count": _media_asset_count(record.media_assets),
                    "promotional_block_count": len(record.promotional_content),
                    "description_bullet_count": len(record.description_bullets),
                    "discovery_rank": record.discovery_rank,
                    "warning_count": len(record.warnings),
                    "issue_count": len(record.issues),
                }
                for record in self.records
            ],
        }
        assert_valid("product_intelligence_batch_report", payload)
        return payload


class ProductIntelligenceMerger:
    def merge_collection(
        self,
        *,
        run_id: str,
        collection_report: dict[str, object],
        discovery_records: list[dict[str, object]],
        detail_records: list[dict[str, object]],
    ) -> ProductIntelligenceBatchResult:
        detail_by_asin = {
            str(record["asin"]).upper(): record
            for record in detail_records
            if record.get("asin") and record.get("status") == "valid"
        }
        discovery_by_asin = {
            str(record["asin"]).upper(): record
            for record in discovery_records
            if record.get("asin") and record.get("status") == "valid"
        }

        selected_candidates = collection_report.get("selected_candidates")
        if not isinstance(selected_candidates, list):
            selected_candidates = []

        merged_records: list[ProductIntelligenceRecord] = []
        for candidate in selected_candidates:
            if not isinstance(candidate, dict) or not candidate.get("asin"):
                continue
            asin = str(candidate["asin"]).upper()
            discovery_record = {**discovery_by_asin.get(asin, {}), **candidate}
            detail_record = detail_by_asin.get(asin)
            merged_records.append(
                self._merge_product(
                    collection_report=collection_report,
                    asin=asin,
                    discovery_record=discovery_record,
                    detail_record=detail_record,
                )
            )

        return ProductIntelligenceBatchResult(run_id=run_id, records=merged_records)

    def _merge_product(
        self,
        *,
        collection_report: dict[str, object],
        asin: str,
        discovery_record: dict[str, object],
        detail_record: dict[str, object] | None,
    ) -> ProductIntelligenceRecord:
        warnings: list[str] = []
        issues: list[ProductIntelligenceIssue] = []
        if detail_record is None:
            issues.append(
                ProductIntelligenceIssue(
                    "missing_detail_record",
                    "selected product did not have a valid product detail record",
                    "error",
                )
            )
            detail_record = {}

        warnings.extend(_prefixed_warnings("discovery", discovery_record.get("warnings")))
        warnings.extend(_prefixed_warnings("detail", detail_record.get("warnings")))

        source_snapshots = {
            "collection_run_id": collection_report.get("run_id"),
            "discovery_snapshot_id": _nested_value(collection_report, "discovery", "snapshot_id"),
            "detail_snapshot_id": _nested_value(collection_report, "detail", "snapshot_id"),
            "discovery_record_id": discovery_record.get("discovery_id"),
            "detail_record_id": detail_record.get("detail_id"),
        }

        fields = {
            "title": _choose_field("title", detail_record, discovery_record),
            "brand": _choose_field("brand", detail_record, discovery_record),
            "product_url": _choose_field("product_url", detail_record, discovery_record),
            "price": _choose_price_field(detail_record, discovery_record),
            "currency": _choose_field("currency", detail_record, discovery_record),
            "rating": _choose_field("rating", detail_record, discovery_record),
            "review_count": _choose_field("review_count", detail_record, discovery_record),
            "availability": _choose_field("availability", detail_record, discovery_record),
            "media_assets": _choose_field("media_assets", detail_record, discovery_record),
            "promotional_content": _choose_field("promotional_content", detail_record, discovery_record),
            "description_bullets": _choose_field("description_bullets", detail_record, discovery_record),
        }

        _sanitize_inconsistent_detail_narrative_fields(fields, warnings)

        required_for_decision = ("title", "product_url", "price", "rating", "review_count")
        for field_name in required_for_decision:
            if fields[field_name]["value"] is None:
                issues.append(
                    ProductIntelligenceIssue(
                        f"missing_{field_name}",
                        f"product intelligence record is missing {field_name}",
                        "error",
                    )
                )

        field_provenance = {
            field_name: {
                "source": selected["source"],
                "record_id": selected["record_id"],
                "raw_payload_uri": selected["raw_payload_uri"],
            }
            for field_name, selected in fields.items()
        }

        return ProductIntelligenceRecord(
            product_id=f"amazon:{asin}",
            asin=asin,
            title=fields["title"]["value"],
            brand=fields["brand"]["value"],
            product_url=fields["product_url"]["value"],
            price=fields["price"]["value"],
            currency=fields["currency"]["value"],
            rating=fields["rating"]["value"],
            review_count=fields["review_count"]["value"],
            availability=fields["availability"]["value"],
            media_assets=fields["media_assets"]["value"] or {},
            promotional_content=fields["promotional_content"]["value"] or [],
            description_bullets=fields["description_bullets"]["value"] or [],
            discovery_rank=_coerce_optional_int(discovery_record.get("rank")),
            sponsored=_coerce_optional_bool(discovery_record.get("sponsored")),
            source_snapshots=source_snapshots,
            field_provenance=field_provenance,
            warnings=warnings,
            issues=issues,
        )


def write_product_intelligence_artifacts(
    output_dir: Path,
    result: ProductIntelligenceBatchResult,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    records_path = output_dir / "product_intelligence_records.json"
    report_path = output_dir / "product_intelligence_report.json"
    records_path.write_text(
        json.dumps([record.to_dict() for record in result.records], indent=2),
        encoding="utf-8",
    )
    report_path.write_text(json.dumps(result.to_report_dict(), indent=2), encoding="utf-8")
    return {
        "product_intelligence_records": str(records_path),
        "product_intelligence_report": str(report_path),
    }


def merge_collection_artifacts(
    *,
    collection_report_path: Path,
    discovery_records_path: Path,
    detail_records_path: Path,
    output_dir: Path,
) -> ProductIntelligenceBatchResult:
    collection_report = _load_json_object(collection_report_path)
    discovery_records = _load_json_list(discovery_records_path)
    detail_records = _load_json_list(detail_records_path)
    result = ProductIntelligenceMerger().merge_collection(
        run_id=str(collection_report["run_id"]),
        collection_report=collection_report,
        discovery_records=discovery_records,
        detail_records=detail_records,
    )
    write_product_intelligence_artifacts(output_dir, result)
    return result


def _choose_field(
    field_name: str,
    detail_record: dict[str, object],
    discovery_record: dict[str, object],
) -> dict[str, object]:
    detail_value = detail_record.get(field_name)
    if detail_value is not None:
        return {
            "value": detail_value,
            "source": "product_detail",
            "record_id": detail_record.get("detail_id"),
            "raw_payload_uri": detail_record.get("raw_payload_uri"),
        }
    discovery_value = discovery_record.get(field_name)
    if discovery_value is not None:
        return {
            "value": discovery_value,
            "source": "discovery",
            "record_id": discovery_record.get("discovery_id"),
            "raw_payload_uri": discovery_record.get("raw_payload_uri"),
        }
    return {"value": None, "source": None, "record_id": None, "raw_payload_uri": None}


def _choose_price_field(
    detail_record: dict[str, object],
    discovery_record: dict[str, object],
) -> dict[str, object]:
    detail_value = detail_record.get("price")
    if detail_value is not None:
        return {
            "value": detail_value,
            "source": "product_detail",
            "record_id": detail_record.get("detail_id"),
            "raw_payload_uri": detail_record.get("raw_payload_uri"),
        }
    if _detail_blocks_price_fallback(detail_record):
        return {
            "value": None,
            "source": "product_detail",
            "record_id": detail_record.get("detail_id"),
            "raw_payload_uri": detail_record.get("raw_payload_uri"),
        }
    discovery_value = discovery_record.get("price")
    if discovery_value is not None:
        return {
            "value": discovery_value,
            "source": "discovery",
            "record_id": discovery_record.get("discovery_id"),
            "raw_payload_uri": discovery_record.get("raw_payload_uri"),
        }
    return {"value": None, "source": None, "record_id": None, "raw_payload_uri": None}


def _detail_blocks_price_fallback(detail_record: dict[str, object]) -> bool:
    provider_metadata = detail_record.get("provider_metadata")
    if isinstance(provider_metadata, dict):
        offer_state = provider_metadata.get("offer_state")
        if isinstance(offer_state, dict) and offer_state.get("price_fallback_blocked") is True:
            return True
    warnings = detail_record.get("warnings")
    return isinstance(warnings, list) and any(
        isinstance(warning, str)
        and "price blocked because amazon detail source did not expose a safe primary offer" in warning.lower()
        for warning in warnings
    )


def _prefixed_warnings(prefix: str, value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [f"{prefix}: {warning}" for warning in value if isinstance(warning, str) and warning.strip()]


def _nested_value(payload: dict[str, object], *keys: str) -> object | None:
    current: object = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _coerce_optional_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _coerce_optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _media_asset_count(media_assets: dict[str, object]) -> int:
    count = 0
    if media_assets.get("primary_image"):
        count += 1
    for key in ("gallery_images", "promotional_images", "videos"):
        value = media_assets.get(key)
        if isinstance(value, list):
            count += len(value)
    return count


def _sanitize_inconsistent_detail_narrative_fields(
    fields: dict[str, dict[str, object]],
    warnings: list[str],
) -> None:
    title = fields["title"]["value"]
    if not isinstance(title, str) or not title.strip():
        return

    description_bullets = fields["description_bullets"]["value"]
    promotional_content = fields["promotional_content"]["value"]
    content_text_parts: list[str] = []
    if isinstance(description_bullets, list):
        content_text_parts.extend(str(item) for item in description_bullets if isinstance(item, str))
    if isinstance(promotional_content, list):
        for block in promotional_content:
            if not isinstance(block, dict):
                continue
            title_text = block.get("title")
            if isinstance(title_text, str) and title_text.strip():
                content_text_parts.append(title_text)

    if not content_text_parts:
        return

    title_family, _ = _dominant_content_family(title)
    content_family, content_score = _dominant_content_family(" ".join(content_text_parts))
    if title_family is None or content_family is None or content_score < 2:
        return
    if (title_family, content_family) not in SEVERE_FAMILY_MISMATCHES:
        return

    fields["description_bullets"]["value"] = []
    fields["promotional_content"]["value"] = []
    warnings.append(
        "detail: narrative content dropped because detail content family "
        f"`{content_family}` conflicted with title family `{title_family}`"
    )


def _dominant_content_family(text: str) -> tuple[str | None, int]:
    tokens = set(TOKEN_RE.findall(text.lower()))
    best_family: str | None = None
    best_score = 0
    for family, keywords in CONTENT_FAMILY_KEYWORDS.items():
        score = len(tokens.intersection(keywords))
        if score > best_score:
            best_family = family
            best_score = score
    return best_family, best_score


def _load_json_object(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object JSON payload in {path}")
    return payload


def _load_json_list(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"expected list JSON payload in {path}")
    return [item for item in payload if isinstance(item, dict)]
