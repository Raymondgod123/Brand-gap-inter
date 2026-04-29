from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import re
from pathlib import Path

from .contracts import assert_valid

VALUE_TERMS = (
    "saver",
    "affordable",
    "without the frills",
    "shop smarter",
    "budget",
    "staples",
    "value",
)
HEALTH_TERMS = (
    "keto",
    "low carb",
    "zero calorie",
    "zero calories",
    "zero net carbs",
    "gluten free",
    "non gmo",
    "organic",
    "natural",
    "allulose",
    "erythritol",
    "plant based",
)
CONVENIENCE_TERMS = (
    "packets",
    "variety pack",
    "stirrers",
    "office",
    "breakrooms",
    "breakroom",
    "home",
    "airbnb",
    "canister",
    "pack of",
    "single serve",
    "single-serve",
    "on the go",
)
USAGE_CONTEXT_TERMS = {
    "baking": ("baking", "baker", "cake batter", "cookies"),
    "coffee": ("coffee",),
    "tea": ("tea",),
    "office": ("office", "breakroom", "breakrooms"),
    "home": ("home", "airbnb", "kitchen"),
    "pantry": ("pantry", "staples", "granulated sugar", "everyday use"),
}


@dataclass(frozen=True)
class BrandPositioningRecord:
    product_id: str
    asin: str
    brand_name: str | None
    title: str | None
    positioning_archetype: str
    price_tier: str
    value_signal: str
    health_signal: str
    convenience_signal: str
    visual_strategy: str
    claim_signals: list[str]
    usage_contexts: list[str]
    packaging_signal_summary: dict[str, object]
    evidence: list[str]
    warnings: list[str]

    def to_dict(self) -> dict:
        payload = {
            "product_id": self.product_id,
            "asin": self.asin,
            "brand_name": self.brand_name,
            "title": self.title,
            "positioning_archetype": self.positioning_archetype,
            "price_tier": self.price_tier,
            "value_signal": self.value_signal,
            "health_signal": self.health_signal,
            "convenience_signal": self.convenience_signal,
            "visual_strategy": self.visual_strategy,
            "claim_signals": self.claim_signals,
            "usage_contexts": self.usage_contexts,
            "packaging_signal_summary": self.packaging_signal_summary,
            "evidence": self.evidence,
            "warnings": self.warnings,
        }
        assert_valid("brand_positioning_record", payload)
        return payload


@dataclass(frozen=True)
class BrandPositioningBatch:
    run_id: str
    records: list[BrandPositioningRecord]

    @property
    def status(self) -> str:
        if not self.records:
            return "failed"
        if any(record.warnings for record in self.records):
            return "partial_success"
        return "success"

    def to_report_dict(self) -> dict:
        archetype_counts = Counter(record.positioning_archetype for record in self.records)
        payload = {
            "run_id": self.run_id,
            "status": self.status,
            "total_products": len(self.records),
            "archetype_counts": dict(archetype_counts),
            "market_themes": _market_themes(self.records),
            "records": [
                {
                    "asin": record.asin,
                    "brand_name": record.brand_name,
                    "title": record.title,
                    "positioning_archetype": record.positioning_archetype,
                    "price_tier": record.price_tier,
                    "value_signal": record.value_signal,
                    "health_signal": record.health_signal,
                    "convenience_signal": record.convenience_signal,
                    "visual_strategy": record.visual_strategy,
                    "claim_signals": record.claim_signals,
                    "usage_contexts": record.usage_contexts,
                    "packaging_signal_summary": record.packaging_signal_summary,
                    "evidence": record.evidence,
                    "warning_count": len(record.warnings),
                }
                for record in self.records
            ],
            "caveats": _report_caveats(self.records),
        }
        assert_valid("brand_positioning_report", payload)
        return payload


class BrandPositioningAnalyzer:
    def analyze_records(self, *, run_id: str, records: list[dict[str, object]]) -> BrandPositioningBatch:
        price_tiers = _price_tiers(records)
        positioning_records = [
            self._analyze_record(
                record,
                price_tiers.get(str(record.get("asin") or "").upper(), "unknown"),
            )
            for record in records
            if record.get("asin")
        ]
        return BrandPositioningBatch(run_id=run_id, records=positioning_records)

    def _analyze_record(self, record: dict[str, object], price_tier: str) -> BrandPositioningRecord:
        asin = str(record["asin"]).upper()
        brand_name = _normalize_brand_name(record.get("brand"))
        title = _coerce_optional_string(record.get("title"))
        text_pool = _collect_text_pool(record, brand_name)

        value_hits = _matched_terms(text_pool, VALUE_TERMS)
        health_hits = _matched_terms(text_pool, HEALTH_TERMS)
        convenience_hits = _matched_terms(text_pool, CONVENIENCE_TERMS)
        claim_signals = _dedupe_preserve_order(value_hits + health_hits + convenience_hits)
        usage_contexts = _usage_contexts(text_pool)
        packaging_signal_summary = _packaging_signal_summary(record.get("media_assets"), record)
        visual_strategy = _visual_strategy(packaging_signal_summary)
        evidence = _evidence(record)

        positioning_archetype = _positioning_archetype(
            price_tier=price_tier,
            value_hits=value_hits,
            health_hits=health_hits,
            convenience_hits=convenience_hits,
            usage_contexts=usage_contexts,
        )
        warnings = _warnings(record, brand_name, packaging_signal_summary)

        return BrandPositioningRecord(
            product_id=str(record.get("product_id") or f"amazon:{asin}"),
            asin=asin,
            brand_name=brand_name,
            title=title,
            positioning_archetype=positioning_archetype,
            price_tier=price_tier,
            value_signal=_signal_level(len(value_hits), price_tier == "budget"),
            health_signal=_signal_level(len(health_hits), False),
            convenience_signal=_signal_level(len(convenience_hits), False),
            visual_strategy=visual_strategy,
            claim_signals=claim_signals,
            usage_contexts=usage_contexts,
            packaging_signal_summary=packaging_signal_summary,
            evidence=evidence,
            warnings=warnings,
        )


def build_brand_positioning_report(*, run_id: str, records: list[dict[str, object]]) -> dict[str, object]:
    return BrandPositioningAnalyzer().analyze_records(run_id=run_id, records=records).to_report_dict()


def write_brand_positioning_artifacts(
    *,
    product_intelligence_records_path: Path,
    output_dir: Path,
    run_id: str | None = None,
) -> dict[str, str]:
    records = _load_json_list(product_intelligence_records_path)
    resolved_run_id = run_id or _infer_run_id(records, product_intelligence_records_path)
    result = BrandPositioningAnalyzer().analyze_records(run_id=resolved_run_id, records=records)
    report = result.to_report_dict()

    output_dir.mkdir(parents=True, exist_ok=True)
    records_path = output_dir / "brand_positioning_records.json"
    report_path = output_dir / "brand_positioning_report.json"
    markdown_path = output_dir / "brand_positioning_report.md"
    records_path.write_text(
        json.dumps([record.to_dict() for record in result.records], indent=2),
        encoding="utf-8",
    )
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown_path.write_text(render_brand_positioning_markdown(report, result.records), encoding="utf-8")
    return {
        "brand_positioning_records": str(records_path),
        "brand_positioning_report": str(report_path),
        "brand_positioning_report_md": str(markdown_path),
    }


def render_brand_positioning_markdown(
    report: dict[str, object],
    records: list[BrandPositioningRecord],
) -> str:
    lines = [
        "# Brand Positioning Report",
        "",
        f"Run: `{report['run_id']}`",
        f"Status: `{report['status']}`",
        f"Products: `{report['total_products']}`",
        "",
        "## Positioning Table",
        "",
        "| ASIN | Brand | Archetype | Price Tier | Value | Health | Convenience | Visual Strategy |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for record in records:
        lines.append(
            "| {asin} | {brand} | {archetype} | {price_tier} | {value_signal} | {health_signal} | {convenience_signal} | {visual_strategy} |".format(
                asin=record.asin,
                brand=_escape_table(record.brand_name or ""),
                archetype=record.positioning_archetype,
                price_tier=record.price_tier,
                value_signal=record.value_signal,
                health_signal=record.health_signal,
                convenience_signal=record.convenience_signal,
                visual_strategy=record.visual_strategy,
            )
        )

    lines.extend(["", "## Market Themes", ""])
    market_themes = report.get("market_themes", [])
    if isinstance(market_themes, list) and market_themes:
        for theme in market_themes:
            lines.append(f"- {theme}")
    else:
        lines.append("- No cross-product themes detected.")

    lines.extend(["", "## Product Notes", ""])
    for record in records:
        headline = record.brand_name or record.title or record.asin
        claims = ", ".join(record.claim_signals) or "no strong claims"
        contexts = ", ".join(record.usage_contexts) or "no clear usage context"
        evidence = record.evidence[0] if record.evidence else "no evidence snippets captured"
        lines.append(
            f"- `{record.asin}` {_escape_table(headline)}: claims `{claims}`; contexts `{contexts}`; evidence `{_escape_table(evidence)}`"
        )

    lines.extend(["", "## Caveats", ""])
    caveats = report.get("caveats", [])
    if isinstance(caveats, list) and caveats:
        for caveat in caveats:
            lines.append(f"- {caveat}")
    else:
        lines.append("- No positioning caveats were recorded.")
    return "\n".join(lines) + "\n"


def _positioning_archetype(
    *,
    price_tier: str,
    value_hits: list[str],
    health_hits: list[str],
    convenience_hits: list[str],
    usage_contexts: list[str],
) -> str:
    if "variety pack" in convenience_hits or ("office" in usage_contexts and convenience_hits):
        return "convenience_bundle"
    if len(health_hits) >= 2:
        return "health_positioned"
    if value_hits and price_tier == "budget":
        return "value_staple"
    if "pantry" in usage_contexts or "baking" in usage_contexts:
        return "pantry_staple"
    return "general_grocery"


def _signal_level(hit_count: int, emphasize_low_price: bool) -> str:
    if hit_count >= 2:
        return "explicit"
    if hit_count == 1 or emphasize_low_price:
        return "moderate"
    return "light"


def _visual_strategy(summary: dict[str, object]) -> str:
    promo_count = int(summary.get("promotional_block_count") or 0)
    video_count = int(summary.get("video_count") or 0)
    gallery_count = int(summary.get("gallery_image_count") or 0)
    if promo_count > 0 and video_count > 0:
        return "packaging_promo_video"
    if promo_count > 0:
        return "packaging_plus_promo"
    if video_count > 0:
        return "packaging_plus_video"
    if gallery_count >= 5:
        return "packaging_gallery_only"
    return "minimal_visual_story"


def _packaging_signal_summary(media_assets: object, record: dict[str, object]) -> dict[str, object]:
    assets = media_assets if isinstance(media_assets, dict) else {}
    gallery_images = assets.get("gallery_images") if isinstance(assets.get("gallery_images"), list) else []
    promotional_images = assets.get("promotional_images") if isinstance(assets.get("promotional_images"), list) else []
    videos = assets.get("videos") if isinstance(assets.get("videos"), list) else []
    bullets = record.get("description_bullets") if isinstance(record.get("description_bullets"), list) else []
    promo_blocks = record.get("promotional_content") if isinstance(record.get("promotional_content"), list) else []
    return {
        "primary_image_present": bool(assets.get("primary_image")),
        "gallery_image_count": len(gallery_images),
        "promotional_image_count": len(promotional_images),
        "video_count": len(videos),
        "description_bullet_count": len(bullets),
        "promotional_block_count": len(promo_blocks),
    }


def _usage_contexts(text_pool: list[str]) -> list[str]:
    contexts: list[str] = []
    normalized_pool = " ".join(text_pool)
    for label, patterns in USAGE_CONTEXT_TERMS.items():
        if any(pattern in normalized_pool for pattern in patterns):
            contexts.append(label)
    return contexts


def _matched_terms(text_pool: list[str], terms: tuple[str, ...]) -> list[str]:
    normalized = " ".join(text_pool)
    return [term for term in terms if term in normalized]


def _collect_text_pool(record: dict[str, object], brand_name: str | None) -> list[str]:
    text_pool: list[str] = []
    for value in (brand_name, record.get("title")):
        text = _coerce_optional_string(value)
        if text is not None:
            text_pool.append(_normalize_text(text))
    for bullet in record.get("description_bullets", []):
        text = _coerce_optional_string(bullet)
        if text is not None:
            text_pool.append(_normalize_text(text))
    for block in record.get("promotional_content", []):
        if not isinstance(block, dict):
            continue
        for key in ("title",):
            text = _coerce_optional_string(block.get(key))
            if text is not None:
                text_pool.append(_normalize_text(text))
    return text_pool


def _evidence(record: dict[str, object]) -> list[str]:
    evidence: list[str] = []
    title = _coerce_optional_string(record.get("title"))
    if title is not None:
        evidence.append(title)
    for bullet in record.get("description_bullets", []):
        text = _coerce_optional_string(bullet)
        if text is not None:
            evidence.append(text)
    for block in record.get("promotional_content", []):
        if not isinstance(block, dict):
            continue
        text = _coerce_optional_string(block.get("title"))
        if text is not None:
            evidence.append(text)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in evidence:
        if item in seen:
            continue
        deduped.append(item)
        seen.add(item)
        if len(deduped) >= 5:
            break
    return deduped


def _warnings(
    record: dict[str, object],
    brand_name: str | None,
    packaging_signal_summary: dict[str, object],
) -> list[str]:
    warnings: list[str] = []
    if brand_name is None:
        warnings.append("brand name could not be normalized")
    if record.get("currency") is None:
        warnings.append("currency missing from product intelligence record")
    if int(packaging_signal_summary["gallery_image_count"]) == 0:
        warnings.append("gallery images missing for packaging analysis")
    if int(packaging_signal_summary["description_bullet_count"]) == 0:
        warnings.append("description bullets missing for positioning analysis")
    if int(packaging_signal_summary["promotional_block_count"]) == 0:
        warnings.append("promotional content missing for positioning analysis")
    upstream_issues = record.get("issues")
    if isinstance(upstream_issues, list) and upstream_issues:
        warnings.append(f"upstream product intelligence has {len(upstream_issues)} issues")
    return warnings


def _price_tiers(records: list[dict[str, object]]) -> dict[str, str]:
    priced_records = [
        (str(record.get("asin") or "").upper(), float(record["price"]))
        for record in records
        if record.get("asin") and isinstance(record.get("price"), (int, float))
    ]
    if not priced_records:
        return {}
    priced_records.sort(key=lambda item: item[1])
    tiers: dict[str, str] = {}
    total = len(priced_records)
    for index, (asin, _price) in enumerate(priced_records):
        percentile = (index + 1) / total
        if percentile <= 0.34:
            tiers[asin] = "budget"
        elif percentile >= 0.67:
            tiers[asin] = "premium"
        else:
            tiers[asin] = "mid"
    return tiers


def _market_themes(records: list[BrandPositioningRecord]) -> list[str]:
    archetype_counts = Counter(record.positioning_archetype for record in records)
    claim_counts = Counter(signal for record in records for signal in record.claim_signals)
    visual_counts = Counter(record.visual_strategy for record in records)
    themes: list[str] = []
    for archetype, count in archetype_counts.most_common(3):
        if count >= 1:
            themes.append(f"{count} products lean toward `{archetype}` positioning.")
    for claim, count in claim_counts.most_common(5):
        if count >= 2:
            themes.append(f"`{claim}` appears across {count} products.")
    for strategy, count in visual_counts.most_common(2):
        if count >= 2:
            themes.append(f"{count} products rely on `{strategy}` visual storytelling.")
    return themes


def _report_caveats(records: list[BrandPositioningRecord]) -> list[str]:
    caveats: list[str] = []
    missing_currency = sum(1 for record in records if "currency missing from product intelligence record" in record.warnings)
    missing_brand = sum(1 for record in records if "brand name could not be normalized" in record.warnings)
    missing_promo = sum(1 for record in records if "promotional content missing for positioning analysis" in record.warnings)
    missing_gallery = sum(1 for record in records if "gallery images missing for packaging analysis" in record.warnings)
    if missing_currency:
        caveats.append(f"{missing_currency} products are missing currency, so value-tier comparisons are relative only.")
    if missing_brand:
        caveats.append(f"{missing_brand} products are missing a normalized brand name.")
    if missing_promo:
        caveats.append(f"{missing_promo} products have no promotional content blocks, so visual positioning is packaging-led only.")
    if missing_gallery:
        caveats.append(f"{missing_gallery} products are missing gallery imagery for packaging analysis.")
    return caveats


def _normalize_brand_name(value: object) -> str | None:
    text = _coerce_optional_string(value)
    if text is None:
        return None
    patterns = (
        (r"^Visit the (.+?) Store$", r"\1"),
        (r"^Brand:\s*(.+)$", r"\1"),
    )
    for pattern, replacement in patterns:
        match = re.match(pattern, text, re.IGNORECASE)
        if match:
            return match.expand(replacement).strip()
    return text.strip()


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", value.lower())


def _coerce_optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        deduped.append(item)
        seen.add(item)
    return deduped


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|")


def _infer_run_id(records: list[dict[str, object]], path: Path) -> str:
    for record in records:
        snapshots = record.get("source_snapshots")
        if isinstance(snapshots, dict) and snapshots.get("collection_run_id"):
            return str(snapshots["collection_run_id"])
    return path.parent.name


def _load_json_list(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"expected list JSON payload in {path}")
    return [item for item in payload if isinstance(item, dict)]
