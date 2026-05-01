from __future__ import annotations

from collections import Counter
import json
import re
from pathlib import Path

from .contracts import assert_valid

CLAIM_TERMS = (
    "keto",
    "zero calorie",
    "zero calories",
    "zero net carbs",
    "low carb",
    "non gmo",
    "gluten free",
    "baking",
    "coffee",
    "tea",
    "allulose",
    "erythritol",
    "sugar substitute",
    "sugar replacement",
)


def build_landscape_report(*, run_id: str, records: list[dict[str, object]]) -> dict[str, object]:
    competitors = [_competitor_summary(record) for record in records]
    price_ladder = sorted(
        [item for item in competitors if item["price"] is not None],
        key=lambda item: float(item["price"]),
    )
    rating_ladder = sorted(
        [item for item in competitors if item["rating"] is not None],
        key=lambda item: float(item["rating"]),
        reverse=True,
    )
    review_ladder = sorted(
        [item for item in competitors if item["review_count"] is not None],
        key=lambda item: int(item["review_count"]),
        reverse=True,
    )

    caveats = _collect_caveats(records)
    status = "failed" if not competitors else "partial_success" if caveats else "success"
    payload = {
        "run_id": run_id,
        "status": status,
        "product_count": len(competitors),
        "competitors": competitors,
        "price_ladder": price_ladder,
        "rating_ladder": rating_ladder,
        "review_ladder": review_ladder,
        "claim_patterns": _claim_patterns(records),
        "caveats": caveats,
    }
    assert_valid("landscape_report", payload)
    return payload


def write_landscape_artifacts(
    *,
    product_intelligence_records_path: Path,
    output_dir: Path,
    run_id: str | None = None,
) -> dict[str, str]:
    records = _load_json_list(product_intelligence_records_path)
    resolved_run_id = run_id or _infer_run_id(records, product_intelligence_records_path)
    report = build_landscape_report(run_id=resolved_run_id, records=records)
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / "landscape_report.json"
    md_path = output_dir / "landscape_report.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    md_path.write_text(render_landscape_markdown(report), encoding="utf-8")
    return {
        "landscape_report": str(json_path),
        "landscape_report_md": str(md_path),
    }


def render_landscape_markdown(report: dict[str, object]) -> str:
    lines = [
        "# Product Landscape Report",
        "",
        f"Run: `{report['run_id']}`",
        f"Status: `{report['status']}`",
        f"Products: `{report['product_count']}`",
        "",
        "## Competitor Table",
        "",
        "| Rank | ASIN | Title | Brand | Price | Rating | Reviews | Media | Bullets | Promo | Availability |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for competitor in report["competitors"]:
        lines.append(
            "| {rank} | {asin} | {title} | {brand} | {price} | {rating} | {reviews} | {media} | {bullets} | {promo} | {availability} |".format(
                rank=_display(competitor.get("discovery_rank")),
                asin=_display(competitor.get("asin")),
                title=_escape_table(_display(competitor.get("title"))),
                brand=_escape_table(_display(competitor.get("brand"))),
                price=_display_money(competitor.get("price"), competitor.get("currency")),
                rating=_display(competitor.get("rating")),
                reviews=_display(competitor.get("review_count")),
                media=_display(competitor.get("media_asset_count")),
                bullets=_display(competitor.get("description_bullet_count")),
                promo=_display(competitor.get("promotional_block_count")),
                availability=_escape_table(_display(competitor.get("availability"))),
            )
        )

    lines.extend(["", "## Claim Patterns", ""])
    claim_patterns = report["claim_patterns"]
    if claim_patterns:
        for pattern in claim_patterns:
            lines.append(f"- `{pattern['claim']}` appears in `{pattern['count']}` selected products")
    else:
        lines.append("- No repeated claim patterns detected in titles.")

    lines.extend(["", "## Price Ladder", ""])
    for item in report["price_ladder"]:
        lines.append(f"- `{_display_money(item.get('price'), item.get('currency'))}`: {_display(item.get('title'))}")

    lines.extend(["", "## Caveats", ""])
    caveats = report["caveats"]
    if caveats:
        for caveat in caveats:
            lines.append(f"- {caveat}")
    else:
        lines.append("- No merge caveats were recorded.")

    return "\n".join(lines) + "\n"


def _competitor_summary(record: dict[str, object]) -> dict[str, object]:
    return {
        "product_id": record.get("product_id"),
        "asin": record.get("asin"),
        "discovery_rank": record.get("discovery_rank"),
        "title": record.get("title"),
        "brand": record.get("brand"),
        "price": record.get("price"),
        "currency": record.get("currency"),
        "rating": record.get("rating"),
        "review_count": record.get("review_count"),
        "availability": record.get("availability"),
        "media_asset_count": _media_asset_count(record.get("media_assets")),
        "promotional_block_count": _list_length(record.get("promotional_content")),
        "description_bullet_count": _list_length(record.get("description_bullets")),
        "warning_count": _list_length(record.get("warnings")),
        "issue_count": _list_length(record.get("issues")),
    }


def _claim_patterns(records: list[dict[str, object]]) -> list[dict[str, object]]:
    counts: Counter[str] = Counter()
    for record in records:
        title = str(record.get("title") or "").lower()
        normalized_title = re.sub(r"[^a-z0-9 ]+", " ", title)
        for claim in CLAIM_TERMS:
            normalized_claim = claim.lower()
            if normalized_claim in normalized_title:
                counts[claim] += 1
    return [
        {"claim": claim, "count": count}
        for claim, count in counts.most_common()
        if count >= 2
    ]


def _collect_caveats(records: list[dict[str, object]]) -> list[str]:
    caveats: list[str] = []
    missing_currency = sum(1 for record in records if record.get("currency") is None)
    missing_brand = sum(1 for record in records if record.get("brand") is None)
    issue_count = sum(_list_length(record.get("issues")) for record in records)
    if missing_currency:
        caveats.append(f"{missing_currency} products are missing currency; do not use price comparisons as final.")
    if missing_brand:
        caveats.append(f"{missing_brand} products are missing brand; use title/ASIN as fallback identity.")
    missing_media = sum(1 for record in records if _media_asset_count(record.get("media_assets")) == 0)
    if missing_media:
        caveats.append(f"{missing_media} products are missing packaging/promotional media.")
    if issue_count:
        caveats.append(f"{issue_count} product intelligence issues require review before decision use.")
    return caveats


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


def _list_length(value: object) -> int:
    return len(value) if isinstance(value, list) else 0


def _media_asset_count(value: object) -> int:
    if not isinstance(value, dict):
        return 0
    count = 0
    if value.get("primary_image"):
        count += 1
    for key in ("gallery_images", "promotional_images", "videos"):
        items = value.get(key)
        if isinstance(items, list):
            count += len(items)
    return count


def _display(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def _display_money(price: object, currency: object) -> str:
    if price is None:
        return ""
    suffix = f" {currency}" if currency else ""
    return f"{price}{suffix}"


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|")
