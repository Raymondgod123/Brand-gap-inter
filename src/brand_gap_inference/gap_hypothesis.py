from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import math

from .contracts import assert_valid


@dataclass(frozen=True)
class GapHypothesisArtifacts:
    opportunity: dict
    report_markdown: str


def build_gap_hypothesis(
    *,
    listing: dict,
    taxonomy_assignment: dict,
    normalization_record: dict,
    snapshot_id: str,
    schema_version: str = "mvp-v1",
    generated_at: str | None = None,
) -> GapHypothesisArtifacts:
    generated_timestamp = generated_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    axes = taxonomy_assignment.get("axes") or {}
    need_state = str(axes.get("need_state", "unknown"))
    occasion = str(axes.get("occasion", "unknown"))
    format_name = str(axes.get("format", "unknown"))
    audience = str(axes.get("audience", "unknown"))
    adjacent_categories = taxonomy_assignment.get("adjacent_categories") or []
    taxonomy_confidence = float(taxonomy_assignment.get("confidence", 0.0) or 0.0)

    product_title = str(listing.get("product_title", "")).strip()
    brand_name = str(listing.get("brand_name", "")).strip()
    source_record_id = str(listing.get("source_record_id", "")).strip()
    availability = str(listing.get("availability", "unknown"))
    category_path = listing.get("category_path") or []

    warnings = list(normalization_record.get("warnings") or [])
    low_confidence_reasons = list(normalization_record.get("low_confidence_reasons") or [])

    demand_signal_score = _compute_demand_proxy(listing)
    supply_gap_score = _compute_supply_gap_proxy(
        taxonomy_confidence=taxonomy_confidence,
        low_confidence_reason_count=len(low_confidence_reasons),
    )
    validation_score = _compute_validation_score(
        availability=availability,
        category_path=category_path,
        warning_count=len(warnings),
        low_confidence_reason_count=len(low_confidence_reasons),
    )
    confidence = round(min(taxonomy_confidence, validation_score), 2)

    hypothesis_core = (
        f"This product appears positioned for {need_state} and {occasion} "
        f"in {format_name} format for {audience}."
    )
    whitespace_note = (
        "Possible whitespace may exist in adjacent segments: "
        + (", ".join(str(item) for item in adjacent_categories) if adjacent_categories else "unknown")
        + ". (Heuristic; single-product MVP cannot measure true supply.)"
    )

    title = f"Gap hypothesis: {need_state} / {occasion} ({format_name})"
    summary = f"{hypothesis_core} {whitespace_note}"

    evidence = [
        {
            "evidence_id": f"evidence-brand-{source_record_id}",
            "kind": "brand_signal",
            "summary": f"Brand parsed as '{brand_name}' from the ingested Amazon page.",
            "source_record_ids": [source_record_id],
            "confidence": round(min(0.95, max(0.3, taxonomy_confidence)), 2),
        },
        {
            "evidence_id": f"evidence-demand-{source_record_id}",
            "kind": "demand_signal",
            "summary": _demand_evidence_summary(listing),
            "source_record_ids": [source_record_id],
            "confidence": round(min(0.9, max(0.2, demand_signal_score)), 2),
        },
        {
            "evidence_id": f"evidence-gap-{source_record_id}",
            "kind": "supply_gap",
            "summary": _supply_gap_evidence_summary(adjacent_categories),
            "source_record_ids": [source_record_id],
            "confidence": round(min(0.8, max(0.1, supply_gap_score)), 2),
        },
        {
            "evidence_id": f"evidence-validation-{source_record_id}",
            "kind": "validation_note",
            "summary": _validation_evidence_summary(warnings, low_confidence_reasons),
            "source_record_ids": [source_record_id],
            "confidence": round(min(0.9, max(0.2, validation_score)), 2),
        },
    ]
    for item in evidence:
        assert_valid("evidence", item)

    opportunity = {
        "opportunity_id": f"opp-mvp-{snapshot_id}-{source_record_id}",
        "title": title,
        "summary": summary,
        "taxonomy_scope": {
            "need_state": need_state,
            "occasion": occasion,
            "format": format_name,
            "audience": audience,
        },
        "demand_signal_score": round(demand_signal_score, 2),
        "supply_gap_score": round(supply_gap_score, 2),
        "validation_score": round(validation_score, 2),
        "confidence": confidence,
        "evidence": evidence,
        "generated_at": generated_timestamp,
        "schema_version": schema_version,
    }
    assert_valid("opportunity", opportunity)

    report = _render_report_markdown(
        snapshot_id=snapshot_id,
        listing=listing,
        taxonomy_assignment=taxonomy_assignment,
        normalization_record=normalization_record,
        opportunity=opportunity,
    )

    return GapHypothesisArtifacts(opportunity=opportunity, report_markdown=report)


def _compute_demand_proxy(listing: dict) -> float:
    review_count = listing.get("review_count")
    rating = listing.get("rating")

    review_score = 0.0
    if isinstance(review_count, int) and review_count >= 0:
        # Map ~100k reviews to ~1.0 using log scaling.
        review_score = min(1.0, math.log10(review_count + 1) / 5.0)

    rating_score = 0.0
    if isinstance(rating, (int, float)) and 0 <= float(rating) <= 5:
        rating_score = float(rating) / 5.0

    if review_score == 0.0 and rating_score == 0.0:
        return 0.15
    if rating_score == 0.0:
        return min(1.0, 0.2 + (0.8 * review_score))
    return min(1.0, (0.7 * review_score) + (0.3 * rating_score))


def _compute_supply_gap_proxy(*, taxonomy_confidence: float, low_confidence_reason_count: int) -> float:
    score = 0.35
    if taxonomy_confidence >= 0.8:
        score += 0.05
    if low_confidence_reason_count == 0:
        score += 0.05
    if low_confidence_reason_count >= 3:
        score -= 0.05
    return min(1.0, max(0.0, score))


def _compute_validation_score(
    *,
    availability: str,
    category_path: object,
    warning_count: int,
    low_confidence_reason_count: int,
) -> float:
    score = 0.9

    if availability == "unknown":
        score -= 0.1

    if category_path == ["uncategorized"]:
        score -= 0.15

    if warning_count > 0:
        score -= 0.05

    score -= min(0.4, 0.08 * low_confidence_reason_count)

    return min(1.0, max(0.1, score))


def _demand_evidence_summary(listing: dict) -> str:
    parts: list[str] = []
    rating = listing.get("rating")
    if isinstance(rating, (int, float)):
        parts.append(f"Rating: {float(rating):.1f}/5.")
    review_count = listing.get("review_count")
    if isinstance(review_count, int):
        parts.append(f"Review count: {review_count}.")
    if not parts:
        parts.append("Demand proxy is limited because rating/review_count were not available.")
    parts.append("This is an on-page proxy only; it is not external demand intelligence.")
    return " ".join(parts)


def _supply_gap_evidence_summary(adjacent_categories: list[object]) -> str:
    if adjacent_categories:
        joined = ", ".join(str(item) for item in adjacent_categories)
        return f"Adjacent segments suggested by taxonomy heuristics: {joined}."
    return "No adjacent segments were suggested by taxonomy heuristics for this listing."


def _validation_evidence_summary(warnings: list[str], low_confidence_reasons: list[dict]) -> str:
    if not warnings and not low_confidence_reasons:
        return "Normalization produced no warnings and no low-confidence reasons for this listing."

    parts: list[str] = []
    if warnings:
        parts.append(f"Normalization warnings: {', '.join(warnings)}.")
    if low_confidence_reasons:
        codes = sorted({str(reason.get('code', '')).strip() for reason in low_confidence_reasons if reason.get("code")})
        if codes:
            parts.append(f"Low-confidence reason codes: {', '.join(codes)}.")
    parts.append("Treat the hypothesis as decision support, not truth.")
    return " ".join(parts)


def _render_report_markdown(
    *,
    snapshot_id: str,
    listing: dict,
    taxonomy_assignment: dict,
    normalization_record: dict,
    opportunity: dict,
) -> str:
    warnings = list(normalization_record.get("warnings") or [])
    low_confidence_reasons = list(normalization_record.get("low_confidence_reasons") or [])

    taxonomy_axes = taxonomy_assignment.get("axes") or {}
    adjacent_categories = taxonomy_assignment.get("adjacent_categories") or []

    lines: list[str] = []
    lines.append(f"# MVP Gap Report")
    lines.append("")
    lines.append(f"Snapshot: `{snapshot_id}`")
    lines.append(f"Listing: `{listing.get('listing_id')}`")
    lines.append("")

    lines.append("## Cleaned Listing")
    lines.append(f"- Title: {listing.get('product_title')}")
    lines.append(f"- Brand: {listing.get('brand_name')}")
    lines.append(f"- Price: {listing.get('price')} {listing.get('currency')}")
    lines.append(f"- Unit price: {listing.get('unit_price')} per {listing.get('unit_measure')}")
    lines.append(f"- Pack count: {listing.get('pack_count')}")
    lines.append(f"- Availability: {listing.get('availability')}")
    if listing.get("rating") is not None:
        lines.append(f"- Rating: {listing.get('rating')}/5")
    if listing.get("review_count") is not None:
        lines.append(f"- Review count: {listing.get('review_count')}")
    lines.append(f"- Category path: {', '.join(str(item) for item in (listing.get('category_path') or []))}")
    lines.append("")

    lines.append("## Taxonomy")
    lines.append(f"- need_state: `{taxonomy_axes.get('need_state')}`")
    lines.append(f"- occasion: `{taxonomy_axes.get('occasion')}`")
    lines.append(f"- format: `{taxonomy_axes.get('format')}`")
    lines.append(f"- audience: `{taxonomy_axes.get('audience')}`")
    lines.append(f"- confidence: `{taxonomy_assignment.get('confidence')}`")
    lines.append(f"- adjacent categories: {', '.join(str(item) for item in adjacent_categories) if adjacent_categories else 'none'}")
    lines.append("")

    lines.append("## Hypothesis")
    lines.append(opportunity.get("summary", ""))
    lines.append("")

    lines.append("## Caveats")
    if not warnings and not low_confidence_reasons:
        lines.append("- No warnings or low-confidence reasons were recorded during normalization.")
    else:
        if warnings:
            lines.append(f"- Warnings: {', '.join(warnings)}")
        if low_confidence_reasons:
            formatted = "; ".join(
                f"{reason.get('code')} ({reason.get('field')}): {reason.get('message')}"
                for reason in low_confidence_reasons
            )
            lines.append(f"- Low-confidence reasons: {formatted}")
    lines.append("- This output is decision support, not an autonomous truth engine.")
    lines.append("")

    lines.append("## Evidence")
    for item in opportunity.get("evidence", []):
        lines.append(f"- [{item.get('kind')}] {item.get('summary')} (confidence={item.get('confidence')})")
    lines.append("")

    return "\n".join(lines)

