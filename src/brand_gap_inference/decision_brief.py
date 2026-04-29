from __future__ import annotations

import json
from pathlib import Path

from .contracts import assert_valid


class DecisionBriefBuilder:
    def build(
        self,
        *,
        run_id: str,
        brand_profile_report: dict[str, object],
        demand_signal_report: dict[str, object],
        gap_validation_report: dict[str, object],
        product_intelligence_records: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        candidates = _candidate_list(gap_validation_report)
        top_candidate = candidates[0] if candidates else {}
        intelligence_by_asin = _intelligence_by_asin(product_intelligence_records or [])
        recommendation_level = _recommendation_level(top_candidate, gap_validation_report)
        category_context = _category_context(brand_profile_report, demand_signal_report, gap_validation_report)
        summary = _opportunity_count_summary(gap_validation_report)

        report = {
            "run_id": run_id,
            "status": "success" if candidates or recommendation_level == "do_not_prioritize_yet" else "partial_success",
            "category_context": category_context,
            "recommendation_level": recommendation_level,
            "headline": _headline(recommendation_level, top_candidate),
            "executive_summary": _executive_summary(recommendation_level, top_candidate, summary),
            "opportunity_count_summary": summary,
            "top_opportunity": _top_opportunity(top_candidate, intelligence_by_asin),
            "decision_rationale": _decision_rationale(top_candidate, gap_validation_report, demand_signal_report),
            "recommended_next_steps": _recommended_next_steps(recommendation_level, top_candidate),
            "validation_requirements": _validation_requirements(recommendation_level),
            "blocked_reasons": _blocked_reasons(recommendation_level, top_candidate, gap_validation_report),
            "quality_warnings": _quality_warnings(brand_profile_report, demand_signal_report, gap_validation_report),
            "source_reports": {
                "brand_profile_report": "brand_profiles/brand_profile_report.json",
                "demand_signal_report": "demand_signals/demand_signal_report.json",
                "gap_validation_report": "gap_validation/gap_validation_report.json",
            },
            "caveats": [
                "This brief is deterministic decision support, not autonomous market truth.",
                "Recommendations are based on the selected competitor set and replayable demand proxies only.",
                "Any launch decision still needs broader demand validation, concept testing, and commercial review.",
            ],
        }
        assert_valid("decision_brief_report", report)
        return report


def write_decision_brief_artifacts(*, collection_dir: Path, output_dir: Path) -> dict[str, str]:
    brand_profile_report = _load_json_object(collection_dir / "brand_profiles" / "brand_profile_report.json")
    demand_signal_report = _load_json_object(collection_dir / "demand_signals" / "demand_signal_report.json")
    gap_validation_report = _load_json_object(collection_dir / "gap_validation" / "gap_validation_report.json")
    product_intelligence_records = _load_json_list(
        collection_dir / "product_intelligence" / "product_intelligence_records.json"
    )
    run_id = str(
        gap_validation_report.get("run_id")
        or demand_signal_report.get("run_id")
        or brand_profile_report.get("run_id")
        or collection_dir.name
    )

    report = DecisionBriefBuilder().build(
        run_id=run_id,
        brand_profile_report=brand_profile_report,
        demand_signal_report=demand_signal_report,
        gap_validation_report=gap_validation_report,
        product_intelligence_records=product_intelligence_records,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "decision_brief_report.json"
    markdown_path = output_dir / "decision_brief_report.md"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown_path.write_text(render_decision_brief_markdown(report), encoding="utf-8")
    return {
        "decision_brief_report": str(report_path),
        "decision_brief_report_md": str(markdown_path),
    }


def render_decision_brief_markdown(report: dict[str, object]) -> str:
    top = report.get("top_opportunity", {})
    if not isinstance(top, dict):
        top = {}
    lines = [
        "# Decision Brief",
        "",
        f"Run: `{report['run_id']}`",
        f"Status: `{report['status']}`",
        f"Recommendation: `{report['recommendation_level']}`",
        f"Category context: `{report.get('category_context') or 'none'}`",
        "",
        "## Headline",
        "",
        str(report["headline"]),
        "",
        "## Executive Summary",
        "",
        str(report["executive_summary"]),
        "",
        "## Top Opportunity",
        "",
        f"- Title: {top.get('title') or 'none'}",
        f"- Candidate space: `{top.get('candidate_space') or 'none'}`",
        f"- Gap status: `{top.get('status') or 'none'}`",
        f"- Validation score: `{top.get('validation_score')}`",
        f"- Demand score: `{top.get('demand_score')}`",
        f"- Traction score: `{top.get('traction_score')}`",
        f"- Supply gap score: `{top.get('supply_gap_score')}`",
        "",
    ]
    adjacent_products = top.get("adjacent_products", [])
    if isinstance(adjacent_products, list) and adjacent_products:
        lines.extend(["## Adjacent Evidence Products", ""])
        for product in adjacent_products:
            if not isinstance(product, dict):
                continue
            lines.append(
                "- `{asin}` {brand} - {title} (rating `{rating}`, reviews `{review_count}`)".format(
                    asin=product.get("asin") or "unknown",
                    brand=product.get("brand") or "unknown brand",
                    title=product.get("title") or "untitled",
                    rating=product.get("rating"),
                    review_count=product.get("review_count"),
                )
            )
        lines.append("")

    lines.extend(["## Decision Rationale", ""])
    for item in report.get("decision_rationale", []):
        lines.append(f"- {item}")

    lines.extend(["", "## Recommended Next Steps", ""])
    for item in report.get("recommended_next_steps", []):
        lines.append(f"- {item}")

    lines.extend(["", "## Validation Requirements", ""])
    for item in report.get("validation_requirements", []):
        lines.append(f"- {item}")

    blocked = report.get("blocked_reasons", [])
    if blocked:
        lines.extend(["", "## Blocked Reasons", ""])
        for item in blocked:
            lines.append(f"- {item}")

    warnings = report.get("quality_warnings", [])
    if warnings:
        lines.extend(["", "## Quality Warnings", ""])
        for item in warnings:
            lines.append(f"- {item}")

    lines.extend(["", "## Caveats", ""])
    for item in report.get("caveats", []):
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def _candidate_list(gap_validation_report: dict[str, object]) -> list[dict[str, object]]:
    candidates = gap_validation_report.get("top_candidates")
    if not isinstance(candidates, list) or not candidates:
        candidates = gap_validation_report.get("records")
    if not isinstance(candidates, list):
        return []
    valid_candidates = [candidate for candidate in candidates if isinstance(candidate, dict)]
    return sorted(valid_candidates, key=lambda item: _number(item.get("validation_score")), reverse=True)


def _recommendation_level(candidate: dict[str, object], gap_validation_report: dict[str, object]) -> str:
    if not candidate:
        if str(gap_validation_report.get("status") or "") == "success":
            return "do_not_prioritize_yet"
        return "insufficient_evidence"
    status = str(candidate.get("status") or "")
    validation_score = _number(candidate.get("validation_score"))
    demand_score = _number(candidate.get("demand_score"))
    traction_score = _number(candidate.get("traction_score"))
    if status == "supported" and validation_score >= 0.78 and demand_score >= 0.70 and traction_score >= 0.55:
        return "validate_now"
    if status == "supported" and validation_score >= 0.70:
        return "validate_with_caution"
    if status == "tentative" and validation_score >= 0.55:
        return "research_before_validation"
    return "do_not_prioritize_yet"


def _headline(recommendation_level: str, candidate: dict[str, object]) -> str:
    if not candidate and recommendation_level == "do_not_prioritize_yet":
        return "No priority gap was found after multi-axis coverage review."
    title = str(candidate.get("title") or "No validated opportunity").strip()
    if recommendation_level == "validate_now":
        return f"Move `{title}` into concept validation, with demand caveats attached."
    if recommendation_level == "validate_with_caution":
        return f"`{title}` is promising but needs stronger validation before major investment."
    if recommendation_level == "research_before_validation":
        return f"`{title}` is directional; research it before treating it as an MVP opportunity."
    if recommendation_level == "do_not_prioritize_yet":
        return "Do not prioritize this opportunity yet; evidence is too weak."
    return "No decision-ready opportunity was produced from this run."


def _executive_summary(recommendation_level: str, candidate: dict[str, object], summary: dict[str, int]) -> str:
    if not candidate:
        if recommendation_level == "do_not_prioritize_yet":
            return (
                "Gap validation produced no candidate gaps after accounting for primary and secondary territory coverage. "
                "Treat this as a useful no-go signal for this selected set, not as proof the whole market has no opportunity."
            )
        return "The run did not produce usable gap candidates. Treat this as an evidence-quality issue, not a market conclusion."
    candidate_space = str(candidate.get("candidate_space") or "the top candidate")
    status = str(candidate.get("status") or "unknown")
    validation_score = _number(candidate.get("validation_score"))
    supported = summary.get("supported_candidates", 0)
    if recommendation_level == "validate_now":
        return (
            f"The strongest candidate is `{candidate_space}` with a `{status}` gap status and "
            f"validation score `{validation_score:.2f}`. This is strong enough for a controlled concept-validation sprint, "
            f"not a launch decision. The run produced `{supported}` supported candidate(s)."
        )
    if recommendation_level == "validate_with_caution":
        return (
            f"The strongest candidate is `{candidate_space}` with a `{status}` gap status, but one or more evidence pillars "
            "are not strong enough for confident acceleration. Use this as a focused validation target."
        )
    if recommendation_level == "research_before_validation":
        return (
            f"The strongest candidate is `{candidate_space}`, but it remains tentative. The right next move is more research, "
            "not brand buildout."
        )
    return (
        f"The strongest candidate is `{candidate_space}`, but the evidence does not justify prioritization yet. "
        "Keep the result as a learning artifact."
    )


def _opportunity_count_summary(gap_validation_report: dict[str, object]) -> dict[str, int]:
    return {
        "total_candidates": _integer(gap_validation_report.get("total_candidates")),
        "supported_candidates": _integer(gap_validation_report.get("supported_candidates")),
        "tentative_candidates": _integer(gap_validation_report.get("tentative_candidates")),
        "weak_candidates": _integer(gap_validation_report.get("weak_candidates")),
    }


def _top_opportunity(candidate: dict[str, object], intelligence_by_asin: dict[str, dict[str, object]]) -> dict[str, object]:
    if not candidate:
        return {
            "gap_id": "",
            "title": "",
            "candidate_space": "",
            "status": "none",
            "validation_score": 0.0,
            "demand_score": 0.0,
            "traction_score": 0.0,
            "supply_gap_score": 0.0,
            "price_realism_score": 0.0,
            "adjacent_asins": [],
            "adjacent_products": [],
        }
    adjacent_asins = candidate.get("adjacent_asins")
    adjacent_asin_list = [str(item).upper() for item in adjacent_asins] if isinstance(adjacent_asins, list) else []
    return {
        "gap_id": str(candidate.get("gap_id") or ""),
        "title": str(candidate.get("title") or ""),
        "candidate_space": str(candidate.get("candidate_space") or ""),
        "status": str(candidate.get("status") or "unknown"),
        "validation_score": round(_number(candidate.get("validation_score")), 2),
        "demand_score": round(_number(candidate.get("demand_score")), 2),
        "traction_score": round(_number(candidate.get("traction_score")), 2),
        "supply_gap_score": round(_number(candidate.get("supply_gap_score")), 2),
        "price_realism_score": round(_number(candidate.get("price_realism_score")), 2),
        "adjacent_asins": adjacent_asin_list,
        "adjacent_products": [
            _adjacent_product_summary(intelligence_by_asin[asin])
            for asin in adjacent_asin_list
            if asin in intelligence_by_asin
        ],
    }


def _decision_rationale(
    candidate: dict[str, object],
    gap_validation_report: dict[str, object],
    demand_signal_report: dict[str, object],
) -> list[str]:
    if not candidate:
        return ["No gap candidate was available to evaluate."]
    rationale = [
        f"Top candidate status is `{candidate.get('status')}` with validation score `{_number(candidate.get('validation_score')):.2f}`.",
        f"Demand score is `{_number(candidate.get('demand_score')):.2f}` from `{gap_validation_report.get('demand_signal_source')}`.",
        f"Traction proxy is `{_number(candidate.get('traction_score')):.2f}` from selected-set reviews and ratings.",
        f"Supply gap score is `{_number(candidate.get('supply_gap_score')):.2f}` based on selected-set market-map coverage.",
    ]
    valid_discovery_count = demand_signal_report.get("valid_discovery_count")
    if isinstance(valid_discovery_count, int):
        rationale.append(f"Demand proxy used `{valid_discovery_count}` valid discovery candidate(s).")
    return rationale


def _recommended_next_steps(recommendation_level: str, candidate: dict[str, object]) -> list[str]:
    if recommendation_level == "validate_now":
        return [
            "Write a one-page concept hypothesis for the top opportunity.",
            "Review adjacent ASINs and packaging/promotional evidence before naming the brand angle.",
            "Run broader keyword discovery for the same territory to test whether demand breadth persists.",
            "Create a lightweight comparison table against the closest existing competitors.",
        ]
    if recommendation_level == "validate_with_caution":
        return [
            "Keep the top candidate in the sprint backlog, but label it validation-first.",
            "Collect a larger competitor set before committing design or brand resources.",
            "Check whether the demand score is concentrated in a few results or broad across the query.",
        ]
    if recommendation_level == "research_before_validation":
        return [
            "Broaden discovery and rerun analysis before product or brand ideation.",
            "Inspect why the candidate is tentative: demand, traction, supply gap, or price realism.",
            "Add a second query variant to test whether the same space appears again.",
        ]
    if recommendation_level == "do_not_prioritize_yet":
        return [
            "Do not allocate concept-build resources from this run alone.",
            "Use the artifact as a category coverage baseline before testing narrower queries.",
            "Only revisit if a strategic reason or a sharper subcategory query exists outside this selected set.",
        ]
    return [
        "Fix the upstream evidence issue before interpreting the market.",
        "Rerun discovery, brand profiles, demand signals, and gap validation.",
    ]


def _validation_requirements(recommendation_level: str) -> list[str]:
    base = [
        "Confirm demand with a stronger external signal such as search volume, click data, or conversion proxy.",
        "Review packaging and promotional imagery qualitatively before locking positioning.",
        "Check unit economics and price-band feasibility outside the selected competitor set.",
    ]
    if recommendation_level in {"validate_now", "validate_with_caution"}:
        return base + ["Run at least one replayable follow-up query to confirm the same opportunity survives."]
    return base + ["Improve evidence quality before using this as a concept-validation input."]


def _blocked_reasons(
    recommendation_level: str,
    candidate: dict[str, object],
    gap_validation_report: dict[str, object],
) -> list[str]:
    reasons: list[str] = []
    if recommendation_level == "insufficient_evidence":
        reasons.append("No usable top candidate was available.")
    elif not candidate:
        reasons.append("No missing territory or price-lane candidate remained after multi-axis coverage review.")
        return reasons
    if _number(candidate.get("demand_score")) < 0.55:
        reasons.append("Demand score is below the research-ready threshold.")
    if _number(candidate.get("traction_score")) < 0.35:
        reasons.append("Adjacent traction evidence is thin.")
    if _integer(gap_validation_report.get("total_candidates")) == 0:
        reasons.append("Gap validation produced zero candidates.")
    return reasons


def _quality_warnings(
    brand_profile_report: dict[str, object],
    demand_signal_report: dict[str, object],
    gap_validation_report: dict[str, object],
) -> list[str]:
    warnings: list[str] = []
    for source_name, report in [
        ("brand profile", brand_profile_report),
        ("demand signal", demand_signal_report),
        ("gap validation", gap_validation_report),
    ]:
        status = str(report.get("status") or "")
        if status and status != "success":
            warnings.append(f"{source_name} report status is `{status}`.")
        caveats = report.get("caveats")
        if isinstance(caveats, list):
            warnings.extend(str(item) for item in caveats[:3] if str(item).strip())
    return _dedupe(warnings)


def _category_context(*reports: dict[str, object]) -> str | None:
    for report in reports:
        value = report.get("category_context")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


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


def _intelligence_by_asin(records: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    by_asin: dict[str, dict[str, object]] = {}
    for record in records:
        asin = str(record.get("asin") or "").upper()
        if asin:
            by_asin[asin] = record
    return by_asin


def _adjacent_product_summary(record: dict[str, object]) -> dict[str, object]:
    media_assets = record.get("media_assets")
    gallery_images = media_assets.get("gallery_images") if isinstance(media_assets, dict) else []
    promotional_images = media_assets.get("promotional_images") if isinstance(media_assets, dict) else []
    promotional_content = record.get("promotional_content")
    description_bullets = record.get("description_bullets")
    return {
        "asin": str(record.get("asin") or "").upper(),
        "title": str(record.get("title") or ""),
        "brand": str(record.get("brand") or ""),
        "price": record.get("price") if isinstance(record.get("price"), (int, float)) else None,
        "currency": record.get("currency") if isinstance(record.get("currency"), str) else None,
        "rating": record.get("rating") if isinstance(record.get("rating"), (int, float)) else None,
        "review_count": record.get("review_count") if isinstance(record.get("review_count"), int) else None,
        "gallery_image_count": len(gallery_images) if isinstance(gallery_images, list) else 0,
        "promotional_image_count": len(promotional_images) if isinstance(promotional_images, list) else 0,
        "promotional_content_count": len(promotional_content) if isinstance(promotional_content, list) else 0,
        "description_bullet_count": len(description_bullets) if isinstance(description_bullets, list) else 0,
    }


def _number(value: object) -> float:
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def _integer(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(0, value)
    return 0


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        text = item.strip()
        if text and text not in seen:
            output.append(text)
            seen.add(text)
    return output
