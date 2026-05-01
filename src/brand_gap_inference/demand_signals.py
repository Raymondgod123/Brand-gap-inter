from __future__ import annotations

from dataclasses import dataclass
import json
import re
from pathlib import Path

from .contracts import assert_valid

DEFAULT_TARGET_TERRITORY_TERMS = {
    "value_pantry_basics": {"saver", "affordable", "budget", "value", "staples"},
    "convenience_beverage_station": {"packets", "variety pack", "coffee", "tea", "office", "home", "airbnb"},
    "premium_pantry_basics": {"premium", "canister", "pantry", "baking", "non gmo", "pure cane"},
    "health_forward_alternative": {
        "zero calorie",
        "zero calories",
        "sugar free",
        "sugar-free",
        "low carb",
        "keto",
        "stevia",
        "allulose",
        "erythritol",
        "natural",
        "organic",
    },
}

CANDY_TARGET_TERRITORY_TERMS = {
    "value_multi_pack_candy": {"value", "count", "bag", "bulk", "share", "on the go"},
    "sharing_variety_pack": {"variety pack", "variety", "office", "share", "pack of", "multi pack"},
    "premium_indulgence_candy": {"chocolate", "caramel", "mint", "sea salt", "vanilla", "gourmet", "indulgent"},
    "health_forward_alternative": {
        "zero calorie",
        "zero calories",
        "sugar free",
        "sugar-free",
        "low carb",
        "keto",
        "stevia",
        "allulose",
        "erythritol",
        "natural",
        "organic",
        "dentist-approved",
        "xylitol",
    },
    "mainstream_zero_sugar_candy": {"hard candy", "gummy", "gummies", "licorice", "lollipops", "drops", "candy"},
}

PROTEIN_BAR_TARGET_TERRITORY_TERMS = {
    "clean_plant_protein_bar": {"organic", "clean", "real food", "natural", "non gmo", "nothing artificial", "simple ingredients", "plant based"},
    "functional_performance_bar": {"keto", "energy", "focus", "recovery", "meal replacement", "high fiber", "mushroom", "mushrooms", "protein"},
    "indulgent_snack_bar": {"chocolate", "cookie", "cookies", "cream", "birthday cake", "caramel", "dessert", "indulgent"},
    "value_variety_protein_bar": {"variety pack", "sample pack", "count", "pack of", "value", "multiple flavors"},
    "family_lifestyle_snack_bar": {"snack", "snacks", "kids", "adults", "on the go", "healthy snacks"},
}

HYDRATION_TARGET_TERRITORY_TERMS = {
    "clean_daily_hydration": {"clean", "natural", "organic", "non gmo", "daily hydration", "no artificial"},
    "performance_electrolyte": {"electrolyte", "electrolytes", "sodium", "potassium", "magnesium", "workout", "sports", "recovery"},
    "zero_sugar_hydration": {"zero sugar", "sugar free", "sugar-free", "zero calorie", "low calorie", "keto"},
    "travel_stick_pack": {"stick", "sticks", "stick pack", "packet", "packets", "on the go", "travel"},
    "family_variety_hydration": {"variety pack", "variety", "multiple flavors", "kids", "family"},
}

PROTEIN_POWDER_TARGET_TERRITORY_TERMS = {
    "clean_plant_protein_powder": {"organic", "clean", "plant based", "plant protein", "vegan", "pea protein", "non gmo"},
    "performance_muscle_protein": {"whey", "isolate", "muscle", "recovery", "workout", "strength", "creatine"},
    "meal_replacement_protein": {"meal replacement", "shake", "nutrition", "superfood", "greens", "smoothie"},
    "value_bulk_protein": {"bulk", "5 lb", "10 lb", "servings", "value", "cost per serving"},
    "flavor_lifestyle_protein": {"vanilla", "chocolate", "flavor", "flavors", "unflavored", "lifestyle"},
}

ENERGY_DRINK_TARGET_TERRITORY_TERMS = {
    "zero_sugar_energy_drink": {"zero sugar", "sugar free", "sugar-free", "zero calorie", "low calorie"},
    "performance_energy_drink": {"performance", "workout", "pre workout", "preworkout", "bcaa", "endurance"},
    "clean_natural_energy": {"clean", "natural", "organic", "yerba", "green tea", "plant based"},
    "focus_functional_energy": {"focus", "mental", "nootropic", "brain", "adaptogen", "mushroom"},
    "flavor_variety_energy": {"variety pack", "variety", "flavor", "flavors", "sparkling", "fruit"},
}


@dataclass(frozen=True)
class DemandSignalRecord:
    signal_id: str
    run_id: str
    query: str
    category_context: str | None
    target_territory: str
    source: str
    match_count: int
    valid_discovery_count: int
    sponsored_match_count: int
    top_rank: int | None
    matched_asins: list[str]
    matched_titles: list[str]
    demand_score: float
    evidence: list[str]
    warnings: list[str]

    def to_dict(self) -> dict[str, object]:
        payload = {
            "signal_id": self.signal_id,
            "run_id": self.run_id,
            "query": self.query,
            "category_context": self.category_context,
            "target_territory": self.target_territory,
            "source": self.source,
            "match_count": self.match_count,
            "valid_discovery_count": self.valid_discovery_count,
            "sponsored_match_count": self.sponsored_match_count,
            "top_rank": self.top_rank,
            "matched_asins": self.matched_asins,
            "matched_titles": self.matched_titles,
            "demand_score": round(self.demand_score, 2),
            "evidence": self.evidence,
            "warnings": self.warnings,
        }
        assert_valid("demand_signal_record", payload)
        return payload


@dataclass(frozen=True)
class DemandSignalBatchResult:
    run_id: str
    query: str
    category_context: str | None
    source: str
    valid_discovery_count: int
    records: list[DemandSignalRecord]

    @property
    def status(self) -> str:
        if not self.records or self.valid_discovery_count == 0:
            return "failed"
        if any(record.warnings for record in self.records):
            return "partial_success"
        return "success"

    def to_report_dict(self) -> dict[str, object]:
        payload = {
            "run_id": self.run_id,
            "status": self.status,
            "query": self.query,
            "category_context": self.category_context,
            "source": self.source,
            "valid_discovery_count": self.valid_discovery_count,
            "total_signals": len(self.records),
            "signals": [record.to_dict() for record in self.records],
            "caveats": _report_caveats(self),
        }
        assert_valid("demand_signal_report", payload)
        return payload


class DemandSignalBuilder:
    def build(
        self,
        *,
        run_id: str,
        discovery_records: list[dict[str, object]],
        brand_profile_report: dict[str, object],
    ) -> DemandSignalBatchResult:
        category_context = _coerce_optional_string(brand_profile_report.get("category_context"))
        query = _infer_query(discovery_records)
        valid_records = [
            record for record in discovery_records if record.get("status") == "valid" and _coerce_optional_string(record.get("title"))
        ]
        terms_by_territory = _target_territory_terms(category_context)
        records = [
            self._build_record(
                run_id=run_id,
                query=query,
                category_context=category_context,
                target_territory=territory,
                terms=terms,
                valid_records=valid_records,
            )
            for territory, terms in terms_by_territory.items()
        ]
        return DemandSignalBatchResult(
            run_id=run_id,
            query=query,
            category_context=category_context,
            source="discovery_breadth",
            valid_discovery_count=len(valid_records),
            records=records,
        )

    def _build_record(
        self,
        *,
        run_id: str,
        query: str,
        category_context: str | None,
        target_territory: str,
        terms: set[str],
        valid_records: list[dict[str, object]],
    ) -> DemandSignalRecord:
        matches: list[dict[str, object]] = []
        for record in valid_records:
            title = _coerce_optional_string(record.get("title")) or ""
            normalized_title = _normalize_text(title)
            if any(_normalize_text(term) in normalized_title for term in terms):
                matches.append(record)

        ranks = [_coerce_optional_int(record.get("rank")) for record in matches]
        ranks = [rank for rank in ranks if rank is not None]
        top_rank = min(ranks) if ranks else None
        sponsored_count = sum(1 for record in matches if record.get("sponsored") is True)
        demand_score = _demand_score(matches=matches, valid_count=len(valid_records), ranks=ranks)
        warnings: list[str] = []
        if not matches:
            warnings.append("no discovery-breadth matches for this target territory")
        evidence = _evidence(target_territory, matches, valid_count=len(valid_records), demand_score=demand_score)

        return DemandSignalRecord(
            signal_id=f"{run_id}-{target_territory}-discovery-demand",
            run_id=run_id,
            query=query,
            category_context=category_context,
            target_territory=target_territory,
            source="discovery_breadth",
            match_count=len(matches),
            valid_discovery_count=len(valid_records),
            sponsored_match_count=sponsored_count,
            top_rank=top_rank,
            matched_asins=_matched_asins(matches),
            matched_titles=_matched_titles(matches),
            demand_score=demand_score,
            evidence=evidence,
            warnings=warnings,
        )


def write_demand_signal_artifacts(
    *,
    collection_dir: Path,
    output_dir: Path,
) -> dict[str, str]:
    discovery_records = _load_json_list(collection_dir / "discovery" / "discovery_records.json")
    brand_profile_report = _load_json_object(collection_dir / "brand_profiles" / "brand_profile_report.json")
    run_id = str(brand_profile_report.get("run_id") or collection_dir.name)
    result = DemandSignalBuilder().build(
        run_id=run_id,
        discovery_records=discovery_records,
        brand_profile_report=brand_profile_report,
    )
    report = result.to_report_dict()

    output_dir.mkdir(parents=True, exist_ok=True)
    records_path = output_dir / "demand_signal_records.json"
    report_path = output_dir / "demand_signal_report.json"
    markdown_path = output_dir / "demand_signal_report.md"
    records_path.write_text(json.dumps([record.to_dict() for record in result.records], indent=2), encoding="utf-8")
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown_path.write_text(render_demand_signal_markdown(report), encoding="utf-8")
    return {
        "demand_signal_records": str(records_path),
        "demand_signal_report": str(report_path),
        "demand_signal_report_md": str(markdown_path),
    }


def render_demand_signal_markdown(report: dict[str, object]) -> str:
    lines = [
        "# Demand Signal Report",
        "",
        f"Run: `{report['run_id']}`",
        f"Status: `{report['status']}`",
        f"Query: `{report['query']}`",
        f"Category context: `{report.get('category_context') or 'none'}`",
        f"Valid discovery candidates: `{report['valid_discovery_count']}`",
        "",
        "## Signals",
        "",
        "| Territory | Demand | Matches | Top Rank | Sponsored |",
        "| --- | --- | --- | --- | --- |",
    ]
    for record in report.get("signals", []):
        if not isinstance(record, dict):
            continue
        lines.append(
            "| {territory} | {score} | {matches} | {top_rank} | {sponsored} |".format(
                territory=record.get("target_territory"),
                score=record.get("demand_score"),
                matches=record.get("match_count"),
                top_rank=record.get("top_rank") or "",
                sponsored=record.get("sponsored_match_count"),
            )
        )
    lines.extend(["", "## Caveats", ""])
    caveats = report.get("caveats", [])
    if isinstance(caveats, list) and caveats:
        for caveat in caveats:
            lines.append(f"- {caveat}")
    else:
        lines.append("- No caveats recorded.")
    lines.append("")
    return "\n".join(lines)


def _target_territory_terms(category_context: str | None) -> dict[str, set[str]]:
    if category_context == "candy":
        return CANDY_TARGET_TERRITORY_TERMS
    if category_context == "protein_bar":
        return PROTEIN_BAR_TARGET_TERRITORY_TERMS
    if category_context == "hydration":
        return HYDRATION_TARGET_TERRITORY_TERMS
    if category_context == "protein_powder":
        return PROTEIN_POWDER_TARGET_TERRITORY_TERMS
    if category_context == "energy_drink":
        return ENERGY_DRINK_TARGET_TERRITORY_TERMS
    return DEFAULT_TARGET_TERRITORY_TERMS


def _demand_score(*, matches: list[dict[str, object]], valid_count: int, ranks: list[int]) -> float:
    if not matches or valid_count <= 0:
        return 0.0
    coverage_score = min(1.0, (len(matches) / valid_count) * 3.0)
    rank_score = 0.0
    if ranks:
        rank_score = sum((valid_count - rank + 1) / valid_count for rank in ranks if rank <= valid_count) / len(ranks)
        rank_score = max(0.0, min(1.0, rank_score))
    sponsored_score = sum(1 for record in matches if record.get("sponsored") is True) / max(1, len(matches))
    return round((0.65 * coverage_score) + (0.25 * rank_score) + (0.10 * sponsored_score), 4)


def _evidence(target_territory: str, matches: list[dict[str, object]], *, valid_count: int, demand_score: float) -> list[str]:
    if not matches:
        return [f"No valid discovery candidates matched `{target_territory}` demand terms."]
    top_rank = min(_coerce_optional_int(record.get("rank")) or valid_count for record in matches)
    return [
        f"`{target_territory}` matched `{len(matches)}` of `{valid_count}` valid discovery candidates.",
        f"Best matching discovery rank was `{top_rank}`.",
        f"Discovery-breadth demand score is `{demand_score:.2f}`.",
    ]


def _matched_asins(matches: list[dict[str, object]]) -> list[str]:
    asins: list[str] = []
    for record in matches[:10]:
        asin = _coerce_optional_string(record.get("asin"))
        if asin is not None:
            asins.append(asin.upper())
    return _dedupe_preserve_order(asins)


def _matched_titles(matches: list[dict[str, object]]) -> list[str]:
    titles: list[str] = []
    for record in matches[:5]:
        title = _coerce_optional_string(record.get("title"))
        if title is not None:
            titles.append(title)
    return _dedupe_preserve_order(titles)


def _report_caveats(result: DemandSignalBatchResult) -> list[str]:
    caveats = [
        "Demand signals currently use replayable discovery breadth, not search volume or conversion data.",
        "A low demand score can mean weak observed demand or simply that the provider result set did not expose the territory clearly.",
    ]
    missing = sum(1 for record in result.records if record.match_count == 0)
    if missing:
        caveats.append(f"{missing} target territories had no discovery-breadth matches.")
    return caveats


def _infer_query(discovery_records: list[dict[str, object]]) -> str:
    for record in discovery_records:
        query = _coerce_optional_string(record.get("query"))
        if query is not None:
            return query
    return "unknown query"


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", value.lower())


def _coerce_optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_optional_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        deduped.append(item)
        seen.add(item)
    return deduped


def _load_json_list(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"expected list JSON payload in {path}")
    return [item for item in payload if isinstance(item, dict)]


def _load_json_object(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object JSON payload in {path}")
    return payload
