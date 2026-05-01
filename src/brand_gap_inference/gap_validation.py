from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import math
from pathlib import Path

from .contracts import assert_valid

DEFAULT_TARGET_TERRITORY_TERMS = {
    "value_pantry_basics": {"saver", "affordable", "budget", "value", "staples"},
    "convenience_beverage_station": {"packets", "variety pack", "stirrers", "coffee", "tea", "office", "home", "airbnb"},
    "premium_pantry_basics": {"premium", "canister", "pantry", "baking", "non gmo"},
    "health_forward_alternative": {"zero calorie", "zero calories", "sugar free", "sugar-free", "low carb", "keto", "stevia", "allulose", "erythritol", "natural", "organic"},
}

CANDY_TARGET_TERRITORY_TERMS = {
    "value_multi_pack_candy": {"value", "count", "bag", "bulk", "share", "on the go"},
    "sharing_variety_pack": {"variety pack", "variety", "office", "share", "pack of", "multi pack"},
    "premium_indulgence_candy": {"chocolate", "caramel", "mint", "sea salt", "vanilla", "gourmet", "indulgent"},
    "health_forward_alternative": {"zero calorie", "zero calories", "sugar free", "sugar-free", "low carb", "keto", "stevia", "allulose", "erythritol", "natural", "organic", "dentist-approved"},
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
PRICE_STANCES = ("budget_anchor", "mid_market", "premium_pantry")


@dataclass(frozen=True)
class GapValidationRecord:
    gap_id: str
    title: str
    candidate_space: str
    target_territory: str
    target_pricing_stance: str
    whitespace_type: str
    status: str
    supply_gap_score: float
    traction_score: float
    demand_score: float
    price_realism_score: float
    validation_score: float
    adjacent_asins: list[str]
    evidence: list[str]
    caveats: list[str]

    def to_dict(self) -> dict:
        payload = {
            "gap_id": self.gap_id,
            "title": self.title,
            "candidate_space": self.candidate_space,
            "target_territory": self.target_territory,
            "target_pricing_stance": self.target_pricing_stance,
            "whitespace_type": self.whitespace_type,
            "status": self.status,
            "supply_gap_score": round(self.supply_gap_score, 2),
            "traction_score": round(self.traction_score, 2),
            "demand_score": round(self.demand_score, 2),
            "price_realism_score": round(self.price_realism_score, 2),
            "validation_score": round(self.validation_score, 2),
            "adjacent_asins": self.adjacent_asins,
            "evidence": self.evidence,
            "caveats": self.caveats,
        }
        assert_valid("gap_validation_record", payload)
        return payload


@dataclass(frozen=True)
class GapValidationBatchResult:
    run_id: str
    category_context: str | None
    demand_signal_source: str
    records: list[GapValidationRecord]

    @property
    def supported_candidates(self) -> int:
        return sum(1 for record in self.records if record.status == "supported")

    @property
    def tentative_candidates(self) -> int:
        return sum(1 for record in self.records if record.status == "tentative")

    @property
    def weak_candidates(self) -> int:
        return sum(1 for record in self.records if record.status == "weak")

    @property
    def status(self) -> str:
        if self.supported_candidates > 0:
            return "partial_success"
        return "success"

    def to_report_dict(self) -> dict:
        top_candidates = [record.to_dict() for record in sorted(self.records, key=lambda item: item.validation_score, reverse=True)[:5]]
        payload = {
            "run_id": self.run_id,
            "status": self.status,
            "category_context": self.category_context,
            "total_candidates": len(self.records),
            "supported_candidates": self.supported_candidates,
            "tentative_candidates": self.tentative_candidates,
            "weak_candidates": self.weak_candidates,
            "demand_signal_source": self.demand_signal_source,
            "top_candidates": top_candidates,
            "records": [record.to_dict() for record in self.records],
            "caveats": _report_caveats(self.records),
        }
        assert_valid("gap_validation_report", payload)
        return payload


class GapValidationBuilder:
    def build(
        self,
        *,
        run_id: str,
        product_intelligence_records: list[dict[str, object]],
        brand_profile_records: list[dict[str, object]],
        brand_profile_report: dict[str, object],
        demand_signal_report: dict[str, object] | None = None,
    ) -> GapValidationBatchResult:
        category_context = str(brand_profile_report.get("category_context") or "").strip() or None
        demand_by_territory = _demand_by_territory(demand_signal_report)
        demand_signal_source = _demand_signal_source(demand_signal_report)
        intelligence_by_asin = {
            str(record["asin"]).upper(): record
            for record in product_intelligence_records
            if isinstance(record, dict) and record.get("asin")
        }
        profiles = [
            record for record in brand_profile_records if isinstance(record, dict) and record.get("asin")
        ]
        territory_counter = Counter(
            territory
            for record in profiles
            for territory in _profile_territories(record)
        )
        records: list[GapValidationRecord] = []
        territory_terms = _target_territory_terms(category_context)

        for territory, signal_terms in territory_terms.items():
            observed_profiles = [
                record for record in profiles if territory in _profile_territories(record)
            ]
            related_profiles = _related_profiles(profiles, territory, signal_terms)
            price_realism_base = _price_realism_base(profiles)
            demand_score = demand_by_territory.get(territory, 0.5)

            if not observed_profiles:
                records.append(
                    self._build_missing_territory_candidate(
                        run_id=run_id,
                        territory=territory,
                        related_profiles=related_profiles,
                        intelligence_by_asin=intelligence_by_asin,
                        price_realism_base=price_realism_base,
                        demand_score=demand_score,
                        brand_profile_report=brand_profile_report,
                        total_profiles=len(profiles),
                    )
                )
                continue

            territory_pricing = {
                str(record.get("pricing_stance") or "unknown")
                for record in observed_profiles
            }
            for pricing_stance in PRICE_STANCES:
                if pricing_stance in territory_pricing:
                    continue
                records.append(
                    self._build_missing_price_gap(
                        run_id=run_id,
                        territory=territory,
                        pricing_stance=pricing_stance,
                        observed_profiles=observed_profiles,
                        intelligence_by_asin=intelligence_by_asin,
                        profiles=profiles,
                        demand_score=demand_score,
                        total_profiles=len(profiles),
                    )
                )

        records = sorted(records, key=lambda item: item.validation_score, reverse=True)
        return GapValidationBatchResult(
            run_id=run_id,
            category_context=category_context,
            demand_signal_source=demand_signal_source,
            records=records,
        )

    def _build_missing_territory_candidate(
        self,
        *,
        run_id: str,
        territory: str,
        related_profiles: list[dict[str, object]],
        intelligence_by_asin: dict[str, dict[str, object]],
        price_realism_base: float,
        demand_score: float,
        brand_profile_report: dict[str, object],
        total_profiles: int,
    ) -> GapValidationRecord:
        traction_score = _average_traction(related_profiles, intelligence_by_asin)
        supply_gap_score = 0.95
        validation_score = _validation_score(
            supply_gap_score=supply_gap_score,
            traction_score=traction_score,
            demand_score=demand_score,
            price_realism_score=price_realism_base,
            total_profiles=total_profiles,
        )
        status = _candidate_status(validation_score)
        adjacent_asins = [str(record["asin"]).upper() for record in related_profiles[:5] if record.get("asin")]
        underrepresented_spaces = brand_profile_report.get("underrepresented_spaces")
        territory_note = next(
            (
                str(item)
                for item in underrepresented_spaces
                if isinstance(item, str) and territory.split("_")[0] in item.lower()
            ),
            None,
        ) if isinstance(underrepresented_spaces, list) else None
        evidence = [
            f"No selected profile is currently mapped to `{territory}`.",
            f"Related adjacent profiles contribute a traction proxy of `{traction_score:.2f}` from reviews and ratings.",
            f"Discovery-breadth demand score is `{demand_score:.2f}`.",
            f"Price realism is `{price_realism_base:.2f}` based on the selected set's existing price bands.",
        ]
        if territory_note:
            evidence.append(territory_note)
        caveats = [
            "This is a selected-set gap, not a full market census.",
        ]
        if not related_profiles:
            caveats.append("There is thin adjacent evidence for this territory in the current set.")
        title = f"Missing territory: {territory.replace('_', ' ')}"
        candidate_space = territory.replace("_", " ")
        return GapValidationRecord(
            gap_id=f"{run_id}-{territory}-missing-territory",
            title=title,
            candidate_space=candidate_space,
            target_territory=territory,
            target_pricing_stance="unspecified",
            whitespace_type="missing_territory",
            status=status,
            supply_gap_score=supply_gap_score,
            traction_score=traction_score,
            demand_score=demand_score,
            price_realism_score=price_realism_base,
            validation_score=validation_score,
            adjacent_asins=adjacent_asins,
            evidence=evidence,
            caveats=caveats,
        )

    def _build_missing_price_gap(
        self,
        *,
        run_id: str,
        territory: str,
        pricing_stance: str,
        observed_profiles: list[dict[str, object]],
        intelligence_by_asin: dict[str, dict[str, object]],
        profiles: list[dict[str, object]],
        demand_score: float,
        total_profiles: int,
    ) -> GapValidationRecord:
        traction_score = _average_traction(observed_profiles, intelligence_by_asin)
        price_realism_score = 1.0 if any(str(record.get("pricing_stance")) == pricing_stance for record in profiles) else 0.6
        supply_gap_score = 0.55
        validation_score = _validation_score(
            supply_gap_score=supply_gap_score,
            traction_score=traction_score,
            demand_score=demand_score,
            price_realism_score=price_realism_score,
            total_profiles=total_profiles,
        )
        status = _candidate_status(validation_score)
        adjacent_asins = [str(record["asin"]).upper() for record in observed_profiles[:5] if record.get("asin")]
        evidence = [
            f"`{territory}` is present in the selected set but missing the `{pricing_stance}` lane.",
            f"Observed profiles in this territory carry a traction proxy of `{traction_score:.2f}`.",
            f"Discovery-breadth demand score is `{demand_score:.2f}`.",
            f"Price realism for `{pricing_stance}` is `{price_realism_score:.2f}` based on the selected set.",
        ]
        caveats = ["This is a price-lane inference from the selected set only."]
        title = f"Open lane: {territory.replace('_', ' ')} without {pricing_stance.replace('_', ' ')}"
        return GapValidationRecord(
            gap_id=f"{run_id}-{territory}-{pricing_stance}-missing-price-lane",
            title=title,
            candidate_space=f"{territory.replace('_', ' ')} x {pricing_stance.replace('_', ' ')}",
            target_territory=territory,
            target_pricing_stance=pricing_stance,
            whitespace_type="missing_price_lane",
            status=status,
            supply_gap_score=supply_gap_score,
            traction_score=traction_score,
            demand_score=demand_score,
            price_realism_score=price_realism_score,
            validation_score=validation_score,
            adjacent_asins=adjacent_asins,
            evidence=evidence,
            caveats=caveats,
        )


def write_gap_validation_artifacts(
    *,
    collection_dir: Path,
    output_dir: Path,
) -> dict[str, str]:
    product_intelligence_records = _load_json_list(
        collection_dir / "product_intelligence" / "product_intelligence_records.json"
    )
    brand_profile_records = _load_json_list(
        collection_dir / "brand_profiles" / "brand_profile_records.json"
    )
    brand_profile_report = _load_json_object(
        collection_dir / "brand_profiles" / "brand_profile_report.json"
    )
    demand_signal_report_path = collection_dir / "demand_signals" / "demand_signal_report.json"
    demand_signal_report = _load_json_object(demand_signal_report_path) if demand_signal_report_path.exists() else None
    run_id = str(brand_profile_report.get("run_id") or collection_dir.name)
    result = GapValidationBuilder().build(
        run_id=run_id,
        product_intelligence_records=product_intelligence_records,
        brand_profile_records=brand_profile_records,
        brand_profile_report=brand_profile_report,
        demand_signal_report=demand_signal_report,
    )
    report = result.to_report_dict()

    output_dir.mkdir(parents=True, exist_ok=True)
    records_path = output_dir / "gap_validation_records.json"
    report_path = output_dir / "gap_validation_report.json"
    markdown_path = output_dir / "gap_validation_report.md"
    records_path.write_text(
        json.dumps([record.to_dict() for record in result.records], indent=2),
        encoding="utf-8",
    )
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown_path.write_text(render_gap_validation_markdown(report), encoding="utf-8")
    return {
        "gap_validation_records": str(records_path),
        "gap_validation_report": str(report_path),
        "gap_validation_report_md": str(markdown_path),
    }


def render_gap_validation_markdown(report: dict[str, object]) -> str:
    lines = [
        "# Gap Validation Report",
        "",
        f"Run: `{report['run_id']}`",
        f"Status: `{report['status']}`",
        f"Category context: `{report.get('category_context') or 'none'}`",
        f"Candidates: `{report['total_candidates']}`",
        f"Supported: `{report['supported_candidates']}`",
        f"Tentative: `{report['tentative_candidates']}`",
        f"Weak: `{report['weak_candidates']}`",
        f"Demand signal source: `{report['demand_signal_source']}`",
        "",
        "## Top Candidates",
        "",
    ]
    top_candidates = report.get("top_candidates", [])
    if isinstance(top_candidates, list) and top_candidates:
        for candidate in top_candidates:
            if not isinstance(candidate, dict):
                continue
            lines.extend(
                [
                    f"### {candidate.get('title')}",
                    "",
                    f"- Space: `{candidate.get('candidate_space')}`",
                    f"- Status: `{candidate.get('status')}`",
                    f"- Validation score: `{candidate.get('validation_score')}`",
                    f"- Supply gap: `{candidate.get('supply_gap_score')}`",
                    f"- Traction: `{candidate.get('traction_score')}`",
                    f"- Demand: `{candidate.get('demand_score')}`",
                    f"- Price realism: `{candidate.get('price_realism_score')}`",
                    f"- Adjacent ASINs: {', '.join(candidate.get('adjacent_asins', [])) if candidate.get('adjacent_asins') else 'none'}",
                ]
            )
            evidence = candidate.get("evidence", [])
            if isinstance(evidence, list) and evidence:
                lines.append("- Evidence:")
                for item in evidence:
                    lines.append(f"  - {item}")
            lines.append("")
    else:
        lines.append("- No candidate gaps were generated.")
        lines.append("")

    lines.append("## Caveats")
    caveats = report.get("caveats", [])
    if isinstance(caveats, list) and caveats:
        for item in caveats:
            lines.append(f"- {item}")
    else:
        lines.append("- No caveats recorded.")
    lines.append("")
    return "\n".join(lines)


def _related_profiles(profiles: list[dict[str, object]], territory: str, signal_terms: set[str]) -> list[dict[str, object]]:
    matched: list[dict[str, object]] = []
    for record in profiles:
        claims = {str(item).lower() for item in record.get("primary_claims", []) if str(item).strip()} if isinstance(record.get("primary_claims"), list) else set()
        proof_points = " ".join(str(item).lower() for item in record.get("proof_points", []) if str(item).strip()) if isinstance(record.get("proof_points"), list) else ""
        if signal_terms.intersection(claims) or any(term in proof_points for term in signal_terms):
            matched.append(record)
    return matched


def _profile_territories(record: dict[str, object]) -> list[str]:
    territories: list[str] = []
    primary = str(record.get("positioning_territory") or "").strip()
    if primary:
        territories.append(primary)
    secondary = record.get("secondary_territories")
    if isinstance(secondary, list):
        for item in secondary:
            territory = str(item).strip()
            if territory:
                territories.append(territory)
    deduped: list[str] = []
    seen: set[str] = set()
    for territory in territories:
        if territory in seen:
            continue
        deduped.append(territory)
        seen.add(territory)
    return deduped


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


def _price_realism_base(profiles: list[dict[str, object]]) -> float:
    observed_pricing = {
        str(record.get("pricing_stance") or "")
        for record in profiles
        if record.get("pricing_stance")
    }
    if len(observed_pricing) >= 3:
        return 1.0
    if len(observed_pricing) == 2:
        return 0.8
    if len(observed_pricing) == 1:
        return 0.6
    return 0.3


def _average_traction(profiles: list[dict[str, object]], intelligence_by_asin: dict[str, dict[str, object]]) -> float:
    if not profiles:
        return 0.25
    scores = []
    for profile in profiles:
        asin = str(profile.get("asin") or "").upper()
        intelligence = intelligence_by_asin.get(asin, {})
        review_count = intelligence.get("review_count")
        rating = intelligence.get("rating")

        review_score = 0.0
        if isinstance(review_count, int) and review_count >= 0:
            review_score = min(1.0, math.log10(review_count + 1) / 5.0)

        rating_score = 0.0
        if isinstance(rating, (int, float)) and 0 <= float(rating) <= 5:
            rating_score = float(rating) / 5.0

        if review_score == 0.0 and rating_score == 0.0:
            scores.append(0.15)
        else:
            scores.append((0.7 * review_score) + (0.3 * rating_score))
    return round(sum(scores) / len(scores), 4)


def _validation_score(
    *,
    supply_gap_score: float,
    traction_score: float,
    demand_score: float,
    price_realism_score: float,
    total_profiles: int,
) -> float:
    score = (
        (0.35 * supply_gap_score)
        + (0.25 * traction_score)
        + (0.20 * demand_score)
        + (0.20 * price_realism_score)
    )
    if total_profiles < 5:
        score -= 0.10
    elif total_profiles < 10:
        score -= 0.05
    return round(max(0.1, min(1.0, score)), 4)


def _candidate_status(validation_score: float) -> str:
    if validation_score >= 0.70:
        return "supported"
    if validation_score >= 0.55:
        return "tentative"
    return "weak"


def _demand_by_territory(demand_signal_report: dict[str, object] | None) -> dict[str, float]:
    if not isinstance(demand_signal_report, dict):
        return {}
    signals = demand_signal_report.get("signals")
    if not isinstance(signals, list):
        return {}
    scores: dict[str, float] = {}
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        territory = str(signal.get("target_territory") or "").strip()
        score = signal.get("demand_score")
        if territory and isinstance(score, (int, float)):
            scores[territory] = max(0.0, min(1.0, float(score)))
    return scores


def _demand_signal_source(demand_signal_report: dict[str, object] | None) -> str:
    if not isinstance(demand_signal_report, dict):
        return "neutral_default"
    source = str(demand_signal_report.get("source") or "").strip()
    return source or "unknown"


def _report_caveats(records: list[GapValidationRecord]) -> list[str]:
    caveats = [
        "Gap validation currently uses discovery-breadth demand proxies and selected-set review/rating traction, not search volume or conversion data.",
        "A supported candidate is still a directional opportunity, not a launch-ready decision.",
    ]
    if records and all(record.status != "supported" for record in records):
        caveats.append("No candidate crossed the supported threshold in this selected set.")
    return caveats


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
