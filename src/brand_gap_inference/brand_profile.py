from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import re
from pathlib import Path

from .contracts import assert_valid

PACKAGE_FORMAT_RULES = (
    ("packets", ("packet", "packets")),
    ("canister", ("canister",)),
    ("bottle", ("bottle",)),
    ("jar", ("jar",)),
    ("box", ("box",)),
    ("bag", ("bag", "lb", "pound")),
)

DEFAULT_TERRITORY_ORDER = (
    "value_pantry_basics",
    "convenience_beverage_station",
    "premium_pantry_basics",
    "health_forward_alternative",
)

CANDY_TERRITORY_ORDER = (
    "value_multi_pack_candy",
    "sharing_variety_pack",
    "premium_indulgence_candy",
    "health_forward_alternative",
    "mainstream_zero_sugar_candy",
)

PROTEIN_BAR_TERRITORY_ORDER = (
    "clean_plant_protein_bar",
    "functional_performance_bar",
    "indulgent_snack_bar",
    "value_variety_protein_bar",
    "family_lifestyle_snack_bar",
)

HYDRATION_TERRITORY_ORDER = (
    "clean_daily_hydration",
    "performance_electrolyte",
    "zero_sugar_hydration",
    "travel_stick_pack",
    "family_variety_hydration",
)

PROTEIN_POWDER_TERRITORY_ORDER = (
    "clean_plant_protein_powder",
    "performance_muscle_protein",
    "meal_replacement_protein",
    "value_bulk_protein",
    "flavor_lifestyle_protein",
)

ENERGY_DRINK_TERRITORY_ORDER = (
    "zero_sugar_energy_drink",
    "performance_energy_drink",
    "clean_natural_energy",
    "focus_functional_energy",
    "flavor_variety_energy",
)

TERRITORY_ORDER = (
    "value_pantry_basics",
    "convenience_beverage_station",
    "premium_pantry_basics",
    "health_forward_alternative",
)

UNDERREPRESENTED_SPACE_MESSAGES = {
    "value_pantry_basics": "No clear value-led pantry basics player appears in this selected set.",
    "convenience_beverage_station": "No clear convenience-led beverage station player appears in this selected set.",
    "premium_pantry_basics": "No clear premium pantry sugar player appears in this selected set.",
    "health_forward_alternative": "No clear health-forward sugar alternative appears in this selected set.",
}

CANDY_UNDERREPRESENTED_SPACE_MESSAGES = {
    "value_multi_pack_candy": "No clear value-led zero-sugar candy multi-pack appears in this selected set.",
    "sharing_variety_pack": "No clear variety-pack or sharing-led zero-sugar candy player appears in this selected set.",
    "premium_indulgence_candy": "No clear premium indulgence zero-sugar candy player appears in this selected set.",
    "health_forward_alternative": "No clear health-forward zero-sugar candy alternative appears in this selected set.",
    "mainstream_zero_sugar_candy": "No clear mainstream zero-sugar candy player appears in this selected set.",
}

PROTEIN_BAR_UNDERREPRESENTED_SPACE_MESSAGES = {
    "clean_plant_protein_bar": "No clear clean plant-protein bar player appears in this selected set.",
    "functional_performance_bar": "No clear functional performance protein bar player appears in this selected set.",
    "indulgent_snack_bar": "No clear indulgent vegan protein snack bar player appears in this selected set.",
    "value_variety_protein_bar": "No clear value-led vegan protein bar variety-pack player appears in this selected set.",
    "family_lifestyle_snack_bar": "No clear family or lifestyle vegan protein snack player appears in this selected set.",
}

HYDRATION_UNDERREPRESENTED_SPACE_MESSAGES = {
    "clean_daily_hydration": "No clear clean daily hydration player appears in this selected set.",
    "performance_electrolyte": "No clear performance electrolyte player appears in this selected set.",
    "zero_sugar_hydration": "No clear zero-sugar hydration player appears in this selected set.",
    "travel_stick_pack": "No clear travel stick-pack hydration player appears in this selected set.",
    "family_variety_hydration": "No clear family or variety-led hydration player appears in this selected set.",
}

PROTEIN_POWDER_UNDERREPRESENTED_SPACE_MESSAGES = {
    "clean_plant_protein_powder": "No clear clean plant protein powder player appears in this selected set.",
    "performance_muscle_protein": "No clear performance muscle protein powder player appears in this selected set.",
    "meal_replacement_protein": "No clear meal-replacement protein powder player appears in this selected set.",
    "value_bulk_protein": "No clear value or bulk protein powder player appears in this selected set.",
    "flavor_lifestyle_protein": "No clear flavor-led lifestyle protein powder player appears in this selected set.",
}

ENERGY_DRINK_UNDERREPRESENTED_SPACE_MESSAGES = {
    "zero_sugar_energy_drink": "No clear zero-sugar energy drink player appears in this selected set.",
    "performance_energy_drink": "No clear performance energy drink player appears in this selected set.",
    "clean_natural_energy": "No clear clean or natural energy drink player appears in this selected set.",
    "focus_functional_energy": "No clear focus or functional energy drink player appears in this selected set.",
    "flavor_variety_energy": "No clear flavor or variety-led energy drink player appears in this selected set.",
}


@dataclass(frozen=True)
class BrandProfileContext:
    query_family: str | None = None


@dataclass(frozen=True)
class VisualBrandSignalsRecord:
    product_id: str
    asin: str
    brand_name: str | None
    package_format: str
    pack_configuration: str
    visual_density: str
    promotional_stack: str
    message_architecture: str
    visual_cues: list[str]
    evidence: list[str]
    warnings: list[str]

    def to_dict(self) -> dict:
        payload = {
            "product_id": self.product_id,
            "asin": self.asin,
            "brand_name": self.brand_name,
            "package_format": self.package_format,
            "pack_configuration": self.pack_configuration,
            "visual_density": self.visual_density,
            "promotional_stack": self.promotional_stack,
            "message_architecture": self.message_architecture,
            "visual_cues": self.visual_cues,
            "evidence": self.evidence,
            "warnings": self.warnings,
        }
        assert_valid("visual_brand_signals_record", payload)
        return payload


@dataclass(frozen=True)
class BrandProfileRecord:
    product_id: str
    asin: str
    brand_name: str | None
    positioning_territory: str
    secondary_territories: list[str]
    target_audience: str
    value_proposition: str
    tone_of_voice: str
    pricing_stance: str
    visual_story: str
    proof_points: list[str]
    primary_claims: list[str]
    evidence_refs: list[str]
    warnings: list[str]

    def to_dict(self) -> dict:
        payload = {
            "product_id": self.product_id,
            "asin": self.asin,
            "brand_name": self.brand_name,
            "positioning_territory": self.positioning_territory,
            "secondary_territories": self.secondary_territories,
            "target_audience": self.target_audience,
            "value_proposition": self.value_proposition,
            "tone_of_voice": self.tone_of_voice,
            "pricing_stance": self.pricing_stance,
            "visual_story": self.visual_story,
            "proof_points": self.proof_points,
            "primary_claims": self.primary_claims,
            "evidence_refs": self.evidence_refs,
            "warnings": self.warnings,
        }
        assert_valid("brand_profile_record", payload)
        return payload


@dataclass(frozen=True)
class BrandProfileBatchResult:
    run_id: str
    context: BrandProfileContext
    visual_signals: list[VisualBrandSignalsRecord]
    profiles: list[BrandProfileRecord]

    @property
    def status(self) -> str:
        if not self.profiles:
            return "failed"
        if any(profile.warnings for profile in self.profiles):
            return "partial_success"
        return "success"

    def to_report_dict(self) -> dict:
        territory_counts = Counter(profile.positioning_territory for profile in self.profiles)
        territory_coverage_counts = Counter(
            territory
            for profile in self.profiles
            for territory in _profile_coverage_territories(profile)
        )
        pricing_counts = Counter(profile.pricing_stance for profile in self.profiles)
        payload = {
            "run_id": self.run_id,
            "status": self.status,
            "category_context": self.context.query_family,
            "total_profiles": len(self.profiles),
            "territory_counts": dict(territory_counts),
            "territory_coverage_counts": dict(territory_coverage_counts),
            "pricing_counts": dict(pricing_counts),
            "crowded_territories": _crowded_territories(territory_counts),
            "underrepresented_spaces": _underrepresented_spaces(territory_coverage_counts, self.context),
            "profiles": [profile.to_dict() for profile in self.profiles],
            "caveats": _report_caveats(self.profiles, self.visual_signals),
        }
        assert_valid("brand_profile_report", payload)
        return payload


class BrandProfileBuilder:
    def build(
        self,
        *,
        run_id: str,
        product_intelligence_records: list[dict[str, object]],
        brand_positioning_records: list[dict[str, object]],
        category_context: BrandProfileContext | None = None,
    ) -> BrandProfileBatchResult:
        resolved_context = category_context or BrandProfileContext()
        positioning_by_asin = {
            str(record["asin"]).upper(): record
            for record in brand_positioning_records
            if isinstance(record, dict) and record.get("asin")
        }
        visual_signals: list[VisualBrandSignalsRecord] = []
        profiles: list[BrandProfileRecord] = []

        for product_record in product_intelligence_records:
            if not isinstance(product_record, dict) or not product_record.get("asin"):
                continue
            asin = str(product_record["asin"]).upper()
            positioning_record = positioning_by_asin.get(asin, {})
            visual_signal = self._build_visual_signal(product_record, positioning_record)
            profile = self._build_profile(product_record, positioning_record, visual_signal, resolved_context)
            visual_signals.append(visual_signal)
            profiles.append(profile)

        return BrandProfileBatchResult(
            run_id=run_id,
            context=resolved_context,
            visual_signals=visual_signals,
            profiles=profiles,
        )

    def _build_visual_signal(
        self,
        product_record: dict[str, object],
        positioning_record: dict[str, object],
    ) -> VisualBrandSignalsRecord:
        asin = str(product_record["asin"]).upper()
        brand_name = _coerce_optional_string(positioning_record.get("brand_name")) or _coerce_optional_string(
            product_record.get("brand")
        )
        normalized_text = _normalized_text_pool(product_record, positioning_record)
        packaging_summary = positioning_record.get("packaging_signal_summary")
        summary = packaging_summary if isinstance(packaging_summary, dict) else {}

        package_format = _package_format(normalized_text)
        pack_configuration = _pack_configuration(normalized_text)
        visual_density = _visual_density(summary)
        promotional_stack = _promotional_stack(positioning_record, summary)
        message_architecture = _message_architecture(positioning_record)
        visual_cues = _visual_cues(
            package_format=package_format,
            pack_configuration=pack_configuration,
            positioning_record=positioning_record,
            summary=summary,
        )
        evidence = _signal_evidence(product_record, positioning_record)
        warnings = _signal_warnings(product_record, positioning_record, summary)

        return VisualBrandSignalsRecord(
            product_id=str(product_record.get("product_id") or f"amazon:{asin}"),
            asin=asin,
            brand_name=brand_name,
            package_format=package_format,
            pack_configuration=pack_configuration,
            visual_density=visual_density,
            promotional_stack=promotional_stack,
            message_architecture=message_architecture,
            visual_cues=visual_cues,
            evidence=evidence,
            warnings=warnings,
        )

    def _build_profile(
        self,
        product_record: dict[str, object],
        positioning_record: dict[str, object],
        visual_signal: VisualBrandSignalsRecord,
        context: BrandProfileContext,
    ) -> BrandProfileRecord:
        asin = str(product_record["asin"]).upper()
        brand_name = visual_signal.brand_name
        positioning_territory = _positioning_territory(positioning_record, visual_signal, context)
        secondary_territories = _secondary_territories(
            primary_territory=positioning_territory,
            positioning_record=positioning_record,
            visual_signal=visual_signal,
            context=context,
        )
        target_audience = _target_audience(positioning_territory, positioning_record, visual_signal, context)
        value_proposition = _value_proposition(positioning_territory, context)
        tone_of_voice = _tone_of_voice(visual_signal.message_architecture)
        pricing_stance = _pricing_stance(positioning_record)
        visual_story = _visual_story(positioning_territory, visual_signal, context)
        primary_claims = _primary_claims(positioning_record)
        proof_points = _proof_points(product_record, positioning_record, visual_signal)
        evidence_refs = [
            f"product_intelligence:{asin}",
            f"brand_positioning:{asin}",
        ]
        warnings = _dedupe_preserve_order(
            list(visual_signal.warnings)
            + _coerce_string_list(positioning_record.get("warnings"))
        )

        return BrandProfileRecord(
            product_id=str(product_record.get("product_id") or f"amazon:{asin}"),
            asin=asin,
            brand_name=brand_name,
            positioning_territory=positioning_territory,
            secondary_territories=secondary_territories,
            target_audience=target_audience,
            value_proposition=value_proposition,
            tone_of_voice=tone_of_voice,
            pricing_stance=pricing_stance,
            visual_story=visual_story,
            proof_points=proof_points,
            primary_claims=primary_claims,
            evidence_refs=evidence_refs,
            warnings=warnings,
        )


def write_brand_profile_artifacts(
    *,
    collection_dir: Path,
    output_dir: Path,
) -> dict[str, str]:
    product_intelligence_records = _load_json_list(
        collection_dir / "product_intelligence" / "product_intelligence_records.json"
    )
    brand_positioning_records = _load_json_list(
        collection_dir / "brand_positioning" / "brand_positioning_records.json"
    )
    run_id = _infer_run_id(product_intelligence_records, brand_positioning_records, collection_dir)
    category_context = _load_category_context(collection_dir)
    result = BrandProfileBuilder().build(
        run_id=run_id,
        product_intelligence_records=product_intelligence_records,
        brand_positioning_records=brand_positioning_records,
        category_context=category_context,
    )
    report = result.to_report_dict()

    output_dir.mkdir(parents=True, exist_ok=True)
    visual_signals_path = output_dir / "visual_brand_signals_records.json"
    profiles_path = output_dir / "brand_profile_records.json"
    report_path = output_dir / "brand_profile_report.json"
    markdown_path = output_dir / "brand_profile_report.md"

    visual_signals_path.write_text(
        json.dumps([record.to_dict() for record in result.visual_signals], indent=2),
        encoding="utf-8",
    )
    profiles_path.write_text(
        json.dumps([record.to_dict() for record in result.profiles], indent=2),
        encoding="utf-8",
    )
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_brand_profile_markdown(report, result.visual_signals, result.profiles),
        encoding="utf-8",
    )
    return {
        "visual_brand_signals_records": str(visual_signals_path),
        "brand_profile_records": str(profiles_path),
        "brand_profile_report": str(report_path),
        "brand_profile_report_md": str(markdown_path),
    }


def render_brand_profile_markdown(
    report: dict[str, object],
    visual_signals: list[VisualBrandSignalsRecord],
    profiles: list[BrandProfileRecord],
) -> str:
    lines = [
        "# Brand Profile Report",
        "",
        f"Run: `{report['run_id']}`",
        f"Status: `{report['status']}`",
        f"Category context: `{report.get('category_context') or 'none'}`",
        f"Profiles: `{report['total_profiles']}`",
        "",
        "## Visual Signals",
        "",
        "| ASIN | Brand | Package | Configuration | Density | Promo Stack | Message Architecture |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for record in visual_signals:
        lines.append(
            "| {asin} | {brand} | {package_format} | {pack_configuration} | {visual_density} | {promotional_stack} | {message_architecture} |".format(
                asin=record.asin,
                brand=_escape_table(record.brand_name or ""),
                package_format=record.package_format,
                pack_configuration=record.pack_configuration,
                visual_density=record.visual_density,
                promotional_stack=record.promotional_stack,
                message_architecture=record.message_architecture,
            )
        )

    lines.extend(["", "## Brand Profiles", ""])
    for profile in profiles:
        lines.extend(
            [
                f"### {profile.brand_name or profile.asin} (`{profile.asin}`)",
                "",
                f"- Territory: `{profile.positioning_territory}`",
                f"- Secondary territories: {', '.join(profile.secondary_territories) if profile.secondary_territories else 'none'}",
                f"- Audience: {profile.target_audience}",
                f"- Pricing stance: `{profile.pricing_stance}`",
                f"- Tone: `{profile.tone_of_voice}`",
                f"- Value proposition: {profile.value_proposition}",
                f"- Visual story: {profile.visual_story}",
                f"- Proof points: {', '.join(profile.proof_points) if profile.proof_points else 'none'}",
                f"- Primary claims: {', '.join(profile.primary_claims) if profile.primary_claims else 'none'}",
                "",
            ]
        )

    lines.extend(["## Market Map", ""])
    territory_counts = report.get("territory_counts", {})
    if isinstance(territory_counts, dict) and territory_counts:
        for territory, count in territory_counts.items():
            lines.append(f"- `{territory}`: `{count}` profiles")
    else:
        lines.append("- No territory clusters available.")

    lines.extend(["", "## Multi-Axis Territory Coverage", ""])
    coverage_counts = report.get("territory_coverage_counts", {})
    if isinstance(coverage_counts, dict) and coverage_counts:
        for territory, count in coverage_counts.items():
            lines.append(f"- `{territory}`: `{count}` profiles with primary or secondary coverage")
    else:
        lines.append("- No multi-axis territory coverage available.")

    lines.extend(["", "## Crowded Territories", ""])
    crowded = report.get("crowded_territories", [])
    if isinstance(crowded, list) and crowded:
        for item in crowded:
            lines.append(f"- {item}")
    else:
        lines.append("- No crowded territories in the selected set.")

    lines.extend(["", "## Underrepresented Spaces", ""])
    underrepresented = report.get("underrepresented_spaces", [])
    if isinstance(underrepresented, list) and underrepresented:
        for item in underrepresented:
            lines.append(f"- {item}")
    else:
        lines.append("- No obvious unrepresented spaces detected in the selected set.")

    lines.extend(["", "## Caveats", ""])
    caveats = report.get("caveats", [])
    if isinstance(caveats, list) and caveats:
        for item in caveats:
            lines.append(f"- {item}")
    else:
        lines.append("- No caveats recorded.")
    return "\n".join(lines) + "\n"


def _package_format(normalized_text: str) -> str:
    for label, patterns in PACKAGE_FORMAT_RULES:
        if any(pattern in normalized_text for pattern in patterns):
            return label
    return "unknown"


def _pack_configuration(normalized_text: str) -> str:
    if "variety pack" in normalized_text:
        return "variety_pack"
    if "pack of" in normalized_text:
        return "multi_pack"
    count_match = re.search(r"\b(\d+)\s*count\b", normalized_text)
    if count_match:
        count = int(count_match.group(1))
        if count >= 50:
            return "bulk_count"
    return "single_unit"


def _visual_density(summary: dict[str, object]) -> str:
    score = (
        int(summary.get("gallery_image_count") or 0)
        + 2 * int(summary.get("promotional_block_count") or 0)
        + int(summary.get("video_count") or 0)
    )
    if score >= 10:
        return "rich"
    if score >= 5:
        return "moderate"
    return "sparse"


def _promotional_stack(positioning_record: dict[str, object], summary: dict[str, object]) -> str:
    visual_strategy = _coerce_optional_string(positioning_record.get("visual_strategy"))
    if visual_strategy == "packaging_promo_video":
        return "full_story_stack"
    if visual_strategy == "packaging_plus_promo":
        return "packaging_plus_promo"
    if visual_strategy == "packaging_plus_video":
        return "packaging_plus_video"
    promo_count = int(summary.get("promotional_block_count") or 0)
    if promo_count > 0:
        return "packaging_plus_promo"
    return "packaging_only"


def _message_architecture(positioning_record: dict[str, object]) -> str:
    archetype = _coerce_optional_string(positioning_record.get("positioning_archetype")) or ""
    if archetype == "value_staple":
        return "value_led"
    if archetype == "convenience_bundle":
        return "convenience_led"
    if archetype == "health_positioned":
        return "health_led"
    if archetype == "pantry_staple":
        return "pantry_classic"
    return "utilitarian"


def _visual_cues(
    *,
    package_format: str,
    pack_configuration: str,
    positioning_record: dict[str, object],
    summary: dict[str, object],
) -> list[str]:
    cues: list[str] = []
    if package_format in {"bag", "canister"}:
        cues.append("pantry")
    if package_format == "packets":
        cues.append("portion_control")
    if pack_configuration in {"variety_pack", "bulk_count"}:
        cues.append("assortment")
    if _coerce_optional_string(positioning_record.get("value_signal")) in {"moderate", "explicit"}:
        cues.append("value")
    if _coerce_optional_string(positioning_record.get("health_signal")) in {"moderate", "explicit"}:
        cues.append("health")
    usage_contexts = positioning_record.get("usage_contexts")
    if isinstance(usage_contexts, list) and any(context in {"office", "home", "coffee", "tea"} for context in usage_contexts):
        cues.append("hospitality")
    if int(summary.get("video_count") or 0) > 0:
        cues.append("social_proof")
    if int(summary.get("promotional_block_count") or 0) > 0:
        cues.append("brand_story")
    return _dedupe_preserve_order(cues)


def _signal_evidence(product_record: dict[str, object], positioning_record: dict[str, object]) -> list[str]:
    evidence: list[str] = []
    for source in (
        product_record.get("title"),
        *_coerce_string_list(product_record.get("description_bullets"))[:2],
        *_coerce_signal_evidence(positioning_record),
    ):
        text = _coerce_optional_string(source)
        if text is not None:
            evidence.append(text)
    return _dedupe_preserve_order(evidence)[:5]


def _signal_warnings(
    product_record: dict[str, object],
    positioning_record: dict[str, object],
    summary: dict[str, object],
) -> list[str]:
    warnings = []
    if int(summary.get("gallery_image_count") or 0) == 0:
        warnings.append("gallery imagery missing for visual signal analysis")
    if int(summary.get("promotional_block_count") or 0) == 0:
        warnings.append("promotional content missing for richer visual signal analysis")
    if product_record.get("currency") is None:
        warnings.append("currency missing from upstream product intelligence")
    if not positioning_record:
        warnings.append("brand positioning input missing")
    return _dedupe_preserve_order(warnings)


def _positioning_territory(
    positioning_record: dict[str, object],
    visual_signal: VisualBrandSignalsRecord,
    context: BrandProfileContext,
) -> str:
    archetype = _coerce_optional_string(positioning_record.get("positioning_archetype")) or ""
    price_tier = _coerce_optional_string(positioning_record.get("price_tier")) or "unknown"
    if context.query_family == "candy":
        if archetype == "health_positioned":
            return "health_forward_alternative"
        if archetype == "value_staple" or "value" in visual_signal.visual_cues:
            return "value_multi_pack_candy"
        if visual_signal.pack_configuration in {"variety_pack", "multi_pack", "bulk_count"} or archetype == "convenience_bundle":
            return "sharing_variety_pack"
        if price_tier == "premium" and _looks_like_premium_indulgence(positioning_record, visual_signal):
            return "premium_indulgence_candy"
        return "mainstream_zero_sugar_candy"
    if context.query_family == "protein_bar":
        evidence_text = _profile_text(positioning_record, visual_signal)
        if _contains_any(evidence_text, {"focus", "energy", "recovery", "mushroom", "mushrooms", "lion", "meal replacement", "keto"}):
            return "functional_performance_bar"
        if _contains_any(evidence_text, {"chocolate", "cookie", "cookies", "cream", "birthday cake", "caramel", "dessert", "indulgent", "sweet"}):
            return "indulgent_snack_bar"
        if _contains_any(evidence_text, {"organic", "real food", "clean", "non gmo", "natural", "nothing artificial", "no sugar alcohol", "simple ingredients"}):
            return "clean_plant_protein_bar"
        if _contains_any(evidence_text, {"variety pack", "sample pack", "count", "value", "pack of"}) or visual_signal.pack_configuration in {"variety_pack", "multi_pack", "bulk_count"}:
            return "value_variety_protein_bar"
        return "family_lifestyle_snack_bar"
    if context.query_family == "hydration":
        evidence_text = _profile_text(positioning_record, visual_signal)
        if _contains_any(evidence_text, {"workout", "sport", "sports", "sweat", "endurance", "recovery", "rapid", "sodium", "potassium", "magnesium"}):
            return "performance_electrolyte"
        if _contains_any(evidence_text, {"zero sugar", "sugar free", "sugar-free", "zero calorie", "low calorie", "keto"}):
            return "zero_sugar_hydration"
        if _contains_any(evidence_text, {"clean", "natural", "organic", "non gmo", "no artificial", "plant based"}):
            return "clean_daily_hydration"
        if _contains_any(evidence_text, {"stick", "sticks", "packet", "packets", "on the go", "travel"}) or visual_signal.package_format == "packets":
            return "travel_stick_pack"
        if _contains_any(evidence_text, {"variety", "variety pack", "kids", "family", "multiple flavors"}) or visual_signal.pack_configuration == "variety_pack":
            return "family_variety_hydration"
        return "clean_daily_hydration"
    if context.query_family == "protein_powder":
        evidence_text = _profile_text(positioning_record, visual_signal)
        if _contains_any(evidence_text, {"whey", "isolate", "muscle", "recovery", "workout", "strength", "mass", "creatine"}):
            return "performance_muscle_protein"
        if _contains_any(evidence_text, {"meal replacement", "shake", "superfood", "nutrition", "greens", "daily nutrition"}):
            return "meal_replacement_protein"
        if _contains_any(evidence_text, {"organic", "clean", "plant", "plant based", "vegan", "pea", "non gmo", "natural"}):
            return "clean_plant_protein_powder"
        if _contains_any(evidence_text, {"bulk", "5 lb", "10 lb", "servings", "value", "cost per serving"}) or visual_signal.pack_configuration == "bulk_count":
            return "value_bulk_protein"
        return "flavor_lifestyle_protein"
    if context.query_family == "energy_drink":
        evidence_text = _profile_text(positioning_record, visual_signal)
        if _contains_any(evidence_text, {"zero sugar", "sugar free", "sugar-free", "zero calorie", "low calorie", "keto"}):
            return "zero_sugar_energy_drink"
        if _contains_any(evidence_text, {"focus", "mental", "nootropic", "brain", "adaptogen", "mushroom"}):
            return "focus_functional_energy"
        if _contains_any(evidence_text, {"workout", "performance", "pre workout", "preworkout", "bcaa", "electrolyte", "endurance"}):
            return "performance_energy_drink"
        if _contains_any(evidence_text, {"clean", "natural", "organic", "yerba", "tea", "green tea", "plant based"}):
            return "clean_natural_energy"
        return "flavor_variety_energy"
    if archetype == "value_staple":
        return "value_pantry_basics"
    if archetype == "convenience_bundle":
        return "convenience_beverage_station"
    if archetype == "health_positioned":
        return "health_forward_alternative"
    if archetype == "pantry_staple" and price_tier == "premium":
        return "premium_pantry_basics"
    if archetype == "pantry_staple":
        return "pantry_classic"
    if visual_signal.pack_configuration == "bulk_count":
        return "hospitality_bulk_solution"
    return "general_household"


def _secondary_territories(
    *,
    primary_territory: str,
    positioning_record: dict[str, object],
    visual_signal: VisualBrandSignalsRecord,
    context: BrandProfileContext,
) -> list[str]:
    evidence_text = _profile_text(positioning_record, visual_signal)
    territories: list[str] = []
    if context.query_family == "protein_bar":
        if _contains_any(
            evidence_text,
            {"organic", "clean", "real food", "natural", "nothing artificial", "no sugar alcohol", "simple ingredients"},
        ):
            territories.append("clean_plant_protein_bar")
        if _contains_any(
            evidence_text,
            {"focus", "energy", "recovery", "mushroom", "mushrooms", "lion", "meal replacement", "keto", "high fiber"},
        ):
            territories.append("functional_performance_bar")
        if _contains_any(evidence_text, {"chocolate", "cookie", "cookies", "cream", "birthday cake", "caramel", "dessert", "indulgent", "sweet"}):
            territories.append("indulgent_snack_bar")
        if _contains_any(evidence_text, {"variety pack", "sample pack", "value", "multiple flavors"}) or visual_signal.pack_configuration == "variety_pack":
            territories.append("value_variety_protein_bar")
        if _contains_any(evidence_text, {"kids", "adults", "on the go", "healthy snacks"}):
            territories.append("family_lifestyle_snack_bar")
    elif context.query_family == "hydration":
        if _contains_any(evidence_text, {"clean", "natural", "organic", "non gmo", "no artificial", "daily hydration"}):
            territories.append("clean_daily_hydration")
        if _contains_any(evidence_text, {"workout", "sport", "sports", "sweat", "endurance", "recovery", "rapid", "sodium", "potassium", "magnesium"}):
            territories.append("performance_electrolyte")
        if _contains_any(evidence_text, {"zero sugar", "sugar free", "sugar-free", "zero calorie", "low calorie", "keto"}):
            territories.append("zero_sugar_hydration")
        if _contains_any(evidence_text, {"stick", "sticks", "packet", "packets", "on the go", "travel"}) or visual_signal.package_format == "packets":
            territories.append("travel_stick_pack")
        if _contains_any(evidence_text, {"variety", "variety pack", "kids", "family", "multiple flavors"}) or visual_signal.pack_configuration == "variety_pack":
            territories.append("family_variety_hydration")
    elif context.query_family == "protein_powder":
        if _contains_any(evidence_text, {"organic", "clean", "plant", "plant based", "vegan", "pea", "non gmo", "natural"}):
            territories.append("clean_plant_protein_powder")
        if _contains_any(evidence_text, {"whey", "isolate", "muscle", "recovery", "workout", "strength", "mass", "creatine"}):
            territories.append("performance_muscle_protein")
        if _contains_any(evidence_text, {"meal replacement", "shake", "superfood", "nutrition", "greens", "daily nutrition"}):
            territories.append("meal_replacement_protein")
        if _contains_any(evidence_text, {"bulk", "5 lb", "10 lb", "servings", "value", "cost per serving"}) or visual_signal.pack_configuration == "bulk_count":
            territories.append("value_bulk_protein")
        if _contains_any(evidence_text, {"vanilla", "chocolate", "flavor", "flavors", "smoothie", "lifestyle"}):
            territories.append("flavor_lifestyle_protein")
    elif context.query_family == "energy_drink":
        if _contains_any(evidence_text, {"zero sugar", "sugar free", "sugar-free", "zero calorie", "low calorie", "keto"}):
            territories.append("zero_sugar_energy_drink")
        if _contains_any(evidence_text, {"workout", "performance", "pre workout", "preworkout", "bcaa", "electrolyte", "endurance"}):
            territories.append("performance_energy_drink")
        if _contains_any(evidence_text, {"clean", "natural", "organic", "yerba", "tea", "green tea", "plant based"}):
            territories.append("clean_natural_energy")
        if _contains_any(evidence_text, {"focus", "mental", "nootropic", "brain", "adaptogen", "mushroom"}):
            territories.append("focus_functional_energy")
        if _contains_any(evidence_text, {"variety", "flavor", "flavors", "pack", "sparkling", "fruit"}):
            territories.append("flavor_variety_energy")
    return [territory for territory in _dedupe_preserve_order(territories) if territory != primary_territory]


def _target_audience(
    positioning_territory: str,
    positioning_record: dict[str, object],
    visual_signal: VisualBrandSignalsRecord,
    context: BrandProfileContext,
) -> str:
    usage_contexts = positioning_record.get("usage_contexts")
    usage_context_list = usage_contexts if isinstance(usage_contexts, list) else []
    if context.query_family == "candy":
        if positioning_territory == "value_multi_pack_candy":
            return "value-seeking households and candy-bowl restockers"
        if positioning_territory == "sharing_variety_pack":
            return "households, offices, and group occasions looking for shareable candy formats"
        if positioning_territory == "premium_indulgence_candy":
            return "treat-focused shoppers willing to pay more for indulgent flavor or format"
        if positioning_territory == "health_forward_alternative":
            return "health-conscious shoppers looking for lower-guilt candy alternatives"
        return "mainstream zero-sugar candy shoppers seeking familiar candy formats"
    if context.query_family == "protein_bar":
        protein_bar_mapping = {
            "clean_plant_protein_bar": "clean-label and plant-based nutrition shoppers",
            "functional_performance_bar": "active shoppers looking for protein, energy, focus, recovery, or meal replacement",
            "indulgent_snack_bar": "snackers who want dessert-like flavors with vegan protein credentials",
            "value_variety_protein_bar": "households and repeat buyers looking for variety, count, and value",
            "family_lifestyle_snack_bar": "busy adults and families looking for convenient better-for-you snacks",
        }
        return protein_bar_mapping.get(positioning_territory, "vegan protein bar shoppers")
    if context.query_family == "hydration":
        hydration_mapping = {
            "clean_daily_hydration": "daily wellness shoppers looking for clean electrolyte hydration",
            "performance_electrolyte": "active shoppers replacing sweat losses during sports, workouts, or recovery",
            "zero_sugar_hydration": "low-sugar hydration shoppers watching calories or carbs",
            "travel_stick_pack": "commuters, travelers, and gym-bag users looking for portable hydration",
            "family_variety_hydration": "households looking for approachable hydration flavors and variety",
        }
        return hydration_mapping.get(positioning_territory, "hydration shoppers")
    if context.query_family == "protein_powder":
        protein_powder_mapping = {
            "clean_plant_protein_powder": "clean-label and plant-based protein powder shoppers",
            "performance_muscle_protein": "training-focused shoppers looking for muscle, recovery, or strength support",
            "meal_replacement_protein": "routine nutrition shoppers using protein as a shake or meal bridge",
            "value_bulk_protein": "high-frequency protein buyers watching servings and cost per use",
            "flavor_lifestyle_protein": "smoothie and everyday wellness shoppers choosing protein by flavor and habit fit",
        }
        return protein_powder_mapping.get(positioning_territory, "protein powder shoppers")
    if context.query_family == "energy_drink":
        energy_mapping = {
            "zero_sugar_energy_drink": "energy drink shoppers looking for caffeine without sugar",
            "performance_energy_drink": "fitness and performance users looking for energy around training",
            "clean_natural_energy": "wellness-oriented shoppers looking for cleaner caffeine sources",
            "focus_functional_energy": "work and study users looking for focus, mood, or functional benefits",
            "flavor_variety_energy": "repeat energy drink buyers choosing flavor variety and pack convenience",
        }
        return energy_mapping.get(positioning_territory, "energy drink shoppers")
    if positioning_territory == "value_pantry_basics":
        return "budget-focused household pantry shoppers"
    if positioning_territory == "convenience_beverage_station":
        return "office, hospitality, and home beverage-station restockers"
    if positioning_territory == "premium_pantry_basics":
        return "household bakers and pantry stockers willing to pay up for format or brand"
    if positioning_territory == "health_forward_alternative":
        return "health-conscious shoppers looking to reduce or replace sugar"
    if "office" in usage_context_list or visual_signal.pack_configuration == "bulk_count":
        return "shared-space and hospitality restockers"
    return "general household grocery shoppers"


def _value_proposition(positioning_territory: str, context: BrandProfileContext) -> str:
    if context.query_family == "candy":
        candy_mapping = {
            "value_multi_pack_candy": "Affordable zero-sugar candy positioned around count, sharing, or bag value.",
            "sharing_variety_pack": "Shareable zero-sugar candy assortment designed for variety, portioning, and repeat snacking.",
            "premium_indulgence_candy": "Higher-priced zero-sugar candy framed as a more indulgent or elevated treat.",
            "health_forward_alternative": "Cleaner-label or lower-guilt candy positioned as a healthier alternative to mainstream sweets.",
            "mainstream_zero_sugar_candy": "Familiar candy formats translated into a zero-sugar or reduced-sugar everyday treat.",
        }
        return candy_mapping.get(
            positioning_territory,
            "Zero-sugar candy offer without a sharply differentiated positioning story.",
        )
    if context.query_family == "protein_bar":
        protein_bar_mapping = {
            "clean_plant_protein_bar": "Plant-based protein framed around clean ingredients, dietary compatibility, and label trust.",
            "functional_performance_bar": "Protein bar positioned around performance, satiety, energy, recovery, or functional benefits.",
            "indulgent_snack_bar": "Vegan protein bar sold as a dessert-like or treat-forward snack without abandoning nutrition.",
            "value_variety_protein_bar": "Vegan protein bar offer built around assortment, pack count, and repeat-use value.",
            "family_lifestyle_snack_bar": "Everyday vegan protein snack for busy routines, family pantry use, or on-the-go occasions.",
        }
        return protein_bar_mapping.get(
            positioning_territory,
            "Vegan protein bar offer without a sharply differentiated positioning story.",
        )
    if context.query_family == "hydration":
        hydration_mapping = {
            "clean_daily_hydration": "Electrolyte hydration framed around clean daily wellness and ingredient trust.",
            "performance_electrolyte": "Hydration positioned around sports, sweat replacement, recovery, and electrolyte performance.",
            "zero_sugar_hydration": "Hydration offer built around electrolyte support without sugar or excess calories.",
            "travel_stick_pack": "Portable hydration positioned around stick packs, packets, and on-the-go use.",
            "family_variety_hydration": "Hydration assortment framed around approachable flavors, variety, and household use.",
        }
        return hydration_mapping.get(positioning_territory, "Hydration offer without a sharply differentiated positioning story.")
    if context.query_family == "protein_powder":
        protein_powder_mapping = {
            "clean_plant_protein_powder": "Protein powder framed around plant-based or clean-label nutrition.",
            "performance_muscle_protein": "Protein powder positioned around training, muscle, strength, and recovery.",
            "meal_replacement_protein": "Protein powder sold as a shake, meal bridge, or daily nutrition routine.",
            "value_bulk_protein": "Protein powder offer built around bulk size, serving count, and repeat-use value.",
            "flavor_lifestyle_protein": "Protein powder differentiated by flavor, smoothie fit, or lifestyle habit cues.",
        }
        return protein_powder_mapping.get(positioning_territory, "Protein powder offer without a sharply differentiated positioning story.")
    if context.query_family == "energy_drink":
        energy_mapping = {
            "zero_sugar_energy_drink": "Energy drink positioned around caffeine with zero sugar or lower-calorie credentials.",
            "performance_energy_drink": "Energy drink framed around training, endurance, or pre-workout occasions.",
            "clean_natural_energy": "Energy drink built around cleaner caffeine sources and natural ingredient cues.",
            "focus_functional_energy": "Energy drink positioned around focus, mental energy, or added functional benefits.",
            "flavor_variety_energy": "Energy drink offer built around flavors, variety packs, and repeat purchase.",
        }
        return energy_mapping.get(positioning_territory, "Energy drink offer without a sharply differentiated positioning story.")
    mapping = {
        "value_pantry_basics": "Affordable everyday sugar basics for routine pantry replenishment.",
        "convenience_beverage_station": "Convenient multi-format sweetening for coffee, tea, guests, and shared spaces.",
        "premium_pantry_basics": "Higher-priced pantry sugar framed as a dependable baking and beverage staple.",
        "pantry_classic": "Classic pantry sugar positioned around everyday kitchen reliability.",
        "health_forward_alternative": "Cleaner-label or lower-guilt sweetening positioned as a healthier alternative.",
        "hospitality_bulk_solution": "Large-count sugar restock built for frequent beverage service and shared use.",
    }
    return mapping.get(positioning_territory, "General-purpose household sugar without a sharply differentiated story.")


def _tone_of_voice(message_architecture: str) -> str:
    mapping = {
        "value_led": "plainspoken_value",
        "convenience_led": "service_oriented",
        "health_led": "wellness_reassuring",
        "pantry_classic": "household_practical",
    }
    return mapping.get(message_architecture, "utilitarian")


def _pricing_stance(positioning_record: dict[str, object]) -> str:
    price_tier = _coerce_optional_string(positioning_record.get("price_tier"))
    if price_tier == "budget":
        return "budget_anchor"
    if price_tier == "mid":
        return "mid_market"
    if price_tier == "premium":
        return "premium_pantry"
    return "unknown"


def _visual_story(
    positioning_territory: str,
    visual_signal: VisualBrandSignalsRecord,
    context: BrandProfileContext,
) -> str:
    if context.query_family == "candy":
        if visual_signal.promotional_stack == "full_story_stack":
            return "Uses packaging, promotional media, and video together to sell a candy-specific flavor and lifestyle story."
        if positioning_territory == "sharing_variety_pack":
            return "Leans on assortment visuals, pack count, and flavor variety to sell shareability and repeat snacking."
        if positioning_territory == "premium_indulgence_candy":
            return "Uses pack shots and richer flavor cues to signal a more indulgent zero-sugar treat."
        if positioning_territory == "value_multi_pack_candy":
            return "Uses straightforward packaging and count/value cues to emphasize affordability and everyday stocking."
        return "Relies primarily on familiar candy packaging and functional copy to communicate the zero-sugar offer."
    if context.query_family == "protein_bar":
        if visual_signal.promotional_stack == "full_story_stack":
            return "Uses packaging, promotional media, and video together to sell a vegan protein-bar nutrition and flavor story."
        if positioning_territory == "indulgent_snack_bar":
            return "Leans on flavor and treat cues to make protein feel like a snack reward."
        if positioning_territory == "functional_performance_bar":
            return "Uses functional benefit and macro cues to support performance or energy positioning."
        if positioning_territory == "value_variety_protein_bar":
            return "Uses assortment, pack-count, and flavor variety cues to support repeat purchase."
        return "Relies on clean-label and dietary-compatibility copy to communicate the vegan protein offer."
    if context.query_family == "hydration":
        if visual_signal.promotional_stack == "full_story_stack":
            return "Uses packaging, promotional media, and video together to sell an electrolyte hydration routine."
        if positioning_territory == "performance_electrolyte":
            return "Uses electrolyte, sweat, sport, and recovery cues to support performance hydration."
        if positioning_territory == "travel_stick_pack":
            return "Leans on stick-pack and packet cues to make hydration feel portable and routine-friendly."
        if positioning_territory == "family_variety_hydration":
            return "Uses flavor and assortment cues to make electrolyte hydration approachable for households."
        return "Relies on clean hydration and electrolyte copy to communicate daily wellness use."
    if context.query_family == "protein_powder":
        if visual_signal.promotional_stack == "full_story_stack":
            return "Uses packaging, promotional media, and video together to sell a protein powder nutrition routine."
        if positioning_territory == "performance_muscle_protein":
            return "Uses macro, training, recovery, and muscle cues to support performance positioning."
        if positioning_territory == "value_bulk_protein":
            return "Uses tub size, serving count, and bulk cues to support repeat-use value."
        if positioning_territory == "meal_replacement_protein":
            return "Frames protein as a shake or daily nutrition bridge rather than a narrow workout supplement."
        return "Relies on ingredient, flavor, and usage cues to communicate protein powder fit."
    if context.query_family == "energy_drink":
        if visual_signal.promotional_stack == "full_story_stack":
            return "Uses packaging, promotional media, and video together to sell an energy drink occasion."
        if positioning_territory == "focus_functional_energy":
            return "Uses focus, mental energy, or functional-benefit cues to move beyond basic caffeine."
        if positioning_territory == "performance_energy_drink":
            return "Uses training, performance, and pre-workout cues to frame energy around activity."
        if positioning_territory == "clean_natural_energy":
            return "Uses cleaner caffeine and ingredient cues to soften mainstream energy drink signals."
        return "Relies on flavor, sugar, caffeine, and pack cues to communicate the energy drink offer."
    if visual_signal.promotional_stack == "full_story_stack":
        return "Uses packaging, promotional media, and video together to reinforce a clear brand story."
    if positioning_territory == "convenience_beverage_station":
        return "Uses packet-heavy merchandising and video proof to sell service convenience and assortment."
    if positioning_territory == "premium_pantry_basics":
        return "Relies on classic pantry pack shots and functional kitchen cues more than brand-world storytelling."
    if positioning_territory == "value_pantry_basics":
        return "Leans on straightforward pantry packaging with supporting promo assets to reinforce affordability."
    return "Relies primarily on core packaging and functional copy to communicate the offer."


def _proof_points(
    product_record: dict[str, object],
    positioning_record: dict[str, object],
    visual_signal: VisualBrandSignalsRecord,
) -> list[str]:
    candidates: list[str] = []
    title = _coerce_optional_string(product_record.get("title"))
    if title is not None:
        candidates.append(title)
    candidates.extend(_coerce_string_list(product_record.get("description_bullets"))[:2])
    if visual_signal.package_format != "unknown":
        candidates.append(f"{visual_signal.package_format} format")
    if visual_signal.pack_configuration != "single_unit":
        candidates.append(visual_signal.pack_configuration.replace("_", " "))
    candidates.extend(_primary_claims(positioning_record)[:2])
    return _dedupe_preserve_order(candidates)[:5]


def _looks_like_premium_indulgence(
    positioning_record: dict[str, object],
    visual_signal: VisualBrandSignalsRecord,
) -> bool:
    claims = set(_primary_claims(positioning_record))
    indulgence_markers = {"chocolate", "caramel", "mint", "sea salt", "vanilla", "gourmet"}
    if claims.intersection(indulgence_markers):
        return True
    evidence = " ".join(visual_signal.evidence).lower()
    return any(marker in evidence for marker in indulgence_markers)


def _primary_claims(positioning_record: dict[str, object]) -> list[str]:
    claims = positioning_record.get("claim_signals")
    if not isinstance(claims, list):
        return []
    return [str(claim) for claim in claims if str(claim).strip()][:5]


def _crowded_territories(territory_counts: Counter[str]) -> list[str]:
    return [f"{territory} ({count})" for territory, count in territory_counts.items() if count >= 2]


def _profile_coverage_territories(profile: BrandProfileRecord) -> list[str]:
    return _dedupe_preserve_order([profile.positioning_territory, *profile.secondary_territories])


def _underrepresented_spaces(territory_counts: Counter[str], context: BrandProfileContext) -> list[str]:
    if context.query_family == "candy":
        territory_order = CANDY_TERRITORY_ORDER
        messages = CANDY_UNDERREPRESENTED_SPACE_MESSAGES
    elif context.query_family == "protein_bar":
        territory_order = PROTEIN_BAR_TERRITORY_ORDER
        messages = PROTEIN_BAR_UNDERREPRESENTED_SPACE_MESSAGES
    elif context.query_family == "hydration":
        territory_order = HYDRATION_TERRITORY_ORDER
        messages = HYDRATION_UNDERREPRESENTED_SPACE_MESSAGES
    elif context.query_family == "protein_powder":
        territory_order = PROTEIN_POWDER_TERRITORY_ORDER
        messages = PROTEIN_POWDER_UNDERREPRESENTED_SPACE_MESSAGES
    elif context.query_family == "energy_drink":
        territory_order = ENERGY_DRINK_TERRITORY_ORDER
        messages = ENERGY_DRINK_UNDERREPRESENTED_SPACE_MESSAGES
    else:
        territory_order = DEFAULT_TERRITORY_ORDER
        messages = UNDERREPRESENTED_SPACE_MESSAGES
    spaces: list[str] = []
    for territory in territory_order:
        if territory not in territory_counts:
            spaces.append(messages[territory])
    return spaces


def _report_caveats(
    profiles: list[BrandProfileRecord],
    visual_signals: list[VisualBrandSignalsRecord],
) -> list[str]:
    caveats: list[str] = []
    currency_missing = sum(1 for profile in profiles if "currency missing from upstream product intelligence" in profile.warnings)
    promo_missing = sum(1 for signal in visual_signals if "promotional content missing for richer visual signal analysis" in signal.warnings)
    if currency_missing:
        caveats.append(f"{currency_missing} profiles are missing currency, so pricing stance is relative only.")
    if promo_missing:
        caveats.append(f"{promo_missing} profiles rely mostly on packaging plus copy because promotional content is missing.")
    caveats.append("This market map is directional only and should not be treated as validated whitespace without demand grounding.")
    return caveats


def _normalized_text_pool(
    product_record: dict[str, object],
    positioning_record: dict[str, object],
) -> str:
    parts: list[str] = []
    for value in (
        product_record.get("title"),
        product_record.get("brand"),
        positioning_record.get("brand_name"),
    ):
        text = _coerce_optional_string(value)
        if text is not None:
            parts.append(text)
    parts.extend(_coerce_string_list(product_record.get("description_bullets")))
    promotional_content = product_record.get("promotional_content")
    if isinstance(promotional_content, list):
        for block in promotional_content:
            if not isinstance(block, dict):
                continue
            title = _coerce_optional_string(block.get("title"))
            if title is not None:
                parts.append(title)
    return _normalize_text(" ".join(parts))


def _coerce_signal_evidence(positioning_record: dict[str, object]) -> list[str]:
    evidence = positioning_record.get("evidence")
    return _coerce_string_list(evidence)


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", value.lower())


def _profile_text(positioning_record: dict[str, object], visual_signal: VisualBrandSignalsRecord) -> str:
    parts = list(visual_signal.evidence)
    parts.extend(_primary_claims(positioning_record))
    parts.extend(_coerce_string_list(positioning_record.get("usage_contexts")))
    return _normalize_text(" ".join(parts))


def _contains_any(text: str, markers: set[str]) -> bool:
    return any(_normalize_text(marker) in text for marker in markers)


def _coerce_optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for item in value:
        text = _coerce_optional_string(item)
        if text is not None:
            items.append(text)
    return items


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        deduped.append(item)
        seen.add(item)
    return deduped


def _infer_run_id(
    product_intelligence_records: list[dict[str, object]],
    brand_positioning_records: list[dict[str, object]],
    collection_dir: Path,
) -> str:
    for record in product_intelligence_records:
        snapshots = record.get("source_snapshots")
        if isinstance(snapshots, dict) and snapshots.get("collection_run_id"):
            return str(snapshots["collection_run_id"])
    if brand_positioning_records:
        run_id = brand_positioning_records[0].get("run_id")
        if isinstance(run_id, str) and run_id.strip():
            return run_id
    return collection_dir.name


def _load_category_context(collection_dir: Path) -> BrandProfileContext:
    selection_report_path = collection_dir / "selection_report.json"
    if selection_report_path.exists():
        payload = json.loads(selection_report_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            query_family = _coerce_optional_string(payload.get("query_family"))
            return BrandProfileContext(query_family=query_family)

    data_collection_report_path = collection_dir / "data_collection_report.json"
    if data_collection_report_path.exists():
        payload = json.loads(data_collection_report_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            selection = payload.get("selection")
            if isinstance(selection, dict):
                query_family = _coerce_optional_string(selection.get("query_family"))
                return BrandProfileContext(query_family=query_family)

    return BrandProfileContext()


def _load_json_list(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"expected list JSON payload in {path}")
    return [item for item in payload if isinstance(item, dict)]


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|")
