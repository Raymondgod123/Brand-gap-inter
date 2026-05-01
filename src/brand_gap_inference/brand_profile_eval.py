from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path

from .brand_profile import BrandProfileBuilder


@dataclass(frozen=True)
class GoldenBrandProfileBatch:
    batch_id: str
    description: str
    run_id: str
    product_intelligence_records: list[dict]
    brand_positioning_records: list[dict]
    expected: dict


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_batches(path: Path) -> list[GoldenBrandProfileBatch]:
    raw_batches = list(load_json(path))
    return [
        GoldenBrandProfileBatch(
            batch_id=batch["batch_id"],
            description=batch["description"],
            run_id=batch["run_id"],
            product_intelligence_records=batch["product_intelligence_records"],
            brand_positioning_records=batch["brand_positioning_records"],
            expected=batch["expected"],
        )
        for batch in raw_batches
    ]


def load_thresholds(path: Path) -> dict:
    return dict(load_json(path))


def evaluate_batches(batches: list[GoldenBrandProfileBatch], thresholds: dict) -> dict:
    failures: list[str] = []
    status_hits = 0
    visual_signal_hits = 0
    territory_hits = 0
    pricing_hits = 0
    market_map_hits = 0
    stability_hits = 0
    total_batches = len(batches)
    batch_results: list[dict] = []

    for batch in batches:
        first_result = BrandProfileBuilder().build(
            run_id=batch.run_id,
            product_intelligence_records=batch.product_intelligence_records,
            brand_positioning_records=batch.brand_positioning_records,
        )
        second_result = BrandProfileBuilder().build(
            run_id=batch.run_id,
            product_intelligence_records=batch.product_intelligence_records,
            brand_positioning_records=batch.brand_positioning_records,
        )
        first_signals = [record.to_dict() for record in first_result.visual_signals]
        second_signals = [record.to_dict() for record in second_result.visual_signals]
        first_profiles = [record.to_dict() for record in first_result.profiles]
        second_profiles = [record.to_dict() for record in second_result.profiles]
        first_report = first_result.to_report_dict()
        second_report = second_result.to_report_dict()

        stable = (
            first_signals == second_signals
            and first_profiles == second_profiles
            and first_report == second_report
        )
        if stable:
            stability_hits += 1
        else:
            failures.append(f"{batch.batch_id} brand profile output is unstable across repeat runs")

        expected = batch.expected
        if first_report["status"] == expected.get("status"):
            status_hits += 1
        else:
            failures.append(
                f"{batch.batch_id} status mismatch: expected {expected.get('status')!r}, got {first_report['status']!r}"
            )

        if _signals_match(first_signals, expected.get("signals", {}), failures, batch.batch_id):
            visual_signal_hits += 1

        if _profiles_match(first_profiles, expected.get("profiles", {}), failures, batch.batch_id):
            territory_hits += 1

        if _pricing_match(first_profiles, expected.get("pricing_stances", {}), failures, batch.batch_id):
            pricing_hits += 1

        if _market_map_matches(first_report, expected.get("underrepresented_fragments", []), failures, batch.batch_id):
            market_map_hits += 1

        batch_results.append(
            {
                "batch_id": batch.batch_id,
                "status": first_report["status"],
                "stable": stable,
                "total_profiles": first_report["total_profiles"],
            }
        )

    metrics = {
        "brand_profile_status_accuracy": _safe_ratio(status_hits, total_batches),
        "brand_profile_visual_signal_accuracy": _safe_ratio(visual_signal_hits, total_batches),
        "brand_profile_territory_accuracy": _safe_ratio(territory_hits, total_batches),
        "brand_profile_pricing_accuracy": _safe_ratio(pricing_hits, total_batches),
        "brand_profile_market_map_accuracy": _safe_ratio(market_map_hits, total_batches),
        "brand_profile_repeat_run_stability": _safe_ratio(stability_hits, total_batches),
    }
    failures.extend(_evaluate_thresholds(metrics, thresholds))
    return {"passed": not failures, "metrics": metrics, "failures": failures, "batches": batch_results}


def _signals_match(records: list[dict], expectations: dict, failures: list[str], batch_id: str) -> bool:
    by_asin = {record["asin"]: record for record in records}
    matched = True
    for asin, fields in expectations.items():
        record = by_asin.get(asin)
        if record is None:
            failures.append(f"{batch_id}:{asin} missing from visual brand signals output")
            matched = False
            continue
        for field_name, expected_value in fields.items():
            if record.get(field_name) != expected_value:
                failures.append(
                    f"{batch_id}:{asin}:{field_name} mismatch: expected {expected_value!r}, got {record.get(field_name)!r}"
                )
                matched = False
    return matched


def _profiles_match(records: list[dict], expectations: dict, failures: list[str], batch_id: str) -> bool:
    by_asin = {record["asin"]: record for record in records}
    matched = True
    for asin, expected_territory in expectations.items():
        record = by_asin.get(asin)
        actual_territory = record.get("positioning_territory") if record else None
        if actual_territory != expected_territory:
            failures.append(
                f"{batch_id}:{asin} territory mismatch: expected {expected_territory!r}, got {actual_territory!r}"
            )
            matched = False
    return matched


def _pricing_match(records: list[dict], expectations: dict, failures: list[str], batch_id: str) -> bool:
    by_asin = {record["asin"]: record for record in records}
    matched = True
    for asin, expected_pricing in expectations.items():
        record = by_asin.get(asin)
        actual_pricing = record.get("pricing_stance") if record else None
        if actual_pricing != expected_pricing:
            failures.append(
                f"{batch_id}:{asin} pricing stance mismatch: expected {expected_pricing!r}, got {actual_pricing!r}"
            )
            matched = False
    return matched


def _market_map_matches(report: dict, expected_fragments: list[str], failures: list[str], batch_id: str) -> bool:
    spaces = report.get("underrepresented_spaces", [])
    if not isinstance(spaces, list):
        spaces = []
    matched = True
    for fragment in expected_fragments:
        if not any(fragment.lower() in str(item).lower() for item in spaces):
            failures.append(f"{batch_id} missing underrepresented-space fragment {fragment!r}")
            matched = False
    return matched


def _evaluate_thresholds(metrics: dict[str, float], thresholds: dict) -> list[str]:
    failures: list[str] = []
    for metric_name, config in thresholds.get("metrics", {}).items():
        value = metrics.get(metric_name, 0.0)
        minimum = config.get("minimum")
        maximum = config.get("maximum")
        if minimum is not None and value < minimum:
            failures.append(f"metric {metric_name}={value:.3f} is below minimum {minimum:.3f}")
        if maximum is not None and value > maximum:
            failures.append(f"metric {metric_name}={value:.3f} is above maximum {maximum:.3f}")
    return failures


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 4)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run brand-profile and market-map evals.")
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--thresholds", type=Path, required=True)
    args = parser.parse_args(argv)

    report = evaluate_batches(load_batches(args.cases), load_thresholds(args.thresholds))
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
