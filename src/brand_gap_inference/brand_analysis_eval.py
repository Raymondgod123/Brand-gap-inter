from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path

from .brand_analysis import BrandPositioningAnalyzer


@dataclass(frozen=True)
class GoldenBrandPositioningBatch:
    batch_id: str
    description: str
    run_id: str
    records: list[dict]
    expected: dict


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_batches(path: Path) -> list[GoldenBrandPositioningBatch]:
    raw_batches = list(load_json(path))
    return [
        GoldenBrandPositioningBatch(
            batch_id=batch["batch_id"],
            description=batch["description"],
            run_id=batch["run_id"],
            records=batch["records"],
            expected=batch["expected"],
        )
        for batch in raw_batches
    ]


def load_thresholds(path: Path) -> dict:
    return dict(load_json(path))


def evaluate_batches(batches: list[GoldenBrandPositioningBatch], thresholds: dict) -> dict:
    failures: list[str] = []
    status_hits = 0
    brand_hits = 0
    archetype_hits = 0
    signal_hits = 0
    theme_hits = 0
    stability_hits = 0
    total_batches = len(batches)
    batch_results: list[dict] = []

    for batch in batches:
        first_result = BrandPositioningAnalyzer().analyze_records(run_id=batch.run_id, records=batch.records)
        second_result = BrandPositioningAnalyzer().analyze_records(run_id=batch.run_id, records=batch.records)
        first_records = [record.to_dict() for record in first_result.records]
        second_records = [record.to_dict() for record in second_result.records]
        first_report = first_result.to_report_dict()
        second_report = second_result.to_report_dict()

        stable = first_records == second_records and first_report == second_report
        if stable:
            stability_hits += 1
        else:
            failures.append(f"{batch.batch_id} brand positioning output is unstable across repeat runs")

        expected = batch.expected
        expected_status = expected.get("status")
        if first_report["status"] == expected_status:
            status_hits += 1
        else:
            failures.append(
                f"{batch.batch_id} status mismatch: expected {expected_status!r}, got {first_report['status']!r}"
            )

        if _brands_match(first_records, expected.get("brands", {}), failures, batch.batch_id):
            brand_hits += 1

        if _archetypes_match(first_records, expected.get("archetypes", {}), failures, batch.batch_id):
            archetype_hits += 1

        if _signals_match(first_records, expected.get("signals", {}), failures, batch.batch_id):
            signal_hits += 1

        if _themes_match(first_report, expected.get("market_theme_fragments", []), failures, batch.batch_id):
            theme_hits += 1

        batch_results.append(
            {
                "batch_id": batch.batch_id,
                "status": first_report["status"],
                "total_products": first_report["total_products"],
                "stable": stable,
            }
        )

    metrics = {
        "brand_positioning_status_accuracy": _safe_ratio(status_hits, total_batches),
        "brand_positioning_brand_normalization_accuracy": _safe_ratio(brand_hits, total_batches),
        "brand_positioning_archetype_accuracy": _safe_ratio(archetype_hits, total_batches),
        "brand_positioning_signal_accuracy": _safe_ratio(signal_hits, total_batches),
        "brand_positioning_market_theme_accuracy": _safe_ratio(theme_hits, total_batches),
        "brand_positioning_repeat_run_stability": _safe_ratio(stability_hits, total_batches),
    }
    failures.extend(_evaluate_thresholds(metrics, thresholds))
    return {"passed": not failures, "metrics": metrics, "failures": failures, "batches": batch_results}


def _brands_match(
    records: list[dict],
    expectations: dict,
    failures: list[str],
    batch_id: str,
) -> bool:
    by_asin = {record["asin"]: record for record in records}
    matched = True
    for asin, expected_brand in expectations.items():
        record = by_asin.get(asin)
        actual_brand = record.get("brand_name") if record else None
        if actual_brand != expected_brand:
            failures.append(
                f"{batch_id}:{asin} normalized brand mismatch: expected {expected_brand!r}, got {actual_brand!r}"
            )
            matched = False
    return matched


def _archetypes_match(
    records: list[dict],
    expectations: dict,
    failures: list[str],
    batch_id: str,
) -> bool:
    by_asin = {record["asin"]: record for record in records}
    matched = True
    for asin, expected_archetype in expectations.items():
        record = by_asin.get(asin)
        actual_archetype = record.get("positioning_archetype") if record else None
        if actual_archetype != expected_archetype:
            failures.append(
                f"{batch_id}:{asin} archetype mismatch: expected {expected_archetype!r}, got {actual_archetype!r}"
            )
            matched = False
    return matched


def _signals_match(
    records: list[dict],
    expectations: dict,
    failures: list[str],
    batch_id: str,
) -> bool:
    by_asin = {record["asin"]: record for record in records}
    matched = True
    for asin, signal_expectations in expectations.items():
        record = by_asin.get(asin)
        if record is None:
            failures.append(f"{batch_id}:{asin} missing from brand positioning output")
            matched = False
            continue
        for field_name, expected_value in signal_expectations.items():
            actual_value = record.get(field_name)
            if actual_value != expected_value:
                failures.append(
                    f"{batch_id}:{asin}:{field_name} mismatch: expected {expected_value!r}, got {actual_value!r}"
                )
                matched = False
    return matched


def _themes_match(report: dict, expected_fragments: list[str], failures: list[str], batch_id: str) -> bool:
    themes = report.get("market_themes", [])
    if not isinstance(themes, list):
        themes = []
    matched = True
    for fragment in expected_fragments:
        if not any(fragment in str(theme) for theme in themes):
            failures.append(f"{batch_id} missing market theme containing {fragment!r}")
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
    parser = argparse.ArgumentParser(description="Run brand-positioning evals.")
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--thresholds", type=Path, required=True)
    args = parser.parse_args(argv)

    report = evaluate_batches(load_batches(args.cases), load_thresholds(args.thresholds))
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
