from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path

from .gap_validation import GapValidationBuilder


@dataclass(frozen=True)
class GoldenGapValidationBatch:
    batch_id: str
    description: str
    run_id: str
    product_intelligence_records: list[dict]
    brand_profile_records: list[dict]
    brand_profile_report: dict
    expected: dict


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_batches(path: Path) -> list[GoldenGapValidationBatch]:
    raw_batches = list(load_json(path))
    return [
        GoldenGapValidationBatch(
            batch_id=batch["batch_id"],
            description=batch["description"],
            run_id=batch["run_id"],
            product_intelligence_records=batch["product_intelligence_records"],
            brand_profile_records=batch["brand_profile_records"],
            brand_profile_report=batch["brand_profile_report"],
            expected=batch["expected"],
        )
        for batch in raw_batches
    ]


def load_thresholds(path: Path) -> dict:
    return dict(load_json(path))


def evaluate_batches(batches: list[GoldenGapValidationBatch], thresholds: dict) -> dict:
    failures: list[str] = []
    status_hits = 0
    top_candidate_hits = 0
    supported_count_hits = 0
    market_space_hits = 0
    stability_hits = 0
    total_batches = len(batches)
    batch_results: list[dict] = []

    for batch in batches:
        first_result = GapValidationBuilder().build(
            run_id=batch.run_id,
            product_intelligence_records=batch.product_intelligence_records,
            brand_profile_records=batch.brand_profile_records,
            brand_profile_report=batch.brand_profile_report,
        )
        second_result = GapValidationBuilder().build(
            run_id=batch.run_id,
            product_intelligence_records=batch.product_intelligence_records,
            brand_profile_records=batch.brand_profile_records,
            brand_profile_report=batch.brand_profile_report,
        )
        first_records = [record.to_dict() for record in first_result.records]
        second_records = [record.to_dict() for record in second_result.records]
        first_report = first_result.to_report_dict()
        second_report = second_result.to_report_dict()

        stable = first_records == second_records and first_report == second_report
        if stable:
            stability_hits += 1
        else:
            failures.append(f"{batch.batch_id} gap validation output is unstable across repeat runs")

        expected = batch.expected
        if first_report["status"] == expected.get("status"):
            status_hits += 1
        else:
            failures.append(
                f"{batch.batch_id} status mismatch: expected {expected.get('status')!r}, got {first_report['status']!r}"
            )

        if _top_candidate_matches(first_report, expected.get("top_candidate", {}), failures, batch.batch_id):
            top_candidate_hits += 1

        if first_report["supported_candidates"] == expected.get("supported_candidates"):
            supported_count_hits += 1
        else:
            failures.append(
                f"{batch.batch_id} supported candidate count mismatch: expected {expected.get('supported_candidates')!r}, got {first_report['supported_candidates']!r}"
            )

        if _market_space_matches(first_report, expected.get("candidate_space_fragment"), failures, batch.batch_id):
            market_space_hits += 1

        batch_results.append(
            {
                "batch_id": batch.batch_id,
                "status": first_report["status"],
                "supported_candidates": first_report["supported_candidates"],
                "stable": stable,
            }
        )

    metrics = {
        "gap_validation_status_accuracy": _safe_ratio(status_hits, total_batches),
        "gap_validation_top_candidate_accuracy": _safe_ratio(top_candidate_hits, total_batches),
        "gap_validation_supported_count_accuracy": _safe_ratio(supported_count_hits, total_batches),
        "gap_validation_market_space_accuracy": _safe_ratio(market_space_hits, total_batches),
        "gap_validation_repeat_run_stability": _safe_ratio(stability_hits, total_batches),
    }
    failures.extend(_evaluate_thresholds(metrics, thresholds))
    return {"passed": not failures, "metrics": metrics, "failures": failures, "batches": batch_results}


def _top_candidate_matches(report: dict, expectation: dict, failures: list[str], batch_id: str) -> bool:
    top_candidates = report.get("top_candidates", [])
    if not isinstance(top_candidates, list) or not top_candidates:
        failures.append(f"{batch_id} missing top candidates")
        return False
    top_candidate = top_candidates[0]
    if not isinstance(top_candidate, dict):
        failures.append(f"{batch_id} top candidate is not an object")
        return False
    matched = True
    for field_name, expected_value in expectation.items():
        if top_candidate.get(field_name) != expected_value:
            failures.append(
                f"{batch_id} top candidate {field_name} mismatch: expected {expected_value!r}, got {top_candidate.get(field_name)!r}"
            )
            matched = False
    return matched


def _market_space_matches(report: dict, fragment: object, failures: list[str], batch_id: str) -> bool:
    if not isinstance(fragment, str):
        return True
    records = report.get("records", [])
    if not isinstance(records, list):
        records = []
    candidate_text = " ".join(
        f"{record.get('candidate_space', '')} {record.get('title', '')}"
        for record in records
        if isinstance(record, dict)
    )
    if fragment.lower() not in candidate_text.lower():
        failures.append(f"{batch_id} missing candidate-space fragment {fragment!r}")
        return False
    return True


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
    parser = argparse.ArgumentParser(description="Run gap-validation evals.")
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--thresholds", type=Path, required=True)
    args = parser.parse_args(argv)

    report = evaluate_batches(load_batches(args.cases), load_thresholds(args.thresholds))
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
