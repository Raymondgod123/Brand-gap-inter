from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from .taxonomy import TaxonomyAssigner


@dataclass(frozen=True)
class GoldenTaxonomyCase:
    case_id: str
    description: str
    listing: dict
    expected: dict


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_cases(path: Path) -> list[GoldenTaxonomyCase]:
    raw_cases = list(load_json(path))
    return [
        GoldenTaxonomyCase(
            case_id=case["case_id"],
            description=case["description"],
            listing=case["listing"],
            expected=case["expected"],
        )
        for case in raw_cases
    ]


def load_thresholds(path: Path) -> dict:
    return dict(load_json(path))


def evaluate_cases(cases: list[GoldenTaxonomyCase], thresholds: dict) -> dict:
    assigner = TaxonomyAssigner()
    failures: list[str] = []
    axis_names = ("need_state", "occasion", "format", "audience")
    case_hits = 0
    warning_expectation_hits = 0
    invalid_count = 0
    stability_hits = 0
    axis_hits = {axis_name: 0 for axis_name in axis_names}
    case_results: list[dict] = []

    for case in cases:
        first_run = assigner.assign_batch([case.listing], snapshot_id=f"eval-{case.case_id}", assigned_at="2026-04-22T00:00:00Z")
        second_run = assigner.assign_batch([case.listing], snapshot_id=f"eval-{case.case_id}", assigned_at="2026-04-22T00:00:00Z")

        first_record = first_run.records[0]
        second_record = second_run.records[0]

        if first_record.status != "assigned":
            invalid_count += 1
            failures.append(f"{case.case_id} did not receive a taxonomy assignment")
            case_results.append(
                {
                    "case_id": case.case_id,
                    "status": "invalid",
                    "issues": [issue.message for issue in first_record.issues],
                }
            )
            continue

        assignment = first_run.assignments[0]
        expected_axes = case.expected["axes"]
        actual_axes = assignment["axes"]
        exact_match = actual_axes == expected_axes
        if exact_match:
            case_hits += 1
        else:
            failures.append(f"{case.case_id} axes mismatch: expected {expected_axes!r}, got {actual_axes!r}")

        for axis_name in axis_names:
            if actual_axes[axis_name] == expected_axes[axis_name]:
                axis_hits[axis_name] += 1

        warning_count = len(first_record.warnings)
        expected_warning_count = case.expected.get("warning_count")
        if expected_warning_count is None or warning_count == expected_warning_count:
            warning_expectation_hits += 1
        else:
            failures.append(
                f"{case.case_id} warning count mismatch: expected {expected_warning_count}, got {warning_count}"
            )

        min_confidence = case.expected.get("min_confidence")
        if min_confidence is not None and assignment["confidence"] < min_confidence:
            failures.append(
                f"{case.case_id} confidence {assignment['confidence']:.2f} is below minimum {min_confidence:.2f}"
            )

        max_confidence = case.expected.get("max_confidence")
        if max_confidence is not None and assignment["confidence"] > max_confidence:
            failures.append(
                f"{case.case_id} confidence {assignment['confidence']:.2f} exceeds maximum {max_confidence:.2f}"
            )

        if assignment == second_run.assignments[0]:
            stability_hits += 1
        else:
            failures.append(f"{case.case_id} assignment is unstable across repeat runs")

        case_results.append(
            {
                "case_id": case.case_id,
                "status": "assigned",
                "exact_match": exact_match,
                "confidence": assignment["confidence"],
                "warning_count": warning_count,
                "axes": actual_axes,
            }
        )

    total_cases = len(cases)
    metrics = {
        "taxonomy_case_accuracy": _safe_ratio(case_hits, total_cases),
        "taxonomy_axis_accuracy_need_state": _safe_ratio(axis_hits["need_state"], total_cases),
        "taxonomy_axis_accuracy_occasion": _safe_ratio(axis_hits["occasion"], total_cases),
        "taxonomy_axis_accuracy_format": _safe_ratio(axis_hits["format"], total_cases),
        "taxonomy_axis_accuracy_audience": _safe_ratio(axis_hits["audience"], total_cases),
        "taxonomy_invalid_assignment_rate": _safe_ratio(invalid_count, total_cases),
        "taxonomy_warning_expectation_pass_rate": _safe_ratio(warning_expectation_hits, total_cases),
        "taxonomy_repeat_run_stability": _safe_ratio(stability_hits, total_cases),
    }

    threshold_failures = _evaluate_thresholds(metrics, thresholds)
    failures.extend(threshold_failures)

    return {
        "passed": not failures,
        "metrics": metrics,
        "failures": failures,
        "cases": case_results,
    }


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
    parser = argparse.ArgumentParser(description="Run taxonomy quality evals against golden labeled fixtures.")
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--thresholds", type=Path, required=True)
    args = parser.parse_args(argv)

    report = evaluate_cases(load_cases(args.cases), load_thresholds(args.thresholds))
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
