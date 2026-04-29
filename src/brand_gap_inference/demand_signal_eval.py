from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path

from .demand_signals import DemandSignalBuilder


@dataclass(frozen=True)
class GoldenDemandSignalBatch:
    batch_id: str
    description: str
    run_id: str
    discovery_records: list[dict]
    brand_profile_report: dict
    expected: dict


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_batches(path: Path) -> list[GoldenDemandSignalBatch]:
    raw_batches = list(load_json(path))
    return [
        GoldenDemandSignalBatch(
            batch_id=batch["batch_id"],
            description=batch["description"],
            run_id=batch["run_id"],
            discovery_records=batch["discovery_records"],
            brand_profile_report=batch["brand_profile_report"],
            expected=batch["expected"],
        )
        for batch in raw_batches
    ]


def load_thresholds(path: Path) -> dict:
    return dict(load_json(path))


def evaluate_batches(batches: list[GoldenDemandSignalBatch], thresholds: dict) -> dict:
    failures: list[str] = []
    status_hits = 0
    signal_hits = 0
    top_rank_hits = 0
    stability_hits = 0
    total_batches = len(batches)
    batch_results: list[dict] = []

    for batch in batches:
        first_result = DemandSignalBuilder().build(
            run_id=batch.run_id,
            discovery_records=batch.discovery_records,
            brand_profile_report=batch.brand_profile_report,
        )
        second_result = DemandSignalBuilder().build(
            run_id=batch.run_id,
            discovery_records=batch.discovery_records,
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
            failures.append(f"{batch.batch_id} demand signal output is unstable across repeat runs")

        expected = batch.expected
        if first_report["status"] == expected.get("status"):
            status_hits += 1
        else:
            failures.append(
                f"{batch.batch_id} status mismatch: expected {expected.get('status')!r}, got {first_report['status']!r}"
            )

        if _signals_match(first_records, expected.get("signals", {}), failures, batch.batch_id):
            signal_hits += 1

        if _top_ranks_match(first_records, expected.get("top_ranks", {}), failures, batch.batch_id):
            top_rank_hits += 1

        batch_results.append(
            {
                "batch_id": batch.batch_id,
                "status": first_report["status"],
                "stable": stable,
                "valid_discovery_count": first_report["valid_discovery_count"],
                "total_signals": first_report["total_signals"],
            }
        )

    metrics = {
        "demand_signal_status_accuracy": _safe_ratio(status_hits, total_batches),
        "demand_signal_score_accuracy": _safe_ratio(signal_hits, total_batches),
        "demand_signal_top_rank_accuracy": _safe_ratio(top_rank_hits, total_batches),
        "demand_signal_repeat_run_stability": _safe_ratio(stability_hits, total_batches),
    }
    failures.extend(_evaluate_thresholds(metrics, thresholds))
    return {"passed": not failures, "metrics": metrics, "failures": failures, "batches": batch_results}


def _signals_match(records: list[dict], expectations: dict, failures: list[str], batch_id: str) -> bool:
    by_territory = {record["target_territory"]: record for record in records}
    matched = True
    for territory, expected in expectations.items():
        record = by_territory.get(territory)
        if record is None:
            failures.append(f"{batch_id}:{territory} missing demand signal")
            matched = False
            continue
        min_score = expected.get("min_score")
        max_score = expected.get("max_score")
        actual_score = record.get("demand_score")
        if min_score is not None and actual_score < min_score:
            failures.append(f"{batch_id}:{territory} demand score {actual_score!r} below {min_score!r}")
            matched = False
        if max_score is not None and actual_score > max_score:
            failures.append(f"{batch_id}:{territory} demand score {actual_score!r} above {max_score!r}")
            matched = False
        expected_match_count = expected.get("match_count")
        if expected_match_count is not None and record.get("match_count") != expected_match_count:
            failures.append(
                f"{batch_id}:{territory} match_count mismatch: expected {expected_match_count!r}, got {record.get('match_count')!r}"
            )
            matched = False
    return matched


def _top_ranks_match(records: list[dict], expectations: dict, failures: list[str], batch_id: str) -> bool:
    by_territory = {record["target_territory"]: record for record in records}
    matched = True
    for territory, expected_rank in expectations.items():
        record = by_territory.get(territory)
        actual_rank = record.get("top_rank") if record else None
        if actual_rank != expected_rank:
            failures.append(
                f"{batch_id}:{territory} top_rank mismatch: expected {expected_rank!r}, got {actual_rank!r}"
            )
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
    parser = argparse.ArgumentParser(description="Run demand signal evals against golden fixtures.")
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--thresholds", type=Path, required=True)
    args = parser.parse_args(argv)

    report = evaluate_batches(load_batches(args.cases), load_thresholds(args.thresholds))
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
