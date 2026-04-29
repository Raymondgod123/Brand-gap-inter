from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path

from .decision_brief import DecisionBriefBuilder


@dataclass(frozen=True)
class GoldenDecisionBriefBatch:
    batch_id: str
    description: str
    run_id: str
    brand_profile_report: dict[str, object]
    demand_signal_report: dict[str, object]
    gap_validation_report: dict[str, object]
    expected: dict[str, object]


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_batches(path: Path) -> list[GoldenDecisionBriefBatch]:
    raw_batches = list(load_json(path))
    return [
        GoldenDecisionBriefBatch(
            batch_id=batch["batch_id"],
            description=batch["description"],
            run_id=batch["run_id"],
            brand_profile_report=batch["brand_profile_report"],
            demand_signal_report=batch["demand_signal_report"],
            gap_validation_report=batch["gap_validation_report"],
            expected=batch["expected"],
        )
        for batch in raw_batches
    ]


def load_thresholds(path: Path) -> dict:
    return dict(load_json(path))


def evaluate_batches(batches: list[GoldenDecisionBriefBatch], thresholds: dict) -> dict:
    failures: list[str] = []
    status_hits = 0
    recommendation_hits = 0
    top_space_hits = 0
    actionability_hits = 0
    stability_hits = 0
    total_batches = len(batches)
    batch_results: list[dict[str, object]] = []

    for batch in batches:
        builder = DecisionBriefBuilder()
        first_report = builder.build(
            run_id=batch.run_id,
            brand_profile_report=batch.brand_profile_report,
            demand_signal_report=batch.demand_signal_report,
            gap_validation_report=batch.gap_validation_report,
        )
        second_report = builder.build(
            run_id=batch.run_id,
            brand_profile_report=batch.brand_profile_report,
            demand_signal_report=batch.demand_signal_report,
            gap_validation_report=batch.gap_validation_report,
        )
        stable = first_report == second_report
        if stable:
            stability_hits += 1
        else:
            failures.append(f"{batch.batch_id} decision brief output is unstable across repeat runs")

        expected = batch.expected
        if first_report["status"] == expected.get("status"):
            status_hits += 1
        else:
            failures.append(
                f"{batch.batch_id} status mismatch: expected {expected.get('status')!r}, got {first_report['status']!r}"
            )

        if first_report["recommendation_level"] == expected.get("recommendation_level"):
            recommendation_hits += 1
        else:
            failures.append(
                f"{batch.batch_id} recommendation mismatch: expected {expected.get('recommendation_level')!r}, got {first_report['recommendation_level']!r}"
            )

        top_opportunity = first_report.get("top_opportunity", {})
        top_space = top_opportunity.get("candidate_space") if isinstance(top_opportunity, dict) else None
        if top_space == expected.get("top_candidate_space"):
            top_space_hits += 1
        else:
            failures.append(
                f"{batch.batch_id} top candidate space mismatch: expected {expected.get('top_candidate_space')!r}, got {top_space!r}"
            )

        if _has_required_actions(first_report, expected.get("required_next_step_fragments", []), failures, batch.batch_id):
            actionability_hits += 1

        batch_results.append(
            {
                "batch_id": batch.batch_id,
                "status": first_report["status"],
                "recommendation_level": first_report["recommendation_level"],
                "top_candidate_space": top_space,
                "stable": stable,
            }
        )

    metrics = {
        "decision_brief_status_accuracy": _safe_ratio(status_hits, total_batches),
        "decision_brief_recommendation_accuracy": _safe_ratio(recommendation_hits, total_batches),
        "decision_brief_top_space_accuracy": _safe_ratio(top_space_hits, total_batches),
        "decision_brief_actionability_accuracy": _safe_ratio(actionability_hits, total_batches),
        "decision_brief_repeat_run_stability": _safe_ratio(stability_hits, total_batches),
    }
    failures.extend(_evaluate_thresholds(metrics, thresholds))
    return {"passed": not failures, "metrics": metrics, "failures": failures, "batches": batch_results}


def _has_required_actions(report: dict[str, object], fragments: object, failures: list[str], batch_id: str) -> bool:
    if not isinstance(fragments, list):
        fragments = []
    next_steps = report.get("recommended_next_steps", [])
    requirements = report.get("validation_requirements", [])
    combined = " ".join(str(item).lower() for item in [*next_steps, *requirements] if str(item).strip())
    matched = True
    for fragment in fragments:
        text = str(fragment).lower()
        if text not in combined:
            failures.append(f"{batch_id} missing actionability fragment {fragment!r}")
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
    parser = argparse.ArgumentParser(description="Run decision-brief evals.")
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--thresholds", type=Path, required=True)
    args = parser.parse_args(argv)

    report = evaluate_batches(load_batches(args.cases), load_thresholds(args.thresholds))
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
