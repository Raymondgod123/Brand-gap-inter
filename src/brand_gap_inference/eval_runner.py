from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from .contracts import validate_document
from .run_metadata import RunManifest, RunTaskEnvelope


@dataclass(frozen=True)
class FixtureBundle:
    normalized_listings: list[dict]
    taxonomy_assignments: list[dict]
    opportunities: list[dict]
    run_manifest: dict
    task_envelopes: list[dict]


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_fixture_dir(fixtures_dir: Path) -> FixtureBundle:
    return FixtureBundle(
        normalized_listings=list(load_json(fixtures_dir / "normalized_listings.json")),
        taxonomy_assignments=list(load_json(fixtures_dir / "taxonomy_assignments.json")),
        opportunities=list(load_json(fixtures_dir / "opportunities.json")),
        run_manifest=dict(load_json(fixtures_dir / "run_manifest.json")),
        task_envelopes=list(load_json(fixtures_dir / "task_envelopes.json")),
    )


def load_thresholds(path: Path) -> dict:
    return dict(load_json(path))


def evaluate_fixture_dir(fixtures_dir: Path, thresholds_path: Path) -> dict:
    bundle = load_fixture_dir(fixtures_dir)
    thresholds = load_thresholds(thresholds_path)
    return evaluate_bundle(bundle, thresholds)


def evaluate_bundle(bundle: FixtureBundle, thresholds: dict) -> dict:
    failures: list[str] = []

    valid_listings = 0
    for index, listing in enumerate(bundle.normalized_listings):
        issues = validate_document("normalized_listing", listing)
        if issues:
            failures.extend(_format_issues("normalized_listings", index, issues))
        else:
            valid_listings += 1

    valid_taxonomy = 0
    for index, assignment in enumerate(bundle.taxonomy_assignments):
        issues = validate_document("taxonomy_assignment", assignment)
        if issues:
            failures.extend(_format_issues("taxonomy_assignments", index, issues))
        else:
            valid_taxonomy += 1

    valid_opportunities = 0
    evidence_bound_opportunities = 0
    known_listing_ids = {listing["listing_id"] for listing in bundle.normalized_listings}
    known_source_record_ids = {listing["source_record_id"] for listing in bundle.normalized_listings}
    assigned_listing_ids = {assignment["listing_id"] for assignment in bundle.taxonomy_assignments}

    for index, opportunity in enumerate(bundle.opportunities):
        issues = validate_document("opportunity", opportunity)
        if issues:
            failures.extend(_format_issues("opportunities", index, issues))
            continue

        valid_opportunities += 1
        evidence_ok = True
        for evidence_index, evidence in enumerate(opportunity["evidence"]):
            evidence_issues = validate_document("evidence", evidence)
            if evidence_issues:
                evidence_ok = False
                failures.extend(_format_issues(f"opportunities[{index}].evidence", evidence_index, evidence_issues))
                continue
            for source_record_id in evidence["source_record_ids"]:
                if source_record_id not in known_source_record_ids:
                    evidence_ok = False
                    failures.append(
                        f"opportunities[{index}].evidence[{evidence_index}] references unknown source_record_id {source_record_id!r}"
                    )
        if evidence_ok:
            evidence_bound_opportunities += 1

    run_metadata_units = 0
    run_metadata_valid = 0

    run_metadata_units += 1
    run_manifest_issues = validate_document("run_manifest", bundle.run_manifest)
    if run_manifest_issues:
        failures.extend(_format_issues("run_manifest", None, run_manifest_issues))
    else:
        RunManifest.from_dict(bundle.run_manifest)
        run_metadata_valid += 1

    task_ids = {task["task_id"] for task in bundle.task_envelopes}
    dependency_integrity_hits = 0
    for index, task in enumerate(bundle.task_envelopes):
        run_metadata_units += 1
        task_issues = validate_document("run_task_envelope", task)
        if task_issues:
            failures.extend(_format_issues("task_envelopes", index, task_issues))
            continue

        RunTaskEnvelope.from_dict(task)
        run_metadata_valid += 1
        dependencies = task["dependencies"]
        if task["run_id"] != bundle.run_manifest["run_id"]:
            failures.append(f"task_envelopes[{index}].run_id does not match run_manifest.run_id")
            continue
        if any(dependency == task["task_id"] for dependency in dependencies):
            failures.append(f"task_envelopes[{index}] cannot depend on itself")
            continue
        unknown_dependencies = [dependency for dependency in dependencies if dependency not in task_ids]
        if unknown_dependencies:
            failures.append(
                f"task_envelopes[{index}] references unknown dependencies {unknown_dependencies!r}"
            )
            continue
        dependency_integrity_hits += 1

    taxonomy_coverage_hits = sum(1 for listing_id in known_listing_ids if listing_id in assigned_listing_ids)

    metrics = {
        "listing_validation_pass_rate": _safe_ratio(valid_listings, len(bundle.normalized_listings)),
        "taxonomy_validation_pass_rate": _safe_ratio(valid_taxonomy, len(bundle.taxonomy_assignments)),
        "taxonomy_assignment_coverage": _safe_ratio(taxonomy_coverage_hits, len(bundle.normalized_listings)),
        "opportunity_validation_pass_rate": _safe_ratio(valid_opportunities, len(bundle.opportunities)),
        "opportunity_evidence_coverage": _safe_ratio(evidence_bound_opportunities, len(bundle.opportunities)),
        "run_metadata_pass_rate": _safe_ratio(run_metadata_valid, run_metadata_units),
        "task_dependency_integrity": _safe_ratio(dependency_integrity_hits, len(bundle.task_envelopes)),
    }

    threshold_failures = []
    for metric_name, config in thresholds.get("metrics", {}).items():
        minimum = config.get("minimum")
        if minimum is not None and metrics.get(metric_name, 0.0) < minimum:
            threshold_failures.append(
                f"metric {metric_name}={metrics.get(metric_name, 0.0):.3f} is below minimum {minimum:.3f}"
            )

    failures.extend(threshold_failures)

    return {
        "passed": not failures,
        "metrics": metrics,
        "failures": failures,
    }


def _format_issues(collection_name: str, index: int | None, issues: list) -> list[str]:
    if index is None:
        prefix = collection_name
    else:
        prefix = f"{collection_name}[{index}]"
    return [f"{prefix} {issue.path}: {issue.message}" for issue in issues]


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 4)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the phase 1 contract and eval gate.")
    parser.add_argument("--fixtures-dir", type=Path, required=True)
    parser.add_argument("--thresholds", type=Path, required=True)
    args = parser.parse_args(argv)

    report = evaluate_fixture_dir(args.fixtures_dir, args.thresholds)
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
