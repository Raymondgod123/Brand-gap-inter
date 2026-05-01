from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from .connectors import RawSourceRecord
from .discovery import DiscoveryExtractor
from .raw_store import SourceSnapshotManifest


@dataclass(frozen=True)
class GoldenDiscoveryBatch:
    batch_id: str
    description: str
    manifest: dict
    records: list[dict]
    expected: dict


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_batches(path: Path) -> list[GoldenDiscoveryBatch]:
    raw_batches = list(load_json(path))
    return [
        GoldenDiscoveryBatch(
            batch_id=batch["batch_id"],
            description=batch["description"],
            manifest=batch["manifest"],
            records=batch["records"],
            expected=batch["expected"],
        )
        for batch in raw_batches
    ]


def load_thresholds(path: Path) -> dict:
    return dict(load_json(path))


def evaluate_batches(batches: list[GoldenDiscoveryBatch], thresholds: dict) -> dict:
    extractor = DiscoveryExtractor()
    failures: list[str] = []
    run_status_hits = 0
    summary_hits = 0
    status_hits = 0
    required_field_hits = 0
    stability_hits = 0

    total_batches = len(batches)
    total_records = 0
    total_required_field_expectations = 0
    total_summaries = 0
    batch_results: list[dict] = []

    for batch in batches:
        records = [RawSourceRecord.from_dict(item) for item in batch.records]
        manifest = _manifest_from_fixture(batch.manifest, records)
        expected = batch.expected

        first_run = extractor.extract_snapshot(manifest, records)
        second_run = extractor.extract_snapshot(manifest, records)
        stable = _stable_digest(first_run) == _stable_digest(second_run)
        if stable:
            stability_hits += 1
        else:
            failures.append(f"{batch.batch_id} discovery output is unstable across repeat runs")

        expected_run_status = expected.get("run_status")
        if expected_run_status is not None:
            if first_run.summary.run_status == expected_run_status:
                run_status_hits += 1
            else:
                failures.append(
                    f"{batch.batch_id} run_status mismatch: expected {expected_run_status!r}, got {first_run.summary.run_status!r}"
                )

        expected_summary = expected.get("summary")
        if isinstance(expected_summary, dict):
            total_summaries += 1
            if _summary_matches(first_run, expected_summary):
                summary_hits += 1
            else:
                failures.append(f"{batch.batch_id} summary mismatch: expected {expected_summary!r}")

        required_fields = expected.get("required_valid_fields", [])
        record_expectations = expected.get("record_expectations", {})
        actual_by_id = {record.discovery_id: record for record in first_run.records}
        total_records += len(record_expectations)

        case_results: list[dict] = []
        for discovery_id, record_expected in record_expectations.items():
            actual = actual_by_id.get(discovery_id)
            if actual is None:
                failures.append(f"{batch.batch_id}:{discovery_id} missing from discovery output")
                case_results.append({"discovery_id": discovery_id, "status": "missing"})
                continue

            expected_status = record_expected.get("status")
            status_match = actual.status == expected_status
            if status_match:
                status_hits += 1
            else:
                failures.append(
                    f"{batch.batch_id}:{discovery_id} status mismatch: expected {expected_status!r}, got {actual.status!r}"
                )

            required_issue_substrings = record_expected.get("required_issue_substrings", [])
            if required_issue_substrings:
                issue_messages = [issue.message for issue in actual.issues]
                for substring in required_issue_substrings:
                    if not any(substring in message for message in issue_messages):
                        failures.append(
                            f"{batch.batch_id}:{discovery_id} expected issue substring {substring!r} not found in {issue_messages!r}"
                        )

            required_warning_substrings = record_expected.get("required_warning_substrings", [])
            if required_warning_substrings:
                for substring in required_warning_substrings:
                    if not any(substring in message for message in actual.warnings):
                        failures.append(
                            f"{batch.batch_id}:{discovery_id} expected warning substring {substring!r} not found in {actual.warnings!r}"
                        )

            if expected_status == "valid":
                total_required_field_expectations += 1
                missing_fields = []
                for field_name in required_fields:
                    value = getattr(actual, field_name)
                    if value is None:
                        missing_fields.append(field_name)
                if not missing_fields:
                    required_field_hits += 1
                else:
                    failures.append(
                        f"{batch.batch_id}:{discovery_id} missing required valid fields {missing_fields!r}"
                    )

            case_results.append(
                {
                    "discovery_id": discovery_id,
                    "status_match": status_match,
                    "actual_status": actual.status,
                    "warning_count": len(actual.warnings),
                    "issue_count": len(actual.issues),
                    "asin": actual.asin,
                }
            )

        batch_results.append(
            {
                "batch_id": batch.batch_id,
                "run_status": first_run.summary.run_status,
                "stable": stable,
                "records": case_results,
            }
        )

    metrics = {
        "discovery_batch_run_status_accuracy": _safe_ratio(run_status_hits, total_batches),
        "discovery_summary_accuracy": _safe_ratio(summary_hits, total_summaries),
        "discovery_record_status_accuracy": _safe_ratio(status_hits, total_records),
        "discovery_required_field_coverage": _safe_ratio(required_field_hits, total_required_field_expectations),
        "discovery_repeat_run_stability": _safe_ratio(stability_hits, total_batches),
    }
    failures.extend(_evaluate_thresholds(metrics, thresholds))

    return {
        "passed": not failures,
        "metrics": metrics,
        "failures": failures,
        "batches": batch_results,
    }


def _manifest_from_fixture(manifest_payload: dict, records: list[RawSourceRecord]) -> SourceSnapshotManifest:
    snapshot_id = str(manifest_payload.get("snapshot_id") or records[0].snapshot_id)
    source = str(manifest_payload.get("source") or records[0].source)
    captured_at = str(manifest_payload.get("captured_at") or records[0].captured_at)
    storage_uri = str(manifest_payload.get("storage_uri") or f"fixtures://{source}/{snapshot_id}")
    return SourceSnapshotManifest(
        snapshot_id=snapshot_id,
        source=source,
        captured_at=captured_at,
        record_count=len(records),
        record_ids=[record.record_id for record in records],
        storage_uri=storage_uri,
    )


def _summary_matches(result: object, expected: dict) -> bool:
    summary = getattr(result, "summary", None)
    if summary is None:
        return False
    return (
        summary.total_candidates == expected.get("total_candidates")
        and summary.valid_candidates == expected.get("valid_candidates")
        and summary.invalid_candidates == expected.get("invalid_candidates")
        and summary.warning_records == expected.get("warning_records")
    )


def _stable_digest(result: object) -> dict:
    summary = getattr(result, "summary", None)
    records = getattr(result, "records", [])
    return {
        "summary": {
            "run_status": getattr(summary, "run_status", None),
            "total_candidates": getattr(summary, "total_candidates", None),
            "valid_candidates": getattr(summary, "valid_candidates", None),
            "invalid_candidates": getattr(summary, "invalid_candidates", None),
            "warning_records": getattr(summary, "warning_records", None),
        },
        "records": [
            {
                "discovery_id": record.discovery_id,
                "status": record.status,
                "rank": record.rank,
                "title": record.title,
                "product_url": record.product_url,
                "asin": record.asin,
                "warnings": list(record.warnings),
                "issue_codes": [issue.code for issue in record.issues],
            }
            for record in records
        ],
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
    parser = argparse.ArgumentParser(description="Run discovery quality evals against golden SERP API fixtures.")
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--thresholds", type=Path, required=True)
    args = parser.parse_args(argv)

    report = evaluate_batches(load_batches(args.cases), load_thresholds(args.thresholds))
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
