from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from .connectors import RawSourceRecord
from .normalization import BatchNormalizer
from .raw_store import SourceSnapshotManifest


@dataclass(frozen=True)
class GoldenNormalizationBatch:
    batch_id: str
    description: str
    manifest: dict
    records: list[dict]
    expected: dict


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_batches(path: Path) -> list[GoldenNormalizationBatch]:
    raw_batches = list(load_json(path))
    return [
        GoldenNormalizationBatch(
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


def evaluate_batches(batches: list[GoldenNormalizationBatch], thresholds: dict) -> dict:
    failures: list[str] = []
    batch_run_status_hits = 0
    summary_hits = 0
    status_hits = 0
    duplicate_link_hits = 0
    provenance_hits = 0
    low_confidence_hits = 0
    stability_hits = 0

    total_batches = len(batches)
    total_records = 0
    total_duplicates = 0
    total_low_confidence_expectations = 0
    total_provenance_expectations = 0
    total_summaries = 0
    batch_results: list[dict] = []

    normalizer = BatchNormalizer()

    for batch in batches:
        records = [RawSourceRecord.from_dict(item) for item in batch.records]
        manifest = _manifest_from_fixture(batch.manifest, records)
        expected = batch.expected

        first_run = normalizer.normalize_snapshot(manifest, records)
        second_run = normalizer.normalize_snapshot(manifest, records)

        stable = _stable_digest(first_run) == _stable_digest(second_run)
        if stable:
            stability_hits += 1
        else:
            failures.append(f"{batch.batch_id} normalization output is unstable across repeat runs")

        expected_run_status = expected.get("run_status")
        if expected_run_status is not None:
            if first_run.summary.run_status == expected_run_status:
                batch_run_status_hits += 1
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

        required_provenance_fields = expected.get("required_provenance_fields", [])
        record_expectations = expected.get("record_expectations", {})
        if not isinstance(record_expectations, dict):
            failures.append(f"{batch.batch_id} record_expectations must be an object")
            continue

        actual_by_record_id = {record.source_record_id: record for record in first_run.records}
        total_records += len(record_expectations)

        batch_case_results: list[dict] = []
        for record_id, record_expected in record_expectations.items():
            if record_id not in actual_by_record_id:
                failures.append(f"{batch.batch_id}:{record_id} missing from normalization output")
                batch_case_results.append({"record_id": record_id, "status": "missing"})
                continue

            actual = actual_by_record_id[record_id]
            expected_status = record_expected.get("status")
            if expected_status is None:
                failures.append(f"{batch.batch_id}:{record_id} expected.status is required")
                continue

            status_match = actual.status == expected_status
            if status_match:
                status_hits += 1
            else:
                failures.append(
                    f"{batch.batch_id}:{record_id} status mismatch: expected {expected_status!r}, got {actual.status!r}"
                )

            if expected_status == "duplicate":
                total_duplicates += 1
                expected_duplicate_of = record_expected.get("duplicate_of")
                if expected_duplicate_of and actual.duplicate_of == expected_duplicate_of:
                    duplicate_link_hits += 1
                else:
                    failures.append(
                        f"{batch.batch_id}:{record_id} duplicate_of mismatch: expected {expected_duplicate_of!r}, got {actual.duplicate_of!r}"
                    )

            required_issue_substrings = record_expected.get("required_issue_substrings", [])
            if required_issue_substrings:
                issue_messages = [issue.message for issue in actual.issues]
                for substring in required_issue_substrings:
                    if not any(substring in message for message in issue_messages):
                        failures.append(
                            f"{batch.batch_id}:{record_id} expected issue substring {substring!r} not found in {issue_messages!r}"
                        )

            required_codes = record_expected.get("required_low_confidence_codes", [])
            if required_codes:
                total_low_confidence_expectations += 1
                actual_codes = {reason.get("code", "") for reason in actual.low_confidence_reasons}
                missing_codes = [code for code in required_codes if code not in actual_codes]
                if not missing_codes:
                    low_confidence_hits += 1
                else:
                    failures.append(
                        f"{batch.batch_id}:{record_id} missing low-confidence codes {missing_codes!r}; got {sorted(actual_codes)!r}"
                    )

            if expected_status == "normalized":
                total_provenance_expectations += 1
                missing_fields = [field for field in required_provenance_fields if field not in actual.field_provenance]
                if not missing_fields:
                    provenance_hits += 1
                else:
                    failures.append(
                        f"{batch.batch_id}:{record_id} missing provenance for fields {missing_fields!r}"
                    )

            batch_case_results.append(
                {
                    "record_id": record_id,
                    "status_match": status_match,
                    "actual_status": actual.status,
                    "listing_id": actual.listing_id,
                    "warning_count": len(actual.warnings),
                    "issue_count": len(actual.issues),
                    "low_confidence_reason_count": len(actual.low_confidence_reasons),
                }
            )

        batch_results.append(
            {
                "batch_id": batch.batch_id,
                "run_status": first_run.summary.run_status,
                "stable": stable,
                "records": batch_case_results,
            }
        )

    metrics = {
        "normalization_batch_run_status_accuracy": _safe_ratio(batch_run_status_hits, total_batches),
        "normalization_summary_accuracy": _safe_ratio(summary_hits, total_summaries),
        "normalization_record_status_accuracy": _safe_ratio(status_hits, total_records),
        "normalization_duplicate_link_accuracy": _safe_ratio(duplicate_link_hits, total_duplicates),
        "normalization_provenance_coverage": _safe_ratio(provenance_hits, total_provenance_expectations),
        "normalization_low_confidence_reason_coverage": _safe_ratio(low_confidence_hits, total_low_confidence_expectations),
        "normalization_repeat_run_stability": _safe_ratio(stability_hits, total_batches),
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
        summary.total_records == expected.get("total_records")
        and summary.normalized_records == expected.get("normalized_records")
        and summary.duplicate_records == expected.get("duplicate_records")
        and summary.invalid_records == expected.get("invalid_records")
        and summary.low_confidence_records == expected.get("low_confidence_records")
    )


def _stable_digest(result: object) -> dict:
    summary = getattr(result, "summary", None)
    records = getattr(result, "records", [])
    listings = getattr(result, "normalized_listings", [])
    return {
        "summary": {
            "run_status": getattr(summary, "run_status", None),
            "total_records": getattr(summary, "total_records", None),
            "normalized_records": getattr(summary, "normalized_records", None),
            "duplicate_records": getattr(summary, "duplicate_records", None),
            "invalid_records": getattr(summary, "invalid_records", None),
            "warning_records": getattr(summary, "warning_records", None),
            "low_confidence_records": getattr(summary, "low_confidence_records", None),
        },
        "records": [
            {
                "source_record_id": record.source_record_id,
                "status": record.status,
                "listing_id": record.listing_id,
                "duplicate_of": record.duplicate_of,
                "warnings": list(record.warnings),
                "low_confidence_codes": [reason.get("code") for reason in record.low_confidence_reasons],
                "field_provenance_keys": sorted(list(record.field_provenance.keys())),
            }
            for record in records
        ],
        "normalized_listings": listings,
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
    parser = argparse.ArgumentParser(description="Run normalization quality evals against golden dirty-data fixtures.")
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--thresholds", type=Path, required=True)
    args = parser.parse_args(argv)

    report = evaluate_batches(load_batches(args.cases), load_thresholds(args.thresholds))
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

