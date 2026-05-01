from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path

from .landscape_report import build_landscape_report
from .product_intelligence import ProductIntelligenceMerger


@dataclass(frozen=True)
class GoldenProductIntelligenceBatch:
    batch_id: str
    description: str
    collection_report: dict
    discovery_records: list[dict]
    detail_records: list[dict]
    expected: dict


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_batches(path: Path) -> list[GoldenProductIntelligenceBatch]:
    raw_batches = list(load_json(path))
    return [
        GoldenProductIntelligenceBatch(
            batch_id=batch["batch_id"],
            description=batch["description"],
            collection_report=batch["collection_report"],
            discovery_records=batch["discovery_records"],
            detail_records=batch["detail_records"],
            expected=batch["expected"],
        )
        for batch in raw_batches
    ]


def load_thresholds(path: Path) -> dict:
    return dict(load_json(path))


def evaluate_batches(batches: list[GoldenProductIntelligenceBatch], thresholds: dict) -> dict:
    failures: list[str] = []
    merge_status_hits = 0
    summary_hits = 0
    provenance_hits = 0
    claim_hits = 0
    sanitization_hits = 0
    source_failure_hits = 0
    stability_hits = 0
    total_batches = len(batches)
    batch_results: list[dict] = []

    for batch in batches:
        first_merge = ProductIntelligenceMerger().merge_collection(
            run_id=batch.collection_report["run_id"],
            collection_report=batch.collection_report,
            discovery_records=batch.discovery_records,
            detail_records=batch.detail_records,
        )
        second_merge = ProductIntelligenceMerger().merge_collection(
            run_id=batch.collection_report["run_id"],
            collection_report=batch.collection_report,
            discovery_records=batch.discovery_records,
            detail_records=batch.detail_records,
        )
        first_records = [record.to_dict() for record in first_merge.records]
        second_records = [record.to_dict() for record in second_merge.records]
        first_landscape = build_landscape_report(run_id=first_merge.run_id, records=first_records)
        second_landscape = build_landscape_report(run_id=second_merge.run_id, records=second_records)

        stable = first_records == second_records and first_landscape == second_landscape
        if stable:
            stability_hits += 1
        else:
            failures.append(f"{batch.batch_id} product-intelligence output is unstable across repeat runs")

        expected = batch.expected
        expected_status = expected.get("merge_status")
        if first_merge.status == expected_status:
            merge_status_hits += 1
        else:
            failures.append(
                f"{batch.batch_id} merge_status mismatch: expected {expected_status!r}, got {first_merge.status!r}"
            )

        expected_summary = expected.get("summary", {})
        if _summary_matches(first_merge, expected_summary):
            summary_hits += 1
        else:
            failures.append(f"{batch.batch_id} summary mismatch: expected {expected_summary!r}")

        if _provenance_matches(first_records, expected.get("field_provenance_expectations", {}), failures, batch.batch_id):
            provenance_hits += 1

        if _claims_match(first_landscape, expected.get("claim_patterns", []), failures, batch.batch_id):
            claim_hits += 1

        if _sanitization_matches(first_records, expected.get("sanitization_expectations", {}), failures, batch.batch_id):
            sanitization_hits += 1

        if _source_failure_matches(first_records, expected.get("source_failure_expectations", {}), failures, batch.batch_id):
            source_failure_hits += 1

        batch_results.append(
            {
                "batch_id": batch.batch_id,
                "merge_status": first_merge.status,
                "landscape_status": first_landscape["status"],
                "stable": stable,
                "total_products": len(first_merge.records),
                "complete_products": first_merge.complete_products,
                "warning_products": first_merge.warning_products,
                "issue_products": first_merge.issue_products,
            }
        )

    metrics = {
        "product_intelligence_merge_status_accuracy": _safe_ratio(merge_status_hits, total_batches),
        "product_intelligence_summary_accuracy": _safe_ratio(summary_hits, total_batches),
        "product_intelligence_provenance_accuracy": _safe_ratio(provenance_hits, total_batches),
        "landscape_claim_pattern_accuracy": _safe_ratio(claim_hits, total_batches),
        "product_intelligence_sanitization_accuracy": _safe_ratio(sanitization_hits, total_batches),
        "product_intelligence_source_failure_accuracy": _safe_ratio(source_failure_hits, total_batches),
        "product_intelligence_repeat_run_stability": _safe_ratio(stability_hits, total_batches),
    }
    failures.extend(_evaluate_thresholds(metrics, thresholds))
    return {"passed": not failures, "metrics": metrics, "failures": failures, "batches": batch_results}


def _summary_matches(result: object, expected: dict) -> bool:
    return (
        len(result.records) == expected.get("total_products")
        and result.complete_products == expected.get("complete_products")
        and result.warning_products == expected.get("warning_products")
        and result.issue_products == expected.get("issue_products")
    )


def _provenance_matches(
    records: list[dict],
    expectations: dict,
    failures: list[str],
    batch_id: str,
) -> bool:
    by_asin = {record["asin"]: record for record in records}
    matched = True
    for asin, field_expectations in expectations.items():
        record = by_asin.get(asin)
        if record is None:
            failures.append(f"{batch_id}:{asin} missing from product intelligence output")
            matched = False
            continue
        provenance = record.get("field_provenance", {})
        if not isinstance(provenance, dict):
            failures.append(f"{batch_id}:{asin} missing field_provenance")
            matched = False
            continue
        for field_name, expected_source in field_expectations.items():
            field_provenance = provenance.get(field_name)
            actual_source = field_provenance.get("source") if isinstance(field_provenance, dict) else None
            if actual_source != expected_source:
                failures.append(
                    f"{batch_id}:{asin}:{field_name} provenance mismatch: expected {expected_source!r}, got {actual_source!r}"
                )
                matched = False
    return matched


def _claims_match(landscape: dict, expected_claims: list[str], failures: list[str], batch_id: str) -> bool:
    actual_claims = {pattern["claim"] for pattern in landscape.get("claim_patterns", []) if isinstance(pattern, dict)}
    missing_claims = [claim for claim in expected_claims if claim not in actual_claims]
    if missing_claims:
        failures.append(f"{batch_id} missing landscape claim patterns {missing_claims!r}")
        return False
    return True


def _sanitization_matches(
    records: list[dict],
    expectations: dict,
    failures: list[str],
    batch_id: str,
) -> bool:
    by_asin = {record["asin"]: record for record in records}
    matched = True
    for asin, record_expectations in expectations.items():
        record = by_asin.get(asin)
        if record is None:
            failures.append(f"{batch_id}:{asin} missing from product intelligence output")
            matched = False
            continue
        if record_expectations.get("description_bullets_empty") and record.get("description_bullets"):
            failures.append(f"{batch_id}:{asin} expected description bullets to be empty after sanitization")
            matched = False
        if record_expectations.get("promotional_content_empty") and record.get("promotional_content"):
            failures.append(f"{batch_id}:{asin} expected promotional content to be empty after sanitization")
            matched = False
        warnings = record.get("warnings", [])
        for snippet in record_expectations.get("warning_contains", []):
            if not any(snippet in warning for warning in warnings):
                failures.append(f"{batch_id}:{asin} missing warning containing {snippet!r}")
                matched = False
    return matched


def _source_failure_matches(
    records: list[dict],
    expectations: dict,
    failures: list[str],
    batch_id: str,
) -> bool:
    by_asin = {record["asin"]: record for record in records}
    matched = True
    for asin, record_expectations in expectations.items():
        record = by_asin.get(asin)
        if record is None:
            failures.append(f"{batch_id}:{asin} missing from product intelligence output")
            matched = False
            continue
        if record_expectations.get("price_is_null") and record.get("price") is not None:
            failures.append(f"{batch_id}:{asin} expected price to remain null after unsafe offer block")
            matched = False
        expected_availability = record_expectations.get("availability")
        if expected_availability is not None and record.get("availability") != expected_availability:
            failures.append(
                f"{batch_id}:{asin} availability mismatch: expected {expected_availability!r}, got {record.get('availability')!r}"
            )
            matched = False
        issue_codes = {
            issue.get("code")
            for issue in record.get("issues", [])
            if isinstance(issue, dict)
        }
        for expected_code in record_expectations.get("issue_codes", []):
            if expected_code not in issue_codes:
                failures.append(f"{batch_id}:{asin} missing issue code {expected_code!r}")
                matched = False
        warnings = record.get("warnings", [])
        for snippet in record_expectations.get("warning_contains", []):
            if not any(snippet in warning for warning in warnings):
                failures.append(f"{batch_id}:{asin} missing warning containing {snippet!r}")
                matched = False
        provenance = record.get("field_provenance", {})
        expected_price_source = record_expectations.get("price_provenance_source")
        if expected_price_source is not None:
            price_provenance = provenance.get("price") if isinstance(provenance, dict) else None
            actual_source = price_provenance.get("source") if isinstance(price_provenance, dict) else None
            if actual_source != expected_price_source:
                failures.append(
                    f"{batch_id}:{asin}:price provenance mismatch: expected {expected_price_source!r}, got {actual_source!r}"
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
    parser = argparse.ArgumentParser(description="Run product-intelligence merge and landscape evals.")
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--thresholds", type=Path, required=True)
    args = parser.parse_args(argv)

    report = evaluate_batches(load_batches(args.cases), load_thresholds(args.thresholds))
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
