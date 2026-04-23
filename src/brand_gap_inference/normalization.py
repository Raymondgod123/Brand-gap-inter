from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path

from .amazon_normalizer import AmazonListingNormalizer
from .connectors import RawSourceRecord
from .contracts import assert_valid, validate_document
from .raw_store import SourceSnapshotManifest


@dataclass(frozen=True)
class NormalizationIssue:
    code: str
    message: str
    severity: str


@dataclass(frozen=True)
class RecordNormalizationResult:
    source_record_id: str
    status: str
    issues: list[NormalizationIssue]
    warnings: list[str]
    normalized_listing: dict | None = None
    duplicate_of: str | None = None
    listing_id: str | None = None
    raw_payload_uri: str | None = None
    field_provenance: dict[str, dict[str, object]] = field(default_factory=dict)
    low_confidence_reasons: list[dict[str, str]] = field(default_factory=list)

    def to_report_dict(self) -> dict:
        payload = {
            "source_record_id": self.source_record_id,
            "status": self.status,
            "warning_count": len(self.warnings),
            "issue_count": len(self.issues),
            "low_confidence_reason_count": len(self.low_confidence_reasons),
        }
        if self.duplicate_of:
            payload["duplicate_of"] = self.duplicate_of
        listing_id = self.listing_id or (self.normalized_listing["listing_id"] if self.normalized_listing is not None else None)
        if listing_id is not None:
            payload["listing_id"] = listing_id
        return payload


@dataclass(frozen=True)
class NormalizationSummary:
    run_status: str
    total_records: int
    normalized_records: int
    duplicate_records: int
    invalid_records: int
    warning_records: int
    low_confidence_records: int


@dataclass(frozen=True)
class NormalizationBatchResult:
    summary: NormalizationSummary
    records: list[RecordNormalizationResult]

    @property
    def normalized_listings(self) -> list[dict]:
        return [record.normalized_listing for record in self.records if record.normalized_listing is not None]

    def to_report_dict(self, manifest: SourceSnapshotManifest) -> dict:
        payload = {
            "snapshot_id": manifest.snapshot_id,
            "source": manifest.source,
            "run_status": self.summary.run_status,
            "total_records": self.summary.total_records,
            "normalized_records": self.summary.normalized_records,
            "duplicate_records": self.summary.duplicate_records,
            "invalid_records": self.summary.invalid_records,
            "warning_records": self.summary.warning_records,
            "low_confidence_records": self.summary.low_confidence_records,
            "records": [record.to_report_dict() for record in self.records],
        }
        assert_valid("normalization_batch_report", payload)
        return payload


class BatchNormalizer:
    def __init__(self) -> None:
        self._normalizers = {
            "amazon": AmazonListingNormalizer(),
        }

    def normalize_snapshot(
        self,
        manifest: SourceSnapshotManifest,
        records: list[RawSourceRecord],
    ) -> NormalizationBatchResult:
        results: list[RecordNormalizationResult] = []
        seen_listing_ids: dict[str, str] = {}

        for record in records:
            raw_payload_uri = f"{manifest.storage_uri}/{record.record_id}.json"
            normalizer = self._normalizers.get(record.source)
            if normalizer is None:
                results.append(
                    RecordNormalizationResult(
                        source_record_id=record.record_id,
                        status="invalid",
                        issues=[NormalizationIssue("unsupported_source", f"no normalizer registered for source {record.source}", "error")],
                        warnings=[],
                        raw_payload_uri=raw_payload_uri,
                    )
                )
                continue

            outcome = normalizer.normalize(record, raw_payload_uri)
            if outcome.listing is None:
                results.append(
                    RecordNormalizationResult(
                        source_record_id=record.record_id,
                        status="invalid",
                        issues=[NormalizationIssue("normalization_failed", message, "error") for message in outcome.errors],
                        warnings=outcome.warnings,
                        raw_payload_uri=raw_payload_uri,
                        listing_id=None,
                        field_provenance=outcome.field_provenance,
                        low_confidence_reasons=outcome.low_confidence_reasons,
                    )
                )
                continue

            schema_issues = validate_document("normalized_listing", outcome.listing)
            if schema_issues:
                results.append(
                    RecordNormalizationResult(
                        source_record_id=record.record_id,
                        status="invalid",
                        issues=[
                            NormalizationIssue("schema_validation_failed", f"{issue.path}: {issue.message}", "error")
                            for issue in schema_issues
                        ],
                        warnings=outcome.warnings,
                        raw_payload_uri=raw_payload_uri,
                        listing_id=str(outcome.listing.get("listing_id")) if isinstance(outcome.listing.get("listing_id"), str) else None,
                        field_provenance=outcome.field_provenance,
                        low_confidence_reasons=outcome.low_confidence_reasons,
                    )
                )
                continue

            listing_id = outcome.listing["listing_id"]
            if listing_id in seen_listing_ids:
                results.append(
                    RecordNormalizationResult(
                        source_record_id=record.record_id,
                        status="duplicate",
                        issues=[
                            NormalizationIssue(
                                "duplicate_listing",
                                f"listing {listing_id} duplicates source record {seen_listing_ids[listing_id]}",
                                "warning",
                            )
                        ],
                        warnings=outcome.warnings,
                        duplicate_of=seen_listing_ids[listing_id],
                        listing_id=listing_id,
                        raw_payload_uri=raw_payload_uri,
                        field_provenance=outcome.field_provenance,
                        low_confidence_reasons=outcome.low_confidence_reasons,
                    )
                )
                continue

            seen_listing_ids[listing_id] = record.record_id
            results.append(
                RecordNormalizationResult(
                    source_record_id=record.record_id,
                    status="normalized",
                    issues=[],
                    warnings=outcome.warnings,
                    normalized_listing=outcome.listing,
                    listing_id=listing_id,
                    raw_payload_uri=raw_payload_uri,
                    field_provenance=outcome.field_provenance,
                    low_confidence_reasons=outcome.low_confidence_reasons,
                )
            )

        normalized_count = sum(1 for result in results if result.status == "normalized")
        duplicate_count = sum(1 for result in results if result.status == "duplicate")
        invalid_count = sum(1 for result in results if result.status == "invalid")
        warning_count = sum(1 for result in results if result.warnings)
        low_confidence_count = sum(1 for result in results if result.low_confidence_reasons)
        run_status = _compute_run_status(normalized_count, invalid_count, len(results))

        return NormalizationBatchResult(
            summary=NormalizationSummary(
                run_status=run_status,
                total_records=len(results),
                normalized_records=normalized_count,
                duplicate_records=duplicate_count,
                invalid_records=invalid_count,
                warning_records=warning_count,
                low_confidence_records=low_confidence_count,
            ),
            records=results,
        )


def _compute_run_status(normalized_count: int, invalid_count: int, total_records: int) -> str:
    if total_records == 0 or normalized_count == 0:
        return "failed"
    if invalid_count > 0:
        return "partial_success"
    return "success"


def write_normalization_artifacts(
    output_dir: Path,
    manifest: SourceSnapshotManifest,
    result: NormalizationBatchResult,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    listings_path = output_dir / "normalized_listings.json"
    report_path = output_dir / "normalization_report.json"
    record_results_path = output_dir / "normalization_records.json"

    listings_path.write_text(json.dumps(result.normalized_listings, indent=2), encoding="utf-8")
    report_path.write_text(json.dumps(result.to_report_dict(manifest), indent=2), encoding="utf-8")

    record_payloads = [
        {
            "source_record_id": record.source_record_id,
            "status": record.status,
            "listing_id": record.listing_id,
            "raw_payload_uri": record.raw_payload_uri,
            "warnings": record.warnings,
            "low_confidence_reasons": record.low_confidence_reasons,
            "field_provenance": record.field_provenance,
            "issues": [{"code": issue.code, "message": issue.message, "severity": issue.severity} for issue in record.issues],
            "duplicate_of": record.duplicate_of,
        }
        for record in result.records
    ]
    for payload in record_payloads:
        assert_valid("normalization_record_result", payload)

    record_results_path.write_text(
        json.dumps(
            record_payloads,
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        "normalized_listings": str(listings_path),
        "normalization_report": str(report_path),
        "normalization_records": str(record_results_path),
    }
