from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path

from .contracts import assert_valid


@dataclass(frozen=True)
class TaxonomyIssue:
    code: str
    message: str
    severity: str


@dataclass(frozen=True)
class TaxonomyRecordResult:
    listing_id: str
    status: str
    warnings: list[str]
    issues: list[TaxonomyIssue]
    assignment: dict | None = None

    def to_report_dict(self) -> dict:
        payload = {
            "listing_id": self.listing_id,
            "status": self.status,
            "warning_count": len(self.warnings),
            "issue_count": len(self.issues),
        }
        if self.assignment is not None:
            payload["confidence"] = self.assignment["confidence"]
        return payload


@dataclass(frozen=True)
class TaxonomySummary:
    run_status: str
    total_listings: int
    assigned_count: int
    failed_count: int
    warning_records: int


@dataclass(frozen=True)
class TaxonomyBatchResult:
    taxonomy_version: str
    summary: TaxonomySummary
    records: list[TaxonomyRecordResult]

    @property
    def assignments(self) -> list[dict]:
        return [record.assignment for record in self.records if record.assignment is not None]

    def to_report_dict(self, snapshot_id: str) -> dict:
        payload = {
            "snapshot_id": snapshot_id,
            "taxonomy_version": self.taxonomy_version,
            "run_status": self.summary.run_status,
            "total_listings": self.summary.total_listings,
            "assigned_count": self.summary.assigned_count,
            "failed_count": self.summary.failed_count,
            "warning_records": self.summary.warning_records,
            "records": [record.to_report_dict() for record in self.records],
        }
        assert_valid("taxonomy_batch_report", payload)
        return payload


class TaxonomyAssigner:
    def __init__(self, taxonomy_version: str = "taxonomy-v1") -> None:
        self.taxonomy_version = taxonomy_version

    def assign_batch(self, listings: list[dict], snapshot_id: str, assigned_at: str | None = None) -> TaxonomyBatchResult:
        assigned_timestamp = assigned_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        records: list[TaxonomyRecordResult] = []

        for listing in listings:
            listing_id = str(listing.get("listing_id", "unknown-listing"))
            issues: list[TaxonomyIssue] = []
            warnings: list[str] = []

            try:
                assignment, warnings = self.assign_listing(listing, assigned_timestamp)
                records.append(
                    TaxonomyRecordResult(
                        listing_id=listing_id,
                        status="assigned",
                        warnings=warnings,
                        issues=[],
                        assignment=assignment,
                    )
                )
            except ValueError as error:
                issues.append(TaxonomyIssue("taxonomy_assignment_failed", str(error), "error"))
                records.append(
                    TaxonomyRecordResult(
                        listing_id=listing_id,
                        status="invalid",
                        warnings=[],
                        issues=issues,
                    )
                )

        assigned_count = sum(1 for record in records if record.status == "assigned")
        failed_count = sum(1 for record in records if record.status == "invalid")
        warning_records = sum(1 for record in records if record.warnings)
        run_status = _compute_taxonomy_run_status(assigned_count, failed_count, len(records))

        return TaxonomyBatchResult(
            taxonomy_version=self.taxonomy_version,
            summary=TaxonomySummary(
                run_status=run_status,
                total_listings=len(records),
                assigned_count=assigned_count,
                failed_count=failed_count,
                warning_records=warning_records,
            ),
            records=records,
        )

    def assign_listing(self, listing: dict, assigned_at: str) -> tuple[dict, list[str]]:
        product_title = _get_required_text(listing, "product_title")
        category_path = listing.get("category_path")
        if not isinstance(category_path, list) or not category_path:
            raise ValueError("listing is missing category_path")

        signals = " ".join([product_title, " ".join(str(item) for item in category_path), str(listing.get("brand_name", ""))]).lower()
        warnings: list[str] = []
        evidence_hits = 0

        need_state = "general_wellness"
        if any(token in signals for token in ["monk fruit", "sugar substitute", "sweetener", "baking", "keto"]):
            need_state = "sugar_replacement"
            evidence_hits += 1
        elif any(token in signals for token in ["hydration", "electrolyte", "tablet", "powder stick"]):
            need_state = "rapid_hydration"
            evidence_hits += 1
        elif any(token in signals for token in ["protein", "bar", "shake"]):
            need_state = "satiety_support"
            evidence_hits += 1
        elif any(token in signals for token in ["energy", "caffeine"]):
            need_state = "energy_boost"
            evidence_hits += 1

        occasion = "daily_use"
        if any(token in signals for token in ["baking", "cooking"]):
            occasion = "baking"
            evidence_hits += 1
        elif any(token in signals for token in ["travel", "on_the_go", "stick", "bar"]):
            occasion = "on_the_go"
            evidence_hits += 1
        elif any(token in signals for token in ["workout", "hydration", "electrolyte"]):
            occasion = "workout"
            evidence_hits += 1

        format_name = _infer_format(signals, listing.get("unit_measure"))
        if format_name != "general":
            evidence_hits += 1
        else:
            warnings.append("format inferred with generic fallback")

        audience = "general_adults"
        if any(token in signals for token in ["keto", "low carb", "sugar substitute"]):
            audience = "keto_shoppers"
            evidence_hits += 1
        elif any(token in signals for token in ["baking", "sweetener"]):
            audience = "bakers"
            evidence_hits += 1
        elif any(token in signals for token in ["hydration", "electrolyte", "protein", "energy"]):
            audience = "active_adults"
            evidence_hits += 1

        adjacent_categories = _infer_adjacent_categories(category_path, need_state)
        confidence = min(0.95, round(0.45 + (0.1 * evidence_hits), 2))
        if confidence < 0.65:
            warnings.append("taxonomy confidence is low; assignment is heuristic")

        assignment = {
            "listing_id": listing["listing_id"],
            "taxonomy_version": self.taxonomy_version,
            "axes": {
                "need_state": need_state,
                "occasion": occasion,
                "format": format_name,
                "audience": audience,
            },
            "adjacent_categories": adjacent_categories,
            "confidence": confidence,
            "assigned_at": assigned_at,
        }
        assert_valid("taxonomy_assignment", assignment)
        return assignment, warnings


def write_taxonomy_artifacts(output_dir: Path, snapshot_id: str, result: TaxonomyBatchResult) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    assignments_path = output_dir / "taxonomy_assignments.json"
    report_path = output_dir / "taxonomy_report.json"
    record_results_path = output_dir / "taxonomy_records.json"

    assignments_path.write_text(json.dumps(result.assignments, indent=2), encoding="utf-8")
    report_path.write_text(json.dumps(result.to_report_dict(snapshot_id), indent=2), encoding="utf-8")
    record_results_path.write_text(
        json.dumps(
            [
                {
                    "listing_id": record.listing_id,
                    "status": record.status,
                    "warnings": record.warnings,
                    "issues": [
                        {"code": issue.code, "message": issue.message, "severity": issue.severity}
                        for issue in record.issues
                    ],
                }
                for record in result.records
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "taxonomy_assignments": str(assignments_path),
        "taxonomy_report": str(report_path),
        "taxonomy_records": str(record_results_path),
    }


def _infer_format(signals: str, unit_measure: object) -> str:
    if any(token in signals for token in ["tablet", "tab "]):
        return "tablet"
    if any(token in signals for token in ["powder", "sweetener", "granule"]):
        return "powder"
    if any(token in signals for token in ["gummy", "gummies"]):
        return "gummy"
    if any(token in signals for token in ["chew", "chews"]):
        return "chew"
    if any(token in signals for token in ["bar", "bars"]):
        return "bar"
    if any(token in signals for token in ["drink", "beverage", "water"]):
        return "ready_to_drink"
    if unit_measure in {"lb", "oz", "kg", "g"}:
        return "powder"
    return "general"


def _infer_adjacent_categories(category_path: list[object], need_state: str) -> list[str]:
    joined = " ".join(str(item) for item in category_path).lower()
    if need_state == "sugar_replacement":
        return ["baking", "coffee-tea", "keto-pantry"]
    if need_state == "rapid_hydration":
        return ["sports-nutrition", "wellness", "travel-health"]
    if "protein" in joined:
        return ["fitness-snacks", "meal-replacement"]
    return ["general-wellness"]


def _get_required_text(listing: dict, field_name: str) -> str:
    value = listing.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"listing is missing {field_name}")
    return value


def _compute_taxonomy_run_status(assigned_count: int, failed_count: int, total_listings: int) -> str:
    if total_listings == 0 or assigned_count == 0:
        return "failed"
    if failed_count > 0:
        return "partial_success"
    return "success"
