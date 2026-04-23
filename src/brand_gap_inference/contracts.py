from __future__ import annotations

from .schema_registry import load_schema
from .schema_subset import ValidationIssue, validate_instance

SCHEMA_FILES = {
    "normalized_listing": "normalized_listing.schema.json",
    "normalization_batch_report": "normalization_batch_report.schema.json",
    "normalization_record_result": "normalization_record_result.schema.json",
    "taxonomy_assignment": "taxonomy_assignment.schema.json",
    "taxonomy_batch_report": "taxonomy_batch_report.schema.json",
    "evidence": "evidence.schema.json",
    "opportunity": "opportunity.schema.json",
    "raw_source_record": "raw_source_record.schema.json",
    "source_snapshot_manifest": "source_snapshot_manifest.schema.json",
    "run_manifest": "run_manifest.schema.json",
    "run_task_envelope": "run_task_envelope.schema.json",
}


def validate_document(schema_name: str, document: dict) -> list[ValidationIssue]:
    schema = load_schema(SCHEMA_FILES[schema_name])
    return validate_instance(document, schema)


def assert_valid(schema_name: str, document: dict) -> None:
    issues = validate_document(schema_name, document)
    if issues:
        formatted_issues = ", ".join(f"{issue.path}: {issue.message}" for issue in issues)
        raise ValueError(f"{schema_name} validation failed: {formatted_issues}")
