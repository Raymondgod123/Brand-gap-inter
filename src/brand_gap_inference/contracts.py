from __future__ import annotations

from .schema_registry import load_schema
from .schema_subset import ValidationIssue, validate_instance

SCHEMA_FILES = {
    "analysis_stack_report": "analysis_stack_report.schema.json",
    "brand_profile_record": "brand_profile_record.schema.json",
    "brand_profile_report": "brand_profile_report.schema.json",
    "brand_positioning_record": "brand_positioning_record.schema.json",
    "brand_positioning_report": "brand_positioning_report.schema.json",
    "deep_brand_inference_report": "deep_brand_inference_report.schema.json",
    "decision_brief_report": "decision_brief_report.schema.json",
    "data_collection_report": "data_collection_report.schema.json",
    "demand_signal_record": "demand_signal_record.schema.json",
    "demand_signal_report": "demand_signal_report.schema.json",
    "discovery_result_record": "discovery_result_record.schema.json",
    "discovery_batch_report": "discovery_batch_report.schema.json",
    "evidence_workbench_manifest": "evidence_workbench_manifest.schema.json",
    "gap_validation_record": "gap_validation_record.schema.json",
    "gap_validation_report": "gap_validation_report.schema.json",
    "landscape_report": "landscape_report.schema.json",
    "normalized_listing": "normalized_listing.schema.json",
    "normalization_batch_report": "normalization_batch_report.schema.json",
    "normalization_record_result": "normalization_record_result.schema.json",
    "taxonomy_assignment": "taxonomy_assignment.schema.json",
    "taxonomy_batch_report": "taxonomy_batch_report.schema.json",
    "evidence": "evidence.schema.json",
    "opportunity": "opportunity.schema.json",
    "product_detail_record": "product_detail_record.schema.json",
    "product_intelligence_batch_report": "product_intelligence_batch_report.schema.json",
    "product_intelligence_record": "product_intelligence_record.schema.json",
    "raw_source_record": "raw_source_record.schema.json",
    "source_snapshot_manifest": "source_snapshot_manifest.schema.json",
    "run_manifest": "run_manifest.schema.json",
    "run_task_envelope": "run_task_envelope.schema.json",
    "visual_brand_signals_record": "visual_brand_signals_record.schema.json",
}


def validate_document(schema_name: str, document: dict) -> list[ValidationIssue]:
    schema = load_schema(SCHEMA_FILES[schema_name])
    return validate_instance(document, schema)


def assert_valid(schema_name: str, document: dict) -> None:
    issues = validate_document(schema_name, document)
    if issues:
        formatted_issues = ", ".join(f"{issue.path}: {issue.message}" for issue in issues)
        raise ValueError(f"{schema_name} validation failed: {formatted_issues}")
