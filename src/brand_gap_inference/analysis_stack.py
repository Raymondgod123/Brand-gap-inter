from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Callable

from .brand_analysis import write_brand_positioning_artifacts
from .brand_profile import write_brand_profile_artifacts
from .contracts import assert_valid
from .data_collection import DataCollectionRunResult, run_data_collection
from .deep_inference import DeepInferenceClient, write_deep_inference_artifacts
from .demand_signals import write_demand_signal_artifacts
from .decision_brief import write_decision_brief_artifacts
from .evidence_workbench import write_evidence_workbench_artifacts
from .landscape_report import write_landscape_artifacts
from .gap_validation import write_gap_validation_artifacts


@dataclass(frozen=True)
class AnalysisStepResult:
    step: str
    required: bool
    status: str
    report_status: str
    depends_on: list[str]
    output_dir: str
    artifacts: dict[str, str]
    warnings: list[str]
    error: str

    def to_dict(self) -> dict[str, object]:
        return {
            "step": self.step,
            "required": self.required,
            "status": self.status,
            "report_status": self.report_status,
            "depends_on": self.depends_on,
            "output_dir": self.output_dir,
            "artifacts": self.artifacts,
            "warnings": self.warnings,
            "error": self.error,
        }


@dataclass(frozen=True)
class AnalysisStackRunResult:
    run_id: str
    status: str
    output_dir: Path
    artifacts: dict[str, str]
    report: dict[str, object]
    steps: list[AnalysisStepResult]


@dataclass(frozen=True)
class CollectionAndAnalysisRunResult:
    status: str
    collection: DataCollectionRunResult
    analysis: AnalysisStackRunResult | None
    artifacts: dict[str, str]


@dataclass(frozen=True)
class _StepDefinition:
    name: str
    required: bool
    depends_on: list[str]
    output_dir: Path
    report_artifact_key: str
    runner: Callable[[], dict[str, str]]


def run_analysis_stack(
    *,
    collection_dir: Path,
    output_dir: Path | None = None,
    include_deep_inference: bool = False,
    deep_inference_model: str = "gpt-5.4",
    deep_inference_reasoning_effort: str = "high",
    deep_inference_client: DeepInferenceClient | None = None,
    generated_at: str | None = None,
) -> AnalysisStackRunResult:
    resolved_collection_dir = collection_dir.resolve()
    resolved_output_dir = output_dir or (resolved_collection_dir / "analysis_stack")
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_generated_at = generated_at or _utc_now()
    run_id = _infer_run_id(resolved_collection_dir)

    step_definitions: list[_StepDefinition] = [
        _StepDefinition(
            name="landscape",
            required=True,
            depends_on=[],
            output_dir=resolved_collection_dir / "landscape",
            report_artifact_key="landscape_report",
            runner=lambda: write_landscape_artifacts(
                product_intelligence_records_path=resolved_collection_dir / "product_intelligence" / "product_intelligence_records.json",
                output_dir=resolved_collection_dir / "landscape",
                run_id=run_id,
            ),
        ),
        _StepDefinition(
            name="brand_positioning",
            required=True,
            depends_on=[],
            output_dir=resolved_collection_dir / "brand_positioning",
            report_artifact_key="brand_positioning_report",
            runner=lambda: write_brand_positioning_artifacts(
                product_intelligence_records_path=resolved_collection_dir / "product_intelligence" / "product_intelligence_records.json",
                output_dir=resolved_collection_dir / "brand_positioning",
                run_id=run_id,
            ),
        ),
        _StepDefinition(
            name="brand_profiles",
            required=True,
            depends_on=["brand_positioning"],
            output_dir=resolved_collection_dir / "brand_profiles",
            report_artifact_key="brand_profile_report",
            runner=lambda: write_brand_profile_artifacts(
                collection_dir=resolved_collection_dir,
                output_dir=resolved_collection_dir / "brand_profiles",
            ),
        ),
        _StepDefinition(
            name="demand_signals",
            required=True,
            depends_on=["brand_profiles"],
            output_dir=resolved_collection_dir / "demand_signals",
            report_artifact_key="demand_signal_report",
            runner=lambda: write_demand_signal_artifacts(
                collection_dir=resolved_collection_dir,
                output_dir=resolved_collection_dir / "demand_signals",
            ),
        ),
        _StepDefinition(
            name="gap_validation",
            required=True,
            depends_on=["brand_profiles", "demand_signals"],
            output_dir=resolved_collection_dir / "gap_validation",
            report_artifact_key="gap_validation_report",
            runner=lambda: write_gap_validation_artifacts(
                collection_dir=resolved_collection_dir,
                output_dir=resolved_collection_dir / "gap_validation",
            ),
        ),
        _StepDefinition(
            name="decision_brief",
            required=True,
            depends_on=["brand_profiles", "demand_signals", "gap_validation"],
            output_dir=resolved_collection_dir / "decision_brief",
            report_artifact_key="decision_brief_report",
            runner=lambda: write_decision_brief_artifacts(
                collection_dir=resolved_collection_dir,
                output_dir=resolved_collection_dir / "decision_brief",
            ),
        ),
        _StepDefinition(
            name="evidence_workbench",
            required=False,
            depends_on=["decision_brief"],
            output_dir=resolved_collection_dir / "evidence_workbench",
            report_artifact_key="evidence_workbench_manifest",
            runner=lambda: write_evidence_workbench_artifacts(
                collection_dir=resolved_collection_dir,
                output_dir=resolved_collection_dir / "evidence_workbench",
                generated_at=resolved_generated_at,
            ),
        ),
    ]
    if include_deep_inference:
        step_definitions.append(
            _StepDefinition(
                name="deep_inference",
                required=False,
                depends_on=["landscape", "brand_positioning", "brand_profiles", "demand_signals", "gap_validation", "decision_brief"],
                output_dir=resolved_collection_dir / "deep_inference",
                report_artifact_key="deep_brand_inference_report",
                runner=lambda: write_deep_inference_artifacts(
                    collection_dir=resolved_collection_dir,
                    output_dir=resolved_collection_dir / "deep_inference",
                    client=deep_inference_client,
                    model=deep_inference_model,
                    reasoning_effort=deep_inference_reasoning_effort,
                ),
            )
        )

    step_results: list[AnalysisStepResult] = []
    successful_steps: set[str] = set()
    aggregated_artifacts: dict[str, str] = {}

    for definition in step_definitions:
        blocked_by = [
            dependency
            for dependency in definition.depends_on
            if dependency not in successful_steps
        ]
        if blocked_by:
            step_results.append(
                AnalysisStepResult(
                    step=definition.name,
                    required=definition.required,
                    status="skipped",
                    report_status="not_run",
                    depends_on=definition.depends_on,
                    output_dir=str(definition.output_dir),
                    artifacts={},
                    warnings=[],
                    error=f"blocked by dependency state: {', '.join(blocked_by)}",
                )
            )
            continue

        try:
            artifacts = definition.runner()
        except (ValueError, FileNotFoundError, OSError, RuntimeError) as error:
            step_results.append(
                AnalysisStepResult(
                    step=definition.name,
                    required=definition.required,
                    status="failed",
                    report_status="failed",
                    depends_on=definition.depends_on,
                    output_dir=str(definition.output_dir),
                    artifacts={},
                    warnings=[],
                    error=str(error),
                )
            )
            continue

        report_status, warnings = _summarize_step_report(definition.report_artifact_key, artifacts)
        step_results.append(
            AnalysisStepResult(
                step=definition.name,
                required=definition.required,
                status="success",
                report_status=report_status,
                depends_on=definition.depends_on,
                output_dir=str(definition.output_dir),
                artifacts=artifacts,
                warnings=warnings,
                error="",
            )
        )
        successful_steps.add(definition.name)
        aggregated_artifacts.update(artifacts)

    report = _build_report(
        run_id=run_id,
        generated_at=resolved_generated_at,
        collection_dir=resolved_collection_dir,
        include_deep_inference=include_deep_inference,
        step_results=step_results,
        artifacts=aggregated_artifacts,
    )
    assert_valid("analysis_stack_report", report)

    report_path = resolved_output_dir / "analysis_stack_report.json"
    markdown_path = resolved_output_dir / "analysis_stack_report.md"
    manifest_path = resolved_output_dir / "analysis_stack_bundle_manifest.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown_path.write_text(render_analysis_stack_markdown(report), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "status": report["status"],
                "generated_at": resolved_generated_at,
                "artifacts": {
                    **aggregated_artifacts,
                    "analysis_stack_report": str(report_path),
                    "analysis_stack_report_md": str(markdown_path),
                    "analysis_stack_bundle_manifest": str(manifest_path),
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    final_artifacts = {
        **aggregated_artifacts,
        "analysis_stack_report": str(report_path),
        "analysis_stack_report_md": str(markdown_path),
        "analysis_stack_bundle_manifest": str(manifest_path),
    }
    return AnalysisStackRunResult(
        run_id=run_id,
        status=str(report["status"]),
        output_dir=resolved_output_dir,
        artifacts=final_artifacts,
        report=report,
        steps=step_results,
    )


def run_collection_and_analysis(
    *,
    keyword: str | None = None,
    discovery_snapshot_id: str | None = None,
    store_dir: Path,
    output_dir: Path | None = None,
    max_products: int = 5,
    detail_mode: str = "serpapi_product",
    post_analysis: str = "none",
    discovery_connector: object | None = None,
    product_client: object | None = None,
    captured_at: str | None = None,
    deep_inference_model: str = "gpt-5.4",
    deep_inference_reasoning_effort: str = "high",
    deep_inference_client: DeepInferenceClient | None = None,
) -> CollectionAndAnalysisRunResult:
    if post_analysis not in {"none", "deterministic", "deep_inference"}:
        raise ValueError("post_analysis must be none, deterministic, or deep_inference")
    if post_analysis != "none" and detail_mode == "none":
        raise ValueError("post_analysis requires product detail collection; detail_mode cannot be none")

    collection_result = run_data_collection(
        keyword=keyword,
        discovery_snapshot_id=discovery_snapshot_id,
        store_dir=store_dir,
        output_dir=output_dir,
        max_products=max_products,
        detail_mode=detail_mode,
        discovery_connector=discovery_connector,
        product_client=product_client,
        captured_at=captured_at,
    )

    analysis_result: AnalysisStackRunResult | None = None
    if post_analysis != "none" and "product_intelligence_records" in collection_result.artifacts:
        analysis_result = run_analysis_stack(
            collection_dir=collection_result.output_dir,
            include_deep_inference=post_analysis == "deep_inference",
            deep_inference_model=deep_inference_model,
            deep_inference_reasoning_effort=deep_inference_reasoning_effort,
            deep_inference_client=deep_inference_client,
            generated_at=captured_at,
        )

    combined_status = collection_result.status
    if analysis_result is not None:
        combined_status = _combine_statuses(collection_result.status, analysis_result.status)

    combined_artifacts = dict(collection_result.artifacts)
    if analysis_result is not None:
        combined_artifacts.update(analysis_result.artifacts)

    return CollectionAndAnalysisRunResult(
        status=combined_status,
        collection=collection_result,
        analysis=analysis_result,
        artifacts=combined_artifacts,
    )


def render_analysis_stack_markdown(report: dict[str, object]) -> str:
    lines = [
        "# Analysis Stack Report",
        "",
        f"Run: `{report['run_id']}`",
        f"Status: `{report['status']}`",
        f"Collection dir: `{report['collection_dir']}`",
        f"Deep inference requested: `{report['include_deep_inference']}`",
        "",
        "## Step Summary",
        "",
        "| Step | Required | Build Status | Report Status | Notes |",
        "| --- | --- | --- | --- | --- |",
    ]
    for step in report.get("step_results", []):
        if not isinstance(step, dict):
            continue
        note = step.get("error") or "; ".join(step.get("warnings", []))
        lines.append(
            "| {step_name} | {required} | {status} | {report_status} | {note} |".format(
                step_name=step.get("step", ""),
                required="yes" if step.get("required") else "no",
                status=step.get("status", ""),
                report_status=step.get("report_status", ""),
                note=_escape_table(str(note or "")),
            )
        )

    lines.extend(["", "## Warnings", ""])
    warnings = report.get("warnings", [])
    if isinstance(warnings, list) and warnings:
        for item in warnings:
            lines.append(f"- {item}")
    else:
        lines.append("- No stack-level warnings recorded.")
    lines.append("")
    return "\n".join(lines)


def _build_report(
    *,
    run_id: str,
    generated_at: str,
    collection_dir: Path,
    include_deep_inference: bool,
    step_results: list[AnalysisStepResult],
    artifacts: dict[str, str],
) -> dict[str, object]:
    step_dicts = [step.to_dict() for step in step_results]
    completed_steps = [step.step for step in step_results if step.status == "success"]
    failed_steps = [step.step for step in step_results if step.status == "failed"]
    warnings = _stack_warnings(step_results)
    report = {
        "run_id": run_id,
        "generated_at": generated_at,
        "collection_dir": str(collection_dir),
        "status": _stack_status(step_results),
        "include_deep_inference": include_deep_inference,
        "step_results": step_dicts,
        "completed_steps": completed_steps,
        "failed_steps": failed_steps,
        "artifacts": artifacts,
        "warnings": warnings,
    }
    return report


def _summarize_step_report(report_artifact_key: str, artifacts: dict[str, str]) -> tuple[str, list[str]]:
    report_path = artifacts.get(report_artifact_key)
    if report_path is None:
        return "unknown", ["report artifact path missing from step output"]

    payload = _load_json_object(Path(report_path))
    report_status = _coerce_status(payload.get("status"))
    warnings = _coerce_string_list(payload.get("caveats"))
    return report_status, warnings


def _stack_status(step_results: list[AnalysisStepResult]) -> str:
    required_failures = [step for step in step_results if step.required and step.status == "failed"]
    optional_failures = [step for step in step_results if not step.required and step.status == "failed"]
    non_success_reports = [
        step
        for step in step_results
        if step.status == "success" and step.report_status != "success"
    ]

    if required_failures:
        successful_required = [step for step in step_results if step.required and step.status == "success"]
        return "partial_success" if successful_required else "failed"
    if optional_failures or non_success_reports:
        return "partial_success"
    if not any(step.status == "success" for step in step_results):
        return "failed"
    return "success"


def _stack_warnings(step_results: list[AnalysisStepResult]) -> list[str]:
    warnings: list[str] = []
    for step in step_results:
        if step.status == "failed":
            warnings.append(f"{step.step} failed: {step.error}")
            continue
        if step.status == "skipped":
            warnings.append(f"{step.step} skipped: {step.error}")
            continue
        if step.report_status != "success":
            warnings.append(f"{step.step} completed with report status `{step.report_status}`")
        for warning in step.warnings:
            warnings.append(f"{step.step}: {warning}")
    return warnings


def _infer_run_id(collection_dir: Path) -> str:
    report_path = collection_dir / "data_collection_report.json"
    if report_path.exists():
        payload = _load_json_object(report_path)
        run_id = payload.get("run_id")
        if isinstance(run_id, str) and run_id.strip():
            return run_id
    return collection_dir.name


def _combine_statuses(left: str, right: str) -> str:
    severity = {"success": 0, "partial_success": 1, "failed": 2}
    if severity.get(left, 2) >= severity.get(right, 2):
        return left
    return right


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _coerce_status(value: object) -> str:
    text = str(value or "").strip()
    if text in {"success", "partial_success", "failed"}:
        return text
    if text == "not_run":
        return "not_run"
    return "unknown"


def _coerce_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            items.append(text)
    return items


def _load_json_object(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object JSON payload in {path}")
    return payload


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|")
