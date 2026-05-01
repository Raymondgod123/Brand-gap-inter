from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Protocol
from urllib import request

from .contracts import assert_valid
from .schema_registry import load_schema

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"


class DeepInferenceClient(Protocol):
    def infer(
        self,
        *,
        model: str,
        reasoning_effort: str,
        prompt: str,
        schema: dict[str, object],
    ) -> dict[str, object]: ...


class OpenAIResponsesDeepInferenceClient:
    def __init__(self, *, api_key: str | None = None, base_url: str = OPENAI_RESPONSES_URL) -> None:
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY is required for GPT-5.4 deep inference")
        self.base_url = base_url

    def infer(
        self,
        *,
        model: str,
        reasoning_effort: str,
        prompt: str,
        schema: dict[str, object],
    ) -> dict[str, object]:
        payload = {
            "model": model,
            "reasoning": {"effort": reasoning_effort},
            "input": [
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": (
                                "You are an evidence-first product intelligence analyst. "
                                "Use only the supplied records and caveats. "
                                "Do not invent missing facts. "
                                "When evidence is thin, express caveats explicitly."
                            ),
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                },
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "deep_brand_inference_report",
                    "strict": True,
                    "schema": schema,
                }
            },
        }
        req = request.Request(
            self.base_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with request.urlopen(req, timeout=180) as response:
            body = json.loads(response.read().decode("utf-8"))
        text = _extract_response_text(body)
        report = json.loads(text)
        if not isinstance(report, dict):
            raise ValueError("deep inference response was not a JSON object")
        return report


def write_deep_inference_artifacts(
    *,
    collection_dir: Path,
    output_dir: Path,
    client: DeepInferenceClient | None = None,
    model: str = "gpt-5.4",
    reasoning_effort: str = "high",
) -> dict[str, str]:
    product_intelligence_records = _load_json(
        collection_dir / "product_intelligence" / "product_intelligence_records.json"
    )
    landscape_report = _load_json(collection_dir / "landscape" / "landscape_report.json")
    brand_positioning_report = _load_json(collection_dir / "brand_positioning" / "brand_positioning_report.json")
    brand_profile_report = _load_json(collection_dir / "brand_profiles" / "brand_profile_report.json")
    demand_signal_report = _load_json(collection_dir / "demand_signals" / "demand_signal_report.json")
    gap_validation_report = _load_json(collection_dir / "gap_validation" / "gap_validation_report.json")
    decision_brief_report = _load_json(collection_dir / "decision_brief" / "decision_brief_report.json")

    if not isinstance(product_intelligence_records, list):
        raise ValueError("expected product intelligence records list")
    if not isinstance(landscape_report, dict):
        raise ValueError("expected landscape report object")
    if not isinstance(brand_positioning_report, dict):
        raise ValueError("expected brand positioning report object")
    if not isinstance(brand_profile_report, dict):
        raise ValueError("expected brand profile report object")
    if not isinstance(demand_signal_report, dict):
        raise ValueError("expected demand signal report object")
    if not isinstance(gap_validation_report, dict):
        raise ValueError("expected gap validation report object")
    if not isinstance(decision_brief_report, dict):
        raise ValueError("expected decision brief report object")

    schema = load_schema("deep_brand_inference_report.schema.json")
    run_id = str(
        gap_validation_report.get("run_id")
        or brand_profile_report.get("run_id")
        or brand_positioning_report.get("run_id")
        or landscape_report.get("run_id")
        or collection_dir.name
    )
    prompt = _build_prompt(
        run_id=run_id,
        product_intelligence_records=product_intelligence_records,
        landscape_report=landscape_report,
        brand_positioning_report=brand_positioning_report,
        brand_profile_report=brand_profile_report,
        demand_signal_report=demand_signal_report,
        gap_validation_report=gap_validation_report,
        decision_brief_report=decision_brief_report,
    )
    resolved_client = client or OpenAIResponsesDeepInferenceClient()
    report = resolved_client.infer(
        model=model,
        reasoning_effort=reasoning_effort,
        prompt=prompt,
        schema=schema,
    )
    report["run_id"] = run_id
    report["model"] = model
    assert_valid("deep_brand_inference_report", report)

    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "deep_brand_inference_report.json"
    md_path = output_dir / "deep_brand_inference_report.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    md_path.write_text(render_deep_inference_markdown(report), encoding="utf-8")
    return {
        "deep_brand_inference_report": str(json_path),
        "deep_brand_inference_report_md": str(md_path),
    }


def render_deep_inference_markdown(report: dict[str, object]) -> str:
    lines = [
        "# Deep Brand Inference Report",
        "",
        f"Run: `{report['run_id']}`",
        f"Model: `{report['model']}`",
        f"Status: `{report['status']}`",
        "",
        "## Executive Summary",
        "",
        str(report["executive_summary"]),
        "",
        "## Market Overview",
        "",
        str(report["market_overview"]),
        "",
        "## Brand Profiles",
        "",
    ]
    for profile in report["brand_profiles"]:
        lines.extend(
            [
                f"### {profile['brand_name']} (`{profile['asin']}`)",
                "",
                f"- Positioning: {profile['positioning_summary']}",
                f"- Audience: {profile['target_audience']}",
                f"- Pricing posture: {profile['pricing_posture']}",
                f"- Visual identity: {profile['visual_identity']}",
                f"- Confidence: {profile['confidence']}",
                f"- Claim themes: {', '.join(profile['claim_themes']) if profile['claim_themes'] else 'none'}",
                f"- Evidence refs: {', '.join(profile['evidence_refs']) if profile['evidence_refs'] else 'none'}",
                "",
            ]
        )

    lines.extend(["## Whitespace Opportunities", ""])
    whitespace = report.get("whitespace_opportunities", [])
    if whitespace:
        for item in whitespace:
            lines.append(f"- {item}")
    else:
        lines.append("- None identified.")

    lines.extend(["", "## Risks", ""])
    risks = report.get("risks", [])
    if risks:
        for item in risks:
            lines.append(f"- {item}")
    else:
        lines.append("- No major risks identified.")

    lines.extend(["", "## Evidence Notes", ""])
    notes = report.get("evidence_notes", [])
    if notes:
        for item in notes:
            lines.append(f"- {item}")
    else:
        lines.append("- No additional evidence notes.")

    lines.extend(["", "## Caveats", ""])
    caveats = report.get("caveats", [])
    if caveats:
        for item in caveats:
            lines.append(f"- {item}")
    else:
        lines.append("- No caveats recorded.")

    return "\n".join(lines) + "\n"


def _build_prompt(
    *,
    run_id: str,
    product_intelligence_records: list[dict[str, object]],
    landscape_report: dict[str, object],
    brand_positioning_report: dict[str, object],
    brand_profile_report: dict[str, object],
    demand_signal_report: dict[str, object],
    gap_validation_report: dict[str, object],
    decision_brief_report: dict[str, object],
) -> str:
    return (
        "Build a final deep inference report for this competitive set. "
        "Use only the supplied structured records and caveats. "
        "Keep the analysis evidence-first and avoid invented facts.\n\n"
        f"Run ID: {run_id}\n\n"
        "Landscape report JSON:\n"
        f"{json.dumps(landscape_report, indent=2)}\n\n"
        "Deterministic brand positioning report JSON:\n"
        f"{json.dumps(brand_positioning_report, indent=2)}\n\n"
        "Brand profile report JSON:\n"
        f"{json.dumps(brand_profile_report, indent=2)}\n\n"
        "Demand signal report JSON:\n"
        f"{json.dumps(demand_signal_report, indent=2)}\n\n"
        "Gap validation report JSON:\n"
        f"{json.dumps(gap_validation_report, indent=2)}\n\n"
        "Decision brief report JSON:\n"
        f"{json.dumps(decision_brief_report, indent=2)}\n\n"
        "Product intelligence records JSON:\n"
        f"{json.dumps(product_intelligence_records, indent=2)}\n"
    )


def _load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _extract_response_text(response_body: dict[str, object]) -> str:
    output_text = response_body.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    output = response_body.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                text = block.get("text")
                if isinstance(text, str) and text.strip():
                    return text
                if block.get("type") == "output_text" and isinstance(block.get("text"), str):
                    return str(block["text"])
    raise ValueError("could not extract structured text from OpenAI response")
