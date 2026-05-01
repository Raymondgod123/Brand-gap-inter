from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import shutil

from .deep_inference import write_deep_inference_artifacts

TMP_ROOT = Path(__file__).resolve().parents[2] / ".tmp-tests"


@dataclass(frozen=True)
class GoldenDeepInferenceBatch:
    batch_id: str
    description: str
    product_intelligence_records: list[dict]
    landscape_report: dict
    brand_positioning_report: dict
    brand_profile_report: dict
    demand_signal_report: dict
    gap_validation_report: dict
    decision_brief_report: dict
    client_response: dict
    expected: dict


class FixtureDeepInferenceClient:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response

    def infer(
        self,
        *,
        model: str,
        reasoning_effort: str,
        prompt: str,
        schema: dict[str, object],
    ) -> dict[str, object]:
        if "Landscape report JSON" not in prompt or "Product intelligence records JSON" not in prompt:
            raise ValueError("deep inference prompt is missing required upstream context blocks")
        if "Brand profile report JSON" not in prompt or "Gap validation report JSON" not in prompt:
            raise ValueError("deep inference prompt is missing brand profile or gap validation context")
        if "Demand signal report JSON" not in prompt:
            raise ValueError("deep inference prompt is missing demand signal context")
        if "Decision brief report JSON" not in prompt:
            raise ValueError("deep inference prompt is missing decision brief context")
        if model != "gpt-5.4":
            raise ValueError(f"unexpected model {model!r} in deep inference eval")
        if reasoning_effort != "high":
            raise ValueError(f"unexpected reasoning effort {reasoning_effort!r} in deep inference eval")
        if schema.get("title") != "DeepBrandInferenceReport":
            raise ValueError("unexpected schema title for deep inference eval")
        return dict(self.response)


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_batches(path: Path) -> list[GoldenDeepInferenceBatch]:
    raw_batches = list(load_json(path))
    return [
        GoldenDeepInferenceBatch(
            batch_id=batch["batch_id"],
            description=batch["description"],
            product_intelligence_records=batch["product_intelligence_records"],
            landscape_report=batch["landscape_report"],
            brand_positioning_report=batch["brand_positioning_report"],
            brand_profile_report=batch["brand_profile_report"],
            demand_signal_report=batch["demand_signal_report"],
            gap_validation_report=batch["gap_validation_report"],
            decision_brief_report=batch["decision_brief_report"],
            client_response=batch["client_response"],
            expected=batch["expected"],
        )
        for batch in raw_batches
    ]


def load_thresholds(path: Path) -> dict:
    return dict(load_json(path))


def evaluate_batches(batches: list[GoldenDeepInferenceBatch], thresholds: dict) -> dict:
    failures: list[str] = []
    status_hits = 0
    brand_hits = 0
    whitespace_hits = 0
    caveat_hits = 0
    stability_hits = 0
    total_batches = len(batches)
    batch_results: list[dict] = []

    for batch in batches:
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        scratch_dir = TMP_ROOT / f"deep-inference-eval-{batch.batch_id}"
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        try:
            collection_dir = scratch_dir / "collection"
            _write_collection_fixture(collection_dir, batch)

            first_output_dir = collection_dir / "deep_inference_first"
            second_output_dir = collection_dir / "deep_inference_second"

            first_artifacts = write_deep_inference_artifacts(
                collection_dir=collection_dir,
                output_dir=first_output_dir,
                client=FixtureDeepInferenceClient(batch.client_response),
                model="gpt-5.4",
                reasoning_effort="high",
            )
            second_artifacts = write_deep_inference_artifacts(
                collection_dir=collection_dir,
                output_dir=second_output_dir,
                client=FixtureDeepInferenceClient(batch.client_response),
                model="gpt-5.4",
                reasoning_effort="high",
            )

            first_report = load_json(Path(first_artifacts["deep_brand_inference_report"]))
            second_report = load_json(Path(second_artifacts["deep_brand_inference_report"]))
            first_markdown = Path(first_artifacts["deep_brand_inference_report_md"]).read_text(encoding="utf-8")
            second_markdown = Path(second_artifacts["deep_brand_inference_report_md"]).read_text(encoding="utf-8")
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

        stable = first_report == second_report and first_markdown == second_markdown
        if stable:
            stability_hits += 1
        else:
            failures.append(f"{batch.batch_id} deep inference output is unstable across repeat runs")

        expected = batch.expected
        if first_report.get("status") == expected.get("status"):
            status_hits += 1
        else:
            failures.append(
                f"{batch.batch_id} status mismatch: expected {expected.get('status')!r}, got {first_report.get('status')!r}"
            )

        if _brand_profiles_match(first_report, expected.get("brand_profiles", {}), failures, batch.batch_id):
            brand_hits += 1

        if _string_list_matches(
            actual=list(first_report.get("whitespace_opportunities", [])),
            expected=list(expected.get("whitespace_opportunities", [])),
            label="whitespace opportunities",
            failures=failures,
            batch_id=batch.batch_id,
        ):
            whitespace_hits += 1

        if _string_list_matches(
            actual=list(first_report.get("caveats", [])),
            expected=list(expected.get("caveats", [])),
            label="caveats",
            failures=failures,
            batch_id=batch.batch_id,
        ):
            caveat_hits += 1

        batch_results.append(
            {
                "batch_id": batch.batch_id,
                "status": first_report.get("status"),
                "stable": stable,
                "brand_profiles": len(first_report.get("brand_profiles", [])),
                "whitespace_opportunities": len(first_report.get("whitespace_opportunities", [])),
            }
        )

    metrics = {
        "deep_inference_status_accuracy": _safe_ratio(status_hits, total_batches),
        "deep_inference_brand_profile_accuracy": _safe_ratio(brand_hits, total_batches),
        "deep_inference_whitespace_accuracy": _safe_ratio(whitespace_hits, total_batches),
        "deep_inference_caveat_accuracy": _safe_ratio(caveat_hits, total_batches),
        "deep_inference_repeat_run_stability": _safe_ratio(stability_hits, total_batches),
    }
    failures.extend(_evaluate_thresholds(metrics, thresholds))
    return {"passed": not failures, "metrics": metrics, "failures": failures, "batches": batch_results}


def _write_collection_fixture(collection_dir: Path, batch: GoldenDeepInferenceBatch) -> None:
    (collection_dir / "product_intelligence").mkdir(parents=True, exist_ok=True)
    (collection_dir / "landscape").mkdir(parents=True, exist_ok=True)
    (collection_dir / "brand_positioning").mkdir(parents=True, exist_ok=True)
    (collection_dir / "brand_profiles").mkdir(parents=True, exist_ok=True)
    (collection_dir / "demand_signals").mkdir(parents=True, exist_ok=True)
    (collection_dir / "gap_validation").mkdir(parents=True, exist_ok=True)
    (collection_dir / "decision_brief").mkdir(parents=True, exist_ok=True)
    (collection_dir / "product_intelligence" / "product_intelligence_records.json").write_text(
        json.dumps(batch.product_intelligence_records, indent=2),
        encoding="utf-8",
    )
    (collection_dir / "landscape" / "landscape_report.json").write_text(
        json.dumps(batch.landscape_report, indent=2),
        encoding="utf-8",
    )
    (collection_dir / "brand_positioning" / "brand_positioning_report.json").write_text(
        json.dumps(batch.brand_positioning_report, indent=2),
        encoding="utf-8",
    )
    (collection_dir / "brand_profiles" / "brand_profile_report.json").write_text(
        json.dumps(batch.brand_profile_report, indent=2),
        encoding="utf-8",
    )
    (collection_dir / "demand_signals" / "demand_signal_report.json").write_text(
        json.dumps(batch.demand_signal_report, indent=2),
        encoding="utf-8",
    )
    (collection_dir / "gap_validation" / "gap_validation_report.json").write_text(
        json.dumps(batch.gap_validation_report, indent=2),
        encoding="utf-8",
    )
    (collection_dir / "decision_brief" / "decision_brief_report.json").write_text(
        json.dumps(batch.decision_brief_report, indent=2),
        encoding="utf-8",
    )


def _brand_profiles_match(report: dict, expectations: dict, failures: list[str], batch_id: str) -> bool:
    by_asin = {profile.get("asin"): profile for profile in report.get("brand_profiles", []) if isinstance(profile, dict)}
    matched = True
    for asin, expected_profile in expectations.items():
        profile = by_asin.get(asin)
        if profile is None:
            failures.append(f"{batch_id}:{asin} missing from deep inference brand profiles")
            matched = False
            continue
        for field_name, expected_value in expected_profile.items():
            actual_value = profile.get(field_name)
            if actual_value != expected_value:
                failures.append(
                    f"{batch_id}:{asin}:{field_name} mismatch: expected {expected_value!r}, got {actual_value!r}"
                )
                matched = False
    return matched


def _string_list_matches(
    *,
    actual: list[str],
    expected: list[str],
    label: str,
    failures: list[str],
    batch_id: str,
) -> bool:
    missing = [item for item in expected if item not in actual]
    if missing:
        failures.append(f"{batch_id} missing {label} {missing!r}")
        return False
    return True


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
    parser = argparse.ArgumentParser(description="Run deep brand inference evals against golden fixtures.")
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--thresholds", type=Path, required=True)
    args = parser.parse_args(argv)

    report = evaluate_batches(load_batches(args.cases), load_thresholds(args.thresholds))
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
