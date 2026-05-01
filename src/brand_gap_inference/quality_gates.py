"""Run the local quality gates that mirror CI."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class QualityGate:
    name: str
    command: tuple[str, ...]


def build_gate_commands(python_executable: str = sys.executable, *, include_unit_tests: bool = True) -> list[QualityGate]:
    """Return the local gate plan in the same order as the CI workflow."""

    gates: list[QualityGate] = []
    if include_unit_tests:
        gates.append(
            QualityGate(
                name="unit_tests",
                command=(python_executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"),
            )
        )

    gates.extend(
        [
            QualityGate(
                name="phase1_eval",
                command=(
                    python_executable,
                    "-m",
                    "brand_gap_inference.eval_runner",
                    "--fixtures-dir",
                    "eval/fixtures/phase1",
                    "--thresholds",
                    "eval/thresholds.json",
                ),
            ),
            QualityGate(
                name="discovery_eval",
                command=(
                    python_executable,
                    "-m",
                    "brand_gap_inference.discovery_eval",
                    "--cases",
                    "eval/fixtures/discovery_golden/batches.json",
                    "--thresholds",
                    "eval/thresholds_discovery.json",
                ),
            ),
            QualityGate(
                name="taxonomy_eval",
                command=(
                    python_executable,
                    "-m",
                    "brand_gap_inference.taxonomy_eval",
                    "--cases",
                    "eval/fixtures/taxonomy_golden/cases.json",
                    "--thresholds",
                    "eval/taxonomy_thresholds.json",
                ),
            ),
            QualityGate(
                name="normalization_eval",
                command=(
                    python_executable,
                    "-m",
                    "brand_gap_inference.normalization_eval",
                    "--cases",
                    "eval/fixtures/normalization_golden/batches.json",
                    "--thresholds",
                    "eval/normalization_thresholds.json",
                ),
            ),
            QualityGate(
                name="product_intelligence_eval",
                command=(
                    python_executable,
                    "-m",
                    "brand_gap_inference.product_intelligence_eval",
                    "--cases",
                    "eval/fixtures/product_intelligence_golden/batches.json",
                    "--thresholds",
                    "eval/thresholds_product_intelligence.json",
                ),
            ),
            QualityGate(
                name="brand_positioning_eval",
                command=(
                    python_executable,
                    "-m",
                    "brand_gap_inference.brand_analysis_eval",
                    "--cases",
                    "eval/fixtures/brand_positioning_golden/batches.json",
                    "--thresholds",
                    "eval/thresholds_brand_positioning.json",
                ),
            ),
            QualityGate(
                name="brand_profile_eval",
                command=(
                    python_executable,
                    "-m",
                    "brand_gap_inference.brand_profile_eval",
                    "--cases",
                    "eval/fixtures/brand_profile_golden/batches.json",
                    "--thresholds",
                    "eval/thresholds_brand_profile.json",
                ),
            ),
            QualityGate(
                name="demand_signal_eval",
                command=(
                    python_executable,
                    "-m",
                    "brand_gap_inference.demand_signal_eval",
                    "--cases",
                    "eval/fixtures/demand_signal_golden/batches.json",
                    "--thresholds",
                    "eval/thresholds_demand_signal.json",
                ),
            ),
            QualityGate(
                name="gap_validation_eval",
                command=(
                    python_executable,
                    "-m",
                    "brand_gap_inference.gap_validation_eval",
                    "--cases",
                    "eval/fixtures/gap_validation_golden/batches.json",
                    "--thresholds",
                    "eval/thresholds_gap_validation.json",
                ),
            ),
            QualityGate(
                name="decision_brief_eval",
                command=(
                    python_executable,
                    "-m",
                    "brand_gap_inference.decision_brief_eval",
                    "--cases",
                    "eval/fixtures/decision_brief_golden/batches.json",
                    "--thresholds",
                    "eval/thresholds_decision_brief.json",
                ),
            ),
            QualityGate(
                name="deep_inference_eval",
                command=(
                    python_executable,
                    "-m",
                    "brand_gap_inference.deep_inference_eval",
                    "--cases",
                    "eval/fixtures/deep_inference_golden/batches.json",
                    "--thresholds",
                    "eval/thresholds_deep_inference.json",
                ),
            ),
        ]
    )
    return gates


def _local_env(repo_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    existing = env.get("PYTHONPATH")
    if existing:
        paths = existing.split(os.pathsep)
        if src_path not in paths:
            env["PYTHONPATH"] = os.pathsep.join([src_path, existing])
    else:
        env["PYTHONPATH"] = src_path
    return env


def run_gates(gates: Iterable[QualityGate], *, repo_root: Path, fail_fast: bool) -> dict[str, object]:
    gate_list = list(gates)
    started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    results: list[dict[str, object]] = []
    env = _local_env(repo_root)

    for gate in gate_list:
        print(f"[quality-gates] running {gate.name}", flush=True)
        completed = subprocess.run(
            gate.command,
            cwd=repo_root,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        status = "passed" if completed.returncode == 0 else "failed"
        print(f"[quality-gates] {gate.name}: {status}", flush=True)
        results.append(
            {
                "name": gate.name,
                "status": status,
                "return_code": completed.returncode,
                "command": list(gate.command),
                "stdout": completed.stdout,
                "stderr": completed.stderr,
            }
        )
        if fail_fast and completed.returncode != 0:
            break

    passed = all(result["status"] == "passed" for result in results) and len(results) == len(gate_list)
    finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "status": "passed" if passed else "failed",
        "started_at": started_at,
        "finished_at": finished_at,
        "total_gates": len(results),
        "passed_gates": sum(1 for result in results if result["status"] == "passed"),
        "failed_gates": sum(1 for result in results if result["status"] == "failed"),
        "results": results,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local CI quality gates.")
    parser.add_argument("--output", default="artifacts/quality-gates-latest.json")
    parser.add_argument("--skip-unit-tests", action="store_true")
    parser.add_argument("--fail-fast", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    repo_root = Path.cwd()
    gates = build_gate_commands(include_unit_tests=not args.skip_unit_tests)
    report = run_gates(gates, repo_root=repo_root, fail_fast=args.fail_fast)

    output_path = repo_root / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("status", "total_gates", "passed_gates", "failed_gates")}, indent=2))
    print(f"[quality-gates] wrote {output_path}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
