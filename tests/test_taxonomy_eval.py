from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.taxonomy_eval import evaluate_cases, load_cases, load_thresholds

ROOT = Path(__file__).resolve().parents[1]
CASES_PATH = ROOT / "eval" / "fixtures" / "taxonomy_golden" / "cases.json"
THRESHOLDS_PATH = ROOT / "eval" / "taxonomy_thresholds.json"
SCRATCH_ROOT = ROOT / ".tmp-tests"


class TaxonomyEvalTests(unittest.TestCase):
    def test_taxonomy_eval_passes_for_current_golden_cases(self) -> None:
        report = evaluate_cases(load_cases(CASES_PATH), load_thresholds(THRESHOLDS_PATH))
        self.assertTrue(report["passed"])
        self.assertEqual(1.0, report["metrics"]["taxonomy_repeat_run_stability"])

    def test_taxonomy_eval_fails_for_bad_expected_axes(self) -> None:
        scratch_dir = self._make_scratch_dir("taxonomy-eval-bad-axes")
        try:
            temp_path = scratch_dir / "cases.json"
            cases = json.loads(CASES_PATH.read_text(encoding="utf-8"))
            cases[0]["expected"]["axes"]["need_state"] = "energy_boost"
            temp_path.write_text(json.dumps(cases, indent=2), encoding="utf-8")

            report = evaluate_cases(load_cases(temp_path), load_thresholds(THRESHOLDS_PATH))
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

        self.assertFalse(report["passed"])
        self.assertTrue(any("axes mismatch" in failure for failure in report["failures"]))
        self.assertLess(report["metrics"]["taxonomy_case_accuracy"], 1.0)

    def test_taxonomy_eval_fails_when_warning_expectation_drifts(self) -> None:
        scratch_dir = self._make_scratch_dir("taxonomy-eval-warning-drift")
        try:
            temp_path = scratch_dir / "cases.json"
            cases = json.loads(CASES_PATH.read_text(encoding="utf-8"))
            cases[-1]["expected"]["warning_count"] = 5
            temp_path.write_text(json.dumps(cases, indent=2), encoding="utf-8")

            report = evaluate_cases(load_cases(temp_path), load_thresholds(THRESHOLDS_PATH))
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

        self.assertFalse(report["passed"])
        self.assertTrue(any("warning count mismatch" in failure for failure in report["failures"]))

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
