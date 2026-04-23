from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.normalization_eval import evaluate_batches, load_batches, load_thresholds

ROOT = Path(__file__).resolve().parents[1]
CASES_PATH = ROOT / "eval" / "fixtures" / "normalization_golden" / "batches.json"
THRESHOLDS_PATH = ROOT / "eval" / "normalization_thresholds.json"
SCRATCH_ROOT = ROOT / ".tmp-tests"


class NormalizationEvalTests(unittest.TestCase):
    def test_normalization_eval_passes_for_current_golden_batches(self) -> None:
        report = evaluate_batches(load_batches(CASES_PATH), load_thresholds(THRESHOLDS_PATH))
        self.assertTrue(report["passed"])
        self.assertEqual(1.0, report["metrics"]["normalization_record_status_accuracy"])
        self.assertEqual(1.0, report["metrics"]["normalization_repeat_run_stability"])

    def test_normalization_eval_fails_when_expected_status_is_wrong(self) -> None:
        scratch_dir = self._make_scratch_dir("normalization-eval-bad-status")
        try:
            temp_path = scratch_dir / "batches.json"
            batches = json.loads(CASES_PATH.read_text(encoding="utf-8"))
            batches[0]["expected"]["record_expectations"]["clean-1"]["status"] = "invalid"
            temp_path.write_text(json.dumps(batches, indent=2), encoding="utf-8")

            report = evaluate_batches(load_batches(temp_path), load_thresholds(THRESHOLDS_PATH))
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

        self.assertFalse(report["passed"])
        self.assertTrue(any("status mismatch" in failure for failure in report["failures"]))

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()

