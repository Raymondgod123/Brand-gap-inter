from __future__ import annotations

import unittest
from pathlib import Path

from brand_gap_inference.gap_validation_eval import evaluate_batches, load_batches, load_thresholds

ROOT = Path(__file__).resolve().parents[1]
CASES_PATH = ROOT / "eval" / "fixtures" / "gap_validation_golden" / "batches.json"
THRESHOLDS_PATH = ROOT / "eval" / "thresholds_gap_validation.json"


class GapValidationEvalTests(unittest.TestCase):
    def test_gap_validation_golden_fixture_passes_eval_gate(self) -> None:
        report = evaluate_batches(load_batches(CASES_PATH), load_thresholds(THRESHOLDS_PATH))
        self.assertTrue(report["passed"])


if __name__ == "__main__":
    unittest.main()
