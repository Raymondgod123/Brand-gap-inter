from __future__ import annotations

import unittest
from pathlib import Path

from brand_gap_inference.brand_analysis_eval import evaluate_batches, load_batches, load_thresholds

ROOT = Path(__file__).resolve().parents[1]
CASES_PATH = ROOT / "eval" / "fixtures" / "brand_positioning_golden" / "batches.json"
THRESHOLDS_PATH = ROOT / "eval" / "thresholds_brand_positioning.json"


class BrandAnalysisEvalTests(unittest.TestCase):
    def test_brand_positioning_golden_fixture_passes_eval_gate(self) -> None:
        report = evaluate_batches(load_batches(CASES_PATH), load_thresholds(THRESHOLDS_PATH))
        self.assertTrue(report["passed"])


if __name__ == "__main__":
    unittest.main()
