from __future__ import annotations

import json
import unittest
from copy import deepcopy
from pathlib import Path

from brand_gap_inference.eval_runner import evaluate_bundle, load_fixture_dir, load_thresholds

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "eval" / "fixtures" / "phase1"
THRESHOLDS_PATH = ROOT / "eval" / "thresholds.json"


class EvalRunnerTests(unittest.TestCase):
    def test_phase1_fixture_bundle_passes_eval_gate(self) -> None:
        bundle = load_fixture_dir(FIXTURE_DIR)
        thresholds = load_thresholds(THRESHOLDS_PATH)
        report = evaluate_bundle(bundle, thresholds)
        self.assertTrue(report["passed"])

    def test_unknown_evidence_source_fails_eval_gate(self) -> None:
        bundle = load_fixture_dir(FIXTURE_DIR)
        thresholds = load_thresholds(THRESHOLDS_PATH)
        mutated_bundle = deepcopy(bundle)
        mutated_bundle.opportunities[0]["evidence"][0]["source_record_ids"] = ["missing-source"]
        report = evaluate_bundle(mutated_bundle, thresholds)
        self.assertFalse(report["passed"])
        self.assertTrue(any("unknown source_record_id" in failure for failure in report["failures"]))


if __name__ == "__main__":
    unittest.main()
