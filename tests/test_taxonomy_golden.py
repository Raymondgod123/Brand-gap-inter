from __future__ import annotations

import json
import unittest
from pathlib import Path

from brand_gap_inference.taxonomy import TaxonomyAssigner

ROOT = Path(__file__).resolve().parents[1]
GOLDEN_CASES_PATH = ROOT / "eval" / "fixtures" / "taxonomy_golden" / "cases.json"


class TaxonomyGoldenFixtureTests(unittest.TestCase):
    def test_golden_taxonomy_cases_match_expected_axes(self) -> None:
        cases = json.loads(GOLDEN_CASES_PATH.read_text(encoding="utf-8"))
        assigner = TaxonomyAssigner()

        for case in cases:
            with self.subTest(case_id=case["case_id"]):
                result = assigner.assign_batch([case["listing"]], snapshot_id=f"golden-{case['case_id']}")
                self.assertEqual("success", result.summary.run_status)

                record = result.records[0]
                assignment = result.assignments[0]
                expected = case["expected"]

                self.assertEqual(expected["axes"], assignment["axes"])
                if "min_confidence" in expected:
                    self.assertGreaterEqual(assignment["confidence"], expected["min_confidence"])
                if "max_confidence" in expected:
                    self.assertLessEqual(assignment["confidence"], expected["max_confidence"])
                self.assertEqual(expected["warning_count"], len(record.warnings))

    def test_golden_fixture_file_is_readable_and_complete(self) -> None:
        cases = json.loads(GOLDEN_CASES_PATH.read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(cases), 6)

        for case in cases:
            with self.subTest(case_id=case["case_id"]):
                self.assertIn("description", case)
                self.assertIn("listing", case)
                self.assertIn("expected", case)
                self.assertIn("axes", case["expected"])
                self.assertTrue(case["description"].strip())


if __name__ == "__main__":
    unittest.main()
