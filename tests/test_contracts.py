from __future__ import annotations

import json
import unittest
from pathlib import Path

from brand_gap_inference.contracts import validate_document
from brand_gap_inference.run_metadata import RunManifest, RunTaskEnvelope

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "eval" / "fixtures" / "phase1"


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


class ContractValidationTests(unittest.TestCase):
    def test_normalized_listing_fixture_is_valid(self) -> None:
        listings = load_json(FIXTURE_DIR / "normalized_listings.json")
        issues = validate_document("normalized_listing", listings[0])
        self.assertEqual([], issues)

    def test_missing_required_listing_field_fails(self) -> None:
        listing = load_json(FIXTURE_DIR / "normalized_listings.json")[0]
        listing.pop("product_title")
        issues = validate_document("normalized_listing", listing)
        self.assertTrue(any(issue.path == "$.product_title" for issue in issues))

    def test_run_manifest_dataclass_requires_valid_payload(self) -> None:
        manifest = load_json(FIXTURE_DIR / "run_manifest.json")
        instance = RunManifest.from_dict(manifest)
        self.assertEqual("run-2026-04-22T09-00-00Z", instance.run_id)
        self.assertIn("runs", instance.artifact_root_uri)

    def test_task_envelope_dataclass_requires_valid_payload(self) -> None:
        task = load_json(FIXTURE_DIR / "task_envelopes.json")[0]
        instance = RunTaskEnvelope.from_dict(task)
        self.assertEqual("task-001", instance.task_id)


if __name__ == "__main__":
    unittest.main()
