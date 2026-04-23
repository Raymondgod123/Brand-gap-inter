from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.normalization import BatchNormalizer, write_normalization_artifacts
from brand_gap_inference.raw_store import SourceSnapshotManifest
from brand_gap_inference.taxonomy import TaxonomyAssigner, write_taxonomy_artifacts
from brand_gap_inference.connectors import RawSourceRecord

ROOT = Path(__file__).resolve().parents[1]
DIRTY_FIXTURE_PATH = ROOT / "fixtures" / "normalization" / "amazon_dirty_cases.json"
SCRATCH_ROOT = ROOT / ".tmp-tests"


class TaxonomyTests(unittest.TestCase):
    def test_taxonomy_assigns_live_sweetener_listing(self) -> None:
        listing = self._sweetener_listing()

        result = TaxonomyAssigner().assign_batch([listing], snapshot_id="snapshot-live")

        self.assertEqual("success", result.summary.run_status)
        assignment = result.assignments[0]
        self.assertEqual("sugar_replacement", assignment["axes"]["need_state"])
        self.assertEqual("baking", assignment["axes"]["occasion"])
        self.assertEqual("powder", assignment["axes"]["format"])
        self.assertEqual("keto_shoppers", assignment["axes"]["audience"])
        self.assertGreaterEqual(assignment["confidence"], 0.75)

    def test_taxonomy_reports_invalid_listing_clearly(self) -> None:
        result = TaxonomyAssigner().assign_batch([{"listing_id": "broken"}], snapshot_id="snapshot-broken")

        self.assertEqual("failed", result.summary.run_status)
        self.assertEqual(1, result.summary.failed_count)
        self.assertEqual("invalid", result.records[0].status)

    def test_artifact_writers_emit_structured_reports(self) -> None:
        taxonomy_result = TaxonomyAssigner().assign_batch([self._sweetener_listing()], snapshot_id="snapshot-live")
        normalization_record, normalization_manifest = self._fixture_record("clean-1")
        normalization_result = BatchNormalizer().normalize_snapshot(normalization_manifest, [normalization_record])
        scratch_dir = self._make_scratch_dir("artifacts")
        try:
            normalization_artifacts = write_normalization_artifacts(
                scratch_dir,
                normalization_manifest,
                normalization_result,
            )
            taxonomy_artifacts = write_taxonomy_artifacts(scratch_dir, "snapshot-live", taxonomy_result)

            normalization_report = json.loads(Path(normalization_artifacts["normalization_report"]).read_text(encoding="utf-8"))
            taxonomy_report = json.loads(Path(taxonomy_artifacts["taxonomy_report"]).read_text(encoding="utf-8"))

            self.assertEqual("success", normalization_report["run_status"])
            self.assertEqual("success", taxonomy_report["run_status"])
            self.assertEqual(1, normalization_report["normalized_records"])
            self.assertEqual(1, taxonomy_report["assigned_count"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _sweetener_listing(self) -> dict:
        return {
            "listing_id": "amazon:B098H7XWQ6",
            "source": "amazon",
            "source_record_id": "fixture-sweetener-1",
            "captured_at": "2026-04-22T00:00:00Z",
            "product_title": "Lakanto Monk Fruit Sweetener With Erythritol Classic White Sugar Replacement for Baking Keto Friendly 5 lb Bag",
            "brand_name": "Lakanto",
            "category_path": ["grocery", "baking", "sweeteners", "sugar-substitutes"],
            "price": 23.94,
            "currency": "USD",
            "unit_price": 4.788,
            "unit_measure": "lb",
            "pack_count": 1,
            "availability": "limited",
            "rating": 4.6,
            "review_count": 1000,
            "raw_payload_uri": "fixtures://taxonomy/sweetener",
        }

    def _fixture_record(self, record_id: str) -> tuple[RawSourceRecord, SourceSnapshotManifest]:
        fixture = json.loads(DIRTY_FIXTURE_PATH.read_text(encoding="utf-8"))
        payload = next(item for item in fixture if item["record_id"] == record_id)
        record = RawSourceRecord.from_dict(payload)
        manifest = SourceSnapshotManifest(
            snapshot_id=record.snapshot_id,
            source=record.source,
            captured_at=record.captured_at,
            record_count=1,
            record_ids=[record.record_id],
            storage_uri=f"fixtures://normalization/amazon_dirty_cases#{record_id}",
        )
        return record, manifest

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
