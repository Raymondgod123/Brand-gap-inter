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
LIVE_SNAPSHOT_PATH = ROOT / "data" / "raw" / "amazon" / "amazon-B098H7XWQ6-2026-04-22T01-54-11Z" / "B098H7XWQ6.json"
SCRATCH_ROOT = ROOT / ".tmp-tests"


class TaxonomyTests(unittest.TestCase):
    def test_taxonomy_assigns_live_sweetener_listing(self) -> None:
        listing = self._live_normalized_listing()

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
        listing = self._live_normalized_listing()
        taxonomy_result = TaxonomyAssigner().assign_batch([listing], snapshot_id="snapshot-live")
        normalization_result = BatchNormalizer().normalize_snapshot(
            self._live_manifest(),
            [self._live_record()],
        )
        scratch_dir = self._make_scratch_dir("artifacts")
        try:
            normalization_artifacts = write_normalization_artifacts(scratch_dir, self._live_manifest(), normalization_result)
            taxonomy_artifacts = write_taxonomy_artifacts(scratch_dir, "snapshot-live", taxonomy_result)

            normalization_report = json.loads(Path(normalization_artifacts["normalization_report"]).read_text(encoding="utf-8"))
            taxonomy_report = json.loads(Path(taxonomy_artifacts["taxonomy_report"]).read_text(encoding="utf-8"))

            self.assertEqual("success", normalization_report["run_status"])
            self.assertEqual("success", taxonomy_report["run_status"])
            self.assertEqual(1, normalization_report["normalized_records"])
            self.assertEqual(1, taxonomy_report["assigned_count"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _live_record(self) -> RawSourceRecord:
        payload = json.loads(LIVE_SNAPSHOT_PATH.read_text(encoding="utf-8"))
        return RawSourceRecord.from_dict(payload)

    def _live_manifest(self) -> SourceSnapshotManifest:
        record = self._live_record()
        return SourceSnapshotManifest(
            snapshot_id=record.snapshot_id,
            source=record.source,
            captured_at=record.captured_at,
            record_count=1,
            record_ids=[record.record_id],
            storage_uri="data/raw/amazon/amazon-B098H7XWQ6-2026-04-22T01-54-11Z",
        )

    def _live_normalized_listing(self) -> dict:
        result = BatchNormalizer().normalize_snapshot(self._live_manifest(), [self._live_record()])
        return result.normalized_listings[0]

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
