from __future__ import annotations

import shutil
import unittest
from pathlib import Path

from brand_gap_inference.connectors import FixtureConnector, RawSourceRecord
from brand_gap_inference.ingestion import IngestionService
from brand_gap_inference.raw_store import FilesystemRawStore

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = ROOT / "fixtures" / "connectors" / "manual_hydration_source.json"
SCRATCH_ROOT = ROOT / ".tmp-tests"


class IngestionTests(unittest.TestCase):
    def test_fixture_connector_loads_valid_records(self) -> None:
        connector = FixtureConnector(source_name="manual", fixture_path=FIXTURE_PATH)
        records = connector.fetch_snapshot()
        self.assertEqual(2, len(records))
        self.assertIsInstance(records[0], RawSourceRecord)

    def test_ingestion_persists_and_replays_snapshot(self) -> None:
        connector = FixtureConnector(source_name="manual", fixture_path=FIXTURE_PATH)
        scratch_dir = self._make_scratch_dir("persist-replay")
        try:
            store = FilesystemRawStore(scratch_dir)
            service = IngestionService(store)

            result = service.ingest(connector)
            replayed = service.replay("manual", "snapshot-manual-hydration-001")

            self.assertEqual(2, result.manifest.record_count)
            self.assertEqual(result.manifest.snapshot_id, replayed.manifest.snapshot_id)
            self.assertEqual(
                [record.record_id for record in result.records],
                [record.record_id for record in replayed.records],
            )
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_ingestion_rejects_mixed_snapshot_ids(self) -> None:
        records = [
            RawSourceRecord(
                record_id="record-1",
                source="manual",
                snapshot_id="snapshot-a",
                captured_at="2026-04-22T10:00:00Z",
                payload={"title": "A"},
            ),
            RawSourceRecord(
                record_id="record-2",
                source="manual",
                snapshot_id="snapshot-b",
                captured_at="2026-04-22T10:00:00Z",
                payload={"title": "B"},
            ),
        ]
        scratch_dir = self._make_scratch_dir("mixed-snapshot")
        try:
            store = FilesystemRawStore(scratch_dir)
            with self.assertRaises(ValueError):
                store.persist_snapshot(records)
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
