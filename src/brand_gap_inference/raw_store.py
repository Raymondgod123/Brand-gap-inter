from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from .connectors import RawSourceRecord
from .contracts import assert_valid


@dataclass(frozen=True)
class SourceSnapshotManifest:
    snapshot_id: str
    source: str
    captured_at: str
    record_count: int
    record_ids: list[str]
    storage_uri: str

    @classmethod
    def from_dict(cls, payload: dict) -> "SourceSnapshotManifest":
        assert_valid("source_snapshot_manifest", payload)
        return cls(**payload)

    def to_dict(self) -> dict:
        return {
            "snapshot_id": self.snapshot_id,
            "source": self.source,
            "captured_at": self.captured_at,
            "record_count": self.record_count,
            "record_ids": self.record_ids,
            "storage_uri": self.storage_uri,
        }


class FilesystemRawStore:
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir

    def persist_snapshot(self, records: list[RawSourceRecord]) -> SourceSnapshotManifest:
        if not records:
            raise ValueError("cannot persist an empty snapshot")

        snapshot_id = records[0].snapshot_id
        source = records[0].source
        captured_at = records[0].captured_at

        for record in records:
            self._validate_record_consistency(record, snapshot_id, source)

        snapshot_dir = self.root_dir / source / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        record_ids: list[str] = []
        for record in records:
            record_ids.append(record.record_id)
            record_path = snapshot_dir / f"{record.record_id}.json"
            with record_path.open("w", encoding="utf-8") as handle:
                json.dump(record.to_dict(), handle, indent=2)

        manifest = SourceSnapshotManifest(
            snapshot_id=snapshot_id,
            source=source,
            captured_at=captured_at,
            record_count=len(records),
            record_ids=record_ids,
            storage_uri=str(snapshot_dir),
        )
        manifest_path = snapshot_dir / "_manifest.json"
        with manifest_path.open("w", encoding="utf-8") as handle:
            json.dump(manifest.to_dict(), handle, indent=2)
        return manifest

    def load_snapshot(self, source: str, snapshot_id: str) -> tuple[SourceSnapshotManifest, list[RawSourceRecord]]:
        snapshot_dir = self.root_dir / source / snapshot_id
        manifest_path = snapshot_dir / "_manifest.json"
        with manifest_path.open("r", encoding="utf-8") as handle:
            manifest = SourceSnapshotManifest.from_dict(json.load(handle))

        records: list[RawSourceRecord] = []
        for record_id in manifest.record_ids:
            record_path = snapshot_dir / f"{record_id}.json"
            with record_path.open("r", encoding="utf-8") as handle:
                records.append(RawSourceRecord.from_dict(json.load(handle)))
        return manifest, records

    @staticmethod
    def _validate_record_consistency(record: RawSourceRecord, snapshot_id: str, source: str) -> None:
        if record.snapshot_id != snapshot_id:
            raise ValueError("all records in a snapshot must share the same snapshot_id")
        if record.source != source:
            raise ValueError("all records in a snapshot must share the same source")
