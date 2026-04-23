from __future__ import annotations

from dataclasses import dataclass

from .connectors import RawSourceRecord, SourceConnector
from .raw_store import FilesystemRawStore, SourceSnapshotManifest


@dataclass(frozen=True)
class IngestionResult:
    manifest: SourceSnapshotManifest
    records: list[RawSourceRecord]


class IngestionService:
    def __init__(self, raw_store: FilesystemRawStore) -> None:
        self.raw_store = raw_store

    def ingest(self, connector: SourceConnector) -> IngestionResult:
        records = connector.fetch_snapshot()
        manifest = self.raw_store.persist_snapshot(records)
        return IngestionResult(manifest=manifest, records=records)

    def replay(self, source: str, snapshot_id: str) -> IngestionResult:
        manifest, records = self.raw_store.load_snapshot(source, snapshot_id)
        return IngestionResult(manifest=manifest, records=records)
