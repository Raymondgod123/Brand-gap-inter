from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
import json

from .contracts import assert_valid


@dataclass(frozen=True)
class RawSourceRecord:
    record_id: str
    source: str
    snapshot_id: str
    captured_at: str
    payload: dict
    cursor: str | None = None

    @classmethod
    def from_dict(cls, payload: dict) -> "RawSourceRecord":
        assert_valid("raw_source_record", payload)
        return cls(**payload)

    def to_dict(self) -> dict:
        return {
            "record_id": self.record_id,
            "source": self.source,
            "snapshot_id": self.snapshot_id,
            "captured_at": self.captured_at,
            "payload": self.payload,
            "cursor": self.cursor,
        }


class SourceConnector(Protocol):
    source_name: str

    def fetch_snapshot(self) -> list[RawSourceRecord]:
        ...


@dataclass(frozen=True)
class FixtureConnector:
    source_name: str
    fixture_path: Path

    def fetch_snapshot(self) -> list[RawSourceRecord]:
        with self.fixture_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return [RawSourceRecord.from_dict(item) for item in payload]
