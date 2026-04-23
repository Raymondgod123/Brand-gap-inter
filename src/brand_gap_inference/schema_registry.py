from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = PROJECT_ROOT / "schemas"


def schema_path(schema_filename: str) -> Path:
    return SCHEMA_DIR / schema_filename


def load_schema(schema_filename: str) -> dict:
    with schema_path(schema_filename).open("r", encoding="utf-8") as handle:
        return json.load(handle)
