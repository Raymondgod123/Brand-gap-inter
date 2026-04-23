from __future__ import annotations

import argparse
import json
from pathlib import Path

from .normalization import BatchNormalizer, write_normalization_artifacts
from .raw_store import FilesystemRawStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Normalize a stored raw snapshot into validated listings.")
    parser.add_argument("--store-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--source", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args(argv)

    store = FilesystemRawStore(args.store_dir)
    manifest, records = store.load_snapshot(args.source, args.snapshot_id)
    result = BatchNormalizer().normalize_snapshot(manifest, records)

    summary = {
        "run_status": result.summary.run_status,
        "total_records": result.summary.total_records,
        "normalized_records": result.summary.normalized_records,
        "duplicate_records": result.summary.duplicate_records,
        "invalid_records": result.summary.invalid_records,
        "warning_records": result.summary.warning_records,
        "low_confidence_records": result.summary.low_confidence_records,
        "listings": result.normalized_listings,
    }
    if args.output_dir is not None:
        summary["artifacts"] = write_normalization_artifacts(args.output_dir, manifest, result)
    print(json.dumps(summary, indent=2))
    return 0 if result.summary.run_status != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
