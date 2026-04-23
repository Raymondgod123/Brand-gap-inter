from __future__ import annotations

import argparse
import json
from pathlib import Path

from .taxonomy import TaxonomyAssigner, write_taxonomy_artifacts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Assign taxonomy axes to normalized listings.")
    parser.add_argument("--normalized-listings", type=Path, required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args(argv)

    listings = json.loads(args.normalized_listings.read_text(encoding="utf-8"))
    result = TaxonomyAssigner().assign_batch(listings, snapshot_id=args.snapshot_id)

    summary = {
        "run_status": result.summary.run_status,
        "taxonomy_version": result.taxonomy_version,
        "total_listings": result.summary.total_listings,
        "assigned_count": result.summary.assigned_count,
        "failed_count": result.summary.failed_count,
        "warning_records": result.summary.warning_records,
        "assignments": result.assignments,
    }
    if args.output_dir is not None:
        summary["artifacts"] = write_taxonomy_artifacts(args.output_dir, args.snapshot_id, result)
    print(json.dumps(summary, indent=2))
    return 0 if result.summary.run_status != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
