from __future__ import annotations

import argparse
import json
from pathlib import Path

from .evidence_workbench import write_evidence_workbench_artifacts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build a static Evidence Workbench review page from collection artifacts.")
    parser.add_argument("--collection-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    try:
        artifacts = write_evidence_workbench_artifacts(
            collection_dir=args.collection_dir,
            output_dir=args.output_dir,
        )
    except (ValueError, FileNotFoundError, OSError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1

    print(json.dumps({"status": "success", "artifacts": artifacts}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
