from __future__ import annotations

import argparse
import json
from pathlib import Path

from .decision_brief import write_decision_brief_artifacts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build a deterministic PM-facing decision brief from validated analysis artifacts."
    )
    parser.add_argument("--collection-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    output_dir = args.output_dir or args.collection_dir / "decision_brief"
    try:
        artifacts = write_decision_brief_artifacts(
            collection_dir=args.collection_dir,
            output_dir=output_dir,
        )
    except (ValueError, FileNotFoundError, OSError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1

    print(json.dumps({"status": "success", "output_dir": str(output_dir), "artifacts": artifacts}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
