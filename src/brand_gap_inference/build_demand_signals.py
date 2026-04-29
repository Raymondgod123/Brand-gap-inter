from __future__ import annotations

import argparse
import json
from pathlib import Path

from .demand_signals import write_demand_signal_artifacts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build replayable demand signals from discovery breadth and brand-profile context."
    )
    parser.add_argument("--collection-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    output_dir = args.output_dir or args.collection_dir / "demand_signals"
    try:
        artifacts = write_demand_signal_artifacts(collection_dir=args.collection_dir, output_dir=output_dir)
    except (ValueError, FileNotFoundError, OSError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1

    print(json.dumps({"status": "success", "output_dir": str(output_dir), "artifacts": artifacts}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
