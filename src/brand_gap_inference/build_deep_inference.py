from __future__ import annotations

import argparse
import json
from pathlib import Path

from .deep_inference import write_deep_inference_artifacts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run GPT-5.4 deep inference over collection artifacts after deterministic analysis is complete."
    )
    parser.add_argument("--collection-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--model", default="gpt-5.4")
    parser.add_argument("--reasoning-effort", default="high", choices=["none", "low", "medium", "high", "xhigh"])
    args = parser.parse_args(argv)

    output_dir = args.output_dir or args.collection_dir / "deep_inference"
    try:
        artifacts = write_deep_inference_artifacts(
            collection_dir=args.collection_dir,
            output_dir=output_dir,
            model=args.model,
            reasoning_effort=args.reasoning_effort,
        )
    except (ValueError, FileNotFoundError, OSError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1

    print(
        json.dumps(
            {
                "status": "success",
                "output_dir": str(output_dir),
                "model": args.model,
                "artifacts": artifacts,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
