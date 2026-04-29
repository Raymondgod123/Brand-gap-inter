from __future__ import annotations

import argparse
import json
from pathlib import Path

from .analysis_stack import run_analysis_stack


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the deterministic post-collection analysis stack, with optional deep inference."
    )
    parser.add_argument("--collection-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--include-deep-inference", action="store_true")
    parser.add_argument("--deep-inference-model", default="gpt-5.4")
    parser.add_argument(
        "--deep-inference-reasoning-effort",
        default="high",
        choices=["none", "low", "medium", "high", "xhigh"],
    )
    args = parser.parse_args(argv)

    try:
        result = run_analysis_stack(
            collection_dir=args.collection_dir,
            output_dir=args.output_dir,
            include_deep_inference=args.include_deep_inference,
            deep_inference_model=args.deep_inference_model,
            deep_inference_reasoning_effort=args.deep_inference_reasoning_effort,
        )
    except (ValueError, FileNotFoundError, OSError, RuntimeError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1

    print(
        json.dumps(
            {
                "status": result.status,
                "run_id": result.run_id,
                "output_dir": str(result.output_dir),
                "artifacts": result.artifacts,
                "summary": {
                    "completed_steps": result.report["completed_steps"],
                    "failed_steps": result.report["failed_steps"],
                    "warnings": result.report["warnings"],
                },
            },
            indent=2,
        )
    )
    return 0 if result.status != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
