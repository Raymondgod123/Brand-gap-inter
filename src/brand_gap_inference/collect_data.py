from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path

from .analysis_stack import run_collection_and_analysis
from .serpapi_discovery import SerpApiError
from .serpapi_product import SerpApiProductError


def _timestamp_slug(timestamp: str) -> str:
    return timestamp.replace(":", "-").replace(".", "-")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run one-shot data collection: keyword discovery, candidate selection, and product detail capture."
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--keyword", help="Keyword to query through SerpApi discovery")
    source_group.add_argument("--discovery-snapshot-id", help="Replay a stored discovery snapshot before detail capture")
    parser.add_argument("--store-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--max-products", type=int, default=5)
    parser.add_argument("--detail-mode", choices=["serpapi_product", "none"], default="serpapi_product")
    parser.add_argument(
        "--post-analysis",
        choices=["none", "deterministic", "deep_inference"],
        default="none",
        help="Optionally build the post-collection analysis stack after detail enrichment.",
    )
    parser.add_argument("--deep-inference-model", default="gpt-5.4")
    parser.add_argument(
        "--deep-inference-reasoning-effort",
        default="high",
        choices=["none", "low", "medium", "high", "xhigh"],
    )
    args = parser.parse_args(argv)

    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    output_dir = args.output_dir or Path("artifacts") / f"data-collection-{_timestamp_slug(generated_at)}"

    try:
        result = run_collection_and_analysis(
            keyword=args.keyword,
            discovery_snapshot_id=args.discovery_snapshot_id,
            store_dir=args.store_dir,
            output_dir=output_dir,
            max_products=args.max_products,
            detail_mode=args.detail_mode,
            post_analysis=args.post_analysis,
            captured_at=generated_at,
            deep_inference_model=args.deep_inference_model,
            deep_inference_reasoning_effort=args.deep_inference_reasoning_effort,
        )
    except (SerpApiError, SerpApiProductError, ValueError, RuntimeError, FileNotFoundError, OSError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1

    analysis_summary = None
    if result.analysis is not None:
        analysis_summary = {
            "status": result.analysis.status,
            "completed_steps": result.analysis.report["completed_steps"],
            "failed_steps": result.analysis.report["failed_steps"],
            "warnings": result.analysis.report["warnings"],
        }

    print(
        json.dumps(
            {
                "status": result.status,
                "run_id": result.collection.run_id,
                "output_dir": str(result.collection.output_dir),
                "artifacts": result.artifacts,
                "summary": {
                    "keyword": result.collection.report["keyword"],
                    "selected_candidates": len(result.collection.report["selected_candidates"]),
                    "detail": result.collection.report["detail"],
                    "analysis": analysis_summary,
                },
            },
            indent=2,
        )
    )
    return 0 if result.status != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
