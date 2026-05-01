from __future__ import annotations

import argparse
import json
from pathlib import Path

from .data_collection import ProductDetailExtractor, write_product_detail_artifacts
from .ingestion import IngestionService
from .raw_store import FilesystemRawStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay an amazon_api_product snapshot into product detail artifacts.")
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--source", default="amazon_api_product")
    parser.add_argument("--store-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)

    try:
        result = IngestionService(FilesystemRawStore(args.store_dir)).replay(args.source, args.snapshot_id)
        detail_result = ProductDetailExtractor().extract_snapshot(result.manifest, result.records)
        artifacts = write_product_detail_artifacts(args.output_dir, detail_result)
    except (ValueError, FileNotFoundError, OSError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1

    print(
        json.dumps(
            {
                "status": "success" if detail_result.valid_records else "failed",
                "snapshot_id": detail_result.snapshot_id,
                "valid_records": detail_result.valid_records,
                "invalid_records": detail_result.invalid_records,
                "artifacts": artifacts,
            },
            indent=2,
        )
    )
    return 0 if detail_result.valid_records else 1


if __name__ == "__main__":
    raise SystemExit(main())
