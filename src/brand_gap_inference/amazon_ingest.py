from __future__ import annotations

import argparse
import json
from pathlib import Path

from .amazon import AmazonBrowserProductConnector, AmazonProductConnector
from .ingestion import IngestionService
from .raw_store import FilesystemRawStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest a live Amazon product URL into the raw snapshot store.")
    parser.add_argument("--url", required=True, help="Amazon product URL to ingest")
    parser.add_argument(
        "--acquisition-mode",
        choices=("http", "browser"),
        default="http",
        help="Live acquisition method to use (default: http)",
    )
    parser.add_argument("--store-dir", type=Path, default=Path("data/raw"))
    args = parser.parse_args(argv)

    if args.acquisition_mode == "browser":
        connector = AmazonBrowserProductConnector(product_url=args.url)
    else:
        connector = AmazonProductConnector(product_url=args.url)
    service = IngestionService(FilesystemRawStore(args.store_dir))
    result = service.ingest(connector)
    record = result.records[0]
    payload = record.payload

    summary = {
        "source": record.source,
        "record_id": record.record_id,
        "snapshot_id": record.snapshot_id,
        "storage_uri": result.manifest.storage_uri,
        "acquisition_method": payload.get("acquisition_method", "http"),
        "original_url": payload["original_url"],
        "canonical_url": payload["canonical_url"],
        "final_url": payload["final_url"],
        "status_code": payload["status_code"],
        "page_title": payload["page_title"],
        "is_robot_check": payload["is_robot_check"],
    }
    if "capture_diagnostics" in payload:
        summary["capture_diagnostics"] = payload["capture_diagnostics"]
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
