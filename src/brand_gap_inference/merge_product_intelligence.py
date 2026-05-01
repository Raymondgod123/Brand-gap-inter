from __future__ import annotations

import argparse
import json
from pathlib import Path

from .product_intelligence import merge_collection_artifacts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Merge discovery and product-detail artifacts into product-intelligence records."
    )
    parser.add_argument("--collection-dir", type=Path, help="Collection artifact directory to merge")
    parser.add_argument("--collection-report", type=Path, default=None)
    parser.add_argument("--discovery-records", type=Path, default=None)
    parser.add_argument("--detail-records", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    if args.collection_dir is None and (
        args.collection_report is None or args.discovery_records is None or args.detail_records is None
    ):
        parser.error("provide --collection-dir or all artifact paths")

    if args.collection_dir is not None:
        collection_dir = args.collection_dir
        collection_report = args.collection_report or collection_dir / "data_collection_report.json"
        discovery_records = args.discovery_records or collection_dir / "discovery" / "discovery_records.json"
        detail_records = args.detail_records or collection_dir / "details" / "product_detail_records.json"
        output_dir = args.output_dir or collection_dir / "product_intelligence"
    else:
        collection_report = args.collection_report
        discovery_records = args.discovery_records
        detail_records = args.detail_records
        output_dir = args.output_dir or Path("artifacts/product-intelligence-merge")

    assert collection_report is not None
    assert discovery_records is not None
    assert detail_records is not None
    assert output_dir is not None

    try:
        result = merge_collection_artifacts(
            collection_report_path=collection_report,
            discovery_records_path=discovery_records,
            detail_records_path=detail_records,
            output_dir=output_dir,
        )
    except (ValueError, FileNotFoundError, OSError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1

    print(
        json.dumps(
            {
                "status": result.status,
                "output_dir": str(output_dir),
                "total_products": len(result.records),
                "complete_products": result.complete_products,
                "warning_products": result.warning_products,
                "issue_products": result.issue_products,
            },
            indent=2,
        )
    )
    return 0 if result.status != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
