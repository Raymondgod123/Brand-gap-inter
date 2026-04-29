from __future__ import annotations

import argparse
import json
from pathlib import Path

from .brand_analysis import write_brand_positioning_artifacts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build a deterministic brand positioning report from product intelligence records."
    )
    parser.add_argument("--collection-dir", type=Path, help="Collection artifact directory")
    parser.add_argument("--product-intelligence-records", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    if args.collection_dir is None and args.product_intelligence_records is None:
        parser.error("provide --collection-dir or --product-intelligence-records")

    if args.collection_dir is not None:
        records_path = (
            args.product_intelligence_records
            or args.collection_dir / "product_intelligence" / "product_intelligence_records.json"
        )
        output_dir = args.output_dir or args.collection_dir / "brand_positioning"
    else:
        records_path = args.product_intelligence_records
        output_dir = args.output_dir or Path("artifacts/brand-positioning")

    assert records_path is not None
    assert output_dir is not None

    try:
        artifacts = write_brand_positioning_artifacts(
            product_intelligence_records_path=records_path,
            output_dir=output_dir,
        )
    except (ValueError, FileNotFoundError, OSError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1

    print(json.dumps({"status": "success", "output_dir": str(output_dir), "artifacts": artifacts}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
