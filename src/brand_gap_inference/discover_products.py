from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path

from .discovery import DiscoveryBatchResult, DiscoveryExtractor, write_discovery_artifacts
from .ingestion import IngestionService
from .raw_store import FilesystemRawStore
from .serpapi_discovery import SerpApiDiscoveryConnector, SerpApiError


@dataclass(frozen=True)
class DiscoveryRunResult:
    snapshot_id: str
    query: str
    output_dir: Path
    artifacts: dict[str, str]
    summary: dict[str, object]


def _timestamp_slug(timestamp: str) -> str:
    return timestamp.replace(":", "-").replace(".", "-").replace("Z", "Z")


def _write_bundle_manifest(
    output_dir: Path,
    *,
    mode: str,
    snapshot_id: str,
    query: str,
    artifacts: dict[str, str],
    summary: dict[str, object],
) -> str:
    manifest_path = output_dir / "discovery_bundle_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "mode": mode,
                "snapshot_id": snapshot_id,
                "query": query,
                "status": summary["run_status"],
                "artifacts": artifacts,
                "summary": summary,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return str(manifest_path)


def run_discovery(
    *,
    keyword: str,
    store_dir: Path,
    output_dir: Path | None = None,
    connector: SerpApiDiscoveryConnector | None = None,
    captured_at: str | None = None,
) -> DiscoveryRunResult:
    service = IngestionService(FilesystemRawStore(store_dir))
    resolved_connector = connector or SerpApiDiscoveryConnector(keyword=keyword, captured_at=captured_at)
    ingest_result = service.ingest(resolved_connector)
    return _run_discovery_from_ingestion_result(ingest_result=ingest_result, output_dir=output_dir)


def run_discovery_from_snapshot(
    *,
    source: str,
    snapshot_id: str,
    store_dir: Path,
    output_dir: Path | None = None,
) -> DiscoveryRunResult:
    service = IngestionService(FilesystemRawStore(store_dir))
    ingest_result = service.replay(source, snapshot_id)
    return _run_discovery_from_ingestion_result(ingest_result=ingest_result, output_dir=output_dir)


def _run_discovery_from_ingestion_result(
    *,
    ingest_result: object,
    output_dir: Path | None = None,
) -> DiscoveryRunResult:
    manifest = ingest_result.manifest
    records = ingest_result.records
    extractor = DiscoveryExtractor()
    result: DiscoveryBatchResult = extractor.extract_snapshot(manifest, records)

    resolved_output_dir = output_dir or Path("artifacts") / f"discovery-{manifest.snapshot_id}"
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = write_discovery_artifacts(resolved_output_dir, result)
    summary = {
        "run_status": result.summary.run_status,
        "total_candidates": result.summary.total_candidates,
        "valid_candidates": result.summary.valid_candidates,
        "invalid_candidates": result.summary.invalid_candidates,
        "warning_records": result.summary.warning_records,
    }
    artifacts["bundle_manifest"] = _write_bundle_manifest(
        resolved_output_dir,
        mode="discovery",
        snapshot_id=manifest.snapshot_id,
        query=result.query,
        artifacts=artifacts,
        summary=summary,
    )
    return DiscoveryRunResult(
        snapshot_id=manifest.snapshot_id,
        query=result.query,
        output_dir=resolved_output_dir,
        artifacts=artifacts,
        summary=summary,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Discover Amazon product candidates from a keyword via SERP API.")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--keyword", help="Keyword to query through SERP API")
    source_group.add_argument("--snapshot-id", help="Replay a stored discovery snapshot")
    parser.add_argument("--source", default="amazon_api_discovery")
    parser.add_argument("--store-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    run_slug = _timestamp_slug(generated_at)

    try:
        if args.keyword is not None:
            output_dir = args.output_dir or Path("artifacts") / f"discovery-live-{run_slug}"
            result = run_discovery(
                keyword=args.keyword,
                store_dir=args.store_dir,
                output_dir=output_dir,
                captured_at=generated_at,
            )
            print(
                json.dumps(
                    {
                        "status": result.summary["run_status"],
                        "mode": "live",
                        "query": result.query,
                        "snapshot_id": result.snapshot_id,
                        "output_dir": str(result.output_dir),
                        "artifacts": result.artifacts,
                        "summary": result.summary,
                    },
                    indent=2,
                )
            )
            return 0 if result.summary["run_status"] != "failed" else 1

        output_dir = args.output_dir or Path("artifacts") / f"discovery-replay-{args.snapshot_id}"
        result = run_discovery_from_snapshot(
            source=args.source,
            snapshot_id=args.snapshot_id,
            store_dir=args.store_dir,
            output_dir=output_dir,
        )
        print(
            json.dumps(
                {
                    "status": result.summary["run_status"],
                    "mode": "replay",
                    "query": result.query,
                    "snapshot_id": result.snapshot_id,
                    "output_dir": str(result.output_dir),
                    "artifacts": result.artifacts,
                    "summary": result.summary,
                },
                indent=2,
            )
        )
        return 0 if result.summary["run_status"] != "failed" else 1
    except (SerpApiError, ValueError, RuntimeError, FileNotFoundError, OSError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
