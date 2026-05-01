from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.connectors import RawSourceRecord
from brand_gap_inference.discovery import DiscoveryExtractor, write_discovery_artifacts
from brand_gap_inference.discover_products import (
    main as discover_products_main,
    run_discovery,
    run_discovery_from_snapshot,
)
from brand_gap_inference.raw_store import FilesystemRawStore, SourceSnapshotManifest
from brand_gap_inference.serpapi_discovery import (
    SerpApiClient,
    SerpApiDiscoveryConnector,
    build_discovery_record_id,
)

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"


class StubSerpApiClient(SerpApiClient):
    def __init__(self, response: dict) -> None:
        self.response = response
        self.calls: list[str] = []

    def search_amazon_products(self, keyword: str) -> dict:
        self.calls.append(keyword)
        return self.response


class DiscoveryTests(unittest.TestCase):
    def test_serpapi_connector_emits_one_raw_snapshot_record(self) -> None:
        response = {"search_metadata": {"status": "Success"}, "organic_results": [{"title": "Example", "link_clean": "https://www.amazon.com/dp/B00CF2B04Q"}]}
        client = StubSerpApiClient(response)
        connector = SerpApiDiscoveryConnector(
            keyword="monk fruit sweetener",
            client=client,
            captured_at="2026-04-24T12:00:00Z",
        )

        records = connector.fetch_snapshot()

        self.assertEqual(1, len(records))
        record = records[0]
        self.assertEqual("amazon_api_discovery", record.source)
        self.assertEqual(build_discovery_record_id("serpapi", "monk fruit sweetener"), record.record_id)
        self.assertEqual("monk fruit sweetener", client.calls[0])
        self.assertEqual("serpapi", record.payload["provider"])
        self.assertEqual("monk fruit sweetener", record.payload["query"])
        self.assertIsInstance(record.payload["provider_response"], dict)

    def test_extractor_emits_valid_invalid_and_warning_records(self) -> None:
        record = self._make_raw_record()
        manifest = self._make_manifest(record)

        result = DiscoveryExtractor().extract_snapshot(manifest, [record])

        self.assertEqual("partial_success", result.summary.run_status)
        self.assertEqual(5, result.summary.total_candidates)
        self.assertEqual(3, result.summary.valid_candidates)
        self.assertEqual(2, result.summary.invalid_candidates)

        first = result.records[0]
        self.assertEqual("valid", first.status)
        self.assertEqual("B00CF2B04Q", first.asin)
        self.assertEqual("https://www.amazon.com/dp/B00CF2B04Q", first.product_url)

        second = result.records[1]
        self.assertEqual("valid", second.status)
        self.assertEqual("B0CF6L6PRT", second.asin)
        self.assertTrue(any("asin inferred" in warning for warning in second.warnings))

        third = result.records[2]
        self.assertEqual("valid", third.status)
        self.assertTrue(any("price missing" in warning for warning in third.warnings))
        self.assertTrue(any("rating missing" in warning for warning in third.warnings))
        self.assertTrue(any("review_count missing" in warning for warning in third.warnings))

        fourth = result.records[3]
        self.assertEqual("invalid", fourth.status)
        self.assertTrue(any("missing title" in issue.message for issue in fourth.issues))

        fifth = result.records[4]
        self.assertEqual("invalid", fifth.status)
        self.assertTrue(any("valid Amazon product_url" in issue.message for issue in fifth.issues))

    def test_discovery_artifact_writer_validates_contracts(self) -> None:
        record = self._make_raw_record()
        manifest = self._make_manifest(record)
        result = DiscoveryExtractor().extract_snapshot(manifest, [record])
        scratch_dir = self._make_scratch_dir("discovery-artifacts")
        try:
            artifacts = write_discovery_artifacts(scratch_dir, result)
            self.assertTrue(Path(artifacts["discovery_records"]).exists())
            self.assertTrue(Path(artifacts["discovery_report"]).exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_replay_produces_same_discovery_records_as_live_ingestion(self) -> None:
        client = StubSerpApiClient(self._provider_response())
        connector = SerpApiDiscoveryConnector(
            keyword="monk fruit sweetener",
            client=client,
            captured_at="2026-04-24T12:00:00Z",
        )
        scratch_dir = self._make_scratch_dir("discover-products-replay")
        try:
            live_result = run_discovery(
                keyword="monk fruit sweetener",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "live-artifacts",
                connector=connector,
            )
            replay_result = run_discovery_from_snapshot(
                source="amazon_api_discovery",
                snapshot_id=live_result.snapshot_id,
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "replay-artifacts",
            )

            live_records = json.loads(Path(live_result.artifacts["discovery_records"]).read_text(encoding="utf-8"))
            replay_records = json.loads(Path(replay_result.artifacts["discovery_records"]).read_text(encoding="utf-8"))
            self.assertEqual(live_records, replay_records)
            self.assertEqual(live_result.summary, replay_result.summary)
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_discovery_cli_replay_emits_artifacts_from_stored_snapshot(self) -> None:
        client = StubSerpApiClient(self._provider_response())
        connector = SerpApiDiscoveryConnector(
            keyword="monk fruit sweetener",
            client=client,
            captured_at="2026-04-24T12:00:00Z",
        )
        scratch_dir = self._make_scratch_dir("discover-products-cli-replay")
        try:
            live_result = run_discovery(
                keyword="monk fruit sweetener",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "live-artifacts",
                connector=connector,
            )
            captured_stdout = io.StringIO()
            with redirect_stdout(captured_stdout):
                exit_code = discover_products_main(
                    [
                        "--snapshot-id",
                        live_result.snapshot_id,
                        "--store-dir",
                        str(scratch_dir / "raw"),
                        "--output-dir",
                        str(scratch_dir / "cli-replay-artifacts"),
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertTrue((scratch_dir / "cli-replay-artifacts" / "discovery_records.json").exists())
            self.assertTrue((scratch_dir / "cli-replay-artifacts" / "discovery_report.json").exists())
            self.assertTrue((scratch_dir / "cli-replay-artifacts" / "discovery_bundle_manifest.json").exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _provider_response(self) -> dict:
        return {
            "search_metadata": {"status": "Success"},
            "organic_results": [
                {
                    "position": 1,
                    "asin": "B00CF2B04Q",
                    "title": "Lakanto Classic Monk Fruit Sweetener with Erythritol",
                    "link_clean": "https://www.amazon.com/dp/B00CF2B04Q",
                    "rating": 4.6,
                    "reviews": 41234,
                    "extracted_price": 9.99,
                    "currency": "USD",
                    "sponsored": False,
                },
                {
                    "position": 2,
                    "title": "Transparency Foods Liquid Monk Fruit Sweetener",
                    "link_clean": "https://www.amazon.com/Transparency-Foods-Liquid-Monk-Fruit/dp/B0CF6L6PRT/ref=sr_1_2",
                    "rating": 4.2,
                    "reviews": 210,
                    "extracted_price": 18.49,
                    "currency": "USD",
                    "sponsored": True,
                },
                {
                    "position": 3,
                    "asin": "B014RVNVKS",
                    "title": "Pyure Organic Stevia Extract Powder",
                    "link_clean": "https://www.amazon.com/dp/B014RVNVKS",
                    "sponsored": False,
                },
                {
                    "position": 4,
                    "link_clean": "https://www.amazon.com/dp/B01LDNBAC4",
                    "rating": 4.5,
                    "reviews": 1000,
                    "extracted_price": 23.94,
                    "currency": "USD",
                    "sponsored": False,
                },
                {
                    "position": 5,
                    "title": "Off platform result",
                    "link_clean": "https://example.com/not-amazon",
                    "sponsored": False,
                },
            ],
        }

    def _make_raw_record(self) -> RawSourceRecord:
        record_id = build_discovery_record_id("serpapi", "monk fruit sweetener")
        snapshot_id = f"amazon_api_discovery-{record_id}-2026-04-24T00-00-00Z"
        return RawSourceRecord(
            record_id=record_id,
            source="amazon_api_discovery",
            snapshot_id=snapshot_id,
            captured_at="2026-04-24T00:00:00Z",
            payload={
                "provider": "serpapi",
                "query": "monk fruit sweetener",
                "requested_at": "2026-04-24T00:00:00Z",
                "provider_request_metadata": {
                    "endpoint": "https://serpapi.com/search.json",
                    "engine": "amazon",
                    "amazon_domain": "amazon.com",
                    "language": "en_US",
                },
                "provider_response": self._provider_response(),
                "result_count": 5,
            },
            cursor="monk fruit sweetener",
        )

    def _make_manifest(self, record: RawSourceRecord) -> SourceSnapshotManifest:
        return SourceSnapshotManifest(
            snapshot_id=record.snapshot_id,
            source=record.source,
            captured_at=record.captured_at,
            record_count=1,
            record_ids=[record.record_id],
            storage_uri=f"fixtures://discovery/{record.record_id}",
        )

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
