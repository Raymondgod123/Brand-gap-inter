from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.collect_data import main as collect_data_main
from brand_gap_inference.data_collection import (
    ProductDetailExtractor,
    evaluate_collection_candidates,
    run_data_collection,
    select_collection_candidates,
)
from brand_gap_inference.discover_products import run_discovery
from brand_gap_inference.extract_product_details import main as extract_product_details_main
from brand_gap_inference.merge_product_intelligence import main as merge_product_intelligence_main
from brand_gap_inference.ingestion import IngestionService
from brand_gap_inference.raw_store import FilesystemRawStore
from brand_gap_inference.serpapi_discovery import SerpApiClient, SerpApiDiscoveryConnector
from brand_gap_inference.serpapi_product import (
    SerpApiProductClient,
    SerpApiProductConnector,
    build_product_record_id,
    normalize_asins,
)

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"


class StubSerpApiDiscoveryClient(SerpApiClient):
    def __init__(self, response: dict) -> None:
        self.response = response
        self.calls: list[str] = []

    def search_amazon_products(self, keyword: str) -> dict:
        self.calls.append(keyword)
        return self.response


class StubSerpApiProductClient(SerpApiProductClient):
    def __init__(self, responses: dict[str, dict]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def fetch_amazon_product(self, asin: str) -> dict:
        self.calls.append(asin)
        return self.responses[asin]


class DataCollectionTests(unittest.TestCase):
    def test_product_connector_emits_one_raw_record_per_asin(self) -> None:
        client = StubSerpApiProductClient(
            {
                "B00CF2B04Q": self._product_response("B00CF2B04Q"),
                "B0CF6L6PRT": self._product_response("B0CF6L6PRT"),
            }
        )
        connector = SerpApiProductConnector(
            asins=["B00CF2B04Q", "bad", "B0CF6L6PRT", "B00CF2B04Q"],
            client=client,
            captured_at="2026-04-24T16:00:00Z",
        )

        records = connector.fetch_snapshot()

        self.assertEqual(["B00CF2B04Q", "B0CF6L6PRT"], client.calls)
        self.assertEqual(2, len(records))
        self.assertEqual("amazon_api_product", records[0].source)
        self.assertEqual(build_product_record_id("serpapi", "B00CF2B04Q"), records[0].record_id)
        self.assertEqual("amazon_product", records[0].payload["provider_request_metadata"]["engine"])

    def test_product_detail_extractor_validates_and_summarizes_provider_response(self) -> None:
        scratch_dir = self._make_scratch_dir("product-detail-extractor")
        client = StubSerpApiProductClient({"B00CF2B04Q": self._product_response("B00CF2B04Q")})
        connector = SerpApiProductConnector(
            asins=["B00CF2B04Q"],
            client=client,
            captured_at="2026-04-24T16:00:00Z",
        )
        try:
            ingest_result = IngestionService(FilesystemRawStore(scratch_dir / "raw")).ingest(connector)
            result = ProductDetailExtractor().extract_snapshot(ingest_result.manifest, ingest_result.records)

            self.assertEqual(1, result.valid_records)
            detail = result.records[0].to_dict()
            self.assertEqual("valid", detail["status"])
            self.assertEqual("Lakanto Classic Monk Fruit Sweetener", detail["title"])
            self.assertEqual(9.99, detail["price"])
            self.assertEqual("USD", detail["currency"])
            self.assertEqual("https://example.com/main.jpg", detail["media_assets"]["primary_image"])
            self.assertEqual(["https://example.com/gallery-1.jpg"], detail["media_assets"]["gallery_images"])
            self.assertEqual(["Keto friendly"], detail["description_bullets"])
            self.assertEqual("Lifestyle image", detail["promotional_content"][0]["title"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_product_detail_extractor_blocks_price_when_primary_offer_is_unsafe(self) -> None:
        scratch_dir = self._make_scratch_dir("product-detail-unsafe-offer")
        client = StubSerpApiProductClient({"B00CF2B04Q": self._unavailable_product_response("B00CF2B04Q")})
        connector = SerpApiProductConnector(
            asins=["B00CF2B04Q"],
            client=client,
            captured_at="2026-04-24T16:00:00Z",
        )
        try:
            ingest_result = IngestionService(FilesystemRawStore(scratch_dir / "raw")).ingest(connector)
            result = ProductDetailExtractor().extract_snapshot(ingest_result.manifest, ingest_result.records)

            self.assertEqual(1, result.valid_records)
            detail = result.records[0].to_dict()
            self.assertEqual("valid", detail["status"])
            self.assertIsNone(detail["price"])
            self.assertEqual("Currently unavailable", detail["availability"])
            self.assertTrue(detail["provider_metadata"]["offer_state"]["price_fallback_blocked"])
            self.assertTrue(
                any("price blocked" in warning for warning in detail["warnings"])
            )
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_product_detail_extractor_can_use_safe_single_offer_price_and_stock(self) -> None:
        scratch_dir = self._make_scratch_dir("product-detail-single-offer")
        response = self._product_response("B00CF2B04Q")
        response["product_results"].pop("price")
        response["product_results"].pop("extracted_price")
        response["product_results"].pop("stock", None)
        response["product_results"].pop("availability", None)
        response["purchase_options"] = {
            "single_offer": {
                "price": "$8.49",
                "extracted_price": 8.49,
                "stock": "In Stock",
            }
        }
        client = StubSerpApiProductClient({"B00CF2B04Q": response})
        connector = SerpApiProductConnector(
            asins=["B00CF2B04Q"],
            client=client,
            captured_at="2026-04-24T16:00:00Z",
        )
        try:
            ingest_result = IngestionService(FilesystemRawStore(scratch_dir / "raw")).ingest(connector)
            result = ProductDetailExtractor().extract_snapshot(ingest_result.manifest, ingest_result.records)

            detail = result.records[0].to_dict()
            self.assertEqual(8.49, detail["price"])
            self.assertEqual("In Stock", detail["availability"])
            self.assertTrue(detail["provider_metadata"]["offer_state"]["primary_offer_safe"])
            self.assertFalse(any("price blocked" in warning for warning in detail["warnings"]))
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_data_collection_runs_discovery_selection_and_product_detail_capture(self) -> None:
        scratch_dir = self._make_scratch_dir("data-collection")
        discovery_client = StubSerpApiDiscoveryClient(self._discovery_response())
        discovery_connector = SerpApiDiscoveryConnector(
            keyword="monk fruit sweetener",
            client=discovery_client,
            captured_at="2026-04-24T16:00:00Z",
        )
        product_client = StubSerpApiProductClient(
            {
                "B00CF2B04Q": self._product_response("B00CF2B04Q"),
                "B0CF6L6PRT": self._product_response("B0CF6L6PRT"),
            }
        )
        try:
            result = run_data_collection(
                keyword="monk fruit sweetener",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                max_products=2,
                discovery_connector=discovery_connector,
                product_client=product_client,
                captured_at="2026-04-24T16:00:00Z",
            )

            self.assertEqual("success", result.status)
            self.assertEqual(["B00CF2B04Q", "B0CF6L6PRT"], product_client.calls)
            self.assertTrue(Path(result.artifacts["data_collection_report"]).exists())
            self.assertTrue(Path(result.artifacts["product_detail_records"]).exists())
            self.assertTrue(Path(result.artifacts["product_intelligence_records"]).exists())

            report = json.loads(Path(result.artifacts["data_collection_report"]).read_text(encoding="utf-8"))
            self.assertEqual(2, len(report["selected_candidates"]))
            self.assertEqual(2, report["detail"]["valid_records"])
            self.assertEqual("query_fit_v1", report["selection"]["selector_version"])
            self.assertTrue(Path(result.artifacts["selection_report"]).exists())
            self.assertIn("selection_trace", report["selected_candidates"][0])

            intelligence_records = json.loads(
                Path(result.artifacts["product_intelligence_records"]).read_text(encoding="utf-8")
            )
            self.assertEqual(2, len(intelligence_records))
            self.assertEqual("product_detail", intelligence_records[0]["field_provenance"]["price"]["source"])
            self.assertEqual("product_detail", intelligence_records[0]["field_provenance"]["title"]["source"])
            self.assertEqual("product_detail", intelligence_records[0]["field_provenance"]["media_assets"]["source"])
            self.assertEqual("https://example.com/main.jpg", intelligence_records[0]["media_assets"]["primary_image"])
            self.assertEqual(["Keto friendly"], intelligence_records[0]["description_bullets"])
            self.assertEqual("amazon:B00CF2B04Q", intelligence_records[0]["product_id"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_product_intelligence_falls_back_to_discovery_currency(self) -> None:
        scratch_dir = self._make_scratch_dir("data-collection-intelligence-fallback")
        discovery_connector = SerpApiDiscoveryConnector(
            keyword="monk fruit sweetener",
            client=StubSerpApiDiscoveryClient(self._discovery_response()),
            captured_at="2026-04-24T16:00:00Z",
        )
        product_response = self._product_response("B00CF2B04Q")
        product_response["product_results"].pop("currency")
        product_client = StubSerpApiProductClient({"B00CF2B04Q": product_response})
        try:
            result = run_data_collection(
                keyword="monk fruit sweetener",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                max_products=1,
                discovery_connector=discovery_connector,
                product_client=product_client,
                captured_at="2026-04-24T16:00:00Z",
            )

            intelligence_records = json.loads(
                Path(result.artifacts["product_intelligence_records"]).read_text(encoding="utf-8")
            )
            self.assertEqual("USD", intelligence_records[0]["currency"])
            self.assertEqual("discovery", intelligence_records[0]["field_provenance"]["currency"]["source"])
            self.assertTrue(
                any("detail: currency missing" in warning for warning in intelligence_records[0]["warnings"])
            )
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_product_intelligence_blocks_discovery_price_fallback_when_detail_offer_is_unsafe(self) -> None:
        scratch_dir = self._make_scratch_dir("data-collection-unsafe-offer-price")
        discovery_connector = SerpApiDiscoveryConnector(
            keyword="monk fruit sweetener",
            client=StubSerpApiDiscoveryClient(self._discovery_response()),
            captured_at="2026-04-24T16:00:00Z",
        )
        product_client = StubSerpApiProductClient(
            {"B00CF2B04Q": self._unavailable_product_response("B00CF2B04Q")}
        )
        try:
            result = run_data_collection(
                keyword="monk fruit sweetener",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                max_products=1,
                discovery_connector=discovery_connector,
                product_client=product_client,
                captured_at="2026-04-24T16:00:00Z",
            )

            intelligence_records = json.loads(
                Path(result.artifacts["product_intelligence_records"]).read_text(encoding="utf-8")
            )
            product = intelligence_records[0]
            self.assertIsNone(product["price"])
            self.assertEqual("Currently unavailable", product["availability"])
            self.assertEqual("product_detail", product["field_provenance"]["price"]["source"])
            self.assertTrue(any(issue["code"] == "missing_price" for issue in product["issues"]))
            self.assertTrue(any("price blocked" in warning for warning in product["warnings"]))
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_product_intelligence_preserves_selected_rank_when_discovery_has_duplicate_asin(self) -> None:
        scratch_dir = self._make_scratch_dir("data-collection-intelligence-duplicate-rank")
        discovery_response = self._discovery_response()
        discovery_response["organic_results"].append(
            {
                "position": 8,
                "asin": "B00CF2B04Q",
                "title": "Duplicate later listing",
                "link_clean": "https://www.amazon.com/dp/B00CF2B04Q",
                "rating": 4.1,
                "reviews": 20,
                "extracted_price": 12.99,
                "currency": "USD",
                "sponsored": True,
            }
        )
        discovery_connector = SerpApiDiscoveryConnector(
            keyword="monk fruit sweetener",
            client=StubSerpApiDiscoveryClient(discovery_response),
            captured_at="2026-04-24T16:00:00Z",
        )
        product_client = StubSerpApiProductClient({"B00CF2B04Q": self._product_response("B00CF2B04Q")})
        try:
            result = run_data_collection(
                keyword="monk fruit sweetener",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                max_products=1,
                discovery_connector=discovery_connector,
                product_client=product_client,
                captured_at="2026-04-24T16:00:00Z",
            )

            intelligence_records = json.loads(
                Path(result.artifacts["product_intelligence_records"]).read_text(encoding="utf-8")
            )
            self.assertEqual(1, intelligence_records[0]["discovery_rank"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_product_intelligence_drops_cross_product_narrative_content(self) -> None:
        scratch_dir = self._make_scratch_dir("data-collection-intelligence-consistency")
        discovery_response = {
            "search_metadata": {"status": "Success"},
            "organic_results": [
                {
                    "position": 1,
                    "asin": "B000CANDY1",
                    "title": "Zero Sugar Gummies Assorted Fruit Candy Bag",
                    "link_clean": "https://www.amazon.com/dp/B000CANDY1",
                    "rating": 4.4,
                    "reviews": 1200,
                    "extracted_price": 6.99,
                    "currency": "USD",
                    "sponsored": False,
                }
            ],
        }
        discovery_connector = SerpApiDiscoveryConnector(
            keyword="zero sugar candy",
            client=StubSerpApiDiscoveryClient(discovery_response),
            captured_at="2026-04-24T16:00:00Z",
        )
        product_response = {
            "search_metadata": {"status": "Success"},
            "product_results": {
                "asin": "B000CANDY1",
                "title": "Zero Sugar Gummies Assorted Fruit Candy Bag",
                "brand": "Example Candy",
                "product_link": "https://www.amazon.com/dp/B000CANDY1",
                "price": "$6.99",
                "extracted_price": 6.99,
                "currency": "USD",
                "rating": 4.4,
                "reviews": 1200,
                "availability": "In Stock",
                "thumbnail": "https://example.com/candy-main.jpg",
                "thumbnails": ["https://example.com/candy-1.jpg"],
            },
            "about_item": [
                "Contains one bottle of chocolate syrup",
                "Pour over ice cream and desserts",
            ],
            "product_description": [
                {"position": 1, "title": "Chocolate syrup bottle", "image": "https://example.com/syrup-promo.jpg"}
            ],
            "videos": [],
            "purchase_options": [{"seller": "Amazon.com"}],
        }
        product_client = StubSerpApiProductClient({"B000CANDY1": product_response})
        try:
            result = run_data_collection(
                keyword="zero sugar candy",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                max_products=1,
                discovery_connector=discovery_connector,
                product_client=product_client,
                captured_at="2026-04-24T16:00:00Z",
            )

            intelligence_records = json.loads(
                Path(result.artifacts["product_intelligence_records"]).read_text(encoding="utf-8")
            )
            self.assertEqual([], intelligence_records[0]["description_bullets"])
            self.assertEqual([], intelligence_records[0]["promotional_content"])
            self.assertTrue(
                any(
                    "narrative content dropped" in warning
                    for warning in intelligence_records[0]["warnings"]
                )
            )
            self.assertEqual("https://example.com/candy-main.jpg", intelligence_records[0]["media_assets"]["primary_image"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_merge_product_intelligence_cli_regenerates_from_collection_artifacts(self) -> None:
        scratch_dir = self._make_scratch_dir("merge-product-intelligence-cli")
        discovery_connector = SerpApiDiscoveryConnector(
            keyword="monk fruit sweetener",
            client=StubSerpApiDiscoveryClient(self._discovery_response()),
            captured_at="2026-04-24T16:00:00Z",
        )
        product_client = StubSerpApiProductClient({"B00CF2B04Q": self._product_response("B00CF2B04Q")})
        try:
            run_data_collection(
                keyword="monk fruit sweetener",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "collection-artifacts",
                max_products=1,
                discovery_connector=discovery_connector,
                product_client=product_client,
                captured_at="2026-04-24T16:00:00Z",
            )
            captured_stdout = io.StringIO()
            with redirect_stdout(captured_stdout):
                exit_code = merge_product_intelligence_main(
                    [
                        "--collection-dir",
                        str(scratch_dir / "collection-artifacts"),
                        "--output-dir",
                        str(scratch_dir / "regenerated-intelligence"),
                    ]
                )

            self.assertEqual(0, exit_code)
            payload = json.loads(captured_stdout.getvalue())
            self.assertEqual("success", payload["status"])
            self.assertTrue((scratch_dir / "regenerated-intelligence" / "product_intelligence_records.json").exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_extract_product_details_cli_replays_raw_product_snapshot(self) -> None:
        scratch_dir = self._make_scratch_dir("extract-product-details-cli")
        client = StubSerpApiProductClient({"B00CF2B04Q": self._product_response("B00CF2B04Q")})
        connector = SerpApiProductConnector(
            asins=["B00CF2B04Q"],
            client=client,
            captured_at="2026-04-24T16:00:00Z",
        )
        try:
            ingest_result = IngestionService(FilesystemRawStore(scratch_dir / "raw")).ingest(connector)
            captured_stdout = io.StringIO()
            with redirect_stdout(captured_stdout):
                exit_code = extract_product_details_main(
                    [
                        "--snapshot-id",
                        ingest_result.manifest.snapshot_id,
                        "--store-dir",
                        str(scratch_dir / "raw"),
                        "--output-dir",
                        str(scratch_dir / "details"),
                    ]
                )

            self.assertEqual(0, exit_code)
            payload = json.loads(captured_stdout.getvalue())
            self.assertEqual("success", payload["status"])
            records = json.loads((scratch_dir / "details" / "product_detail_records.json").read_text(encoding="utf-8"))
            self.assertEqual("https://example.com/main.jpg", records[0]["media_assets"]["primary_image"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_data_collection_can_replay_discovery_with_detail_disabled(self) -> None:
        scratch_dir = self._make_scratch_dir("data-collection-replay")
        discovery_connector = SerpApiDiscoveryConnector(
            keyword="monk fruit sweetener",
            client=StubSerpApiDiscoveryClient(self._discovery_response()),
            captured_at="2026-04-24T16:00:00Z",
        )
        try:
            discovery_result = run_discovery(
                keyword="monk fruit sweetener",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "discovery-artifacts",
                connector=discovery_connector,
            )
            result = run_data_collection(
                discovery_snapshot_id=discovery_result.snapshot_id,
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "collection-artifacts",
                max_products=1,
                detail_mode="none",
                captured_at="2026-04-24T16:30:00Z",
            )

            self.assertEqual("success", result.status)
            self.assertEqual(1, len(result.report["selected_candidates"]))
            self.assertFalse(result.report["detail"]["enabled"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_collection_cli_replays_discovery_snapshot_without_live_api(self) -> None:
        scratch_dir = self._make_scratch_dir("data-collection-cli-replay")
        discovery_connector = SerpApiDiscoveryConnector(
            keyword="monk fruit sweetener",
            client=StubSerpApiDiscoveryClient(self._discovery_response()),
            captured_at="2026-04-24T16:00:00Z",
        )
        try:
            discovery_result = run_discovery(
                keyword="monk fruit sweetener",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "discovery-artifacts",
                connector=discovery_connector,
            )
            captured_stdout = io.StringIO()
            with redirect_stdout(captured_stdout):
                exit_code = collect_data_main(
                    [
                        "--discovery-snapshot-id",
                        discovery_result.snapshot_id,
                        "--detail-mode",
                        "none",
                        "--store-dir",
                        str(scratch_dir / "raw"),
                        "--output-dir",
                        str(scratch_dir / "collection-artifacts"),
                    ]
                )

            self.assertEqual(0, exit_code)
            payload = json.loads(captured_stdout.getvalue())
            self.assertEqual("success", payload["status"])
            self.assertTrue((scratch_dir / "collection-artifacts" / "data_collection_report.json").exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_selection_keeps_top_valid_candidates_only(self) -> None:
        selected = select_collection_candidates(
            [
                {"rank": 2, "status": "valid", "discovery_id": "second", "asin": "B0CF6L6PRT"},
                {"rank": 1, "status": "invalid", "discovery_id": "first-invalid", "asin": "B000000000"},
                {"rank": 3, "status": "valid", "discovery_id": "third", "asin": "B014RVNVKS"},
            ],
            max_products=1,
        )

        self.assertEqual("second", selected[0]["discovery_id"])

    def test_query_fit_selection_prefers_same_category_over_adjacent_results(self) -> None:
        selection = evaluate_collection_candidates(
            [
                {
                    "rank": 1,
                    "status": "valid",
                    "discovery_id": "energy-drink",
                    "asin": "B000000001",
                    "title": "Zero Sugar Energy Drink Peach Candy Rings",
                    "rating": 4.4,
                    "review_count": 500,
                    "sponsored": True,
                },
                {
                    "rank": 2,
                    "status": "valid",
                    "discovery_id": "fruit-snacks",
                    "asin": "B000000002",
                    "title": "Low Sugar Fruit Snacks for Kids",
                    "rating": 4.3,
                    "review_count": 400,
                    "sponsored": True,
                },
                {
                    "rank": 3,
                    "status": "valid",
                    "discovery_id": "hard-candy",
                    "asin": "B000000003",
                    "title": "Zero Sugar Hard Candy Assorted Fruit Flavored Candy Bag",
                    "rating": 4.7,
                    "review_count": 9000,
                },
                {
                    "rank": 4,
                    "status": "valid",
                    "discovery_id": "gummy-candy",
                    "asin": "B000000004",
                    "title": "Low Calorie Gummy Candy Variety Pack",
                    "rating": 4.5,
                    "review_count": 2100,
                },
            ],
            query="zero calories candy",
            max_products=2,
        )

        self.assertEqual(["hard-candy", "gummy-candy"], [item["discovery_id"] for item in selection.selected_candidates])
        self.assertEqual("candy", selection.context.family_name)
        self.assertEqual(2, selection.filtered_out_count)
        report = selection.to_report_dict()
        filtered = {record["discovery_id"]: record for record in report["records"] if not record["selected"]}
        self.assertEqual("not_selected", filtered["energy-drink"]["selection_bucket"])
        self.assertIn("energy", filtered["energy-drink"]["exclude_hits"])
        self.assertIn("snacks", filtered["fruit-snacks"]["exclude_hits"])

    def test_query_fit_selection_recognizes_protein_bar_family(self) -> None:
        selection = evaluate_collection_candidates(
            [
                {
                    "rank": 1,
                    "status": "valid",
                    "discovery_id": "protein-powder",
                    "asin": "B000000010",
                    "title": "Vegan Protein Powder Chocolate Shake Mix",
                    "rating": 4.6,
                    "review_count": 5000,
                },
                {
                    "rank": 2,
                    "status": "valid",
                    "discovery_id": "protein-bar",
                    "asin": "B000000011",
                    "title": "Vegan Protein Bars Variety Pack Plant Based Snack Bars",
                    "rating": 4.4,
                    "review_count": 2500,
                },
            ],
            query="vegan protein bar",
            max_products=1,
        )

        self.assertEqual("protein_bar", selection.context.family_name)
        self.assertEqual(["protein-bar"], [item["discovery_id"] for item in selection.selected_candidates])
        filtered = {record["discovery_id"]: record for record in selection.to_report_dict()["records"] if not record["selected"]}
        self.assertIn("powder", filtered["protein-powder"]["exclude_hits"])

    def test_query_fit_selection_recognizes_adjacent_cpg_families(self) -> None:
        cases = [
            (
                "electrolyte powder",
                "hydration",
                "hydration-stick",
                [
                    ("protein-powder", "Whey Protein Powder Chocolate Shake"),
                    ("hydration-stick", "Electrolyte Powder Stick Packs Zero Sugar Hydration"),
                ],
            ),
            (
                "protein powder",
                "protein_powder",
                "protein-powder",
                [
                    ("protein-bar", "Vegan Protein Bars Variety Pack Snack Bars"),
                    ("protein-powder", "Plant Based Protein Powder Vanilla Shake"),
                ],
            ),
            (
                "energy drink",
                "energy_drink",
                "energy-drink",
                [
                    ("energy-candy", "Energy Candy Caffeine Gummies"),
                    ("energy-drink", "Zero Sugar Energy Drink Variety Pack"),
                ],
            ),
        ]

        for query, expected_family, expected_selected_id, records in cases:
            with self.subTest(query=query):
                selection = evaluate_collection_candidates(
                    [
                        {
                            "rank": index,
                            "status": "valid",
                            "discovery_id": discovery_id,
                            "asin": f"B00000010{index}",
                            "title": title,
                            "rating": 4.5,
                            "review_count": 1000,
                        }
                        for index, (discovery_id, title) in enumerate(records, start=1)
                    ],
                    query=query,
                    max_products=1,
                )

                self.assertEqual(expected_family, selection.context.family_name)
                self.assertEqual([expected_selected_id], [item["discovery_id"] for item in selection.selected_candidates])

    def test_data_collection_query_fit_backfills_only_when_needed(self) -> None:
        scratch_dir = self._make_scratch_dir("data-collection-query-fit")
        discovery_connector = SerpApiDiscoveryConnector(
            keyword="zero calories candy",
            client=StubSerpApiDiscoveryClient(self._noisy_candy_discovery_response()),
            captured_at="2026-04-24T16:00:00Z",
        )
        try:
            result = run_data_collection(
                keyword="zero calories candy",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                max_products=2,
                detail_mode="none",
                discovery_connector=discovery_connector,
                captured_at="2026-04-24T16:00:00Z",
            )

            report = json.loads(Path(result.artifacts["data_collection_report"]).read_text(encoding="utf-8"))
            self.assertEqual(["B000000003", "B000000004"], [item["asin"] for item in report["selected_candidates"]])
            self.assertEqual(0, report["selection"]["backfill_count"])
            selection_report = json.loads(Path(result.artifacts["selection_report"]).read_text(encoding="utf-8"))
            self.assertEqual("candy", selection_report["query_family"])
            self.assertEqual(2, selection_report["filtered_out_count"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_normalize_asins_removes_invalid_and_duplicates(self) -> None:
        self.assertEqual(["B00CF2B04Q"], normalize_asins(["bad", "b00cf2b04q", "B00CF2B04Q"]))

    def _discovery_response(self) -> dict:
        return {
            "search_metadata": {"status": "Success"},
            "organic_results": [
                {
                    "position": 1,
                    "asin": "B00CF2B04Q",
                    "title": "Lakanto Classic Monk Fruit Sweetener",
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
            ],
        }

    def _product_response(self, asin: str) -> dict:
        titles = {
            "B00CF2B04Q": "Lakanto Classic Monk Fruit Sweetener",
            "B0CF6L6PRT": "Transparency Foods Liquid Monk Fruit Sweetener",
        }
        return {
            "search_metadata": {"status": "Success"},
            "product_results": {
                "asin": asin,
                "title": titles[asin],
                "brand": "Example Brand",
                "product_link": f"https://www.amazon.com/dp/{asin}",
                "price": "$9.99",
                "extracted_price": 9.99,
                "currency": "USD",
                "rating": 4.6,
                "reviews": 41234,
                "availability": "In Stock",
                "thumbnail": "https://example.com/main.jpg",
                "thumbnails": ["https://example.com/gallery-1.jpg"],
            },
            "about_item": ["Keto friendly"],
            "product_description": [
                {
                    "position": 1,
                    "title": "Lifestyle image",
                    "image": "https://example.com/promo.jpg",
                }
            ],
            "videos": [
                {
                    "position": 1,
                    "title": "Usage video",
                    "link": "https://example.com/video.m3u8",
                    "thumbnail": "https://example.com/video.jpg",
                    "duration": "0:30",
                }
            ],
            "purchase_options": [{"seller": "Amazon.com"}],
        }

    def _unavailable_product_response(self, asin: str) -> dict:
        response = self._product_response(asin)
        response["product_results"]["stock"] = "Currently unavailable"
        response["product_results"]["availability"] = "Currently unavailable"
        response["product_results"]["price"] = "$9.99"
        response["product_results"]["extracted_price"] = 9.99
        response["purchase_options"] = {
            "buying_options": [
                {
                    "title": "See all buying options",
                    "message": "No featured offers available for this product.",
                }
            ]
        }
        return response

    def _noisy_candy_discovery_response(self) -> dict:
        return {
            "search_metadata": {"status": "Success"},
            "organic_results": [
                {
                    "position": 1,
                    "asin": "B000000001",
                    "title": "Zero Sugar Energy Drink Peach Candy Rings",
                    "link_clean": "https://www.amazon.com/dp/B000000001",
                    "rating": 4.4,
                    "reviews": 500,
                    "extracted_price": 29.95,
                    "currency": "USD",
                    "sponsored": True,
                },
                {
                    "position": 2,
                    "asin": "B000000002",
                    "title": "Low Sugar Fruit Snacks for Kids",
                    "link_clean": "https://www.amazon.com/dp/B000000002",
                    "rating": 4.3,
                    "reviews": 400,
                    "extracted_price": 4.99,
                    "currency": "USD",
                    "sponsored": True,
                },
                {
                    "position": 3,
                    "asin": "B000000003",
                    "title": "Zero Sugar Hard Candy Assorted Fruit Flavored Candy Bag",
                    "link_clean": "https://www.amazon.com/dp/B000000003",
                    "rating": 4.7,
                    "reviews": 9000,
                    "extracted_price": 6.34,
                    "currency": "USD",
                    "sponsored": False,
                },
                {
                    "position": 4,
                    "asin": "B000000004",
                    "title": "Low Calorie Gummy Candy Variety Pack",
                    "link_clean": "https://www.amazon.com/dp/B000000004",
                    "rating": 4.5,
                    "reviews": 2100,
                    "extracted_price": 9.99,
                    "currency": "USD",
                    "sponsored": False,
                },
            ],
        }

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
