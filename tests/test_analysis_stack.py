from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.analysis_stack import run_analysis_stack, run_collection_and_analysis
from brand_gap_inference.serpapi_discovery import SerpApiClient, SerpApiDiscoveryConnector
from brand_gap_inference.serpapi_product import SerpApiProductClient

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"


class FakeDeepInferenceClient:
    def infer(
        self,
        *,
        model: str,
        reasoning_effort: str,
        prompt: str,
        schema: dict[str, object],
    ) -> dict[str, object]:
        self._assert_prompt(prompt, schema)
        self._assert_model(model, reasoning_effort)
        return {
            "run_id": "will-be-overridden",
            "model": "will-be-overridden",
            "status": "success",
            "executive_summary": "This set is anchored by a value pantry sugar player with limited premium differentiation.",
            "market_overview": "Selected products cluster around pantry basics and everyday use.",
            "brand_profiles": [
                {
                    "asin": "B0TEST0001",
                    "brand_name": "Example Pantry",
                    "positioning_summary": "Budget pantry staple for everyday sugar use.",
                    "target_audience": "value-oriented household shoppers",
                    "pricing_posture": "budget anchor",
                    "claim_themes": ["value", "everyday use"],
                    "visual_identity": "simple pantry-basic presentation",
                    "confidence": "high",
                    "evidence_refs": ["product_intelligence:B0TEST0001", "brand_positioning:B0TEST0001"],
                }
            ],
            "whitespace_opportunities": [
                "A clearer premium pantry sugar story appears underrepresented in this selected set."
            ],
            "risks": ["This is still a selected-set view, not full demand validation."],
            "evidence_notes": ["Built from deterministic upstream reports and product-intelligence evidence."],
            "caveats": ["Do not treat this as final market truth without broader demand grounding."],
        }

    def _assert_model(self, model: str, reasoning_effort: str) -> None:
        assert model == "gpt-5.4"
        assert reasoning_effort == "high"

    def _assert_prompt(self, prompt: str, schema: dict[str, object]) -> None:
        assert "Brand profile report JSON" in prompt
        assert "Gap validation report JSON" in prompt
        assert "Decision brief report JSON" in prompt
        assert schema["title"] == "DeepBrandInferenceReport"


class FailingDeepInferenceClient:
    def infer(
        self,
        *,
        model: str,
        reasoning_effort: str,
        prompt: str,
        schema: dict[str, object],
    ) -> dict[str, object]:
        raise ValueError("simulated deep inference outage")


class StubSerpApiDiscoveryClient(SerpApiClient):
    def __init__(self, response: dict) -> None:
        self.response = response

    def search_amazon_products(self, keyword: str) -> dict:
        return self.response


class StubSerpApiProductClient(SerpApiProductClient):
    def __init__(self, response: dict[str, dict]) -> None:
        self.response = response

    def fetch_amazon_product(self, asin: str) -> dict:
        return self.response[asin]


class AnalysisStackTests(unittest.TestCase):
    def test_run_analysis_stack_builds_deterministic_artifacts(self) -> None:
        scratch_dir = self._make_scratch_dir("analysis-stack-deterministic")
        try:
            collection_dir = self._write_collection_fixture(scratch_dir / "collection")

            result = run_analysis_stack(collection_dir=collection_dir)

            self.assertEqual("partial_success", result.status)
            self.assertTrue(Path(result.artifacts["analysis_stack_report"]).exists())
            self.assertEqual(
                [
                    "landscape",
                    "brand_positioning",
                    "brand_profiles",
                    "demand_signals",
                    "gap_validation",
                    "decision_brief",
                    "evidence_workbench",
                ],
                result.report["completed_steps"],
            )
            self.assertEqual([], result.report["failed_steps"])
            self.assertIn("brand_profile_report", result.artifacts)
            self.assertIn("demand_signal_report", result.artifacts)
            self.assertIn("gap_validation_report", result.artifacts)
            self.assertIn("decision_brief_report", result.artifacts)
            self.assertIn("evidence_workbench_html", result.artifacts)
            self.assertTrue(any("demand_signals completed with report status" in warning for warning in result.report["warnings"]))
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_run_analysis_stack_supports_optional_deep_inference(self) -> None:
        scratch_dir = self._make_scratch_dir("analysis-stack-deep-success")
        try:
            collection_dir = self._write_collection_fixture(scratch_dir / "collection")

            result = run_analysis_stack(
                collection_dir=collection_dir,
                include_deep_inference=True,
                deep_inference_client=FakeDeepInferenceClient(),
            )

            self.assertEqual("partial_success", result.status)
            self.assertIn("deep_inference", result.report["completed_steps"])
            self.assertIn("evidence_workbench", result.report["completed_steps"])
            self.assertTrue(Path(result.artifacts["deep_brand_inference_report"]).exists())
            self.assertTrue(Path(result.artifacts["evidence_workbench_html"]).exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_run_analysis_stack_keeps_deterministic_outputs_when_deep_inference_fails(self) -> None:
        scratch_dir = self._make_scratch_dir("analysis-stack-deep-failure")
        try:
            collection_dir = self._write_collection_fixture(scratch_dir / "collection")

            result = run_analysis_stack(
                collection_dir=collection_dir,
                include_deep_inference=True,
                deep_inference_client=FailingDeepInferenceClient(),
            )

            self.assertEqual("partial_success", result.status)
            self.assertIn("deep_inference", result.report["failed_steps"])
            self.assertTrue(any("deep_inference failed" in warning for warning in result.report["warnings"]))
            self.assertIn("brand_profile_report", result.artifacts)
            self.assertIn("evidence_workbench_html", result.artifacts)
            self.assertNotIn("deep_brand_inference_report", result.artifacts)
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_run_collection_and_analysis_builds_analysis_stack_after_collection(self) -> None:
        scratch_dir = self._make_scratch_dir("analysis-stack-collection-wrapper")
        discovery_connector = SerpApiDiscoveryConnector(
            keyword="granulated sugar",
            client=StubSerpApiDiscoveryClient(self._discovery_response()),
            captured_at="2026-04-25T10:00:00Z",
        )
        product_client = StubSerpApiProductClient(
            {
                "B0TEST0001": self._product_response("B0TEST0001", "Example Pantry Granulated Sugar 4 lb"),
            }
        )
        try:
            result = run_collection_and_analysis(
                keyword="granulated sugar",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                max_products=1,
                post_analysis="deterministic",
                discovery_connector=discovery_connector,
                product_client=product_client,
                captured_at="2026-04-25T10:00:00Z",
            )

            self.assertIsNotNone(result.analysis)
            assert result.analysis is not None
            self.assertNotEqual("failed", result.status)
            self.assertTrue(Path(result.artifacts["analysis_stack_report"]).exists())
            self.assertIn("gap_validation_report", result.artifacts)
            self.assertIn("decision_brief_report", result.artifacts)
            self.assertIn("evidence_workbench_html", result.artifacts)
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _write_collection_fixture(self, collection_dir: Path) -> Path:
        (collection_dir / "product_intelligence").mkdir(parents=True, exist_ok=True)
        (collection_dir / "discovery").mkdir(parents=True, exist_ok=True)
        (collection_dir / "discovery" / "discovery_records.json").write_text(
            json.dumps(self._discovery_records(), indent=2),
            encoding="utf-8",
        )
        (collection_dir / "product_intelligence" / "product_intelligence_records.json").write_text(
            json.dumps([self._product_intelligence_record()], indent=2),
            encoding="utf-8",
        )
        (collection_dir / "data_collection_report.json").write_text(
            json.dumps(
                {
                    "run_id": "analysis-stack-fixture-001",
                    "selection": {"query_family": "sugar"},
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (collection_dir / "selection_report.json").write_text(
            json.dumps({"query_family": "sugar"}, indent=2),
            encoding="utf-8",
        )
        return collection_dir

    def _discovery_records(self) -> list[dict[str, object]]:
        return [
            {
                "discovery_id": "disc-1",
                "snapshot_id": "snapshot-1",
                "source": "amazon_api_discovery",
                "provider": "serpapi",
                "query": "granulated sugar",
                "rank": 1,
                "status": "valid",
                "title": "Example Pantry Granulated Sugar 4 lb",
                "product_url": "https://www.amazon.com/dp/B0TEST0001",
                "asin": "B0TEST0001",
                "price": 4.99,
                "currency": "USD",
                "rating": 4.7,
                "review_count": 1520,
                "sponsored": False,
                "provider_metadata": {},
                "raw_payload_uri": "fixtures://discovery/B0TEST0001",
                "warnings": [],
                "issues": [],
            }
        ]

    def _product_intelligence_record(self) -> dict[str, object]:
        return {
            "product_id": "amazon:B0TEST0001",
            "asin": "B0TEST0001",
            "title": "Example Pantry Granulated Sugar 4 lb",
            "brand": "Example Pantry",
            "product_url": "https://www.amazon.com/dp/B0TEST0001",
            "price": 4.99,
            "currency": "USD",
            "rating": 4.7,
            "review_count": 1520,
            "availability": "In Stock",
            "media_assets": {
                "primary_image": "https://example.com/sugar-main.jpg",
                "gallery_images": ["https://example.com/sugar-gallery-1.jpg"],
                "promotional_images": ["https://example.com/sugar-promo.jpg"],
                "videos": [],
            },
            "promotional_content": [
                {
                    "position": 1,
                    "title": "Affordable pantry staple for baking and daily use",
                    "image": "https://example.com/sugar-promo.jpg",
                }
            ],
            "description_bullets": [
                "Everyday granulated sugar for pantry use.",
                "Great for baking and coffee.",
            ],
            "discovery_rank": 1,
            "sponsored": False,
            "source_snapshots": {"collection_run_id": "analysis-stack-fixture-001"},
            "field_provenance": {},
            "warnings": [],
            "issues": [],
        }

    def _discovery_response(self) -> dict[str, object]:
        return {
            "search_metadata": {"status": "Success"},
            "organic_results": [
                {
                    "position": 1,
                    "asin": "B0TEST0001",
                    "title": "Example Pantry Granulated Sugar 4 lb",
                    "link_clean": "https://www.amazon.com/dp/B0TEST0001",
                    "rating": 4.7,
                    "reviews": 1520,
                    "extracted_price": 4.99,
                    "currency": "USD",
                    "sponsored": False,
                }
            ],
        }

    def _product_response(self, asin: str, title: str) -> dict[str, object]:
        return {
            "search_metadata": {"status": "Success"},
            "product_results": {
                "asin": asin,
                "title": title,
                "brand": "Example Pantry",
                "product_link": f"https://www.amazon.com/dp/{asin}",
                "price": "$4.99",
                "extracted_price": 4.99,
                "currency": "USD",
                "rating": 4.7,
                "reviews": 1520,
                "availability": "In Stock",
                "thumbnail": "https://example.com/sugar-main.jpg",
                "thumbnails": ["https://example.com/sugar-gallery-1.jpg"],
            },
            "about_item": ["Everyday granulated sugar", "Great for baking"],
            "product_description": [
                {
                    "position": 1,
                    "title": "Affordable pantry staple",
                    "image": "https://example.com/sugar-promo.jpg",
                }
            ],
            "videos": [],
            "purchase_options": [{"seller": "Amazon.com"}],
        }

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
