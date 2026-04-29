from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.deep_inference import write_deep_inference_artifacts

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
        assert model == "gpt-5.4"
        assert reasoning_effort == "high"
        assert "Landscape report JSON" in prompt
        assert "Brand profile report JSON" in prompt
        assert "Demand signal report JSON" in prompt
        assert "Gap validation report JSON" in prompt
        assert "Decision brief report JSON" in prompt
        assert schema["title"] == "DeepBrandInferenceReport"
        return {
            "run_id": "will-be-overridden",
            "model": "will-be-overridden",
            "status": "partial_success",
            "executive_summary": "The category splits between value pantry staples and convenience-led formats.",
            "market_overview": "Selected competitors cluster around pantry basics, packets, and branded household sugar.",
            "brand_profiles": [
                {
                    "asin": "B0D1L1KSMZ",
                    "brand_name": "Amazon Saver",
                    "positioning_summary": "Budget pantry staple for everyday sugar use.",
                    "target_audience": "value-oriented household shoppers",
                    "pricing_posture": "budget anchor",
                    "claim_themes": ["value", "everyday use"],
                    "visual_identity": "simple pantry-basic presentation",
                    "confidence": "high",
                    "evidence_refs": ["landscape.competitors[0]", "brand_positioning.records[0]"],
                }
            ],
            "whitespace_opportunities": [
                "There is room for a more premium health-forward cane sugar narrative with stronger packaging differentiation."
            ],
            "risks": ["Currency is missing for some provider records, so price posture should stay relative."],
            "evidence_notes": ["Analysis is grounded in structured collection artifacts and deterministic brand-positioning output."],
            "caveats": ["Do not treat this as final market truth without validating provider omissions."],
        }


class DeepInferenceTests(unittest.TestCase):
    def test_write_deep_inference_artifacts_from_collection_dir(self) -> None:
        scratch_dir = self._make_scratch_dir("deep-inference")
        try:
            collection_dir = scratch_dir / "collection"
            (collection_dir / "product_intelligence").mkdir(parents=True, exist_ok=True)
            (collection_dir / "landscape").mkdir(parents=True, exist_ok=True)
            (collection_dir / "brand_positioning").mkdir(parents=True, exist_ok=True)
            (collection_dir / "brand_profiles").mkdir(parents=True, exist_ok=True)
            (collection_dir / "demand_signals").mkdir(parents=True, exist_ok=True)
            (collection_dir / "gap_validation").mkdir(parents=True, exist_ok=True)
            (collection_dir / "decision_brief").mkdir(parents=True, exist_ok=True)

            (collection_dir / "product_intelligence" / "product_intelligence_records.json").write_text(
                json.dumps(
                    [
                        {
                            "product_id": "amazon:B0D1L1KSMZ",
                            "asin": "B0D1L1KSMZ",
                            "title": "Amazon Saver White Sugar 4 Lb",
                            "brand": "Amazon Saver",
                            "price": 4.99,
                            "currency": None,
                            "rating": 4.6,
                            "review_count": 123,
                            "availability": "in_stock",
                            "media_assets": {"primary_image": "https://example.com/sugar.jpg", "gallery_images": []},
                            "promotional_content": [],
                            "description_bullets": ["Everyday granulated sugar"],
                            "source_snapshots": {"collection_run_id": "data-collection-live-sugar-3"}
                        }
                    ],
                    indent=2,
                ),
                encoding="utf-8",
            )
            (collection_dir / "landscape" / "landscape_report.json").write_text(
                json.dumps(
                    {
                        "run_id": "data-collection-live-sugar-3",
                        "status": "partial_success",
                        "product_count": 1,
                        "competitors": [],
                        "price_ladder": [],
                        "rating_ladder": [],
                        "review_ladder": [],
                        "claim_patterns": [],
                        "caveats": ["1 products are missing currency; do not use price comparisons as final."],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            (collection_dir / "brand_positioning" / "brand_positioning_report.json").write_text(
                json.dumps(
                    {
                        "run_id": "data-collection-live-sugar-3",
                        "status": "partial_success",
                        "total_products": 1,
                        "archetype_counts": {"value_pantry_staple": 1},
                        "market_themes": ["value", "pantry"],
                        "records": [],
                        "caveats": ["Visual strategy is coarse and evidence-backed, not image-semantic."],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            (collection_dir / "brand_profiles" / "brand_profile_report.json").write_text(
                json.dumps(
                    {
                        "run_id": "data-collection-live-sugar-3",
                        "status": "partial_success",
                        "category_context": "sugar",
                        "total_profiles": 1,
                        "territory_counts": {"value_pantry_basics": 1},
                        "pricing_counts": {"budget_anchor": 1},
                        "crowded_territories": [],
                        "underrepresented_spaces": ["No clear premium pantry sugar player appears in this selected set."],
                        "profiles": [],
                        "caveats": ["This market map is directional only."],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            (collection_dir / "demand_signals" / "demand_signal_report.json").write_text(
                json.dumps(
                    {
                        "run_id": "data-collection-live-sugar-3",
                        "status": "partial_success",
                        "query": "sugar",
                        "category_context": "sugar",
                        "source": "discovery_breadth",
                        "valid_discovery_count": 1,
                        "total_signals": 1,
                        "signals": [],
                        "caveats": ["Demand signals are discovery-breadth proxies."],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            (collection_dir / "gap_validation" / "gap_validation_report.json").write_text(
                json.dumps(
                    {
                        "run_id": "data-collection-live-sugar-3",
                        "status": "partial_success",
                        "category_context": "sugar",
                        "total_candidates": 1,
                        "supported_candidates": 1,
                        "tentative_candidates": 0,
                        "weak_candidates": 0,
                        "demand_signal_source": "discovery_breadth",
                        "top_candidates": [],
                        "records": [],
                        "caveats": ["Demand grounding is still selected-set only."],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            (collection_dir / "decision_brief" / "decision_brief_report.json").write_text(
                json.dumps(
                    {
                        "run_id": "data-collection-live-sugar-3",
                        "status": "success",
                        "category_context": "sugar",
                        "recommendation_level": "validate_with_caution",
                        "headline": "Sugar opportunity needs cautious validation.",
                        "executive_summary": "This is a PM decision-support brief.",
                        "opportunity_count_summary": {
                            "total_candidates": 1,
                            "supported_candidates": 1,
                            "tentative_candidates": 0,
                            "weak_candidates": 0,
                        },
                        "top_opportunity": {
                            "gap_id": "gap-1",
                            "title": "Missing territory: premium pantry basics",
                            "candidate_space": "premium pantry basics",
                            "status": "supported",
                            "validation_score": 0.72,
                            "demand_score": 0.68,
                            "traction_score": 0.6,
                            "supply_gap_score": 0.95,
                            "price_realism_score": 1.0,
                            "adjacent_asins": [],
                        },
                        "decision_rationale": ["Evidence is directional."],
                        "recommended_next_steps": ["Validate before investment."],
                        "validation_requirements": ["Confirm demand externally."],
                        "blocked_reasons": [],
                        "quality_warnings": [],
                        "source_reports": {},
                        "caveats": ["Decision support only."],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            artifacts = write_deep_inference_artifacts(
                collection_dir=collection_dir,
                output_dir=collection_dir / "deep_inference",
                client=FakeDeepInferenceClient(),
                model="gpt-5.4",
                reasoning_effort="high",
            )

            json_path = Path(artifacts["deep_brand_inference_report"])
            md_path = Path(artifacts["deep_brand_inference_report_md"])
            self.assertTrue(json_path.exists())
            self.assertTrue(md_path.exists())

            payload = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual("data-collection-live-sugar-3", payload["run_id"])
            self.assertEqual("gpt-5.4", payload["model"])
            self.assertEqual("partial_success", payload["status"])

            markdown = md_path.read_text(encoding="utf-8")
            self.assertIn("Deep Brand Inference Report", markdown)
            self.assertIn("Amazon Saver", markdown)
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
