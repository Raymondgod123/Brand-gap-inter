from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.decision_brief import DecisionBriefBuilder, write_decision_brief_artifacts

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"


class DecisionBriefTests(unittest.TestCase):
    def test_supported_gap_becomes_validate_now_brief(self) -> None:
        report = DecisionBriefBuilder().build(
            run_id="decision-fixture-001",
            brand_profile_report=_brand_profile_report(status="partial_success"),
            demand_signal_report=_demand_signal_report(),
            gap_validation_report=_gap_validation_report(
                status="partial_success",
                candidate_status="supported",
                validation_score=0.86,
                demand_score=0.79,
                traction_score=0.68,
            ),
            product_intelligence_records=[_product_intelligence_record("B01D0WCQOC")],
        )

        self.assertEqual("success", report["status"])
        self.assertEqual("validate_now", report["recommendation_level"])
        self.assertEqual("premium indulgence candy", report["top_opportunity"]["candidate_space"])
        self.assertEqual("B01D0WCQOC", report["top_opportunity"]["adjacent_products"][0]["asin"])
        self.assertTrue(any("concept hypothesis" in step for step in report["recommended_next_steps"]))
        self.assertTrue(any("brand profile report status" in warning for warning in report["quality_warnings"]))

    def test_tentative_gap_becomes_research_before_validation(self) -> None:
        report = DecisionBriefBuilder().build(
            run_id="decision-fixture-002",
            brand_profile_report=_brand_profile_report(status="success"),
            demand_signal_report=_demand_signal_report(),
            gap_validation_report=_gap_validation_report(
                status="success",
                candidate_status="tentative",
                validation_score=0.61,
                demand_score=0.53,
                traction_score=0.25,
            ),
        )

        self.assertEqual("research_before_validation", report["recommendation_level"])
        self.assertTrue(any("Demand score is below" in reason for reason in report["blocked_reasons"]))
        self.assertTrue(any("Broaden discovery" in step for step in report["recommended_next_steps"]))

    def test_empty_successful_gap_report_becomes_no_priority_brief(self) -> None:
        report = DecisionBriefBuilder().build(
            run_id="decision-fixture-003",
            brand_profile_report=_brand_profile_report(status="partial_success"),
            demand_signal_report=_demand_signal_report(),
            gap_validation_report=_empty_gap_validation_report(),
        )

        self.assertEqual("success", report["status"])
        self.assertEqual("do_not_prioritize_yet", report["recommendation_level"])
        self.assertIn("No priority gap", report["headline"])
        self.assertTrue(any("multi-axis coverage" in reason for reason in report["blocked_reasons"]))

    def test_write_decision_brief_artifacts(self) -> None:
        scratch_dir = SCRATCH_ROOT / "decision-brief-artifacts"
        shutil.rmtree(scratch_dir, ignore_errors=True)
        try:
            collection_dir = scratch_dir / "collection"
            (collection_dir / "brand_profiles").mkdir(parents=True, exist_ok=True)
            (collection_dir / "demand_signals").mkdir(parents=True, exist_ok=True)
            (collection_dir / "gap_validation").mkdir(parents=True, exist_ok=True)
            (collection_dir / "product_intelligence").mkdir(parents=True, exist_ok=True)
            (collection_dir / "brand_profiles" / "brand_profile_report.json").write_text(
                json.dumps(_brand_profile_report(status="success"), indent=2),
                encoding="utf-8",
            )
            (collection_dir / "demand_signals" / "demand_signal_report.json").write_text(
                json.dumps(_demand_signal_report(), indent=2),
                encoding="utf-8",
            )
            (collection_dir / "gap_validation" / "gap_validation_report.json").write_text(
                json.dumps(
                    _gap_validation_report(
                        status="partial_success",
                        candidate_status="supported",
                        validation_score=0.86,
                        demand_score=0.79,
                        traction_score=0.68,
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            (collection_dir / "product_intelligence" / "product_intelligence_records.json").write_text(
                json.dumps([_product_intelligence_record("B01D0WCQOC")], indent=2),
                encoding="utf-8",
            )

            artifacts = write_decision_brief_artifacts(
                collection_dir=collection_dir,
                output_dir=collection_dir / "decision_brief",
            )

            self.assertTrue(Path(artifacts["decision_brief_report"]).exists())
            self.assertTrue(Path(artifacts["decision_brief_report_md"]).exists())
            payload = json.loads(Path(artifacts["decision_brief_report"]).read_text(encoding="utf-8"))
            self.assertEqual("Example Candy", payload["top_opportunity"]["adjacent_products"][0]["brand"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)


def _brand_profile_report(*, status: str) -> dict[str, object]:
    return {
        "run_id": "decision-fixture",
        "status": status,
        "category_context": "candy",
        "total_profiles": 15,
        "territory_counts": {"mainstream_zero_sugar_candy": 2},
        "pricing_counts": {"mid_market": 3},
        "crowded_territories": ["mainstream_zero_sugar_candy"],
        "underrepresented_spaces": ["No clear premium indulgence zero-sugar candy player appears."],
        "profiles": [],
        "caveats": ["This market map is directional only."],
    }


def _demand_signal_report() -> dict[str, object]:
    return {
        "run_id": "decision-fixture",
        "status": "success",
        "query": "zero calories candy",
        "category_context": "candy",
        "source": "discovery_breadth",
        "valid_discovery_count": 60,
        "total_signals": 5,
        "signals": [],
        "caveats": ["Demand signals currently use replayable discovery breadth."],
    }


def _gap_validation_report(
    *,
    status: str,
    candidate_status: str,
    validation_score: float,
    demand_score: float,
    traction_score: float,
) -> dict[str, object]:
    candidate = {
        "gap_id": "decision-fixture-premium-indulgence",
        "title": "Missing territory: premium indulgence candy",
        "candidate_space": "premium indulgence candy",
        "target_territory": "premium_indulgence_candy",
        "target_pricing_stance": "unspecified",
        "whitespace_type": "missing_territory",
        "status": candidate_status,
        "supply_gap_score": 0.95,
        "traction_score": traction_score,
        "demand_score": demand_score,
        "price_realism_score": 1.0,
        "validation_score": validation_score,
        "adjacent_asins": ["B01D0WCQOC", "B077Y4R8KC"],
        "evidence": ["No selected profile is currently mapped to premium_indulgence_candy."],
        "caveats": ["This is a selected-set gap, not a full market census."],
    }
    return {
        "run_id": "decision-fixture",
        "status": status,
        "category_context": "candy",
        "total_candidates": 1,
        "supported_candidates": 1 if candidate_status == "supported" else 0,
        "tentative_candidates": 1 if candidate_status == "tentative" else 0,
        "weak_candidates": 1 if candidate_status == "weak" else 0,
        "demand_signal_source": "discovery_breadth",
        "top_candidates": [candidate],
        "records": [candidate],
        "caveats": ["A supported candidate is still directional."],
    }


def _empty_gap_validation_report() -> dict[str, object]:
    return {
        "run_id": "decision-fixture",
        "status": "success",
        "category_context": "protein_bar",
        "total_candidates": 0,
        "supported_candidates": 0,
        "tentative_candidates": 0,
        "weak_candidates": 0,
        "demand_signal_source": "discovery_breadth",
        "top_candidates": [],
        "records": [],
        "caveats": ["A supported candidate is still directional."],
    }


def _product_intelligence_record(asin: str) -> dict[str, object]:
    return {
        "product_id": f"amazon:{asin}",
        "asin": asin,
        "title": "Example Candy Zero Sugar Chocolate Caramel",
        "brand": "Example Candy",
        "product_url": f"https://www.amazon.com/dp/{asin}",
        "price": 14.99,
        "currency": "USD",
        "rating": 4.6,
        "review_count": 1234,
        "availability": "In Stock",
        "media_assets": {
            "primary_image": "https://example.com/main.jpg",
            "gallery_images": ["https://example.com/gallery.jpg"],
            "promotional_images": ["https://example.com/promo.jpg"],
            "videos": [],
        },
        "promotional_content": [{"title": "Rich caramel flavor", "image": "https://example.com/promo.jpg"}],
        "description_bullets": ["Sugar free chocolate caramel candy."],
        "source_snapshots": {},
        "field_provenance": {},
        "warnings": [],
        "issues": [],
    }


if __name__ == "__main__":
    unittest.main()
