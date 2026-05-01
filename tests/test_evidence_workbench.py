from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.build_evidence_workbench import main as build_evidence_workbench_main
from brand_gap_inference.evidence_workbench import write_evidence_workbench_artifacts

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"


class EvidenceWorkbenchTests(unittest.TestCase):
    def test_writer_builds_static_review_page_from_collection_artifacts(self) -> None:
        scratch_dir = self._make_scratch_dir("evidence-workbench-writer")
        try:
            collection_dir = self._write_collection_fixture(scratch_dir / "collection")
            artifacts = write_evidence_workbench_artifacts(collection_dir=collection_dir)

            html_path = Path(artifacts["evidence_workbench_html"])
            manifest_path = Path(artifacts["evidence_workbench_manifest"])
            html = html_path.read_text(encoding="utf-8")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

            self.assertEqual("success", manifest["status"])
            self.assertEqual(1, manifest["review_summary"]["product_count"])
            self.assertEqual(1, manifest["review_summary"]["products_with_primary_image"])
            self.assertEqual(0, manifest["review_summary"]["products_missing_primary_image"])
            self.assertEqual(0, manifest["review_summary"]["products_missing_promotional_content"])
            self.assertEqual(1, manifest["review_summary"]["products_with_warnings"])
            self.assertEqual(2, manifest["review_summary"]["total_warning_count"])
            self.assertEqual(1, manifest["review_summary"]["warning_breakdown"]["currency missing"])
            self.assertEqual(1, manifest["review_summary"]["warning_breakdown"]["promotional content missing"])
            self.assertEqual("do_not_prioritize_yet", manifest["review_summary"]["decision_recommendation"])
            self.assertIn("value_variety_protein_bar", manifest["review_summary"]["territory_options"])
            self.assertEqual("No priority gap found in this selected set.", manifest["dashboard_summary"]["run_conclusion"])
            self.assertEqual("reviewable_with_caveats", manifest["evidence_quality_summary"]["quality_label"])
            self.assertEqual(1.0, manifest["evidence_quality_summary"]["primary_image_coverage"])
            self.assertEqual(1, manifest["market_structure_summary"]["coverage_delta"])
            self.assertEqual(1, manifest["product_matrix_summary"]["total_rows"])
            self.assertEqual("B0WORKBENCH", manifest["product_matrix_summary"]["rows"][0]["asin"])
            self.assertEqual("indulgent_snack_bar", manifest["product_matrix_summary"]["rows"][0]["primary_territory"])
            self.assertIn("Evidence Workbench v0", html)
            self.assertIn("Minimalist Evidence Dashboard", html)
            self.assertIn("Dashboard Summary", html)
            self.assertIn("Evidence Quality", html)
            self.assertIn("Market Structure", html)
            self.assertIn("Product Evidence Matrix", html)
            self.assertIn("Review Readiness", html)
            self.assertIn("Review Controls", html)
            self.assertIn('id="product-search"', html)
            self.assertIn('id="territory-filter"', html)
            self.assertIn('data-territories="indulgent_snack_bar|value_variety_protein_bar"', html)
            self.assertIn("100% primary image coverage", html)
            self.assertIn("No priority gap was found after multi-axis coverage review.", html)
            self.assertIn("Example Vegan Protein Bar Variety Pack", html)
            self.assertIn("https://example.com/protein-main.jpg", html)
            self.assertIn("value_variety_protein_bar", html)
            self.assertIn("1 gallery", html)
            self.assertIn("1 promo images", html)
            self.assertIn("1 promo blocks", html)
            self.assertIn("Top Warning Types", html)
            self.assertIn("currency missing", html)
            self.assertIn("Source Artifacts", html)
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_dashboard_summary_surfaces_supported_gap_candidate(self) -> None:
        scratch_dir = self._make_scratch_dir("evidence-workbench-supported-gap")
        try:
            collection_dir = self._write_collection_fixture(scratch_dir / "collection")
            self._write_json(
                collection_dir / "gap_validation" / "gap_validation_report.json",
                {
                    **self._gap_validation_report(),
                    "total_candidates": 1,
                    "supported_candidates": 1,
                    "tentative_candidates": 0,
                    "weak_candidates": 0,
                    "top_candidates": [
                        {
                            "candidate_space": "clean_value_variety_bar",
                            "status": "supported",
                            "validation_score": 0.82,
                        }
                    ],
                },
            )
            self._write_json(
                collection_dir / "decision_brief" / "decision_brief_report.json",
                {
                    **self._decision_brief_report(),
                    "recommendation_level": "validate_now",
                    "headline": "Clean value variety bars are worth validation.",
                    "recommended_next_steps": ["Validate the supported candidate with concept tests."],
                    "blocked_reasons": [],
                },
            )

            artifacts = write_evidence_workbench_artifacts(collection_dir=collection_dir)
            manifest = json.loads(Path(artifacts["evidence_workbench_manifest"]).read_text(encoding="utf-8"))
            html = Path(artifacts["evidence_workbench_html"]).read_text(encoding="utf-8")

            self.assertEqual("1 supported gap candidate(s) found for validation.", manifest["dashboard_summary"]["run_conclusion"])
            self.assertEqual("validate_now", manifest["dashboard_summary"]["recommendation_level"])
            self.assertEqual("Move the supported candidate(s) into controlled validation, not launch.", manifest["dashboard_summary"]["pm_next_step"])
            self.assertIn("clean_value_variety_bar", html)
            self.assertIn("Top Candidates", html)
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_evidence_quality_flags_missing_assets_and_contamination(self) -> None:
        scratch_dir = self._make_scratch_dir("evidence-workbench-quality-flags")
        try:
            collection_dir = self._write_collection_fixture(scratch_dir / "collection")
            product = self._product_intelligence_record()
            product["media_assets"] = {
                "primary_image": "",
                "gallery_images": [],
                "promotional_images": [],
                "videos": [],
            }
            product["promotional_content"] = []
            product["warnings"] = ["narrative content dropped because detail content family looked contaminated"]
            self._write_json(collection_dir / "product_intelligence" / "product_intelligence_records.json", [product])

            artifacts = write_evidence_workbench_artifacts(collection_dir=collection_dir)
            manifest = json.loads(Path(artifacts["evidence_workbench_manifest"]).read_text(encoding="utf-8"))

            quality = manifest["evidence_quality_summary"]
            self.assertEqual(0.0, quality["primary_image_coverage"])
            self.assertEqual(0.0, quality["promo_content_coverage"])
            self.assertIn("possible detail contamination", quality["top_warning_types"])
            self.assertTrue(any("missing primary image" in item for item in quality["missing_evidence_flags"]))
            self.assertTrue(any("missing promo content" in item for item in quality["missing_evidence_flags"]))
            self.assertTrue(any("Possible detail contamination" in item for item in quality["missing_evidence_flags"]))
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_cli_writes_workbench_artifacts(self) -> None:
        scratch_dir = self._make_scratch_dir("evidence-workbench-cli")
        try:
            collection_dir = self._write_collection_fixture(scratch_dir / "collection")
            captured_stdout = io.StringIO()
            with redirect_stdout(captured_stdout):
                exit_code = build_evidence_workbench_main(["--collection-dir", str(collection_dir)])

            self.assertEqual(0, exit_code)
            payload = json.loads(captured_stdout.getvalue())
            self.assertEqual("success", payload["status"])
            self.assertTrue(Path(payload["artifacts"]["evidence_workbench_html"]).exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_writer_surfaces_missing_required_artifacts_as_partial(self) -> None:
        scratch_dir = self._make_scratch_dir("evidence-workbench-partial")
        try:
            collection_dir = scratch_dir / "collection"
            collection_dir.mkdir(parents=True, exist_ok=True)

            artifacts = write_evidence_workbench_artifacts(collection_dir=collection_dir)
            manifest = json.loads(Path(artifacts["evidence_workbench_manifest"]).read_text(encoding="utf-8"))
            html = Path(artifacts["evidence_workbench_html"]).read_text(encoding="utf-8")

            self.assertEqual("partial_success", manifest["status"])
            self.assertEqual(0, manifest["review_summary"]["product_count"])
            self.assertTrue(any("product_intelligence_records" in item for item in manifest["caveats"]))
            self.assertIn("No product intelligence records found.", html)
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _write_collection_fixture(self, collection_dir: Path) -> Path:
        (collection_dir / "product_intelligence").mkdir(parents=True, exist_ok=True)
        (collection_dir / "brand_profiles").mkdir(parents=True, exist_ok=True)
        (collection_dir / "demand_signals").mkdir(parents=True, exist_ok=True)
        (collection_dir / "gap_validation").mkdir(parents=True, exist_ok=True)
        (collection_dir / "decision_brief").mkdir(parents=True, exist_ok=True)
        (collection_dir / "analysis_stack").mkdir(parents=True, exist_ok=True)

        (collection_dir / "product_intelligence" / "product_intelligence_records.json").write_text(
            json.dumps([self._product_intelligence_record()], indent=2),
            encoding="utf-8",
        )
        brand_profile_report = self._brand_profile_report()
        (collection_dir / "brand_profiles" / "brand_profile_report.json").write_text(
            json.dumps(brand_profile_report, indent=2),
            encoding="utf-8",
        )
        (collection_dir / "brand_profiles" / "brand_profile_records.json").write_text(
            json.dumps(brand_profile_report["profiles"], indent=2),
            encoding="utf-8",
        )
        (collection_dir / "demand_signals" / "demand_signal_report.json").write_text(
            json.dumps(self._demand_signal_report(), indent=2),
            encoding="utf-8",
        )
        (collection_dir / "gap_validation" / "gap_validation_report.json").write_text(
            json.dumps(self._gap_validation_report(), indent=2),
            encoding="utf-8",
        )
        (collection_dir / "decision_brief" / "decision_brief_report.json").write_text(
            json.dumps(self._decision_brief_report(), indent=2),
            encoding="utf-8",
        )
        (collection_dir / "analysis_stack" / "analysis_stack_report.json").write_text(
            json.dumps(
                {
                    "run_id": "evidence-workbench-fixture",
                    "status": "success",
                    "warnings": [],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        return collection_dir

    def _write_json(self, path: Path, payload: object) -> None:
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _product_intelligence_record(self) -> dict[str, object]:
        return {
            "product_id": "amazon:B0WORKBENCH",
            "asin": "B0WORKBENCH",
            "title": "Example Vegan Protein Bar Variety Pack",
            "brand": "Example Bar",
            "product_url": "https://www.amazon.com/dp/B0WORKBENCH",
            "price": 21.99,
            "currency": "USD",
            "rating": 4.6,
            "review_count": 2345,
            "availability": "In Stock",
            "media_assets": {
                "primary_image": "https://example.com/protein-main.jpg",
                "gallery_images": ["https://example.com/protein-gallery.jpg"],
                "promotional_images": ["https://example.com/protein-promo.jpg"],
                "videos": [],
            },
            "promotional_content": [
                {
                    "position": 1,
                    "title": "Multiple flavors for repeat snacking",
                    "image": "https://example.com/protein-promo.jpg",
                }
            ],
            "description_bullets": [
                "Vegan protein bar variety pack with multiple flavors.",
                "Low sugar snack for busy routines.",
            ],
            "warnings": ["currency missing from upstream product intelligence"],
            "issues": [],
        }

    def _brand_profile_report(self) -> dict[str, object]:
        return {
            "run_id": "evidence-workbench-fixture",
            "status": "success",
            "category_context": "protein_bar",
            "total_profiles": 1,
            "territory_counts": {"indulgent_snack_bar": 1},
            "territory_coverage_counts": {
                "indulgent_snack_bar": 1,
                "value_variety_protein_bar": 1,
            },
            "pricing_counts": {"mid_market": 1},
            "crowded_territories": [],
            "underrepresented_spaces": [],
            "profiles": [
                {
                    "product_id": "amazon:B0WORKBENCH",
                    "asin": "B0WORKBENCH",
                    "brand_name": "Example Bar",
                    "positioning_territory": "indulgent_snack_bar",
                    "secondary_territories": ["value_variety_protein_bar"],
                    "target_audience": "snackers who want dessert-like flavors",
                    "value_proposition": "A treat-forward vegan protein snack.",
                    "tone_of_voice": "service_oriented",
                    "pricing_stance": "mid_market",
                    "visual_story": "Uses flavor variety and packaging.",
                    "proof_points": ["Multiple flavors"],
                    "primary_claims": ["variety pack", "low sugar"],
                    "evidence_refs": ["product_intelligence:B0WORKBENCH"],
                    "warnings": ["promotional content missing for richer visual signal analysis"],
                }
            ],
            "caveats": [],
        }

    def _demand_signal_report(self) -> dict[str, object]:
        return {
            "run_id": "evidence-workbench-fixture",
            "status": "success",
            "category_context": "protein_bar",
            "source": "discovery_breadth",
            "signals": [
                {
                    "target_territory": "value_variety_protein_bar",
                    "demand_score": 0.74,
                    "matching_discovery_count": 8,
                }
            ],
            "caveats": ["Demand is a discovery-breadth proxy."],
        }

    def _gap_validation_report(self) -> dict[str, object]:
        return {
            "run_id": "evidence-workbench-fixture",
            "status": "success",
            "category_context": "protein_bar",
            "total_candidates": 0,
            "supported_candidates": 0,
            "tentative_candidates": 0,
            "weak_candidates": 0,
            "demand_signal_source": "discovery_breadth",
            "top_candidates": [],
            "records": [],
            "caveats": [],
        }

    def _decision_brief_report(self) -> dict[str, object]:
        return {
            "run_id": "evidence-workbench-fixture",
            "status": "success",
            "category_context": "protein_bar",
            "recommendation_level": "do_not_prioritize_yet",
            "headline": "No priority gap was found after multi-axis coverage review.",
            "executive_summary": "The selected set already covers the visible protein-bar lanes.",
            "top_opportunity": {
                "title": "",
                "candidate_space": "",
                "validation_score": 0.0,
            },
            "decision_rationale": ["No gap candidate was available to evaluate."],
            "recommended_next_steps": ["Test a sharper subcategory query before concept build."],
            "blocked_reasons": ["No missing territory remained after multi-axis coverage review."],
            "quality_warnings": [],
            "caveats": ["Decision support only."],
        }

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
