from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.build_gap_validation import main as build_gap_validation_main
from brand_gap_inference.gap_validation import GapValidationBuilder, write_gap_validation_artifacts

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"


class GapValidationTests(unittest.TestCase):
    def test_builder_prioritizes_missing_health_forward_space(self) -> None:
        result = GapValidationBuilder().build(
            run_id="collection-1",
            product_intelligence_records=self._product_intelligence_records(),
            brand_profile_records=self._brand_profile_records(),
            brand_profile_report=self._brand_profile_report(),
        )
        report = result.to_report_dict()

        self.assertEqual("success", report["status"])
        self.assertEqual(0, report["supported_candidates"])
        self.assertIn("health_forward_alternative", report["top_candidates"][0]["target_territory"])
        self.assertEqual("tentative", report["top_candidates"][0]["status"])

    def test_gap_validation_cli_writes_json_and_markdown(self) -> None:
        scratch_dir = self._make_scratch_dir("gap-validation-cli")
        try:
            self._write_collection_fixture(scratch_dir / "collection")
            captured_stdout = io.StringIO()
            with redirect_stdout(captured_stdout):
                exit_code = build_gap_validation_main(
                    [
                        "--collection-dir",
                        str(scratch_dir / "collection"),
                        "--output-dir",
                        str(scratch_dir / "collection" / "gap_validation"),
                    ]
                )

            self.assertEqual(0, exit_code)
            payload = json.loads(captured_stdout.getvalue())
            self.assertEqual("success", payload["status"])
            self.assertTrue((scratch_dir / "collection" / "gap_validation" / "gap_validation_report.json").exists())
            self.assertTrue((scratch_dir / "collection" / "gap_validation" / "gap_validation_report.md").exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_builder_uses_candy_context_for_gap_spaces(self) -> None:
        result = GapValidationBuilder().build(
            run_id="collection-candy",
            product_intelligence_records=self._candy_product_intelligence_records(),
            brand_profile_records=self._candy_brand_profile_records(),
            brand_profile_report=self._candy_brand_profile_report(),
        )
        report = result.to_report_dict()

        self.assertEqual("candy", report["category_context"])
        self.assertTrue(any(record["target_territory"] == "premium_indulgence_candy" for record in report["records"]))
        self.assertTrue(any("premium indulgence candy" in record["candidate_space"] for record in report["records"]))

    def test_builder_uses_protein_bar_context_for_gap_spaces(self) -> None:
        result = GapValidationBuilder().build(
            run_id="collection-protein-bar",
            product_intelligence_records=[
                {
                    "asin": "B0FUNCTION",
                    "rating": 4.5,
                    "review_count": 1200,
                }
            ],
            brand_profile_records=[
                {
                    "asin": "B0FUNCTION",
                    "positioning_territory": "functional_performance_bar",
                    "pricing_stance": "premium_pantry",
                    "primary_claims": ["energy", "focus"],
                    "proof_points": ["Vegan protein bar for energy and focus."],
                }
            ],
            brand_profile_report={
                "run_id": "collection-protein-bar",
                "status": "success",
                "category_context": "protein_bar",
                "underrepresented_spaces": [
                    "No clear clean plant-protein bar player appears in this selected set."
                ],
            },
        )
        report = result.to_report_dict()

        self.assertEqual("protein_bar", report["category_context"])
        self.assertTrue(any(record["target_territory"] == "clean_plant_protein_bar" for record in report["records"]))
        self.assertTrue(any("clean plant protein bar" in record["candidate_space"] for record in report["records"]))

    def test_secondary_territory_coverage_suppresses_missing_territory_gap(self) -> None:
        result = GapValidationBuilder().build(
            run_id="collection-protein-bar-secondary",
            product_intelligence_records=[
                {
                    "asin": "B0FUNCTION",
                    "rating": 4.5,
                    "review_count": 1200,
                }
            ],
            brand_profile_records=[
                {
                    "asin": "B0FUNCTION",
                    "positioning_territory": "functional_performance_bar",
                    "secondary_territories": ["clean_plant_protein_bar"],
                    "pricing_stance": "premium_pantry",
                    "primary_claims": ["energy", "clean"],
                    "proof_points": ["Clean vegan protein bar for energy and focus."],
                }
            ],
            brand_profile_report={
                "run_id": "collection-protein-bar-secondary",
                "status": "success",
                "category_context": "protein_bar",
                "underrepresented_spaces": [],
            },
        )
        report = result.to_report_dict()

        missing_territory_records = [
            record for record in report["records"] if record["whitespace_type"] == "missing_territory"
        ]
        self.assertFalse(
            any(record["target_territory"] == "clean_plant_protein_bar" for record in missing_territory_records)
        )
        self.assertTrue(
            any(record["target_territory"] == "value_variety_protein_bar" for record in missing_territory_records)
        )

    def test_builder_uses_adjacent_cpg_contexts_for_gap_spaces(self) -> None:
        cases = [
            ("hydration", "performance_electrolyte", "clean_daily_hydration", "clean daily hydration"),
            ("protein_powder", "performance_muscle_protein", "clean_plant_protein_powder", "clean plant protein powder"),
            ("energy_drink", "zero_sugar_energy_drink", "focus_functional_energy", "focus functional energy"),
        ]

        for category_context, observed_territory, missing_territory, expected_fragment in cases:
            with self.subTest(category_context=category_context):
                result = GapValidationBuilder().build(
                    run_id=f"collection-{category_context}",
                    product_intelligence_records=[
                        {
                            "asin": "B0ADJACENT",
                            "rating": 4.6,
                            "review_count": 1400,
                        }
                    ],
                    brand_profile_records=[
                        {
                            "asin": "B0ADJACENT",
                            "positioning_territory": observed_territory,
                            "secondary_territories": [],
                            "pricing_stance": "premium_pantry",
                            "primary_claims": ["clean", "focus", "electrolyte"],
                            "proof_points": ["Clean focus electrolyte product with strong adjacent evidence."],
                        }
                    ],
                    brand_profile_report={
                        "run_id": f"collection-{category_context}",
                        "status": "success",
                        "category_context": category_context,
                        "underrepresented_spaces": [],
                    },
                )
                report = result.to_report_dict()

                self.assertEqual(category_context, report["category_context"])
                self.assertTrue(any(record["target_territory"] == missing_territory for record in report["records"]))
                self.assertTrue(any(expected_fragment in record["candidate_space"] for record in report["records"]))

    def test_adjacent_secondary_coverage_suppresses_missing_territory_gap(self) -> None:
        result = GapValidationBuilder().build(
            run_id="collection-hydration-secondary",
            product_intelligence_records=[
                {
                    "asin": "B0HYDRATE",
                    "rating": 4.6,
                    "review_count": 1400,
                }
            ],
            brand_profile_records=[
                {
                    "asin": "B0HYDRATE",
                    "positioning_territory": "performance_electrolyte",
                    "secondary_territories": ["zero_sugar_hydration"],
                    "pricing_stance": "premium_pantry",
                    "primary_claims": ["electrolyte", "zero sugar"],
                    "proof_points": ["Electrolyte powder with zero sugar for workouts."],
                }
            ],
            brand_profile_report={
                "run_id": "collection-hydration-secondary",
                "status": "success",
                "category_context": "hydration",
                "underrepresented_spaces": [],
            },
        )
        report = result.to_report_dict()
        missing_territory_records = [
            record for record in report["records"] if record["whitespace_type"] == "missing_territory"
        ]

        self.assertFalse(
            any(record["target_territory"] == "zero_sugar_hydration" for record in missing_territory_records)
        )
        self.assertTrue(
            any(record["target_territory"] == "clean_daily_hydration" for record in missing_territory_records)
        )

    def test_builder_uses_demand_signal_report_in_validation_score(self) -> None:
        baseline = GapValidationBuilder().build(
            run_id="collection-candy",
            product_intelligence_records=self._candy_product_intelligence_records(),
            brand_profile_records=self._candy_brand_profile_records(),
            brand_profile_report=self._candy_brand_profile_report(),
        ).to_report_dict()
        grounded = GapValidationBuilder().build(
            run_id="collection-candy",
            product_intelligence_records=self._candy_product_intelligence_records(),
            brand_profile_records=self._candy_brand_profile_records(),
            brand_profile_report=self._candy_brand_profile_report(),
            demand_signal_report=self._candy_demand_signal_report(),
        ).to_report_dict()

        baseline_candidate = self._candidate_by_territory(baseline, "premium_indulgence_candy")
        grounded_candidate = self._candidate_by_territory(grounded, "premium_indulgence_candy")
        self.assertEqual("neutral_default", baseline["demand_signal_source"])
        self.assertEqual("discovery_breadth", grounded["demand_signal_source"])
        self.assertEqual(0.5, baseline_candidate["demand_score"])
        self.assertEqual(0.95, grounded_candidate["demand_score"])
        self.assertGreater(grounded_candidate["validation_score"], baseline_candidate["validation_score"])

    def test_gap_validation_writer_validates_artifacts(self) -> None:
        scratch_dir = self._make_scratch_dir("gap-validation-writer")
        try:
            collection_dir = scratch_dir / "collection"
            self._write_collection_fixture(collection_dir)
            artifacts = write_gap_validation_artifacts(
                collection_dir=collection_dir,
                output_dir=collection_dir / "gap_validation",
            )
            self.assertTrue(Path(artifacts["gap_validation_records"]).exists())
            self.assertTrue(Path(artifacts["gap_validation_report"]).exists())
            self.assertTrue(Path(artifacts["gap_validation_report_md"]).exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _write_collection_fixture(self, collection_dir: Path) -> None:
        (collection_dir / "product_intelligence").mkdir(parents=True, exist_ok=True)
        (collection_dir / "brand_profiles").mkdir(parents=True, exist_ok=True)
        (collection_dir / "demand_signals").mkdir(parents=True, exist_ok=True)
        (collection_dir / "product_intelligence" / "product_intelligence_records.json").write_text(
            json.dumps(self._product_intelligence_records(), indent=2),
            encoding="utf-8",
        )
        (collection_dir / "brand_profiles" / "brand_profile_records.json").write_text(
            json.dumps(self._brand_profile_records(), indent=2),
            encoding="utf-8",
        )
        (collection_dir / "brand_profiles" / "brand_profile_report.json").write_text(
            json.dumps(self._brand_profile_report(), indent=2),
            encoding="utf-8",
        )
        (collection_dir / "demand_signals" / "demand_signal_report.json").write_text(
            json.dumps(self._demand_signal_report(), indent=2),
            encoding="utf-8",
        )

    def _product_intelligence_records(self) -> list[dict[str, object]]:
        return [
            {
                "product_id": "amazon:B0D1L1KSMZ",
                "asin": "B0D1L1KSMZ",
                "title": "Amazon Saver, White Sugar, 4 Lb",
                "brand": "Visit the Amazon Saver Store",
                "product_url": "https://www.amazon.com/dp/B0D1L1KSMZ",
                "price": 3.07,
                "currency": None,
                "rating": 4.8,
                "review_count": 1543,
                "availability": "In Stock"
            },
            {
                "product_id": "amazon:B0FZXW7LVY",
                "asin": "B0FZXW7LVY",
                "title": "Sweetmo Sugar Packets Variety Pack",
                "brand": "Sweetmo",
                "product_url": "https://www.amazon.com/dp/B0FZXW7LVY",
                "price": 22.49,
                "currency": None,
                "rating": 4.8,
                "review_count": 60,
                "availability": "In Stock"
            },
            {
                "product_id": "amazon:B09RPPBG15",
                "asin": "B09RPPBG15",
                "title": "Domino Granulated Sugar, 20 oz Canister, Pack of 3",
                "brand": "Domino",
                "product_url": "https://www.amazon.com/dp/B09RPPBG15",
                "price": 22.99,
                "currency": None,
                "rating": 4.5,
                "review_count": 917,
                "availability": "In Stock"
            }
        ]

    def _brand_profile_records(self) -> list[dict[str, object]]:
        return [
            {
                "product_id": "amazon:B0D1L1KSMZ",
                "asin": "B0D1L1KSMZ",
                "brand_name": "Amazon Saver",
                "positioning_territory": "value_pantry_basics",
                "target_audience": "budget-focused household pantry shoppers",
                "value_proposition": "Affordable everyday sugar basics for routine pantry replenishment.",
                "tone_of_voice": "plainspoken_value",
                "pricing_stance": "budget_anchor",
                "visual_story": "Uses packaging, promo, and video together.",
                "proof_points": [
                    "Amazon Saver, White Sugar, 4 Lb",
                    "Gluten free"
                ],
                "primary_claims": [
                    "saver",
                    "affordable",
                    "budget"
                ],
                "evidence_refs": [
                    "product_intelligence:B0D1L1KSMZ",
                    "brand_positioning:B0D1L1KSMZ"
                ],
                "warnings": [
                    "currency missing from upstream product intelligence"
                ]
            },
            {
                "product_id": "amazon:B0FZXW7LVY",
                "asin": "B0FZXW7LVY",
                "brand_name": "Sweetmo",
                "positioning_territory": "convenience_beverage_station",
                "target_audience": "office, hospitality, and home beverage-station restockers",
                "value_proposition": "Convenient multi-format sweetening.",
                "tone_of_voice": "service_oriented",
                "pricing_stance": "mid_market",
                "visual_story": "Uses packet-heavy merchandising and video proof.",
                "proof_points": [
                    "Sweetmo Sugar Packets Variety Pack",
                    "Perfect for coffee and tea stations."
                ],
                "primary_claims": [
                    "packets",
                    "variety pack",
                    "office",
                    "home"
                ],
                "evidence_refs": [
                    "product_intelligence:B0FZXW7LVY",
                    "brand_positioning:B0FZXW7LVY"
                ],
                "warnings": [
                    "currency missing from upstream product intelligence"
                ]
            },
            {
                "product_id": "amazon:B09RPPBG15",
                "asin": "B09RPPBG15",
                "brand_name": "Domino",
                "positioning_territory": "premium_pantry_basics",
                "target_audience": "household bakers and pantry stockers willing to pay up for format or brand",
                "value_proposition": "Higher-priced pantry sugar framed as a dependable baking staple.",
                "tone_of_voice": "household_practical",
                "pricing_stance": "premium_pantry",
                "visual_story": "Relies on classic pantry pack shots.",
                "proof_points": [
                    "Domino Granulated Sugar, 20 oz Canister, Pack of 3",
                    "Premium granulated pure cane sugar made with non-GMO pure cane sugar."
                ],
                "primary_claims": [
                    "non gmo",
                    "canister",
                    "pack of"
                ],
                "evidence_refs": [
                    "product_intelligence:B09RPPBG15",
                    "brand_positioning:B09RPPBG15"
                ],
                "warnings": [
                    "currency missing from upstream product intelligence"
                ]
            }
        ]

    def _brand_profile_report(self) -> dict[str, object]:
        return {
            "run_id": "collection-1",
            "status": "partial_success",
            "category_context": None,
            "total_profiles": 3,
            "territory_counts": {
                "value_pantry_basics": 1,
                "convenience_beverage_station": 1,
                "premium_pantry_basics": 1
            },
            "pricing_counts": {
                "budget_anchor": 1,
                "mid_market": 1,
                "premium_pantry": 1
            },
            "crowded_territories": [],
            "underrepresented_spaces": [
                "No clear health-forward sugar alternative appears in this selected set."
            ],
            "profiles": self._brand_profile_records(),
            "caveats": [
                "This market map is directional only and should not be treated as validated whitespace without demand grounding."
            ]
        }

    def _demand_signal_report(self) -> dict[str, object]:
        return {
            "run_id": "collection-1",
            "status": "partial_success",
            "query": "sugar",
            "category_context": None,
            "source": "discovery_breadth",
            "valid_discovery_count": 3,
            "total_signals": 1,
            "signals": [
                {
                    "target_territory": "health_forward_alternative",
                    "demand_score": 0.7,
                }
            ],
            "caveats": ["Demand signals are discovery-breadth proxies."],
        }

    def _candy_product_intelligence_records(self) -> list[dict[str, object]]:
        return [
            {
                "product_id": "amazon:B08MHYBV46",
                "asin": "B08MHYBV46",
                "title": "JOLLY RANCHER Zero Sugar Assorted Fruit Flavored Hard Candy Bag, 6.1 oz",
                "brand": "Jolly Rancher",
                "product_url": "https://www.amazon.com/dp/B08MHYBV46",
                "price": 6.34,
                "currency": None,
                "rating": 4.7,
                "review_count": 9400,
                "availability": "In Stock"
            },
            {
                "product_id": "amazon:B0G4NTX1XL",
                "asin": "B0G4NTX1XL",
                "title": "Shameless Snacks Tropical Paradise Gummy Candy - 6 Pack Variety Gummy Box",
                "brand": "Shameless",
                "product_url": "https://www.amazon.com/dp/B0G4NTX1XL",
                "price": 23.99,
                "currency": None,
                "rating": 4.7,
                "review_count": 136,
                "availability": "In Stock"
            },
            {
                "product_id": "amazon:B08Q4PD52C",
                "asin": "B08Q4PD52C",
                "title": "Zollipops Clean Teeth Pops Sugar Free Candy with Xylitol",
                "brand": "Zollipops",
                "product_url": "https://www.amazon.com/dp/B08Q4PD52C",
                "price": 19.99,
                "currency": None,
                "rating": 4.6,
                "review_count": 3500,
                "availability": "In Stock"
            }
        ]

    def _candy_brand_profile_records(self) -> list[dict[str, object]]:
        return [
            {
                "product_id": "amazon:B08MHYBV46",
                "asin": "B08MHYBV46",
                "brand_name": "Jolly Rancher",
                "positioning_territory": "value_multi_pack_candy",
                "target_audience": "value-seeking households and candy-bowl restockers",
                "value_proposition": "Affordable zero-sugar candy positioned around count, sharing, or bag value.",
                "tone_of_voice": "utilitarian",
                "pricing_stance": "budget_anchor",
                "visual_story": "Uses straightforward packaging and count/value cues.",
                "proof_points": [
                    "JOLLY RANCHER Zero Sugar Assorted Fruit Flavored Hard Candy Bag, 6.1 oz",
                    "Long-lasting hard candy"
                ],
                "primary_claims": [
                    "home",
                    "on the go"
                ],
                "evidence_refs": [
                    "product_intelligence:B08MHYBV46",
                    "brand_positioning:B08MHYBV46"
                ],
                "warnings": [
                    "currency missing from upstream product intelligence"
                ]
            },
            {
                "product_id": "amazon:B0G4NTX1XL",
                "asin": "B0G4NTX1XL",
                "brand_name": "Shameless",
                "positioning_territory": "sharing_variety_pack",
                "target_audience": "households, offices, and group occasions looking for shareable candy formats",
                "value_proposition": "Shareable zero-sugar candy assortment designed for variety, portioning, and repeat snacking.",
                "tone_of_voice": "service_oriented",
                "pricing_stance": "premium_pantry",
                "visual_story": "Uses packaging, promotional media, and video together to sell a candy-specific flavor and lifestyle story.",
                "proof_points": [
                    "Shameless Snacks Tropical Paradise Gummy Candy - 6 Pack Variety Gummy Box",
                    "Variety gummy box"
                ],
                "primary_claims": [
                    "variety pack",
                    "office",
                    "keto"
                ],
                "evidence_refs": [
                    "product_intelligence:B0G4NTX1XL",
                    "brand_positioning:B0G4NTX1XL"
                ],
                "warnings": [
                    "currency missing from upstream product intelligence"
                ]
            },
            {
                "product_id": "amazon:B08Q4PD52C",
                "asin": "B08Q4PD52C",
                "brand_name": "Zollipops",
                "positioning_territory": "health_forward_alternative",
                "target_audience": "health-conscious shoppers looking for lower-guilt candy alternatives",
                "value_proposition": "Cleaner-label or lower-guilt candy positioned as a healthier alternative to mainstream sweets.",
                "tone_of_voice": "wellness_reassuring",
                "pricing_stance": "premium_pantry",
                "visual_story": "Uses packaging, promotional media, and video together to sell a candy-specific flavor and lifestyle story.",
                "proof_points": [
                    "Zollipops Clean Teeth Pops Sugar Free Candy with Xylitol",
                    "Dentist-approved"
                ],
                "primary_claims": [
                    "keto",
                    "erythritol",
                    "natural"
                ],
                "evidence_refs": [
                    "product_intelligence:B08Q4PD52C",
                    "brand_positioning:B08Q4PD52C"
                ],
                "warnings": [
                    "currency missing from upstream product intelligence"
                ]
            }
        ]

    def _candy_brand_profile_report(self) -> dict[str, object]:
        return {
            "run_id": "collection-candy",
            "status": "partial_success",
            "category_context": "candy",
            "total_profiles": 3,
            "territory_counts": {
                "value_multi_pack_candy": 1,
                "sharing_variety_pack": 1,
                "health_forward_alternative": 1
            },
            "pricing_counts": {
                "budget_anchor": 1,
                "premium_pantry": 2
            },
            "crowded_territories": [],
            "underrepresented_spaces": [
                "No clear premium indulgence zero-sugar candy player appears in this selected set.",
                "No clear mainstream zero-sugar candy player appears in this selected set."
            ],
            "profiles": self._candy_brand_profile_records(),
            "caveats": [
                "This market map is directional only and should not be treated as validated whitespace without demand grounding."
            ]
        }

    def _candy_demand_signal_report(self) -> dict[str, object]:
        return {
            "run_id": "collection-candy",
            "status": "partial_success",
            "query": "zero calories candy",
            "category_context": "candy",
            "source": "discovery_breadth",
            "valid_discovery_count": 3,
            "total_signals": 1,
            "signals": [
                {
                    "target_territory": "premium_indulgence_candy",
                    "demand_score": 0.95,
                }
            ],
            "caveats": ["Demand signals are discovery-breadth proxies."],
        }

    def _candidate_by_territory(self, report: dict[str, object], territory: str) -> dict[str, object]:
        for record in report["records"]:
            if record["target_territory"] == territory:
                return record
        raise AssertionError(f"missing candidate for {territory}")

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
