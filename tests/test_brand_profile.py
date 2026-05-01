from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.brand_profile import BrandProfileBuilder, BrandProfileContext, write_brand_profile_artifacts
from brand_gap_inference.build_brand_profiles import main as build_brand_profiles_main

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"


class BrandProfileTests(unittest.TestCase):
    def test_builder_infers_visual_signals_profiles_and_market_map(self) -> None:
        result = BrandProfileBuilder().build(
            run_id="collection-1",
            product_intelligence_records=self._product_intelligence_records(),
            brand_positioning_records=self._brand_positioning_records(),
        )
        profiles = {record.asin: record for record in result.profiles}
        signals = {record.asin: record for record in result.visual_signals}
        report = result.to_report_dict()

        self.assertEqual("partial_success", result.status)
        self.assertEqual("bag", signals["B0D1L1KSMZ"].package_format)
        self.assertEqual("packets", signals["B0FZXW7LVY"].package_format)
        self.assertEqual("canister", signals["B09RPPBG15"].package_format)
        self.assertEqual("full_story_stack", signals["B0D1L1KSMZ"].promotional_stack)
        self.assertEqual("convenience_beverage_station", profiles["B0FZXW7LVY"].positioning_territory)
        self.assertEqual("premium_pantry_basics", profiles["B09RPPBG15"].positioning_territory)
        self.assertTrue(
            any("health-forward sugar alternative" in item.lower() for item in report["underrepresented_spaces"])
        )

    def test_brand_profile_cli_writes_json_and_markdown(self) -> None:
        scratch_dir = self._make_scratch_dir("brand-profile-cli")
        try:
            self._write_collection_fixture(scratch_dir / "collection")
            captured_stdout = io.StringIO()
            with redirect_stdout(captured_stdout):
                exit_code = build_brand_profiles_main(
                    [
                        "--collection-dir",
                        str(scratch_dir / "collection"),
                        "--output-dir",
                        str(scratch_dir / "collection" / "brand_profiles"),
                    ]
                )

            self.assertEqual(0, exit_code)
            payload = json.loads(captured_stdout.getvalue())
            self.assertEqual("success", payload["status"])
            self.assertTrue((scratch_dir / "collection" / "brand_profiles" / "brand_profile_report.json").exists())
            self.assertTrue((scratch_dir / "collection" / "brand_profiles" / "brand_profile_report.md").exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_builder_uses_candy_context_for_candy_market_map(self) -> None:
        result = BrandProfileBuilder().build(
            run_id="collection-candy",
            product_intelligence_records=self._candy_product_intelligence_records(),
            brand_positioning_records=self._candy_brand_positioning_records(),
            category_context=BrandProfileContext(query_family="candy"),
        )
        profiles = {record.asin: record for record in result.profiles}
        report = result.to_report_dict()

        self.assertEqual("candy", report["category_context"])
        self.assertEqual("mainstream_zero_sugar_candy", profiles["B08MHYBV46"].positioning_territory)
        self.assertEqual("sharing_variety_pack", profiles["B0G4NTX1XL"].positioning_territory)
        self.assertEqual("premium_indulgence_candy", profiles["B0DT4YZZ6V"].positioning_territory)
        self.assertTrue(any("zero-sugar candy" in item.lower() for item in report["underrepresented_spaces"]))

    def test_builder_uses_protein_bar_context_for_market_map(self) -> None:
        result = BrandProfileBuilder().build(
            run_id="collection-protein-bar",
            product_intelligence_records=self._protein_bar_product_intelligence_records(),
            brand_positioning_records=self._protein_bar_brand_positioning_records(),
            category_context=BrandProfileContext(query_family="protein_bar"),
        )
        profiles = {record.asin: record for record in result.profiles}
        report = result.to_report_dict()

        self.assertEqual("protein_bar", report["category_context"])
        self.assertEqual("functional_performance_bar", profiles["B0FUNCTION"].positioning_territory)
        self.assertEqual("indulgent_snack_bar", profiles["B0INDULGE"].positioning_territory)
        self.assertEqual("clean_plant_protein_bar", profiles["B0CLEAN"].positioning_territory)
        self.assertEqual(["family_lifestyle_snack_bar"], profiles["B0FUNCTION"].secondary_territories)
        self.assertEqual(1, report["territory_coverage_counts"]["clean_plant_protein_bar"])
        self.assertEqual(3, report["territory_coverage_counts"]["family_lifestyle_snack_bar"])
        self.assertNotIn("value_variety_protein_bar", report["territory_coverage_counts"])
        self.assertTrue(any("protein bar" in item.lower() for item in report["underrepresented_spaces"]))
        self.assertTrue(
            any("variety-pack" in item.lower() for item in report["underrepresented_spaces"])
        )

    def test_builder_uses_adjacent_cpg_contexts_for_market_maps(self) -> None:
        cases = [
            (
                "hydration",
                "B0HYDRATE",
                "LMNT Electrolyte Powder Stick Packs Zero Sugar Hydration",
                ["electrolytes", "sodium", "zero sugar", "stick packs"],
                ["workout", "on the go"],
                "performance_electrolyte",
                {"zero_sugar_hydration", "travel_stick_pack"},
            ),
            (
                "protein_powder",
                "B0POWDER",
                "Organic Plant Based Protein Powder Vanilla Smoothie Mix",
                ["organic", "plant based", "vanilla"],
                ["smoothie"],
                "clean_plant_protein_powder",
                {"flavor_lifestyle_protein"},
            ),
            (
                "energy_drink",
                "B0ENERGY",
                "Zero Sugar Energy Drink for Focus and Mental Energy",
                ["zero sugar", "focus", "mental energy"],
                ["work"],
                "zero_sugar_energy_drink",
                {"focus_functional_energy"},
            ),
        ]

        for query_family, asin, title, claims, usage_contexts, expected_primary, expected_secondary in cases:
            with self.subTest(query_family=query_family):
                result = BrandProfileBuilder().build(
                    run_id=f"collection-{query_family}",
                    product_intelligence_records=[
                        {
                            "product_id": f"amazon:{asin}",
                            "asin": asin,
                            "title": title,
                            "brand": asin,
                            "product_url": f"https://www.amazon.com/dp/{asin}",
                            "price": 24.99,
                            "currency": None,
                            "rating": 4.5,
                            "review_count": 1200,
                            "availability": "In Stock",
                            "media_assets": {},
                            "promotional_content": [
                                {"position": 1, "title": title, "image": "https://example.com/promo.jpg"}
                            ],
                            "description_bullets": [title],
                            "discovery_rank": 1,
                            "sponsored": False,
                            "source_snapshots": {"collection_run_id": f"collection-{query_family}"},
                            "field_provenance": {},
                            "warnings": ["detail: currency missing"],
                            "issues": [],
                        }
                    ],
                    brand_positioning_records=[
                        {
                            "product_id": f"amazon:{asin}",
                            "asin": asin,
                            "brand_name": asin,
                            "title": title,
                            "positioning_archetype": "health_positioned",
                            "price_tier": "premium",
                            "value_signal": "light",
                            "health_signal": "explicit",
                            "convenience_signal": "moderate",
                            "visual_strategy": "packaging_promo_video",
                            "claim_signals": claims,
                            "usage_contexts": usage_contexts,
                            "packaging_signal_summary": {
                                "primary_image_present": True,
                                "gallery_image_count": 8,
                                "promotional_image_count": 3,
                                "video_count": 1,
                                "description_bullet_count": 1,
                                "promotional_block_count": 1,
                            },
                            "evidence": [title],
                            "warnings": ["currency missing from product intelligence record"],
                        }
                    ],
                    category_context=BrandProfileContext(query_family=query_family),
                )
                profile = result.profiles[0]
                report = result.to_report_dict()

                self.assertEqual(query_family, report["category_context"])
                self.assertEqual(expected_primary, profile.positioning_territory)
                self.assertTrue(expected_secondary.issubset(set(profile.secondary_territories)))
                self.assertTrue(report["underrepresented_spaces"])

    def test_brand_profile_writer_validates_artifacts(self) -> None:
        scratch_dir = self._make_scratch_dir("brand-profile-writer")
        try:
            collection_dir = scratch_dir / "collection"
            self._write_collection_fixture(collection_dir)
            artifacts = write_brand_profile_artifacts(
                collection_dir=collection_dir,
                output_dir=collection_dir / "brand_profiles",
            )
            self.assertTrue(Path(artifacts["visual_brand_signals_records"]).exists())
            self.assertTrue(Path(artifacts["brand_profile_records"]).exists())
            self.assertTrue(Path(artifacts["brand_profile_report"]).exists())
            self.assertTrue(Path(artifacts["brand_profile_report_md"]).exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _write_collection_fixture(self, collection_dir: Path) -> None:
        (collection_dir / "product_intelligence").mkdir(parents=True, exist_ok=True)
        (collection_dir / "brand_positioning").mkdir(parents=True, exist_ok=True)
        (collection_dir / "product_intelligence" / "product_intelligence_records.json").write_text(
            json.dumps(self._product_intelligence_records(), indent=2),
            encoding="utf-8",
        )
        (collection_dir / "brand_positioning" / "brand_positioning_records.json").write_text(
            json.dumps(self._brand_positioning_records(), indent=2),
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
                "availability": "In Stock",
                "media_assets": {},
                "promotional_content": [
                    {
                        "position": 1,
                        "title": "Amazon Saver Affordable staples without the frills",
                        "image": "https://example.com/amazon-saver-promo-1.jpg",
                    }
                ],
                "description_bullets": [
                    "One 4 pound bag of white granulated sugar",
                    "Gluten free",
                    "Shop smarter with Amazon Saver",
                ],
                "discovery_rank": 1,
                "sponsored": True,
                "source_snapshots": {"collection_run_id": "collection-1"},
                "field_provenance": {},
                "warnings": ["detail: currency missing"],
                "issues": [],
            },
            {
                "product_id": "amazon:B0FZXW7LVY",
                "asin": "B0FZXW7LVY",
                "title": "Sweetmo Sugar Packets Variety Pack for Coffee, Tea, Office Breakrooms, Home & Airbnb",
                "brand": "Brand: Sweetmo",
                "product_url": "https://www.amazon.com/dp/B0FZXW7LVY",
                "price": 22.49,
                "currency": None,
                "rating": 4.8,
                "review_count": 60,
                "availability": "In Stock",
                "media_assets": {},
                "promotional_content": [],
                "description_bullets": [
                    "Convenient 300-count value pack for home, office, coffee bar, or breakroom.",
                    "Wooden stirrers included.",
                    "Perfect for coffee and tea stations.",
                ],
                "discovery_rank": 2,
                "sponsored": True,
                "source_snapshots": {"collection_run_id": "collection-1"},
                "field_provenance": {},
                "warnings": ["detail: currency missing"],
                "issues": [],
            },
            {
                "product_id": "amazon:B09RPPBG15",
                "asin": "B09RPPBG15",
                "title": "Domino Granulated Sugar, 20 oz Canister, Pack of 3",
                "brand": "Visit the Domino Store",
                "product_url": "https://www.amazon.com/dp/B09RPPBG15",
                "price": 22.99,
                "currency": None,
                "rating": 4.5,
                "review_count": 917,
                "availability": "In Stock",
                "media_assets": {},
                "promotional_content": [],
                "description_bullets": [
                    "Premium granulated pure cane sugar made with non-GMO pure cane sugar.",
                    "Convenient and easy-to-use canister keeps sugar fresh.",
                    "Pantry baking essential for homemade recipes.",
                ],
                "discovery_rank": 3,
                "sponsored": True,
                "source_snapshots": {"collection_run_id": "collection-1"},
                "field_provenance": {},
                "warnings": ["detail: currency missing"],
                "issues": [],
            },
        ]

    def _brand_positioning_records(self) -> list[dict[str, object]]:
        return [
            {
                "product_id": "amazon:B0D1L1KSMZ",
                "asin": "B0D1L1KSMZ",
                "brand_name": "Amazon Saver",
                "title": "Amazon Saver, White Sugar, 4 Lb",
                "positioning_archetype": "value_staple",
                "price_tier": "budget",
                "value_signal": "explicit",
                "health_signal": "moderate",
                "convenience_signal": "light",
                "visual_strategy": "packaging_promo_video",
                "claim_signals": ["saver", "affordable", "gluten free"],
                "usage_contexts": ["pantry"],
                "packaging_signal_summary": {
                    "primary_image_present": True,
                    "gallery_image_count": 6,
                    "promotional_image_count": 2,
                    "video_count": 4,
                    "description_bullet_count": 3,
                    "promotional_block_count": 1,
                },
                "evidence": ["Amazon Saver, White Sugar, 4 Lb"],
                "warnings": ["currency missing from product intelligence record"],
            },
            {
                "product_id": "amazon:B0FZXW7LVY",
                "asin": "B0FZXW7LVY",
                "brand_name": "Sweetmo",
                "title": "Sweetmo Sugar Packets Variety Pack for Coffee, Tea, Office Breakrooms, Home & Airbnb",
                "positioning_archetype": "convenience_bundle",
                "price_tier": "mid",
                "value_signal": "moderate",
                "health_signal": "light",
                "convenience_signal": "explicit",
                "visual_strategy": "packaging_plus_video",
                "claim_signals": ["value", "packets", "variety pack", "office", "home"],
                "usage_contexts": ["coffee", "tea", "office", "home"],
                "packaging_signal_summary": {
                    "primary_image_present": True,
                    "gallery_image_count": 8,
                    "promotional_image_count": 0,
                    "video_count": 2,
                    "description_bullet_count": 3,
                    "promotional_block_count": 0,
                },
                "evidence": ["Sweetmo Sugar Packets Variety Pack"],
                "warnings": [
                    "currency missing from product intelligence record",
                    "promotional content missing for positioning analysis",
                ],
            },
            {
                "product_id": "amazon:B09RPPBG15",
                "asin": "B09RPPBG15",
                "brand_name": "Domino",
                "title": "Domino Granulated Sugar, 20 oz Canister, Pack of 3",
                "positioning_archetype": "pantry_staple",
                "price_tier": "premium",
                "value_signal": "light",
                "health_signal": "moderate",
                "convenience_signal": "explicit",
                "visual_strategy": "packaging_gallery_only",
                "claim_signals": ["non gmo", "canister", "pack of"],
                "usage_contexts": ["baking", "coffee", "tea", "pantry"],
                "packaging_signal_summary": {
                    "primary_image_present": True,
                    "gallery_image_count": 6,
                    "promotional_image_count": 0,
                    "video_count": 0,
                    "description_bullet_count": 3,
                    "promotional_block_count": 0,
                },
                "evidence": ["Domino Granulated Sugar, 20 oz Canister, Pack of 3"],
                "warnings": [
                    "currency missing from product intelligence record",
                    "promotional content missing for positioning analysis",
                ],
            },
        ]

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
                "availability": "In Stock",
                "media_assets": {},
                "promotional_content": [{"position": 1, "title": "Bold fruit flavors", "image": "https://example.com/jr-promo.jpg"}],
                "description_bullets": [
                    "Long-lasting hard candy with zero sugar.",
                    "Individually wrapped for convenience.",
                ],
                "discovery_rank": 5,
                "sponsored": False,
                "source_snapshots": {"collection_run_id": "collection-candy"},
                "field_provenance": {},
                "warnings": ["detail: currency missing"],
                "issues": [],
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
                "availability": "In Stock",
                "media_assets": {},
                "promotional_content": [{"position": 1, "title": "Variety candy box", "image": "https://example.com/shameless-promo.jpg"}],
                "description_bullets": [
                    "6 pack variety gummy box.",
                    "Movie night and office snack alternative.",
                ],
                "discovery_rank": 4,
                "sponsored": True,
                "source_snapshots": {"collection_run_id": "collection-candy"},
                "field_provenance": {},
                "warnings": ["detail: currency missing"],
                "issues": [],
            },
            {
                "product_id": "amazon:B0DT4YZZ6V",
                "asin": "B0DT4YZZ6V",
                "title": "RUSSELL STOVER Sugar Free Dark Chocolate Mint Patties Candy, 4.5 oz. bag",
                "brand": "Russell Stover",
                "product_url": "https://www.amazon.com/dp/B0DT4YZZ6V",
                "price": 14.73,
                "currency": None,
                "rating": 4.7,
                "review_count": 248,
                "availability": "In Stock",
                "media_assets": {},
                "promotional_content": [{"position": 1, "title": "Rich chocolate mint", "image": "https://example.com/rs-promo.jpg"}],
                "description_bullets": [
                    "Dark chocolate mint patties with zero sugar.",
                    "Indulgent treat made with stevia extract.",
                ],
                "discovery_rank": 14,
                "sponsored": True,
                "source_snapshots": {"collection_run_id": "collection-candy"},
                "field_provenance": {},
                "warnings": ["detail: currency missing"],
                "issues": [],
            },
        ]

    def _candy_brand_positioning_records(self) -> list[dict[str, object]]:
        return [
            {
                "product_id": "amazon:B08MHYBV46",
                "asin": "B08MHYBV46",
                "brand_name": "Jolly Rancher",
                "title": "JOLLY RANCHER Zero Sugar Assorted Fruit Flavored Hard Candy Bag, 6.1 oz",
                "positioning_archetype": "general_grocery",
                "price_tier": "mid",
                "value_signal": "light",
                "health_signal": "light",
                "convenience_signal": "explicit",
                "visual_strategy": "packaging_promo_video",
                "claim_signals": ["home", "on the go"],
                "usage_contexts": ["home"],
                "packaging_signal_summary": {
                    "primary_image_present": True,
                    "gallery_image_count": 7,
                    "promotional_image_count": 2,
                    "video_count": 6,
                    "description_bullet_count": 5,
                    "promotional_block_count": 2,
                },
                "evidence": ["Jolly Rancher hard candy"],
                "warnings": ["currency missing from product intelligence record"],
            },
            {
                "product_id": "amazon:B0G4NTX1XL",
                "asin": "B0G4NTX1XL",
                "brand_name": "Shameless",
                "title": "Shameless Snacks Tropical Paradise Gummy Candy - 6 Pack Variety Gummy Box",
                "positioning_archetype": "convenience_bundle",
                "price_tier": "premium",
                "value_signal": "light",
                "health_signal": "explicit",
                "convenience_signal": "explicit",
                "visual_strategy": "packaging_promo_video",
                "claim_signals": ["keto", "gluten free", "variety pack", "office"],
                "usage_contexts": ["office"],
                "packaging_signal_summary": {
                    "primary_image_present": True,
                    "gallery_image_count": 10,
                    "promotional_image_count": 4,
                    "video_count": 10,
                    "description_bullet_count": 5,
                    "promotional_block_count": 4,
                },
                "evidence": ["Shameless variety gummy box"],
                "warnings": ["currency missing from product intelligence record"],
            },
            {
                "product_id": "amazon:B0DT4YZZ6V",
                "asin": "B0DT4YZZ6V",
                "brand_name": "Russell Stover",
                "title": "RUSSELL STOVER Sugar Free Dark Chocolate Mint Patties Candy, 4.5 oz. bag",
                "positioning_archetype": "general_grocery",
                "price_tier": "premium",
                "value_signal": "light",
                "health_signal": "moderate",
                "convenience_signal": "moderate",
                "visual_strategy": "packaging_promo_video",
                "claim_signals": ["plant based", "on the go", "chocolate", "mint"],
                "usage_contexts": ["home"],
                "packaging_signal_summary": {
                    "primary_image_present": True,
                    "gallery_image_count": 9,
                    "promotional_image_count": 4,
                    "video_count": 1,
                    "description_bullet_count": 5,
                    "promotional_block_count": 4,
                },
                "evidence": ["Russell Stover chocolate mint patties"],
                "warnings": ["currency missing from product intelligence record"],
            },
        ]

    def _protein_bar_product_intelligence_records(self) -> list[dict[str, object]]:
        base = {
            "product_url": "https://www.amazon.com/dp/B0PROTEIN",
            "price": 22.99,
            "currency": None,
            "rating": 4.4,
            "review_count": 1200,
            "availability": "In Stock",
            "media_assets": {},
            "promotional_content": [{"position": 1, "title": "Plant based protein story", "image": "https://example.com/promo.jpg"}],
            "discovery_rank": 1,
            "sponsored": False,
            "source_snapshots": {"collection_run_id": "collection-protein-bar"},
            "field_provenance": {},
            "warnings": ["detail: currency missing"],
            "issues": [],
        }
        return [
            {
                **base,
                "product_id": "amazon:B0FUNCTION",
                "asin": "B0FUNCTION",
                "title": "IQBAR Vegan Protein Bars for Focus Energy Meal Replacement",
                "brand": "IQBAR",
                "description_bullets": ["12g plant protein with energy and focus nutrients."],
            },
            {
                **base,
                "product_id": "amazon:B0INDULGE",
                "asin": "B0INDULGE",
                "title": "TRUBAR Vegan Protein Bar Cookies and Cream Dessert Snack",
                "brand": "TRUBAR",
                "description_bullets": ["Dessert-like vegan protein bar with cookie flavor."],
            },
            {
                **base,
                "product_id": "amazon:B0CLEAN",
                "asin": "B0CLEAN",
                "title": "ALOHA Organic Plant Based Protein Bar Peanut Butter",
                "brand": "ALOHA",
                "description_bullets": ["Organic clean plant protein with non GMO ingredients."],
            },
        ]

    def _protein_bar_brand_positioning_records(self) -> list[dict[str, object]]:
        def record(asin: str, title: str, claims: list[str], evidence: list[str]) -> dict[str, object]:
            return {
                "product_id": f"amazon:{asin}",
                "asin": asin,
                "brand_name": asin,
                "title": title,
                "positioning_archetype": "health_positioned",
                "price_tier": "premium",
                "value_signal": "light",
                "health_signal": "explicit",
                "convenience_signal": "moderate",
                "visual_strategy": "packaging_promo_video",
                "claim_signals": claims,
                "usage_contexts": ["on the go"],
                "packaging_signal_summary": {
                    "primary_image_present": True,
                    "gallery_image_count": 8,
                    "promotional_image_count": 3,
                    "video_count": 1,
                    "description_bullet_count": 2,
                    "promotional_block_count": 1,
                },
                "evidence": evidence,
                "warnings": ["currency missing from product intelligence record"],
            }

        return [
            record("B0FUNCTION", "IQBAR Vegan Protein Bars", ["plant based", "energy", "focus"], ["meal replacement energy focus"]),
            record("B0INDULGE", "TRUBAR Vegan Protein Bar", ["plant based", "chocolate", "cookies"], ["cookies and cream dessert snack"]),
            record("B0CLEAN", "ALOHA Organic Plant Protein Bar", ["organic", "non gmo", "clean"], ["organic clean plant protein"]),
        ]

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
