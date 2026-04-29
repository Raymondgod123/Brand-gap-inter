from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.brand_analysis import BrandPositioningAnalyzer, write_brand_positioning_artifacts
from brand_gap_inference.build_brand_positioning import main as build_brand_positioning_main

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"


class BrandAnalysisTests(unittest.TestCase):
    def test_analyzer_infers_archetypes_and_brand_names(self) -> None:
        result = BrandPositioningAnalyzer().analyze_records(run_id="collection-1", records=self._records())
        records = {record.asin: record for record in result.records}

        self.assertEqual("partial_success", result.status)
        self.assertEqual("Amazon Saver", records["B0D1L1KSMZ"].brand_name)
        self.assertEqual("Sweetmo", records["B0FZXW7LVY"].brand_name)
        self.assertEqual("Domino", records["B09RPPBG15"].brand_name)
        self.assertEqual("value_staple", records["B0D1L1KSMZ"].positioning_archetype)
        self.assertEqual("convenience_bundle", records["B0FZXW7LVY"].positioning_archetype)
        self.assertEqual("pantry_staple", records["B09RPPBG15"].positioning_archetype)
        self.assertEqual("packaging_promo_video", records["B0D1L1KSMZ"].visual_strategy)
        self.assertEqual("packaging_plus_video", records["B0FZXW7LVY"].visual_strategy)
        self.assertEqual("packaging_gallery_only", records["B09RPPBG15"].visual_strategy)

    def test_brand_positioning_cli_writes_json_and_markdown(self) -> None:
        scratch_dir = self._make_scratch_dir("brand-positioning-cli")
        records_path = scratch_dir / "product_intelligence_records.json"
        records_path.write_text(json.dumps(self._records(), indent=2), encoding="utf-8")
        try:
            captured_stdout = io.StringIO()
            with redirect_stdout(captured_stdout):
                exit_code = build_brand_positioning_main(
                    [
                        "--product-intelligence-records",
                        str(records_path),
                        "--output-dir",
                        str(scratch_dir / "brand_positioning"),
                    ]
                )

            self.assertEqual(0, exit_code)
            payload = json.loads(captured_stdout.getvalue())
            self.assertEqual("success", payload["status"])
            self.assertTrue((scratch_dir / "brand_positioning" / "brand_positioning_report.json").exists())
            self.assertTrue((scratch_dir / "brand_positioning" / "brand_positioning_report.md").exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_brand_positioning_writer_validates_artifacts(self) -> None:
        scratch_dir = self._make_scratch_dir("brand-positioning-writer")
        records_path = scratch_dir / "records.json"
        records_path.write_text(json.dumps(self._records(), indent=2), encoding="utf-8")
        try:
            artifacts = write_brand_positioning_artifacts(
                product_intelligence_records_path=records_path,
                output_dir=scratch_dir / "brand_positioning",
                run_id="collection-1",
            )
            self.assertTrue(Path(artifacts["brand_positioning_records"]).exists())
            self.assertTrue(Path(artifacts["brand_positioning_report"]).exists())
            self.assertTrue(Path(artifacts["brand_positioning_report_md"]).exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _records(self) -> list[dict[str, object]]:
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
                "media_assets": {
                    "primary_image": "https://example.com/amazon-saver-main.jpg",
                    "gallery_images": [
                        "https://example.com/amazon-saver-gallery-1.jpg",
                        "https://example.com/amazon-saver-gallery-2.jpg",
                        "https://example.com/amazon-saver-gallery-3.jpg",
                        "https://example.com/amazon-saver-gallery-4.jpg",
                        "https://example.com/amazon-saver-gallery-5.jpg",
                        "https://example.com/amazon-saver-gallery-6.jpg",
                    ],
                    "promotional_images": [
                        "https://example.com/amazon-saver-promo-1.jpg",
                    ],
                    "videos": [
                        {
                            "title": "Great Price with Free Delivery",
                            "link": "https://example.com/amazon-saver-video.m3u8",
                            "thumbnail": "https://example.com/amazon-saver-video.jpg",
                            "duration": "0:45",
                        }
                    ],
                },
                "promotional_content": [
                    {
                        "position": 1,
                        "title": "Amazon Saver Affordable staples without the frills",
                        "image": "https://example.com/amazon-saver-promo-1.jpg",
                    }
                ],
                "description_bullets": [
                    "Gluten free",
                    "Made in USA",
                    "With Amazon Saver, you'll find affordable staples without the frills.",
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
                "media_assets": {
                    "primary_image": "https://example.com/sweetmo-main.jpg",
                    "gallery_images": [
                        "https://example.com/sweetmo-gallery-1.jpg",
                        "https://example.com/sweetmo-gallery-2.jpg",
                        "https://example.com/sweetmo-gallery-3.jpg",
                        "https://example.com/sweetmo-gallery-4.jpg",
                        "https://example.com/sweetmo-gallery-5.jpg",
                        "https://example.com/sweetmo-gallery-6.jpg",
                    ],
                    "promotional_images": [],
                    "videos": [
                        {
                            "title": "Honest Review of Sugar Packets",
                            "link": "https://example.com/sweetmo-video.m3u8",
                            "thumbnail": "https://example.com/sweetmo-video.jpg",
                            "duration": "1:45",
                        }
                    ],
                },
                "promotional_content": [],
                "description_bullets": [
                    "Convenient 300-count value pack for home, office, coffee bar, or breakroom.",
                    "Wooden stirrers included.",
                    "Perfect for coffee and tea stations.",
                    "Great for on-the-go use.",
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
                "media_assets": {
                    "primary_image": "https://example.com/domino-main.jpg",
                    "gallery_images": [
                        "https://example.com/domino-gallery-1.jpg",
                        "https://example.com/domino-gallery-2.jpg",
                        "https://example.com/domino-gallery-3.jpg",
                        "https://example.com/domino-gallery-4.jpg",
                        "https://example.com/domino-gallery-5.jpg",
                        "https://example.com/domino-gallery-6.jpg",
                    ],
                    "promotional_images": [],
                    "videos": [],
                },
                "promotional_content": [],
                "description_bullets": [
                    "Premium granulated pure cane sugar made with non-GMO pure cane sugar.",
                    "Convenient and easy-to-use canister keeps sugar fresh.",
                    "Pantry baking essential for homemade recipes.",
                    "Ideal for coffee, tea, and cocktails.",
                    "Pantry basics for everyday use.",
                ],
                "discovery_rank": 3,
                "sponsored": True,
                "source_snapshots": {"collection_run_id": "collection-1"},
                "field_provenance": {},
                "warnings": ["detail: currency missing"],
                "issues": [],
            },
        ]

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
