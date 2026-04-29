from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.build_landscape_report import main as build_landscape_report_main
from brand_gap_inference.landscape_report import build_landscape_report, write_landscape_artifacts

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"


class LandscapeReportTests(unittest.TestCase):
    def test_build_landscape_report_summarizes_competitors_and_claims(self) -> None:
        report = build_landscape_report(run_id="collection-1", records=self._records())

        self.assertEqual("partial_success", report["status"])
        self.assertEqual(2, report["product_count"])
        self.assertEqual("B2", report["price_ladder"][0]["asin"])
        self.assertEqual("B1", report["review_ladder"][0]["asin"])
        self.assertTrue(any(pattern["claim"] == "keto" for pattern in report["claim_patterns"]))
        self.assertTrue(any("missing currency" in caveat for caveat in report["caveats"]))

    def test_landscape_cli_writes_json_and_markdown(self) -> None:
        scratch_dir = self._make_scratch_dir("landscape-report-cli")
        records_path = scratch_dir / "product_intelligence_records.json"
        records_path.write_text(json.dumps(self._records(), indent=2), encoding="utf-8")
        try:
            captured_stdout = io.StringIO()
            with redirect_stdout(captured_stdout):
                exit_code = build_landscape_report_main(
                    [
                        "--product-intelligence-records",
                        str(records_path),
                        "--output-dir",
                        str(scratch_dir / "landscape"),
                    ]
                )

            self.assertEqual(0, exit_code)
            payload = json.loads(captured_stdout.getvalue())
            self.assertEqual("success", payload["status"])
            self.assertTrue((scratch_dir / "landscape" / "landscape_report.json").exists())
            self.assertTrue((scratch_dir / "landscape" / "landscape_report.md").exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_landscape_writer_validates_artifacts(self) -> None:
        scratch_dir = self._make_scratch_dir("landscape-report-writer")
        records_path = scratch_dir / "records.json"
        records_path.write_text(json.dumps(self._records(), indent=2), encoding="utf-8")
        try:
            artifacts = write_landscape_artifacts(
                product_intelligence_records_path=records_path,
                output_dir=scratch_dir / "landscape",
                run_id="collection-1",
            )
            self.assertTrue(Path(artifacts["landscape_report"]).exists())
            self.assertTrue(Path(artifacts["landscape_report_md"]).exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _records(self) -> list[dict[str, object]]:
        return [
            {
                "product_id": "amazon:B1",
                "asin": "B1",
                "title": "Keto zero calorie monk fruit sweetener for baking",
                "brand": "Brand One",
                "product_url": "https://www.amazon.com/dp/B1",
                "price": 10.99,
                "currency": None,
                "rating": 4.6,
                "review_count": 1000,
                "availability": "In Stock",
                "discovery_rank": 1,
                "sponsored": None,
                "source_snapshots": {"collection_run_id": "collection-1"},
                "field_provenance": {},
                "warnings": ["detail: currency missing"],
                "issues": [],
            },
            {
                "product_id": "amazon:B2",
                "asin": "B2",
                "title": "Keto low carb monk fruit sugar substitute",
                "brand": None,
                "product_url": "https://www.amazon.com/dp/B2",
                "price": 8.99,
                "currency": None,
                "rating": 4.4,
                "review_count": 500,
                "availability": "In Stock",
                "discovery_rank": 2,
                "sponsored": None,
                "source_snapshots": {"collection_run_id": "collection-1"},
                "field_provenance": {},
                "warnings": ["detail: brand missing"],
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
