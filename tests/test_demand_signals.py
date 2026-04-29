from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.build_demand_signals import main as build_demand_signals_main
from brand_gap_inference.demand_signals import DemandSignalBuilder, write_demand_signal_artifacts

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"


class DemandSignalTests(unittest.TestCase):
    def test_builder_scores_matching_discovery_breadth_by_territory(self) -> None:
        result = DemandSignalBuilder().build(
            run_id="collection-candy",
            discovery_records=self._candy_discovery_records(),
            brand_profile_report=self._candy_brand_profile_report(),
        )
        report = result.to_report_dict()

        self.assertEqual("partial_success", report["status"])
        by_territory = {record["target_territory"]: record for record in report["signals"]}
        self.assertGreater(by_territory["premium_indulgence_candy"]["demand_score"], 0.0)
        self.assertEqual(1, by_territory["premium_indulgence_candy"]["top_rank"])
        self.assertEqual(0, by_territory["value_multi_pack_candy"]["match_count"])

    def test_builder_uses_protein_bar_demand_terms(self) -> None:
        result = DemandSignalBuilder().build(
            run_id="collection-protein-bar",
            discovery_records=[
                {
                    "discovery_id": "disc-1",
                    "rank": 1,
                    "status": "valid",
                    "title": "Vegan Protein Bars for Focus Energy Meal Replacement",
                    "asin": "B0FUNCTION",
                    "sponsored": True,
                    "query": "vegan protein bar",
                },
                {
                    "discovery_id": "disc-2",
                    "rank": 2,
                    "status": "valid",
                    "title": "Organic Clean Plant Based Protein Bar",
                    "asin": "B0CLEAN",
                    "sponsored": False,
                    "query": "vegan protein bar",
                },
            ],
            brand_profile_report={
                "run_id": "collection-protein-bar",
                "status": "success",
                "category_context": "protein_bar",
            },
        )
        report = result.to_report_dict()
        by_territory = {record["target_territory"]: record for record in report["signals"]}

        self.assertEqual("protein_bar", report["category_context"])
        self.assertGreater(by_territory["functional_performance_bar"]["demand_score"], 0.0)
        self.assertGreater(by_territory["clean_plant_protein_bar"]["demand_score"], 0.0)

    def test_builder_uses_adjacent_cpg_demand_terms(self) -> None:
        cases = [
            ("hydration", "Electrolyte Powder Stick Packs Zero Sugar Hydration", "performance_electrolyte"),
            ("protein_powder", "Plant Based Protein Powder Vanilla Smoothie Mix", "clean_plant_protein_powder"),
            ("energy_drink", "Zero Sugar Energy Drink Variety Pack for Focus", "zero_sugar_energy_drink"),
        ]

        for category_context, title, expected_territory in cases:
            with self.subTest(category_context=category_context):
                result = DemandSignalBuilder().build(
                    run_id=f"collection-{category_context}",
                    discovery_records=[
                        {
                            "discovery_id": "disc-1",
                            "rank": 1,
                            "status": "valid",
                            "title": title,
                            "asin": "B0ADJACENT",
                            "sponsored": True,
                            "query": category_context.replace("_", " "),
                        }
                    ],
                    brand_profile_report={
                        "run_id": f"collection-{category_context}",
                        "status": "success",
                        "category_context": category_context,
                    },
                )
                report = result.to_report_dict()
                by_territory = {record["target_territory"]: record for record in report["signals"]}

                self.assertEqual(category_context, report["category_context"])
                self.assertGreater(by_territory[expected_territory]["demand_score"], 0.0)

    def test_demand_signal_cli_writes_replayable_artifacts(self) -> None:
        scratch_dir = self._make_scratch_dir("demand-signals-cli")
        try:
            collection_dir = scratch_dir / "collection"
            self._write_collection_fixture(collection_dir)
            captured_stdout = io.StringIO()
            with redirect_stdout(captured_stdout):
                exit_code = build_demand_signals_main(
                    [
                        "--collection-dir",
                        str(collection_dir),
                        "--output-dir",
                        str(collection_dir / "demand_signals"),
                    ]
                )

            self.assertEqual(0, exit_code)
            payload = json.loads(captured_stdout.getvalue())
            self.assertEqual("success", payload["status"])
            self.assertTrue((collection_dir / "demand_signals" / "demand_signal_records.json").exists())
            self.assertTrue((collection_dir / "demand_signals" / "demand_signal_report.json").exists())
            self.assertTrue((collection_dir / "demand_signals" / "demand_signal_report.md").exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_writer_validates_artifact_payloads(self) -> None:
        scratch_dir = self._make_scratch_dir("demand-signals-writer")
        try:
            collection_dir = scratch_dir / "collection"
            self._write_collection_fixture(collection_dir)
            artifacts = write_demand_signal_artifacts(
                collection_dir=collection_dir,
                output_dir=collection_dir / "demand_signals",
            )

            report = json.loads(Path(artifacts["demand_signal_report"]).read_text(encoding="utf-8"))
            self.assertEqual("collection-candy", report["run_id"])
            self.assertEqual("discovery_breadth", report["source"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _write_collection_fixture(self, collection_dir: Path) -> None:
        (collection_dir / "discovery").mkdir(parents=True, exist_ok=True)
        (collection_dir / "brand_profiles").mkdir(parents=True, exist_ok=True)
        (collection_dir / "discovery" / "discovery_records.json").write_text(
            json.dumps(self._candy_discovery_records(), indent=2),
            encoding="utf-8",
        )
        (collection_dir / "brand_profiles" / "brand_profile_report.json").write_text(
            json.dumps(self._candy_brand_profile_report(), indent=2),
            encoding="utf-8",
        )

    def _candy_discovery_records(self) -> list[dict[str, object]]:
        return [
            {
                "discovery_id": "disc-1",
                "snapshot_id": "snapshot-1",
                "source": "amazon_api_discovery",
                "provider": "serpapi",
                "query": "zero calories candy",
                "rank": 1,
                "status": "valid",
                "title": "Premium Sugar Free Chocolate Caramel Mint Candy",
                "product_url": "https://www.amazon.com/dp/B0PREMIUM1",
                "asin": "B0PREMIUM1",
                "price": 18.99,
                "currency": "USD",
                "rating": 4.6,
                "review_count": 120,
                "sponsored": True,
                "provider_metadata": {},
                "raw_payload_uri": "fixtures://discovery/B0PREMIUM1",
                "warnings": [],
                "issues": [],
            },
            {
                "discovery_id": "disc-2",
                "snapshot_id": "snapshot-1",
                "source": "amazon_api_discovery",
                "provider": "serpapi",
                "query": "zero calories candy",
                "rank": 2,
                "status": "valid",
                "title": "Keto Xylitol Lollipops Sugar Free Candy",
                "product_url": "https://www.amazon.com/dp/B0HEALTHY1",
                "asin": "B0HEALTHY1",
                "price": 15.99,
                "currency": "USD",
                "rating": 4.5,
                "review_count": 400,
                "sponsored": False,
                "provider_metadata": {},
                "raw_payload_uri": "fixtures://discovery/B0HEALTHY1",
                "warnings": [],
                "issues": [],
            },
        ]

    def _candy_brand_profile_report(self) -> dict[str, object]:
        return {
            "run_id": "collection-candy",
            "status": "partial_success",
            "category_context": "candy",
            "total_profiles": 2,
            "territory_counts": {},
            "pricing_counts": {},
            "crowded_territories": [],
            "underrepresented_spaces": [],
            "profiles": [],
            "caveats": [],
        }

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
