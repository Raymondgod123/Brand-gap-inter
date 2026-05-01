from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.connectors import FixtureConnector, RawSourceRecord
from brand_gap_inference.ingestion import IngestionService
from brand_gap_inference.normalization import BatchNormalizer, write_normalization_artifacts
from brand_gap_inference.raw_store import FilesystemRawStore, SourceSnapshotManifest
from brand_gap_inference.taxonomy import TaxonomyAssigner

ROOT = Path(__file__).resolve().parents[1]
LIVE_SNAPSHOT_PATH = ROOT / "data" / "raw" / "amazon" / "amazon-B098H7XWQ6-2026-04-22T01-54-11Z" / "B098H7XWQ6.json"
DIRTY_FIXTURE_PATH = ROOT / "fixtures" / "normalization" / "amazon_dirty_cases.json"
SCRATCH_ROOT = ROOT / ".tmp-tests"


class NormalizationTests(unittest.TestCase):
    def test_live_amazon_snapshot_fails_clearly_when_price_is_missing(self) -> None:
        record = self._load_live_record()
        manifest = SourceSnapshotManifest(
            snapshot_id=record.snapshot_id,
            source=record.source,
            captured_at=record.captured_at,
            record_count=1,
            record_ids=[record.record_id],
            storage_uri="data/raw/amazon/amazon-B098H7XWQ6-2026-04-22T01-54-11Z",
        )

        result = BatchNormalizer().normalize_snapshot(manifest, [record])

        self.assertEqual("failed", result.summary.run_status)
        self.assertEqual(0, result.summary.normalized_records)
        self.assertEqual(1, result.summary.invalid_records)
        self.assertEqual([], result.normalized_listings)
        record_result = result.records[0]
        self.assertEqual("invalid", record_result.status)
        self.assertTrue(any("missing product price" in issue.message for issue in record_result.issues))

    def test_normalization_emits_provenance_for_key_fields(self) -> None:
        # Use a stable fixture record (not the live snapshot), so this test is deterministic.
        fixture = json.loads(DIRTY_FIXTURE_PATH.read_text(encoding="utf-8"))
        record = RawSourceRecord.from_dict(next(item for item in fixture if item["record_id"] == "clean-1"))
        manifest = self._make_manifest([record], record.snapshot_id)

        result = BatchNormalizer().normalize_snapshot(manifest, [record])
        self.assertEqual("success", result.summary.run_status)
        record_result = result.records[0]

        for field_name in ["brand_name", "price", "pack_count", "unit_measure", "category_path", "availability"]:
            self.assertIn(field_name, record_result.field_provenance)
            provenance = record_result.field_provenance[field_name]
            self.assertIn("source_type", provenance)
            self.assertIn("rule", provenance)

    def test_low_confidence_reasons_capture_fallback_and_uncertainty(self) -> None:
        record = self._make_record(
            record_id="lowconf-1",
            asin="LOWCONF001",
            html_override="""
            <html>
              <body>
                <span id="productTitle">MysteryBrand Hydration Mix</span>
                <div id="corePriceDisplay_desktop_feature_div">
                  <span class="a-offscreen">$12.00</span>
                </div>
              </body>
            </html>
            """,
        )
        # Ensure the URL has no brand-bearing slug so we hit the title fallback.
        record.payload["original_url"] = "https://www.amazon.com/dp/LOWCONF001"
        record.payload["canonical_url"] = "https://www.amazon.com/dp/LOWCONF001"
        record.payload["final_url"] = "https://www.amazon.com/dp/LOWCONF001"
        manifest = self._make_manifest([record], "snapshot-lowconf")

        result = BatchNormalizer().normalize_snapshot(manifest, [record])

        self.assertEqual("success", result.summary.run_status)
        record_result = result.records[0]
        codes = {reason["code"] for reason in record_result.low_confidence_reasons}

        self.assertIn("brand_inferred_from_title", codes)
        self.assertIn("missing_breadcrumb_categories", codes)
        self.assertIn("size_signal_missing", codes)
        self.assertIn("availability_unclear", codes)

    def test_duplicate_records_do_not_inflate_normalized_count(self) -> None:
        record = self._make_record(record_id="duplicate-1", asin="DUPLICATE01")
        duplicate = self._make_record(record_id="duplicate-2", asin="DUPLICATE01")
        manifest = self._make_manifest([record, duplicate], "snapshot-duplicates")

        result = BatchNormalizer().normalize_snapshot(manifest, [record, duplicate])

        self.assertEqual("success", result.summary.run_status)
        self.assertEqual(1, result.summary.normalized_records)
        self.assertEqual(1, result.summary.duplicate_records)
        duplicate_result = next(item for item in result.records if item.source_record_id == "duplicate-2")
        self.assertEqual("duplicate", duplicate_result.status)
        self.assertEqual("duplicate-1", duplicate_result.duplicate_of)

    def test_partial_failure_is_reported_clearly(self) -> None:
        valid = self._make_record(record_id="valid-1", asin="VALID00001")
        blocked = self._make_record(record_id="blocked-1", asin="BLOCK00001", robot_check=True)
        manifest = self._make_manifest([valid, blocked], "snapshot-partial")

        result = BatchNormalizer().normalize_snapshot(manifest, [valid, blocked])

        self.assertEqual("partial_success", result.summary.run_status)
        self.assertEqual(1, result.summary.normalized_records)
        self.assertEqual(1, result.summary.invalid_records)
        blocked_result = next(item for item in result.records if item.source_record_id == "blocked-1")
        self.assertEqual("invalid", blocked_result.status)
        self.assertTrue(any(issue.message == "amazon returned a robot-check page" for issue in blocked_result.issues))

    def test_large_noisy_batch_stays_consistent(self) -> None:
        records: list[RawSourceRecord] = []
        for index in range(120):
            records.append(self._make_record(record_id=f"unique-{index}", asin=f"U{index:09d}"))
        for index in range(40):
            records.append(self._make_record(record_id=f"dup-{index}", asin="DUPL000001"))
        for index in range(25):
            records.append(self._make_record(record_id=f"robot-{index}", asin=f"R{index:09d}", robot_check=True))
        for index in range(15):
            records.append(self._make_record(record_id=f"missing-{index}", asin=f"M{index:09d}", include_html=False))

        manifest = self._make_manifest(records, "snapshot-stress")
        result = BatchNormalizer().normalize_snapshot(manifest, records)

        self.assertEqual("partial_success", result.summary.run_status)
        self.assertEqual(121, result.summary.normalized_records)
        self.assertEqual(39, result.summary.duplicate_records)
        self.assertEqual(40, result.summary.invalid_records)
        self.assertEqual(200, result.summary.total_records)

    def test_price_falls_back_to_core_price_display_block(self) -> None:
        record = self._make_record(
            record_id="variant-price",
            asin="PRICE00001",
            html_override="""
            <html>
              <body>
                <span id="productTitle">Example Brand Sparkling Water 12 Fl Oz (Pack of 3)</span>
                <a id="bylineInfo">Visit the Example Brand Store</a>
                <ul class="a-unordered-list a-horizontal a-size-small">
                  <li><span class="a-list-item"><a class="a-link-normal a-color-tertiary">Grocery &amp; Gourmet Food</a></span></li>
                </ul>
                <div id="corePriceDisplay_desktop_feature_div">
                  <span class="a-price aok-align-center">
                    <span class="a-offscreen">$18.49</span>
                  </span>
                </div>
                <div id="availability">In Stock.</div>
              </body>
            </html>
            """,
        )
        manifest = self._make_manifest([record], "snapshot-price-variant")

        result = BatchNormalizer().normalize_snapshot(manifest, [record])

        self.assertEqual("success", result.summary.run_status)
        listing = result.normalized_listings[0]
        self.assertEqual("Example Brand", listing["brand_name"])
        self.assertEqual("oz", listing["unit_measure"])
        self.assertEqual(3, listing["pack_count"])
        self.assertAlmostEqual(18.49, listing["price"], places=2)
        self.assertAlmostEqual(0.5136, listing["unit_price"], places=4)

    def test_price_parses_from_primary_container_whole_and_fraction(self) -> None:
        record = self._make_record(
            record_id="variant-price-whole-fraction",
            asin="WHOLEFRA01",
            html_override="""
            <html>
              <body>
                <span id="productTitle">Example Brand Sparkling Water 12 Fl Oz (Pack of 3)</span>
                <a id="bylineInfo">Visit the Example Brand Store</a>
                <ul class="a-unordered-list a-horizontal a-size-small">
                  <li><span class="a-list-item"><a class="a-link-normal a-color-tertiary">Grocery &amp; Gourmet Food</a></span></li>
                </ul>
                <div id="corePriceDisplay_desktop_feature_div">
                  <span class="a-price">
                    <span aria-hidden="true">
                      <span class="a-price-symbol">$</span>
                      <span class="a-price-whole">24</span>
                      <span class="a-price-decimal">.</span>
                      <span class="a-price-fraction">99</span>
                    </span>
                  </span>
                </div>
                <div id="availability">In Stock.</div>
              </body>
            </html>
            """,
        )
        manifest = self._make_manifest([record], "snapshot-price-whole-fraction")

        result = BatchNormalizer().normalize_snapshot(manifest, [record])

        self.assertEqual("success", result.summary.run_status)
        listing = result.normalized_listings[0]
        self.assertAlmostEqual(24.99, listing["price"], places=2)
        self.assertAlmostEqual(0.6942, listing["unit_price"], places=4)
        price_provenance = result.records[0].field_provenance["price"]
        self.assertEqual("price_primary_container", price_provenance["rule"])

    def test_limited_availability_from_other_sellers_is_preserved(self) -> None:
        record = self._make_record(
            record_id="variant-availability",
            asin="LIMIT00001",
            html_override="""
            <html>
              <body>
                <span id="productTitle">Example Brand Protein Bar 6 Count</span>
                <a id="bylineInfo">Example Brand</a>
                <ul class="a-unordered-list a-horizontal a-size-small">
                  <li><span class="a-list-item"><a class="a-link-normal a-color-tertiary">Grocery &amp; Gourmet Food</a></span></li>
                </ul>
                <script>var data = {&quot;priceAmount&quot;: 12.50, &quot;asin&quot;: &quot;LIMIT00001&quot;};</script>
                <div id="availability">Available from these sellers.</div>
              </body>
            </html>
            """,
        )
        manifest = self._make_manifest([record], "snapshot-limited-variant")

        result = BatchNormalizer().normalize_snapshot(manifest, [record])

        self.assertEqual("success", result.summary.run_status)
        self.assertEqual("limited", result.normalized_listings[0]["availability"])

    def test_missing_price_reports_no_featured_offers_context(self) -> None:
        record = self._make_record(
            record_id="no-featured-offer-1",
            asin="NOFEATURE01",
            html_override="""
            <html>
              <body>
                <span id="productTitle">Lakanto Monk Fruit Sweetener Keto 5 LB Bag</span>
                <a id="bylineInfo">Lakanto</a>
                <div>No featured offers available</div>
                <div>See All Buying Options</div>
                <script>
                  var data = {
                    &quot;priceAmount&quot;: 15.19,
                    &quot;productTitle&quot;: &quot;Stevia Select Organic Stevia Powder 1.25oz&quot;
                  };
                  var data2 = {
                    &quot;priceAmount&quot;: 12.82,
                    &quot;productTitle&quot;: &quot;Stevia Select Plain Stevia Liquid Drops&quot;
                  };
                </script>
              </body>
            </html>
            """,
        )
        manifest = self._make_manifest([record], "snapshot-no-featured-offers")

        result = BatchNormalizer().normalize_snapshot(manifest, [record])

        self.assertEqual("failed", result.summary.run_status)
        record_result = result.records[0]
        self.assertEqual("invalid", record_result.status)
        self.assertTrue(any("no featured offers available" in issue.message for issue in record_result.issues))
        self.assertIn("Stevia Select Organic Stevia Powder", record_result.field_provenance["price"]["source_detail"])

    def test_missing_price_reports_buying_options_only_context(self) -> None:
        record = self._make_record(
            record_id="buying-options-only-1",
            asin="BUYOPT001",
            html_override="""
            <html>
              <body>
                <span id="productTitle">Lakanto Monk Fruit Sweetener Keto 5 LB Bag</span>
                <a id="bylineInfo">Lakanto</a>
                <div>See All Buying Options</div>
                <script>
                  var data = {
                    &quot;priceAmount&quot;: 19.49,
                    &quot;productTitle&quot;: &quot;Other Brand Sweetener 16 oz&quot;
                  };
                </script>
              </body>
            </html>
            """,
        )
        manifest = self._make_manifest([record], "snapshot-buying-options-only")

        result = BatchNormalizer().normalize_snapshot(manifest, [record])

        self.assertEqual("failed", result.summary.run_status)
        record_result = result.records[0]
        self.assertEqual("invalid", record_result.status)
        self.assertTrue(any("buying-options only" in issue.message for issue in record_result.issues))

    def test_price_parser_ignores_script_widget_container_strings(self) -> None:
        record = self._make_record(
            record_id="script-widget-price-trap-1",
            asin="TRAPPRICE01",
            html_override="""
            <html>
              <body>
                <span id="productTitle">Lakanto Monk Fruit Sweetener Keto 5 LB Bag</span>
                <a id="bylineInfo">Lakanto</a>
                <div id="desktop_buybox">
                  <div id="outOfStockBuyBox_feature_div">No featured offers available</div>
                  <div>See All Buying Options</div>
                </div>
                <script>
                  var updates = [{"divToUpdate":"corePriceDisplay_desktop_feature_div"}];
                  var adHtml = '<div id="corePriceDisplay_desktop_feature_div"><span class="a-offscreen">$675.89</span><span class="a-price-whole">675</span><span class="a-price-fraction">89</span></div>';
                </script>
              </body>
            </html>
            """,
        )
        manifest = self._make_manifest([record], "snapshot-script-widget-price-trap")

        result = BatchNormalizer().normalize_snapshot(manifest, [record])

        self.assertEqual("failed", result.summary.run_status)
        self.assertEqual([], result.normalized_listings)
        record_result = result.records[0]
        self.assertEqual("invalid", record_result.status)
        self.assertTrue(any("no featured offers available" in issue.message for issue in record_result.issues))

    def test_browser_captured_record_normalizes_without_special_case_logic(self) -> None:
        record = self._make_record(
            record_id="browser-capture-1",
            asin="BROWSERN1",
            html_override="""
            <html>
              <body>
                <span id="productTitle">Browser Brand Hydration Drink 16 Fl Oz (Pack of 2)</span>
                <a id="bylineInfo">Visit the Browser Brand Store</a>
                <ul class="a-unordered-list a-horizontal a-size-small">
                  <li><span class="a-list-item"><a class="a-link-normal a-color-tertiary">Grocery &amp; Gourmet Food</a></span></li>
                </ul>
                <div id="priceToPay">
                  <span class="a-offscreen">$21.50</span>
                </div>
                <div id="availability">In Stock.</div>
              </body>
            </html>
            """,
        )
        record.payload["acquisition_method"] = "browser_playwright"
        record.payload["browser_engine"] = "chromium"
        record.payload["headers"] = {}
        record.payload["capture_diagnostics"] = {
            "navigation_ok": True,
            "ready_state": "complete",
            "wait_strategy": "domcontentloaded+body+bounded_settle",
            "timing_ms": {"goto": 120, "settle": 250, "total": 370},
            "visible_offer_signals": {
                "has_no_featured_offers": False,
                "has_buying_options": False,
                "has_currently_unavailable": False,
                "has_price_to_pay_block": True,
                "has_core_price_block": False,
            },
        }
        manifest = self._make_manifest([record], "snapshot-browser-capture")

        result = BatchNormalizer().normalize_snapshot(manifest, [record])

        self.assertEqual("success", result.summary.run_status)
        listing = result.normalized_listings[0]
        self.assertEqual("Browser Brand", listing["brand_name"])
        self.assertAlmostEqual(21.50, listing["price"], places=2)
        self.assertEqual("oz", listing["unit_measure"])
        self.assertEqual(2, listing["pack_count"])
        self.assertEqual("price_primary_container", result.records[0].field_provenance["price"]["rule"])

    def test_dirty_amazon_fixture_batch_is_partial_success_and_traceable(self) -> None:
        connector = FixtureConnector(source_name="amazon", fixture_path=DIRTY_FIXTURE_PATH)
        scratch_dir = self._make_scratch_dir("normalization-dirty-fixture")
        try:
            service = IngestionService(FilesystemRawStore(scratch_dir))
            ingest_result = service.ingest(connector)

            result = BatchNormalizer().normalize_snapshot(ingest_result.manifest, ingest_result.records)

            self.assertEqual("partial_success", result.summary.run_status)
            self.assertEqual(7, result.summary.total_records)
            self.assertEqual(4, result.summary.normalized_records)
            self.assertEqual(1, result.summary.duplicate_records)
            self.assertEqual(2, result.summary.invalid_records)
            self.assertEqual(2, result.summary.low_confidence_records)

            fallback_record = next(item for item in result.records if item.source_record_id == "fallback-1")
            fallback_codes = {reason["code"] for reason in fallback_record.low_confidence_reasons}
            self.assertIn("brand_inferred_from_title", fallback_codes)
            self.assertIn("missing_breadcrumb_categories", fallback_codes)
            self.assertIn("size_signal_missing", fallback_codes)
            self.assertIn("availability_unclear", fallback_codes)
            self.assertNotIn("price_secondary_pattern", fallback_codes)

            multi_size_record = next(item for item in result.records if item.source_record_id == "multi-size-1")
            multi_size_codes = {reason["code"] for reason in multi_size_record.low_confidence_reasons}
            self.assertIn("multiple_size_signals_detected", multi_size_codes)

            dup_record = next(item for item in result.records if item.source_record_id == "dup-2")
            self.assertEqual("duplicate", dup_record.status)
            self.assertEqual("dup-1", dup_record.duplicate_of)

            taxonomy_result = TaxonomyAssigner().assign_batch(result.normalized_listings, snapshot_id="fixture-dirty")
            self.assertEqual("success", taxonomy_result.summary.run_status)
            self.assertEqual(result.summary.normalized_records, taxonomy_result.summary.assigned_count)
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_normalization_artifact_writer_validates_record_contracts(self) -> None:
        record = self._make_record(
            record_id="artifact-1",
            asin="ARTIFACT01",
            html_override="""
            <html>
              <body>
                <span id="productTitle">MysteryBrand Hydration Mix</span>
                <div id="corePriceDisplay_desktop_feature_div">
                  <span class="a-offscreen">$12.00</span>
                </div>
              </body>
            </html>
            """,
        )
        record.payload["original_url"] = "https://www.amazon.com/dp/ARTIFACT01"
        record.payload["canonical_url"] = "https://www.amazon.com/dp/ARTIFACT01"
        record.payload["final_url"] = "https://www.amazon.com/dp/ARTIFACT01"
        manifest = self._make_manifest([record], "snapshot-artifacts")

        result = BatchNormalizer().normalize_snapshot(manifest, [record])

        scratch_dir = self._make_scratch_dir("normalization-artifact-writer")
        try:
            output_dir = scratch_dir / "artifacts"
            paths = write_normalization_artifacts(output_dir, manifest, result)
            for path in paths.values():
                self.assertTrue(Path(path).exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _load_live_record(self) -> RawSourceRecord:
        payload = json.loads(LIVE_SNAPSHOT_PATH.read_text(encoding="utf-8"))
        return RawSourceRecord.from_dict(payload)

    def _make_record(
        self,
        record_id: str,
        asin: str,
        *,
        robot_check: bool = False,
        include_html: bool = True,
        html_override: str | None = None,
    ) -> RawSourceRecord:
        payload = {
            "asin": asin,
            "original_url": f"https://www.amazon.com/Example-Brand-Product/dp/{asin}",
            "canonical_url": f"https://www.amazon.com/dp/{asin}",
            "final_url": f"https://www.amazon.com/dp/{asin}",
            "status_code": 200,
            "page_title": "Example Brand Energy Drink 12 Count : Amazon.com",
            "is_robot_check": robot_check,
            "content_sha256": "hash",
            "headers": {"Content-Type": "text/html"},
        }
        if include_html:
            payload["html"] = html_override or self._build_html(asin)
        else:
            payload["html"] = ""

        return RawSourceRecord(
            record_id=record_id,
            source="amazon",
            snapshot_id="snapshot-generated",
            captured_at="2026-04-22T14:00:00Z",
            payload=payload,
            cursor=payload["canonical_url"],
        )

    def _build_html(self, asin: str) -> str:
        return f"""
        <html>
          <body>
            <span id="productTitle">Example Brand Energy Drink 12 Count</span>
            <a id="bylineInfo">Example Brand</a>
            <span id="acrPopover" title="4.4 out of 5 stars"></span>
            <span id="acrCustomerReviewText">(1,245)</span>
            <ul class="a-unordered-list a-horizontal a-size-small">
              <li><span class="a-list-item"><a class="a-link-normal a-color-tertiary">Grocery &amp; Gourmet Food</a></span></li>
              <li><span class="a-list-item"><a class="a-link-normal a-color-tertiary">Beverages</a></span></li>
            </ul>
            <div id="availability">In Stock.</div>
            <script>var data = {{&quot;priceAmount&quot;: 18.00, &quot;asin&quot;: &quot;{asin}&quot;}};</script>
          </body>
        </html>
        """

    def _make_manifest(self, records: list[RawSourceRecord], snapshot_id: str) -> SourceSnapshotManifest:
        return SourceSnapshotManifest(
            snapshot_id=snapshot_id,
            source="amazon",
            captured_at="2026-04-22T14:00:00Z",
            record_count=len(records),
            record_ids=[record.record_id for record in records],
            storage_uri=f"data/raw/amazon/{snapshot_id}",
        )

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
