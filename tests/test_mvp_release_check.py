from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.connectors import RawSourceRecord
from brand_gap_inference.mvp_release_check import run_release_check
from brand_gap_inference.raw_store import FilesystemRawStore

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"


class MvpReleaseCheckTests(unittest.TestCase):
    def test_release_check_passes_with_replay_success_and_safe_stop(self) -> None:
        scratch_dir = self._make_scratch_dir("mvp-release-check")
        try:
            store = FilesystemRawStore(scratch_dir / "raw")
            success_snapshot_id = "amazon-release-success-001"
            failure_snapshot_id = "amazon-release-failure-001"

            success_record = self._make_success_record(snapshot_id=success_snapshot_id, asin="RELSUCC001")
            failure_record = self._make_failure_record(snapshot_id=failure_snapshot_id, asin="RELFAIL001")
            store.persist_snapshot([success_record])
            store.persist_snapshot([failure_record])

            report = run_release_check(
                source="amazon",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                success_snapshot_id=success_snapshot_id,
                failure_snapshot_id=failure_snapshot_id,
                include_live_attempt=False,
                generated_at="2026-04-24T12:00:00Z",
            )

            self.assertTrue(report["passed"])
            checks = report["checks"]
            self.assertEqual("passed", checks["replay_success"]["status"])
            self.assertFalse(checks["replay_success"]["safe_stop"])
            self.assertEqual("passed", checks["replay_safe_stop"]["status"])
            self.assertTrue(checks["replay_safe_stop"]["report_contains_safe_stop"])
            self.assertTrue(checks["replay_safe_stop"]["bundle_manifest_safe_stop"])

            success_manifest = Path(checks["replay_success"]["bundle_manifest"])
            self.assertTrue(success_manifest.exists())
            safe_stop_manifest = Path(checks["replay_safe_stop"]["bundle_manifest"])
            self.assertTrue(safe_stop_manifest.exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def _make_success_record(self, *, snapshot_id: str, asin: str) -> RawSourceRecord:
        html = """
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
                  <span class="a-price-fraction">99</span>
                </span>
              </span>
            </div>
            <div id="availability">In Stock.</div>
          </body>
        </html>
        """
        payload = {
            "asin": asin,
            "original_url": f"https://www.amazon.com/Example-Brand-Sparkling-Water/dp/{asin}",
            "canonical_url": f"https://www.amazon.com/dp/{asin}",
            "final_url": f"https://www.amazon.com/dp/{asin}",
            "status_code": 200,
            "page_title": "Amazon.com : Example Brand Sparkling Water 12 Fl Oz (Pack of 3) : Grocery & Gourmet Food",
            "is_robot_check": False,
            "content_sha256": "release-success-fixture",
            "headers": {"Content-Type": "text/html"},
            "html": html,
        }
        return RawSourceRecord(
            record_id=asin,
            source="amazon",
            snapshot_id=snapshot_id,
            captured_at="2026-04-24T12:00:00Z",
            payload=payload,
            cursor=payload["canonical_url"],
        )

    def _make_failure_record(self, *, snapshot_id: str, asin: str) -> RawSourceRecord:
        html = """
        <html>
          <body>
            <span id="productTitle">Lakanto Monk Fruit Sweetener Keto 5 LB Bag</span>
            <a id="bylineInfo">Lakanto</a>
            <div>No featured offers available</div>
            <div>See All Buying Options</div>
            <script>
              var data = {&quot;priceAmount&quot;: 15.19, &quot;productTitle&quot;: &quot;Stevia Select Organic Stevia Powder 1.25oz&quot;};
              var data2 = {&quot;priceAmount&quot;: 12.82, &quot;productTitle&quot;: &quot;Stevia Select Plain Stevia Liquid Drops&quot;};
            </script>
          </body>
        </html>
        """
        payload = {
            "asin": asin,
            "original_url": f"https://www.amazon.com/Lakanto-Sweetener/dp/{asin}",
            "canonical_url": f"https://www.amazon.com/dp/{asin}",
            "final_url": f"https://www.amazon.com/dp/{asin}",
            "status_code": 200,
            "page_title": "Amazon.com : Lakanto Monk Fruit Sweetener Keto 5 LB Bag : Grocery & Gourmet Food",
            "is_robot_check": False,
            "content_sha256": "release-failure-fixture",
            "headers": {"Content-Type": "text/html"},
            "html": html,
        }
        return RawSourceRecord(
            record_id=asin,
            source="amazon",
            snapshot_id=snapshot_id,
            captured_at="2026-04-24T12:00:00Z",
            payload=payload,
            cursor=payload["canonical_url"],
        )

    def _make_scratch_dir(self, name: str) -> Path:
        SCRATCH_ROOT.mkdir(exist_ok=True)
        scratch_dir = SCRATCH_ROOT / name
        shutil.rmtree(scratch_dir, ignore_errors=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        return scratch_dir


if __name__ == "__main__":
    unittest.main()
