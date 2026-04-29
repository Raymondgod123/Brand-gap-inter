from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.browser_capture import BrowserCaptureResult, BrowserCaptureRunner
from brand_gap_inference.connectors import FixtureConnector
from brand_gap_inference.contracts import validate_document
from brand_gap_inference.http_client import HttpFetcher, HttpResponse
from brand_gap_inference.ingestion import IngestionService
from brand_gap_inference.mvp_run import (
    MvpFallbackFailed,
    MvpRunFailed,
    load_candidate_urls,
    load_candidate_snapshot_ids,
    run_mvp,
    run_mvp_from_snapshot,
    run_mvp_from_snapshot_with_fallback,
    run_mvp_with_fallback,
)
from brand_gap_inference.raw_store import FilesystemRawStore

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"
DIRTY_FIXTURE_PATH = ROOT / "fixtures" / "normalization" / "amazon_dirty_cases.json"


class StubFetcher(HttpFetcher):
    def __init__(self, response: HttpResponse) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, str] | None, int]] = []

    def fetch(self, url: str, headers: dict[str, str] | None = None, timeout_seconds: int = 30) -> HttpResponse:
        self.calls.append((url, headers, timeout_seconds))
        return self.response


class SequenceFetcher(HttpFetcher):
    def __init__(self, responses_by_url: dict[str, HttpResponse]) -> None:
        self.responses_by_url = responses_by_url
        self.calls: list[tuple[str, dict[str, str] | None, int]] = []

    def fetch(self, url: str, headers: dict[str, str] | None = None, timeout_seconds: int = 30) -> HttpResponse:
        self.calls.append((url, headers, timeout_seconds))
        if url not in self.responses_by_url:
            raise AssertionError(f"unexpected fetch URL {url!r}")
        return self.responses_by_url[url]


class AlwaysFailFetcher(HttpFetcher):
    def fetch(self, url: str, headers: dict[str, str] | None = None, timeout_seconds: int = 30) -> HttpResponse:
        raise RuntimeError(f"simulated network failure for {url}")


class StubBrowserCaptureRunner(BrowserCaptureRunner):
    def __init__(self, result: BrowserCaptureResult) -> None:
        self.result = result
        self.calls: list[tuple[str, int]] = []

    def capture(self, url: str, timeout_seconds: int = 45) -> BrowserCaptureResult:
        self.calls.append((url, timeout_seconds))
        return self.result


class MvpRunTests(unittest.TestCase):
    def test_mvp_run_emits_schema_valid_opportunity_and_report_with_caveats(self) -> None:
        html = """
        <html>
          <head><title>Amazon.com : MysteryBrand Hydration Mix</title></head>
          <body>
            <span id="productTitle">MysteryBrand Hydration Mix</span>
            <div id="corePriceDisplay_desktop_feature_div">
              <span class="a-offscreen">$12.00</span>
            </div>
          </body>
        </html>
        """
        fetcher = StubFetcher(
            HttpResponse(
                status_code=200,
                final_url="https://www.amazon.com/dp/MVPTEST001",
                headers={"content-type": "text/html"},
                body=html,
            )
        )

        scratch_dir = self._make_scratch_dir("mvp-run")
        try:
            result = run_mvp(
                url="https://www.amazon.com/dp/MVPTEST001",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                fetcher=fetcher,
                captured_at="2026-04-23T00:00:00Z",
                generated_at="2026-04-23T00:00:01Z",
            )

            # Machine output: opportunity schema validity
            issues = validate_document("opportunity", result.opportunity)
            self.assertEqual([], issues)

            # Human output: report includes caveats for low-confidence parsing
            report_path = Path(result.artifacts["mvp_report"])
            report_text = report_path.read_text(encoding="utf-8")
            self.assertIn("## Caveats", report_text)
            self.assertIn("Low-confidence reasons:", report_text)
            self.assertIn("## Provenance Snapshot", report_text)
            self.assertIn("brand_name: source_type=", report_text)
            self.assertIn("price: source_type=", report_text)
            self.assertIn("decision support", report_text.lower())
            self.assertTrue(Path(result.artifacts["bundle_manifest"]).exists())
            manifest = json.loads(Path(result.artifacts["bundle_manifest"]).read_text(encoding="utf-8"))
            self.assertEqual("success", manifest["status"])
            self.assertFalse(manifest["safe_stop"])

            # Opportunities artifact is a list with one element.
            opportunities = json.loads(Path(result.artifacts["opportunities"]).read_text(encoding="utf-8"))
            self.assertEqual(1, len(opportunities))
            self.assertEqual(result.opportunity["opportunity_id"], opportunities[0]["opportunity_id"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_mvp_run_writes_failure_report_when_normalization_fails(self) -> None:
        html = """
        <html>
          <head><title>Amazon.com : MysteryBrand Hydration Mix</title></head>
          <body>
            <span id="productTitle">MysteryBrand Hydration Mix</span>
            <!-- Intentionally no price markup to force normalization failure -->
          </body>
        </html>
        """
        fetcher = StubFetcher(
            HttpResponse(
                status_code=200,
                final_url="https://www.amazon.com/dp/MVPFAIL001",
                headers={"content-type": "text/html"},
                body=html,
            )
        )

        scratch_dir = self._make_scratch_dir("mvp-run-failure")
        try:
            with self.assertRaises(MvpRunFailed) as context:
                run_mvp(
                    url="https://www.amazon.com/dp/MVPFAIL001",
                    store_dir=scratch_dir / "raw",
                    output_dir=scratch_dir / "artifacts",
                    fetcher=fetcher,
                    captured_at="2026-04-23T00:00:00Z",
                    generated_at="2026-04-23T00:00:01Z",
                )

            error = context.exception
            self.assertEqual("normalize", error.stage)
            self.assertTrue(Path(error.artifacts["normalization_report"]).exists())
            self.assertTrue(Path(error.artifacts["normalization_records"]).exists())
            self.assertTrue(Path(error.artifacts["mvp_report"]).exists())
            self.assertTrue(Path(error.artifacts["bundle_manifest"]).exists())

            report_text = Path(error.artifacts["mvp_report"]).read_text(encoding="utf-8")
            self.assertIn("MVP Gap Report (Failed)", report_text)
            self.assertIn("Stage: `normalize`", report_text)
            self.assertIn("Status: SAFE STOP", report_text)
            self.assertIn("## Artifact Bundle", report_text)
            manifest = json.loads(Path(error.artifacts["bundle_manifest"]).read_text(encoding="utf-8"))
            self.assertEqual("failed", manifest["status"])
            self.assertTrue(manifest["safe_stop"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_mvp_run_with_fallback_uses_next_url_after_failure(self) -> None:
        failing_html = """
        <html>
          <body>
            <span id="productTitle">Failing Product</span>
          </body>
        </html>
        """
        passing_html = """
        <html>
          <head><title>Amazon.com : Example Brand Hydration Mix</title></head>
          <body>
            <span id="productTitle">Example Brand Hydration Mix 12 Count</span>
            <a id="bylineInfo">Visit the Example Brand Store</a>
            <ul class="a-unordered-list a-horizontal a-size-small">
              <li><span class="a-list-item"><a class="a-link-normal a-color-tertiary">Grocery &amp; Gourmet Food</a></span></li>
            </ul>
            <div id="corePriceDisplay_desktop_feature_div">
              <span class="a-offscreen">$19.99</span>
            </div>
            <div id="availability">In Stock.</div>
          </body>
        </html>
        """
        fetcher = SequenceFetcher(
            {
                "https://www.amazon.com/dp/FAIL000001": HttpResponse(
                    status_code=200,
                    final_url="https://www.amazon.com/dp/FAIL000001",
                    headers={"content-type": "text/html"},
                    body=failing_html,
                ),
                "https://www.amazon.com/dp/PASS000001": HttpResponse(
                    status_code=200,
                    final_url="https://www.amazon.com/dp/PASS000001",
                    headers={"content-type": "text/html"},
                    body=passing_html,
                ),
            }
        )

        scratch_dir = self._make_scratch_dir("mvp-run-fallback")
        try:
            fallback = run_mvp_with_fallback(
                urls=["https://www.amazon.com/dp/FAIL000001", "https://www.amazon.com/dp/PASS000001"],
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                fetcher=fetcher,
                captured_at="2026-04-24T00:00:00Z",
                generated_at="2026-04-24T00:00:01Z",
            )

            self.assertEqual("https://www.amazon.com/dp/PASS000001", fallback.selected_target)
            self.assertEqual(2, len(fallback.attempts))
            self.assertEqual("failed", fallback.attempts[0].status)
            self.assertEqual("normalize", fallback.attempts[0].stage)
            self.assertEqual("success", fallback.attempts[1].status)
            self.assertTrue(Path(fallback.result.artifacts["mvp_report"]).exists())
            self.assertIsNotNone(fallback.summary_artifacts)
            self.assertTrue(Path(fallback.summary_artifacts["fallback_attempts_json"]).exists())
            self.assertTrue(Path(fallback.summary_artifacts["fallback_report"]).exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_mvp_run_browser_mode_emits_success_bundle(self) -> None:
        runner = StubBrowserCaptureRunner(
            BrowserCaptureResult(
                final_url="https://www.amazon.com/dp/BROWMVP001",
                status_code=200,
                page_title="Amazon.com : Browser Brand Sparkling Water 12 Fl Oz (Pack of 3)",
                html="""
                <html>
                  <body>
                    <span id="productTitle">Browser Brand Sparkling Water 12 Fl Oz (Pack of 3)</span>
                    <a id="bylineInfo">Visit the Browser Brand Store</a>
                    <ul class="a-unordered-list a-horizontal a-size-small">
                      <li><span class="a-list-item"><a class="a-link-normal a-color-tertiary">Grocery &amp; Gourmet Food</a></span></li>
                    </ul>
                    <div id="corePriceDisplay_desktop_feature_div">
                      <span class="a-offscreen">$15.49</span>
                    </div>
                    <div id="availability">In Stock.</div>
                  </body>
                </html>
                """,
                is_robot_check=False,
                capture_diagnostics={
                    "navigation_ok": True,
                    "ready_state": "complete",
                    "wait_strategy": "domcontentloaded+body+bounded_settle",
                    "timing_ms": {"goto": 90, "settle": 300, "total": 390},
                    "visible_offer_signals": {
                        "has_no_featured_offers": False,
                        "has_buying_options": False,
                        "has_currently_unavailable": False,
                        "has_price_to_pay_block": False,
                        "has_core_price_block": True,
                    },
                },
            )
        )

        scratch_dir = self._make_scratch_dir("mvp-run-browser-success")
        try:
            result = run_mvp(
                url="https://www.amazon.com/dp/BROWMVP001",
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                acquisition_mode="browser",
                browser_capture_runner=runner,
                captured_at="2026-04-24T14:00:00Z",
                generated_at="2026-04-24T14:00:01Z",
            )

            self.assertEqual("https://www.amazon.com/dp/BROWMVP001", runner.calls[0][0])
            manifest = json.loads(Path(result.artifacts["bundle_manifest"]).read_text(encoding="utf-8"))
            self.assertEqual("success", manifest["status"])
            self.assertFalse(manifest["safe_stop"])

            raw_record_path = scratch_dir / "raw" / "amazon" / result.snapshot_id / "BROWMVP001.json"
            raw_record = json.loads(raw_record_path.read_text(encoding="utf-8"))
            self.assertEqual("browser_playwright", raw_record["payload"]["acquisition_method"])
            self.assertIn("capture_diagnostics", raw_record["payload"])
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_mvp_run_browser_mode_safe_stops_when_price_is_missing(self) -> None:
        runner = StubBrowserCaptureRunner(
            BrowserCaptureResult(
                final_url="https://www.amazon.com/dp/BROWFAIL01",
                status_code=200,
                page_title="Amazon.com : Lakanto Monk Fruit Sweetener Keto 5 LB Bag",
                html="""
                <html>
                  <body>
                    <span id="productTitle">Lakanto Monk Fruit Sweetener Keto 5 LB Bag</span>
                    <a id="bylineInfo">Lakanto</a>
                    <div>No featured offers available</div>
                    <div>See All Buying Options</div>
                  </body>
                </html>
                """,
                is_robot_check=False,
                capture_diagnostics={
                    "navigation_ok": True,
                    "ready_state": "complete",
                    "wait_strategy": "domcontentloaded+body+bounded_settle",
                    "timing_ms": {"goto": 90, "settle": 300, "total": 390},
                    "visible_offer_signals": {
                        "has_no_featured_offers": True,
                        "has_buying_options": True,
                        "has_currently_unavailable": False,
                        "has_price_to_pay_block": False,
                        "has_core_price_block": False,
                    },
                },
            )
        )

        scratch_dir = self._make_scratch_dir("mvp-run-browser-safe-stop")
        try:
            with self.assertRaises(MvpRunFailed) as context:
                run_mvp(
                    url="https://www.amazon.com/dp/BROWFAIL01",
                    store_dir=scratch_dir / "raw",
                    output_dir=scratch_dir / "artifacts",
                    acquisition_mode="browser",
                    browser_capture_runner=runner,
                    captured_at="2026-04-24T14:30:00Z",
                    generated_at="2026-04-24T14:30:01Z",
                )

            error = context.exception
            manifest = json.loads(Path(error.artifacts["bundle_manifest"]).read_text(encoding="utf-8"))
            self.assertEqual("failed", manifest["status"])
            self.assertTrue(manifest["safe_stop"])
            report_text = Path(error.artifacts["mvp_report"]).read_text(encoding="utf-8")
            self.assertIn("SAFE STOP", report_text)
            self.assertIn("missing product price", report_text)
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_load_candidate_urls_ignores_comments_and_blank_lines(self) -> None:
        scratch_dir = self._make_scratch_dir("mvp-run-candidates-file")
        try:
            candidate_file = scratch_dir / "urls.txt"
            candidate_file.write_text(
                "\n".join(
                    [
                        "# preferred demo URLs",
                        "",
                        "https://www.amazon.com/dp/A001",
                        "   https://www.amazon.com/dp/A002   ",
                        "",
                        "# backup",
                        "https://www.amazon.com/dp/A003",
                    ]
                ),
                encoding="utf-8",
            )
            urls = load_candidate_urls(candidate_file)
            self.assertEqual(
                [
                    "https://www.amazon.com/dp/A001",
                    "https://www.amazon.com/dp/A002",
                    "https://www.amazon.com/dp/A003",
                ],
                urls,
            )
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_load_candidate_snapshot_ids_ignores_comments_and_blank_lines(self) -> None:
        scratch_dir = self._make_scratch_dir("mvp-run-snapshot-ids-file")
        try:
            candidate_file = scratch_dir / "snapshot_ids.txt"
            candidate_file.write_text(
                "\n".join(
                    [
                        "# replay snapshots",
                        "",
                        "amazon-SNAP-001",
                        "   amazon-SNAP-002   ",
                        "",
                        "# backup",
                        "amazon-SNAP-003",
                    ]
                ),
                encoding="utf-8",
            )
            snapshot_ids = load_candidate_snapshot_ids(candidate_file)
            self.assertEqual(
                ["amazon-SNAP-001", "amazon-SNAP-002", "amazon-SNAP-003"],
                snapshot_ids,
            )
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_mvp_run_with_fallback_raises_structured_error_when_all_candidates_fail(self) -> None:
        fetcher = AlwaysFailFetcher()
        scratch_dir = self._make_scratch_dir("mvp-run-fallback-all-fail")
        try:
            with self.assertRaises(MvpFallbackFailed) as context:
                run_mvp_with_fallback(
                    urls=["https://www.amazon.com/dp/FAIL000001", "https://www.amazon.com/dp/FAIL000002"],
                    store_dir=scratch_dir / "raw",
                    output_dir=scratch_dir / "artifacts",
                    fetcher=fetcher,
                    captured_at="2026-04-24T00:00:00Z",
                    generated_at="2026-04-24T00:00:01Z",
                )

            error = context.exception
            self.assertEqual(2, len(error.attempts))
            self.assertTrue(all(attempt.status == "failed" for attempt in error.attempts))
            self.assertTrue(all(attempt.stage == "fetch_or_runtime" for attempt in error.attempts))
            self.assertIn("all candidate URLs failed", str(error))
            self.assertIn("fallback_attempts_json", error.summary_artifacts)
            self.assertTrue(Path(error.summary_artifacts["fallback_attempts_json"]).exists())
            self.assertTrue(Path(error.summary_artifacts["fallback_report"]).exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_mvp_run_from_snapshot_with_fallback_uses_next_snapshot_after_failure(self) -> None:
        connector = FixtureConnector(source_name="amazon", fixture_path=DIRTY_FIXTURE_PATH)
        scratch_dir = self._make_scratch_dir("mvp-run-replay-fallback")
        try:
            store = FilesystemRawStore(scratch_dir / "raw")
            service = IngestionService(store)
            ingest_result = service.ingest(connector)
            valid_snapshot_id = ingest_result.manifest.snapshot_id
            invalid_snapshot_id = "amazon-does-not-exist-2026-04-24T00-00-00Z"

            fallback = run_mvp_from_snapshot_with_fallback(
                source="amazon",
                snapshot_ids=[invalid_snapshot_id, valid_snapshot_id],
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                generated_at="2026-04-24T02:00:00Z",
            )

            self.assertEqual(valid_snapshot_id, fallback.selected_target)
            self.assertEqual(2, len(fallback.attempts))
            self.assertEqual("failed", fallback.attempts[0].status)
            self.assertEqual("replay_snapshot", fallback.attempts[0].mode)
            self.assertEqual("replay_or_runtime", fallback.attempts[0].stage)
            self.assertEqual("success", fallback.attempts[1].status)
            self.assertTrue(Path(fallback.result.artifacts["mvp_report"]).exists())
            self.assertIsNotNone(fallback.summary_artifacts)
            self.assertTrue(Path(fallback.summary_artifacts["fallback_attempts_json"]).exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_mvp_run_from_snapshot_with_fallback_raises_structured_error_when_all_fail(self) -> None:
        scratch_dir = self._make_scratch_dir("mvp-run-replay-fallback-all-fail")
        try:
            with self.assertRaises(MvpFallbackFailed) as context:
                run_mvp_from_snapshot_with_fallback(
                    source="amazon",
                    snapshot_ids=[
                        "amazon-missing-001",
                        "amazon-missing-002",
                    ],
                    store_dir=scratch_dir / "raw",
                    output_dir=scratch_dir / "artifacts",
                    generated_at="2026-04-24T02:00:00Z",
                )

            error = context.exception
            self.assertEqual(2, len(error.attempts))
            self.assertTrue(all(attempt.status == "failed" for attempt in error.attempts))
            self.assertTrue(all(attempt.mode == "replay_snapshot" for attempt in error.attempts))
            self.assertTrue(all(attempt.stage == "replay_or_runtime" for attempt in error.attempts))
            self.assertIn("all candidate snapshot ids failed", str(error))
            self.assertIn("fallback_attempts_json", error.summary_artifacts)
            self.assertTrue(Path(error.summary_artifacts["fallback_attempts_json"]).exists())
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    def test_mvp_run_from_snapshot_replays_without_live_fetch(self) -> None:
        connector = FixtureConnector(source_name="amazon", fixture_path=DIRTY_FIXTURE_PATH)
        scratch_dir = self._make_scratch_dir("mvp-run-replay")
        try:
            store = FilesystemRawStore(scratch_dir / "raw")
            service = IngestionService(store)
            ingest_result = service.ingest(connector)

            result = run_mvp_from_snapshot(
                source="amazon",
                snapshot_id=ingest_result.manifest.snapshot_id,
                store_dir=scratch_dir / "raw",
                output_dir=scratch_dir / "artifacts",
                generated_at="2026-04-24T02:00:00Z",
            )

            issues = validate_document("opportunity", result.opportunity)
            self.assertEqual([], issues)
            self.assertTrue(Path(result.artifacts["mvp_report"]).exists())
            self.assertTrue(Path(result.artifacts["normalization_report"]).exists())
            self.assertTrue(Path(result.artifacts["taxonomy_report"]).exists())
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
