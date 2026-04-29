from __future__ import annotations

import unittest

from brand_gap_inference.amazon import (
    AmazonBrowserProductConnector,
    AmazonProductConnector,
    canonicalize_amazon_product_url,
    detect_robot_check,
    extract_amazon_asin,
)
from brand_gap_inference.browser_capture import BrowserCaptureResult, BrowserCaptureRunner
from brand_gap_inference.http_client import HttpFetcher, HttpResponse


class StubFetcher(HttpFetcher):
    def __init__(self, response: HttpResponse) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, str] | None, int]] = []

    def fetch(self, url: str, headers: dict[str, str] | None = None, timeout_seconds: int = 30) -> HttpResponse:
        self.calls.append((url, headers, timeout_seconds))
        return self.response


class StubBrowserCaptureRunner(BrowserCaptureRunner):
    def __init__(self, result: BrowserCaptureResult) -> None:
        self.result = result
        self.calls: list[tuple[str, int]] = []

    def capture(self, url: str, timeout_seconds: int = 45) -> BrowserCaptureResult:
        self.calls.append((url, timeout_seconds))
        return self.result


class AmazonConnectorTests(unittest.TestCase):
    def test_extract_asin_from_tracking_url(self) -> None:
        url = (
            "https://www.amazon.com/Lakanto-Classic-Monk-Fruit-Sweetener/dp/B098H7XWQ6/"
            "ref=sr_1_1_sspa?crid=OOB75XJMOQNZ&keywords=lakanto"
        )
        self.assertEqual("B098H7XWQ6", extract_amazon_asin(url))

    def test_canonicalize_product_url_removes_tracking_parameters(self) -> None:
        url = (
            "https://www.amazon.com/Lakanto-Classic-Monk-Fruit-Sweetener/dp/B098H7XWQ6/"
            "ref=sr_1_1_sspa?crid=OOB75XJMOQNZ&keywords=lakanto"
        )
        self.assertEqual("https://www.amazon.com/dp/B098H7XWQ6", canonicalize_amazon_product_url(url))

    def test_connector_emits_raw_source_record_for_live_html(self) -> None:
        response = HttpResponse(
            status_code=200,
            final_url="https://www.amazon.com/dp/B098H7XWQ6",
            headers={"content-type": "text/html"},
            body="<html><head><title>Lakanto Sweetener</title></head><body>ok</body></html>",
        )
        fetcher = StubFetcher(response)
        connector = AmazonProductConnector(
            product_url="https://www.amazon.com/gp/product/B098H7XWQ6?th=1",
            fetcher=fetcher,
            captured_at="2026-04-22T12:00:00Z",
        )

        records = connector.fetch_snapshot()

        self.assertEqual(1, len(records))
        self.assertEqual("B098H7XWQ6", records[0].record_id)
        self.assertEqual("https://www.amazon.com/dp/B098H7XWQ6", fetcher.calls[0][0])
        self.assertEqual("Lakanto Sweetener", records[0].payload["page_title"])
        self.assertFalse(records[0].payload["is_robot_check"])

    def test_robot_check_is_detected(self) -> None:
        html = (
            "<html><head><title>Robot Check</title></head>"
            "<body>Type the characters you see in this image</body></html>"
        )
        self.assertTrue(detect_robot_check(html))

    def test_product_page_error_copy_does_not_trigger_robot_check(self) -> None:
        html = (
            "<html><head><title>Amazon.com : Lakanto Sweetener</title></head>"
            "<body><h3>Sorry, there was a problem.</h3></body></html>"
        )
        self.assertFalse(detect_robot_check(html))

    def test_browser_connector_emits_raw_source_record_for_rendered_html(self) -> None:
        runner = StubBrowserCaptureRunner(
            BrowserCaptureResult(
                final_url="https://www.amazon.com/dp/BROWSER001",
                status_code=200,
                page_title="Amazon.com : Browser Brand Hydration Mix",
                html=(
                    "<html><body><span id=\"productTitle\">Browser Brand Hydration Mix</span>"
                    "<div id=\"corePriceDisplay_desktop_feature_div\"><span class=\"a-offscreen\">$17.25</span></div>"
                    "</body></html>"
                ),
                is_robot_check=False,
                capture_diagnostics={
                    "navigation_ok": True,
                    "ready_state": "complete",
                    "wait_strategy": "domcontentloaded+body+bounded_settle",
                    "timing_ms": {"goto": 100, "settle": 200, "total": 300},
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
        connector = AmazonBrowserProductConnector(
            product_url="https://www.amazon.com/gp/product/BROWSER001?th=1",
            capture_runner=runner,
            captured_at="2026-04-24T12:00:00Z",
        )

        records = connector.fetch_snapshot()

        self.assertEqual(1, len(records))
        record = records[0]
        self.assertEqual("BROWSER001", record.record_id)
        self.assertEqual("browser_playwright", record.payload["acquisition_method"])
        self.assertEqual("chromium", record.payload["browser_engine"])
        self.assertEqual("https://www.amazon.com/dp/BROWSER001", runner.calls[0][0])
        self.assertEqual({}, record.payload["headers"])
        self.assertIn("capture_diagnostics", record.payload)
        self.assertFalse(record.payload["is_robot_check"])


if __name__ == "__main__":
    unittest.main()
