from __future__ import annotations

import unittest

from brand_gap_inference.amazon import (
    AmazonProductConnector,
    canonicalize_amazon_product_url,
    detect_robot_check,
    extract_amazon_asin,
)
from brand_gap_inference.http_client import HttpFetcher, HttpResponse


class StubFetcher(HttpFetcher):
    def __init__(self, response: HttpResponse) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, str] | None, int]] = []

    def fetch(self, url: str, headers: dict[str, str] | None = None, timeout_seconds: int = 30) -> HttpResponse:
        self.calls.append((url, headers, timeout_seconds))
        return self.response


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


if __name__ == "__main__":
    unittest.main()
