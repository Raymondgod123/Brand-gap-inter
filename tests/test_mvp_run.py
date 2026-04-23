from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from brand_gap_inference.contracts import validate_document
from brand_gap_inference.http_client import HttpFetcher, HttpResponse
from brand_gap_inference.mvp_run import run_mvp

ROOT = Path(__file__).resolve().parents[1]
SCRATCH_ROOT = ROOT / ".tmp-tests"


class StubFetcher(HttpFetcher):
    def __init__(self, response: HttpResponse) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, str] | None, int]] = []

    def fetch(self, url: str, headers: dict[str, str] | None = None, timeout_seconds: int = 30) -> HttpResponse:
        self.calls.append((url, headers, timeout_seconds))
        return self.response


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
            self.assertIn("decision support", report_text.lower())

            # Opportunities artifact is a list with one element.
            opportunities = json.loads(Path(result.artifacts["opportunities"]).read_text(encoding="utf-8"))
            self.assertEqual(1, len(opportunities))
            self.assertEqual(result.opportunity["opportunity_id"], opportunities[0]["opportunity_id"])
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

