from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path

from .amazon import AmazonProductConnector
from .contracts import assert_valid
from .gap_hypothesis import build_gap_hypothesis
from .http_client import HttpFetchError, HttpFetcher, UrllibHttpFetcher
from .ingestion import IngestionService
from .normalization import BatchNormalizer, write_normalization_artifacts
from .raw_store import FilesystemRawStore
from .taxonomy import TaxonomyAssigner, write_taxonomy_artifacts


@dataclass(frozen=True)
class MvpRunResult:
    snapshot_id: str
    output_dir: Path
    artifacts: dict[str, str]
    opportunity: dict


class MvpRunFailed(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        stage: str,
        snapshot_id: str,
        output_dir: Path,
        artifacts: dict[str, str],
    ) -> None:
        super().__init__(message)
        self.stage = stage
        self.snapshot_id = snapshot_id
        self.output_dir = output_dir
        self.artifacts = artifacts


def _write_failure_report(output_dir: Path, *, snapshot_id: str, stage: str, error: str) -> str:
    report_path = output_dir / "mvp_report.md"
    report_path.write_text(
        "\n".join(
            [
                "# MVP Gap Report (Failed)",
                "",
                f"Snapshot: `{snapshot_id}`",
                "",
                f"Stage: `{stage}`",
                "",
                "## What Happened",
                error.strip(),
                "",
                "## Notes",
                "- This is decision support, not an autonomous truth engine.",
                "- The run stopped early to avoid producing silently corrupted output.",
                "",
                "## Next Debug Steps",
                "- Inspect `normalization_records.json` for specific issues and field provenance.",
                "- Inspect the raw snapshot under `data/raw/.../<snapshot_id>/` to see the captured HTML.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return str(report_path)


def run_mvp(
    *,
    url: str,
    store_dir: Path,
    output_dir: Path | None = None,
    fetcher: HttpFetcher | None = None,
    captured_at: str | None = None,
    generated_at: str | None = None,
) -> MvpRunResult:
    store = FilesystemRawStore(store_dir)
    service = IngestionService(store)

    connector = AmazonProductConnector(
        product_url=url,
        fetcher=fetcher or UrllibHttpFetcher(),
        captured_at=captured_at,
    )
    ingest_result = service.ingest(connector)
    snapshot_id = ingest_result.manifest.snapshot_id

    resolved_output_dir = output_dir or Path("artifacts") / f"mvp-{snapshot_id}"
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    normalization_result = BatchNormalizer().normalize_snapshot(ingest_result.manifest, ingest_result.records)
    norm_artifacts = write_normalization_artifacts(resolved_output_dir, ingest_result.manifest, normalization_result)

    if normalization_result.summary.run_status == "failed":
        # Keep artifacts, but stop before taxonomy/opportunity generation.
        issue_bits: list[str] = []
        for record in normalization_result.records:
            for issue in record.issues:
                issue_bits.append(f"{record.source_record_id}: {issue.message}")
        issue_suffix = f" | issues: {'; '.join(issue_bits)}" if issue_bits else ""
        error = (
            f"normalization failed: normalized_records={normalization_result.summary.normalized_records} "
            f"invalid_records={normalization_result.summary.invalid_records}{issue_suffix}"
        )
        artifacts = dict(norm_artifacts)
        artifacts["mvp_report"] = _write_failure_report(
            resolved_output_dir,
            snapshot_id=snapshot_id,
            stage="normalize",
            error=error,
        )
        raise MvpRunFailed(
            error,
            stage="normalize",
            snapshot_id=snapshot_id,
            output_dir=resolved_output_dir,
            artifacts=artifacts,
        )

    listing = normalization_result.normalized_listings[0]
    record_result = next(
        record for record in normalization_result.records if record.source_record_id == listing["source_record_id"]
    )
    normalization_record = {
        "source_record_id": record_result.source_record_id,
        "status": record_result.status,
        "listing_id": record_result.listing_id,
        "raw_payload_uri": record_result.raw_payload_uri,
        "warnings": record_result.warnings,
        "low_confidence_reasons": record_result.low_confidence_reasons,
        "field_provenance": record_result.field_provenance,
    }

    taxonomy_result = TaxonomyAssigner().assign_batch([listing], snapshot_id=snapshot_id)
    tax_artifacts = write_taxonomy_artifacts(resolved_output_dir, snapshot_id, taxonomy_result)

    if taxonomy_result.summary.run_status == "failed":
        issue_bits: list[str] = []
        for record in taxonomy_result.records:
            for issue in record.issues:
                issue_bits.append(f"{record.listing_id}: {issue.message}")
        issue_suffix = f" | issues: {'; '.join(issue_bits)}" if issue_bits else ""
        error = f"taxonomy assignment failed for the normalized listing{issue_suffix}"
        artifacts = {}
        artifacts.update(norm_artifacts)
        artifacts.update(tax_artifacts)
        artifacts["mvp_report"] = _write_failure_report(
            resolved_output_dir,
            snapshot_id=snapshot_id,
            stage="taxonomy",
            error=error,
        )
        raise MvpRunFailed(
            error,
            stage="taxonomy",
            snapshot_id=snapshot_id,
            output_dir=resolved_output_dir,
            artifacts=artifacts,
        )

    taxonomy_assignment = taxonomy_result.assignments[0]

    hypothesis = build_gap_hypothesis(
        listing=listing,
        taxonomy_assignment=taxonomy_assignment,
        normalization_record=normalization_record,
        snapshot_id=snapshot_id,
        generated_at=generated_at,
    )

    opportunities_path = resolved_output_dir / "opportunities.json"
    report_path = resolved_output_dir / "mvp_report.md"

    opportunities_payload = [hypothesis.opportunity]
    opportunities_path.write_text(json.dumps(opportunities_payload, indent=2), encoding="utf-8")
    report_path.write_text(hypothesis.report_markdown, encoding="utf-8")

    artifacts = {}
    artifacts.update(norm_artifacts)
    artifacts.update(tax_artifacts)
    artifacts["opportunities"] = str(opportunities_path)
    artifacts["mvp_report"] = str(report_path)

    # Minimal validation on write.
    assert_valid("opportunity", hypothesis.opportunity)

    return MvpRunResult(
        snapshot_id=snapshot_id,
        output_dir=resolved_output_dir,
        artifacts=artifacts,
        opportunity=hypothesis.opportunity,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Brand Gap Inference MVP flow on one Amazon product URL.")
    parser.add_argument("--url", required=True, help="Amazon product URL to analyze")
    parser.add_argument("--store-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    try:
        result = run_mvp(url=args.url, store_dir=args.store_dir, output_dir=args.output_dir, generated_at=generated_at)
        print(
            json.dumps(
                {
                    "status": "success",
                    "snapshot_id": result.snapshot_id,
                    "output_dir": str(result.output_dir),
                    "artifacts": result.artifacts,
                    "opportunity_id": result.opportunity.get("opportunity_id"),
                    "confidence": result.opportunity.get("confidence"),
                },
                indent=2,
            )
        )
        return 0
    except MvpRunFailed as error:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "stage": error.stage,
                    "snapshot_id": error.snapshot_id,
                    "output_dir": str(error.output_dir),
                    "artifacts": error.artifacts,
                    "error": str(error),
                },
                indent=2,
            )
        )
        return 1
    except (HttpFetchError, ValueError, RuntimeError) as error:
        # Avoid tracebacks for operators; emit a clear summary.
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
