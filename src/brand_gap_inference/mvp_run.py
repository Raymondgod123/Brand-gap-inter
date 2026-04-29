from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path

from .amazon import AmazonBrowserProductConnector, AmazonProductConnector
from .browser_capture import BrowserCaptureRunner
from .contracts import assert_valid
from .gap_hypothesis import build_gap_hypothesis
from .http_client import HttpFetchError, HttpFetcher, UrllibHttpFetcher
from .ingestion import IngestionResult, IngestionService
from .normalization import BatchNormalizer, write_normalization_artifacts
from .raw_store import FilesystemRawStore
from .taxonomy import TaxonomyAssigner, write_taxonomy_artifacts


@dataclass(frozen=True)
class MvpRunResult:
    snapshot_id: str
    output_dir: Path
    artifacts: dict[str, str]
    opportunity: dict


@dataclass(frozen=True)
class MvpAttempt:
    target: str
    status: str
    mode: str = "live_url"
    stage: str | None = None
    snapshot_id: str | None = None
    output_dir: str | None = None
    artifacts: dict[str, str] | None = None
    error: str | None = None


@dataclass(frozen=True)
class MvpFallbackResult:
    selected_target: str
    mode: str
    result: MvpRunResult
    attempts: list[MvpAttempt]
    summary_artifacts: dict[str, str] | None = None


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


class MvpFallbackFailed(RuntimeError):
    def __init__(self, message: str, *, attempts: list[MvpAttempt], summary_artifacts: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.summary_artifacts = summary_artifacts or {}


def _timestamp_slug(timestamp: str) -> str:
    return timestamp.replace(":", "-").replace(".", "-").replace("Z", "Z")


def _write_bundle_manifest(
    output_dir: Path,
    *,
    status: str,
    mode: str,
    stage: str,
    snapshot_id: str,
    safe_stop: bool,
    artifacts: dict[str, str],
    error: str | None = None,
) -> str:
    manifest_path = output_dir / "mvp_bundle_manifest.json"
    payload = {
        "status": status,
        "mode": mode,
        "stage": stage,
        "snapshot_id": snapshot_id,
        "safe_stop": safe_stop,
        "artifacts": artifacts,
    }
    if error:
        payload["error"] = error
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return str(manifest_path)


def _write_failure_report(
    output_dir: Path,
    *,
    snapshot_id: str,
    stage: str,
    error: str,
    artifacts: dict[str, str],
) -> str:
    report_path = output_dir / "mvp_report.md"
    artifact_lines = [f"- `{name}`: `{path}`" for name, path in sorted(artifacts.items())]
    report_path.write_text(
        "\n".join(
            [
                "# MVP Gap Report (Failed)",
                "",
                f"Snapshot: `{snapshot_id}`",
                "",
                f"Stage: `{stage}`",
                "",
                "## Run Outcome",
                "- Status: SAFE STOP",
                "- Decision: The run stopped intentionally to prevent corrupted output.",
                "",
                "## What Failed",
                error.strip(),
                "",
                "## Why This Is Safe",
                "- The system did not guess missing critical fields.",
                "- Unsafe price extraction paths were rejected by design.",
                "- This output is decision support, not an autonomous truth engine.",
                "",
                "## Artifact Bundle",
                *artifact_lines,
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


def _write_fallback_summary(
    *,
    output_dir: Path,
    mode: str,
    attempts: list[MvpAttempt],
    status: str,
    selected_target: str | None = None,
    error: str | None = None,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "fallback_attempts.json"
    md_path = output_dir / "fallback_report.md"

    attempts_payload = [
        {
            "target": attempt.target,
            "mode": attempt.mode,
            "status": attempt.status,
            "stage": attempt.stage,
            "snapshot_id": attempt.snapshot_id,
            "output_dir": attempt.output_dir,
            "error": attempt.error,
        }
        for attempt in attempts
    ]
    summary_payload = {
        "status": status,
        "mode": mode,
        "selected_target": selected_target,
        "attempt_count": len(attempts),
        "attempts": attempts_payload,
    }
    if error:
        summary_payload["error"] = error
    json_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# MVP Fallback Attempt Summary")
    lines.append("")
    lines.append(f"- Mode: `{mode}`")
    lines.append(f"- Final status: `{status}`")
    if selected_target:
        lines.append(f"- Selected target: `{selected_target}`")
    if error:
        lines.append(f"- Final error: {error}")
    lines.append("")
    lines.append("## Attempts")
    if not attempts:
        lines.append("- No attempts were recorded.")
    else:
        for idx, attempt in enumerate(attempts, start=1):
            details: list[str] = []
            if attempt.error:
                details.append(f"error={attempt.error}")
            if attempt.output_dir:
                details.append(f"output={attempt.output_dir}")
            suffix = f" ({'; '.join(details)})" if details else ""
            lines.append(
                f"- Attempt {idx}: `{attempt.status}` target=`{attempt.target}` stage=`{attempt.stage or 'n/a'}`{suffix}"
            )
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")

    return {
        "fallback_attempts_json": str(json_path),
        "fallback_report": str(md_path),
    }


def load_candidate_urls(path: Path) -> list[str]:
    return _load_candidate_lines(path)


def load_candidate_snapshot_ids(path: Path) -> list[str]:
    return _load_candidate_lines(path)


def _load_candidate_lines(path: Path) -> list[str]:
    urls: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        urls.append(line)
    return urls


def run_mvp(
    *,
    url: str,
    store_dir: Path,
    output_dir: Path | None = None,
    acquisition_mode: str = "http",
    fetcher: HttpFetcher | None = None,
    browser_capture_runner: BrowserCaptureRunner | None = None,
    captured_at: str | None = None,
    generated_at: str | None = None,
) -> MvpRunResult:
    store = FilesystemRawStore(store_dir)
    service = IngestionService(store)

    if acquisition_mode == "browser":
        if browser_capture_runner is None:
            connector = AmazonBrowserProductConnector(
                product_url=url,
                captured_at=captured_at,
            )
        else:
            connector = AmazonBrowserProductConnector(
                product_url=url,
                capture_runner=browser_capture_runner,
                captured_at=captured_at,
            )
    elif acquisition_mode == "http":
        connector = AmazonProductConnector(
            product_url=url,
            fetcher=fetcher or UrllibHttpFetcher(),
            captured_at=captured_at,
        )
    else:
        raise ValueError(f"unsupported acquisition mode: {acquisition_mode}")
    ingest_result = service.ingest(connector)
    return _run_mvp_from_ingestion_result(
        ingest_result=ingest_result,
        output_dir=output_dir,
        generated_at=generated_at,
    )


def run_mvp_from_snapshot(
    *,
    source: str,
    snapshot_id: str,
    store_dir: Path,
    output_dir: Path | None = None,
    generated_at: str | None = None,
) -> MvpRunResult:
    store = FilesystemRawStore(store_dir)
    service = IngestionService(store)
    ingest_result = service.replay(source, snapshot_id)
    return _run_mvp_from_ingestion_result(
        ingest_result=ingest_result,
        output_dir=output_dir,
        generated_at=generated_at,
    )


def _run_mvp_from_ingestion_result(
    *,
    ingest_result: IngestionResult,
    output_dir: Path | None = None,
    generated_at: str | None = None,
) -> MvpRunResult:
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
            artifacts=artifacts,
        )
        artifacts["bundle_manifest"] = _write_bundle_manifest(
            resolved_output_dir,
            status="failed",
            mode="single_run",
            stage="normalize",
            snapshot_id=snapshot_id,
            safe_stop=True,
            artifacts=artifacts,
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
            artifacts=artifacts,
        )
        artifacts["bundle_manifest"] = _write_bundle_manifest(
            resolved_output_dir,
            status="failed",
            mode="single_run",
            stage="taxonomy",
            snapshot_id=snapshot_id,
            safe_stop=True,
            artifacts=artifacts,
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
    artifacts["bundle_manifest"] = _write_bundle_manifest(
        resolved_output_dir,
        status="success",
        mode="single_run",
        stage="complete",
        snapshot_id=snapshot_id,
        safe_stop=False,
        artifacts=artifacts,
    )

    # Minimal validation on write.
    assert_valid("opportunity", hypothesis.opportunity)

    return MvpRunResult(
        snapshot_id=snapshot_id,
        output_dir=resolved_output_dir,
        artifacts=artifacts,
        opportunity=hypothesis.opportunity,
    )


def run_mvp_with_fallback(
    *,
    urls: list[str],
    store_dir: Path,
    output_dir: Path | None = None,
    acquisition_mode: str = "http",
    fetcher: HttpFetcher | None = None,
    browser_capture_runner: BrowserCaptureRunner | None = None,
    captured_at: str | None = None,
    generated_at: str | None = None,
) -> MvpFallbackResult:
    if not urls:
        raise ValueError("at least one candidate URL is required")

    live_mode = f"live_url_{acquisition_mode}"
    fallback_mode = f"live_url_fallback_{acquisition_mode}"
    attempts: list[MvpAttempt] = []
    last_error: Exception | None = None
    summary_artifacts: dict[str, str] | None = None
    for index, url in enumerate(urls, start=1):
        attempt_output_dir = output_dir / f"attempt-{index:02d}" if output_dir is not None else None
        try:
            result = run_mvp(
                url=url,
                store_dir=store_dir,
                output_dir=attempt_output_dir,
                acquisition_mode=acquisition_mode,
                fetcher=fetcher,
                browser_capture_runner=browser_capture_runner,
                captured_at=captured_at,
                generated_at=generated_at,
            )
            attempts.append(
                MvpAttempt(
                    target=url,
                    status="success",
                    mode=live_mode,
                    snapshot_id=result.snapshot_id,
                    output_dir=str(result.output_dir),
                    artifacts=result.artifacts,
                )
            )
            if output_dir is not None:
                summary_artifacts = _write_fallback_summary(
                    output_dir=output_dir,
                    mode=fallback_mode,
                    attempts=attempts,
                    status="success",
                    selected_target=url,
                )
            return MvpFallbackResult(
                selected_target=url,
                mode=live_mode,
                result=result,
                attempts=attempts,
                summary_artifacts=summary_artifacts,
            )
        except MvpRunFailed as error:
            attempts.append(
                MvpAttempt(
                    target=url,
                    status="failed",
                    mode=live_mode,
                    stage=error.stage,
                    snapshot_id=error.snapshot_id,
                    output_dir=str(error.output_dir),
                    artifacts=error.artifacts,
                    error=str(error),
                )
            )
            last_error = error
            continue
        except (HttpFetchError, ValueError, RuntimeError) as error:
            attempts.append(
                MvpAttempt(
                    target=url,
                    status="failed",
                    mode=live_mode,
                    stage="fetch_or_runtime",
                    error=str(error),
                )
            )
            last_error = error
            continue

    if isinstance(last_error, MvpRunFailed):
        if output_dir is not None:
            summary_artifacts = _write_fallback_summary(
                output_dir=output_dir,
                mode=fallback_mode,
                attempts=attempts,
                status="failed",
                error=f"all candidate URLs failed; last structured failure: {last_error}",
            )
        raise MvpFallbackFailed(
            f"all candidate URLs failed; last structured failure: {last_error}",
            attempts=attempts,
            summary_artifacts=summary_artifacts,
        )
    if last_error is not None:
        if output_dir is not None:
            summary_artifacts = _write_fallback_summary(
                output_dir=output_dir,
                mode=fallback_mode,
                attempts=attempts,
                status="failed",
                error=f"all candidate URLs failed; last error: {last_error}",
            )
        raise MvpFallbackFailed(
            f"all candidate URLs failed; last error: {last_error}",
            attempts=attempts,
            summary_artifacts=summary_artifacts,
        )
    if output_dir is not None:
        summary_artifacts = _write_fallback_summary(
            output_dir=output_dir,
            mode=fallback_mode,
            attempts=attempts,
            status="failed",
            error="all candidate URLs failed",
        )
    raise MvpFallbackFailed("all candidate URLs failed", attempts=attempts, summary_artifacts=summary_artifacts)


def run_mvp_from_snapshot_with_fallback(
    *,
    source: str,
    snapshot_ids: list[str],
    store_dir: Path,
    output_dir: Path | None = None,
    generated_at: str | None = None,
) -> MvpFallbackResult:
    if not snapshot_ids:
        raise ValueError("at least one candidate snapshot id is required")

    attempts: list[MvpAttempt] = []
    last_error: Exception | None = None
    summary_artifacts: dict[str, str] | None = None
    for index, snapshot_id in enumerate(snapshot_ids, start=1):
        attempt_output_dir = output_dir / f"attempt-{index:02d}" if output_dir is not None else None
        try:
            result = run_mvp_from_snapshot(
                source=source,
                snapshot_id=snapshot_id,
                store_dir=store_dir,
                output_dir=attempt_output_dir,
                generated_at=generated_at,
            )
            attempts.append(
                MvpAttempt(
                    target=snapshot_id,
                    status="success",
                    mode="replay_snapshot",
                    snapshot_id=result.snapshot_id,
                    output_dir=str(result.output_dir),
                    artifacts=result.artifacts,
                )
            )
            if output_dir is not None:
                summary_artifacts = _write_fallback_summary(
                    output_dir=output_dir,
                    mode="replay_snapshot_fallback",
                    attempts=attempts,
                    status="success",
                    selected_target=snapshot_id,
                )
            return MvpFallbackResult(
                selected_target=snapshot_id,
                mode="replay_snapshot",
                result=result,
                attempts=attempts,
                summary_artifacts=summary_artifacts,
            )
        except MvpRunFailed as error:
            attempts.append(
                MvpAttempt(
                    target=snapshot_id,
                    status="failed",
                    mode="replay_snapshot",
                    stage=error.stage,
                    snapshot_id=error.snapshot_id,
                    output_dir=str(error.output_dir),
                    artifacts=error.artifacts,
                    error=str(error),
                )
            )
            last_error = error
            continue
        except (HttpFetchError, ValueError, RuntimeError, FileNotFoundError, OSError) as error:
            attempts.append(
                MvpAttempt(
                    target=snapshot_id,
                    status="failed",
                    mode="replay_snapshot",
                    stage="replay_or_runtime",
                    error=str(error),
                )
            )
            last_error = error
            continue

    if isinstance(last_error, MvpRunFailed):
        if output_dir is not None:
            summary_artifacts = _write_fallback_summary(
                output_dir=output_dir,
                mode="replay_snapshot_fallback",
                attempts=attempts,
                status="failed",
                error=f"all candidate snapshot ids failed; last structured failure: {last_error}",
            )
        raise MvpFallbackFailed(
            f"all candidate snapshot ids failed; last structured failure: {last_error}",
            attempts=attempts,
            summary_artifacts=summary_artifacts,
        )
    if last_error is not None:
        if output_dir is not None:
            summary_artifacts = _write_fallback_summary(
                output_dir=output_dir,
                mode="replay_snapshot_fallback",
                attempts=attempts,
                status="failed",
                error=f"all candidate snapshot ids failed; last error: {last_error}",
            )
        raise MvpFallbackFailed(
            f"all candidate snapshot ids failed; last error: {last_error}",
            attempts=attempts,
            summary_artifacts=summary_artifacts,
        )
    if output_dir is not None:
        summary_artifacts = _write_fallback_summary(
            output_dir=output_dir,
            mode="replay_snapshot_fallback",
            attempts=attempts,
            status="failed",
            error="all candidate snapshot ids failed",
        )
    raise MvpFallbackFailed(
        "all candidate snapshot ids failed",
        attempts=attempts,
        summary_artifacts=summary_artifacts,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Brand Gap Inference MVP flow on one Amazon product URL.")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--url", help="Amazon product URL to analyze")
    source_group.add_argument(
        "--urls-file",
        type=Path,
        help="Path to newline-delimited candidate Amazon URLs. The runner tries each URL until one succeeds.",
    )
    source_group.add_argument(
        "--snapshot-id",
        help="Replay a previously stored snapshot id from data/raw instead of fetching live.",
    )
    source_group.add_argument(
        "--snapshot-ids-file",
        type=Path,
        help="Path to newline-delimited snapshot ids. The runner tries each replay snapshot until one succeeds.",
    )
    parser.add_argument("--source", default="amazon", help="Source name for replay mode (default: amazon)")
    parser.add_argument("--store-dir", type=Path, default=Path("data/raw"))
    parser.add_argument(
        "--acquisition-mode",
        choices=("http", "browser"),
        default="http",
        help="Live acquisition method for --url / --urls-file (default: http)",
    )
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    run_slug = _timestamp_slug(generated_at)

    try:
        if args.url is not None:
            output_dir = args.output_dir or Path("artifacts") / f"mvp-live-{run_slug}"
            result = run_mvp(
                url=args.url,
                store_dir=args.store_dir,
                output_dir=output_dir,
                acquisition_mode=args.acquisition_mode,
                generated_at=generated_at,
            )
            print(
                json.dumps(
                    {
                        "status": "success",
                        "mode": f"live_url_{args.acquisition_mode}",
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

        if args.snapshot_id is not None:
            output_dir = args.output_dir or Path("artifacts") / f"mvp-replay-{args.snapshot_id}"
            result = run_mvp_from_snapshot(
                source=args.source,
                snapshot_id=args.snapshot_id,
                store_dir=args.store_dir,
                output_dir=output_dir,
                generated_at=generated_at,
            )
            print(
                json.dumps(
                    {
                        "status": "success",
                        "mode": "replay",
                        "source": args.source,
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

        if args.snapshot_ids_file is not None:
            output_dir = args.output_dir or Path("artifacts") / f"mvp-replay-fallback-{run_slug}"
            candidate_snapshot_ids = load_candidate_snapshot_ids(args.snapshot_ids_file)
            fallback_result = run_mvp_from_snapshot_with_fallback(
                source=args.source,
                snapshot_ids=candidate_snapshot_ids,
                store_dir=args.store_dir,
                output_dir=output_dir,
                generated_at=generated_at,
            )
            attempts_payload = [
                {
                    "target": attempt.target,
                    "mode": attempt.mode,
                    "status": attempt.status,
                    "stage": attempt.stage,
                    "snapshot_id": attempt.snapshot_id,
                    "output_dir": attempt.output_dir,
                    "error": attempt.error,
                }
                for attempt in fallback_result.attempts
            ]
            print(
                json.dumps(
                    {
                        "status": "success",
                        "mode": "replay_fallback",
                        "selected_snapshot_id": fallback_result.selected_target,
                        "snapshot_id": fallback_result.result.snapshot_id,
                        "output_dir": str(fallback_result.result.output_dir),
                        "artifacts": fallback_result.result.artifacts,
                        "opportunity_id": fallback_result.result.opportunity.get("opportunity_id"),
                        "confidence": fallback_result.result.opportunity.get("confidence"),
                        "attempts": attempts_payload,
                        "summary_artifacts": fallback_result.summary_artifacts or {},
                    },
                    indent=2,
                )
            )
            return 0

        output_dir = args.output_dir or Path("artifacts") / f"mvp-live-fallback-{run_slug}"
        candidate_urls = load_candidate_urls(args.urls_file)
        fallback_result = run_mvp_with_fallback(
            urls=candidate_urls,
            store_dir=args.store_dir,
            output_dir=output_dir,
            acquisition_mode=args.acquisition_mode,
            generated_at=generated_at,
        )
        attempts_payload = [
            {
                "target": attempt.target,
                "mode": attempt.mode,
                "status": attempt.status,
                "stage": attempt.stage,
                "snapshot_id": attempt.snapshot_id,
                "output_dir": attempt.output_dir,
                "error": attempt.error,
            }
            for attempt in fallback_result.attempts
        ]
        print(
            json.dumps(
                {
                    "status": "success",
                    "mode": f"live_url_fallback_{args.acquisition_mode}",
                    "selected_url": fallback_result.selected_target,
                    "snapshot_id": fallback_result.result.snapshot_id,
                    "output_dir": str(fallback_result.result.output_dir),
                    "artifacts": fallback_result.result.artifacts,
                    "opportunity_id": fallback_result.result.opportunity.get("opportunity_id"),
                    "confidence": fallback_result.result.opportunity.get("confidence"),
                    "attempts": attempts_payload,
                    "summary_artifacts": fallback_result.summary_artifacts or {},
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
    except MvpFallbackFailed as error:
        attempts_payload = [
            {
                "target": attempt.target,
                "mode": attempt.mode,
                "status": attempt.status,
                "stage": attempt.stage,
                "snapshot_id": attempt.snapshot_id,
                "output_dir": attempt.output_dir,
                "error": attempt.error,
            }
            for attempt in error.attempts
        ]
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": str(error),
                    "attempts": attempts_payload,
                    "summary_artifacts": error.summary_artifacts,
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
