from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path

from .mvp_run import (
    MvpFallbackFailed,
    load_candidate_snapshot_ids,
    load_candidate_urls,
    run_mvp_from_snapshot,
    run_mvp_from_snapshot_with_fallback,
    run_mvp_with_fallback,
)


def _timestamp_slug(timestamp: str) -> str:
    return timestamp.replace(":", "-").replace(".", "-").replace("Z", "Z")


def _read_first_id(path: Path, *, kind: str) -> str:
    values = load_candidate_snapshot_ids(path)
    if not values:
        raise ValueError(f"{kind} snapshot list is empty: {path}")
    return values[0]


def run_release_check(
    *,
    source: str,
    store_dir: Path,
    output_dir: Path,
    success_snapshot_id: str,
    failure_snapshot_id: str,
    include_live_attempt: bool = False,
    live_urls_file: Path | None = None,
    generated_at: str | None = None,
) -> dict:
    generated_timestamp = generated_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    output_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, object] = {
        "generated_at": generated_timestamp,
        "source": source,
        "store_dir": str(store_dir),
        "output_dir": str(output_dir),
        "checks": {},
        "passed": False,
    }

    success_dir = output_dir / "replay-success"
    success_result = run_mvp_from_snapshot(
        source=source,
        snapshot_id=success_snapshot_id,
        store_dir=store_dir,
        output_dir=success_dir,
        generated_at=generated_timestamp,
    )
    success_manifest_path = Path(success_result.artifacts["bundle_manifest"])
    success_manifest = json.loads(success_manifest_path.read_text(encoding="utf-8"))
    summary["checks"] = {
        "replay_success": {
            "status": "passed",
            "snapshot_id": success_snapshot_id,
            "output_dir": str(success_result.output_dir),
            "bundle_manifest": str(success_manifest_path),
            "safe_stop": bool(success_manifest.get("safe_stop", False)),
        }
    }

    failure_dir = output_dir / "replay-safe-stop"
    failure_check: dict[str, object] = {
        "status": "failed",
        "snapshot_id": failure_snapshot_id,
    }
    try:
        unexpected = run_mvp_from_snapshot_with_fallback(
            source=source,
            snapshot_ids=[failure_snapshot_id],
            store_dir=store_dir,
            output_dir=failure_dir,
            generated_at=generated_timestamp,
        )
        failure_check["error"] = "expected safe-stop failure but run unexpectedly succeeded"
        failure_check["output_dir"] = str(unexpected.result.output_dir)
    except MvpFallbackFailed as error:
        attempt = error.attempts[0] if error.attempts else None
        report_contains_safe_stop = False
        bundle_manifest_safe_stop = False
        bundle_manifest_path = ""
        if attempt and attempt.artifacts:
            report_path_raw = attempt.artifacts.get("mvp_report")
            if report_path_raw:
                report_text = Path(report_path_raw).read_text(encoding="utf-8")
                report_contains_safe_stop = "Status: SAFE STOP" in report_text
            bundle_path_raw = attempt.artifacts.get("bundle_manifest")
            if bundle_path_raw:
                bundle_manifest_path = bundle_path_raw
                bundle_manifest = json.loads(Path(bundle_path_raw).read_text(encoding="utf-8"))
                bundle_manifest_safe_stop = bool(bundle_manifest.get("safe_stop", False))
        failure_check.update(
            {
                "status": "passed" if report_contains_safe_stop and bundle_manifest_safe_stop else "failed",
                "error": str(error),
                "stage": attempt.stage if attempt else None,
                "attempt_output_dir": attempt.output_dir if attempt else None,
                "report_contains_safe_stop": report_contains_safe_stop,
                "bundle_manifest_safe_stop": bundle_manifest_safe_stop,
                "bundle_manifest": bundle_manifest_path or None,
                "summary_artifacts": error.summary_artifacts,
            }
        )

    summary_checks = dict(summary["checks"])
    summary_checks["replay_safe_stop"] = failure_check

    if include_live_attempt:
        live_check: dict[str, object] = {"status": "skipped", "reason": "no live URL file provided"}
        if live_urls_file is not None:
            urls = load_candidate_urls(live_urls_file)
            live_dir = output_dir / "live-attempt"
            try:
                live_result = run_mvp_with_fallback(
                    urls=urls,
                    store_dir=store_dir,
                    output_dir=live_dir,
                    generated_at=generated_timestamp,
                )
                live_check = {
                    "status": "success",
                    "selected_target": live_result.selected_target,
                    "output_dir": str(live_result.result.output_dir),
                    "attempt_count": len(live_result.attempts),
                    "summary_artifacts": live_result.summary_artifacts or {},
                }
            except MvpFallbackFailed as error:
                live_check = {
                    "status": "failed_safely",
                    "error": str(error),
                    "attempt_count": len(error.attempts),
                    "summary_artifacts": error.summary_artifacts,
                }
        summary_checks["live_attempt"] = live_check

    summary["checks"] = summary_checks
    replay_success_ok = (
        summary_checks["replay_success"]["status"] == "passed"
        and summary_checks["replay_success"]["safe_stop"] is False
    )
    replay_safe_stop_ok = summary_checks["replay_safe_stop"]["status"] == "passed"
    summary["passed"] = bool(replay_success_ok and replay_safe_stop_ok)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run MVP stakeholder release checks (replay success + safe-stop).")
    parser.add_argument("--source", default="amazon")
    parser.add_argument("--store-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--success-snapshot-id", default=None)
    parser.add_argument("--failure-snapshot-id", default=None)
    parser.add_argument(
        "--success-snapshot-ids-file",
        type=Path,
        default=Path("fixtures/mvp/demo_snapshot_ids.txt"),
    )
    parser.add_argument(
        "--failure-snapshot-ids-file",
        type=Path,
        default=Path("fixtures/mvp/demo_snapshot_failure_ids.txt"),
    )
    parser.add_argument("--include-live-attempt", action="store_true")
    parser.add_argument("--live-urls-file", type=Path, default=Path("fixtures/mvp/demo_urls.txt"))
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    output_dir = args.output_dir or Path("artifacts") / f"mvp-release-check-{_timestamp_slug(generated_at)}"

    success_snapshot_id = args.success_snapshot_id or _read_first_id(
        args.success_snapshot_ids_file, kind="success"
    )
    failure_snapshot_id = args.failure_snapshot_id or _read_first_id(
        args.failure_snapshot_ids_file, kind="failure"
    )

    report = run_release_check(
        source=args.source,
        store_dir=args.store_dir,
        output_dir=output_dir,
        success_snapshot_id=success_snapshot_id,
        failure_snapshot_id=failure_snapshot_id,
        include_live_attempt=args.include_live_attempt,
        live_urls_file=args.live_urls_file,
        generated_at=generated_at,
    )
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
