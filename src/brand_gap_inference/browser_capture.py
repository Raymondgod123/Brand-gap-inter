from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
from typing import Protocol


class BrowserCaptureError(RuntimeError):
    pass


@dataclass(frozen=True)
class BrowserCaptureResult:
    final_url: str
    status_code: int | None
    page_title: str | None
    html: str
    is_robot_check: bool
    capture_diagnostics: dict[str, object]


class BrowserCaptureRunner(Protocol):
    def capture(self, url: str, timeout_seconds: int = 45) -> BrowserCaptureResult:
        ...


@dataclass(frozen=True)
class NodePlaywrightAmazonCaptureRunner:
    node_executable: Path | None = None
    node_modules_dir: Path | None = None
    script_path: Path | None = None

    def capture(self, url: str, timeout_seconds: int = 45) -> BrowserCaptureResult:
        node_executable, node_modules_dir = _resolve_node_runtime(
            node_executable=self.node_executable,
            node_modules_dir=self.node_modules_dir,
        )
        script_path = self.script_path or Path(__file__).resolve().with_name("amazon_browser_capture.js")
        if not script_path.exists():
            raise BrowserCaptureError(f"browser capture script not found: {script_path}")

        env = os.environ.copy()
        existing_node_path = env.get("NODE_PATH", "")
        env["NODE_PATH"] = (
            str(node_modules_dir)
            if not existing_node_path
            else f"{node_modules_dir}{os.pathsep}{existing_node_path}"
        )

        command = [
            str(node_executable),
            str(script_path),
            url,
            str(timeout_seconds * 1000),
        ]

        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
                cwd=str(script_path.parent),
                timeout=timeout_seconds + 15,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise BrowserCaptureError(f"browser capture timed out after {timeout_seconds} seconds") from error
        except OSError as error:
            raise BrowserCaptureError(f"unable to start browser capture runtime: {error}") from error

        stdout = (completed.stdout or "").strip()
        stderr = (completed.stderr or "").strip()
        if completed.returncode != 0:
            error_detail = stderr or stdout or f"node exited with code {completed.returncode}"
            raise BrowserCaptureError(f"browser capture failed: {error_detail}")

        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as error:
            raise BrowserCaptureError(f"browser capture returned invalid JSON: {error}") from error

        html = payload.get("html")
        if not isinstance(html, str) or not html.strip():
            raise BrowserCaptureError("browser capture returned empty html")

        capture_diagnostics = payload.get("capture_diagnostics")
        if not isinstance(capture_diagnostics, dict):
            raise BrowserCaptureError("browser capture returned invalid capture_diagnostics payload")

        return BrowserCaptureResult(
            final_url=str(payload.get("final_url") or url),
            status_code=_coerce_optional_int(payload.get("status_code")),
            page_title=_coerce_optional_str(payload.get("page_title")),
            html=html,
            is_robot_check=bool(payload.get("is_robot_check", False)),
            capture_diagnostics=capture_diagnostics,
        )


def _resolve_node_runtime(
    *,
    node_executable: Path | None,
    node_modules_dir: Path | None,
) -> tuple[Path, Path]:
    resolved_executable = node_executable or _resolve_node_executable_from_env()
    resolved_modules = node_modules_dir or _resolve_node_modules_from_env()

    if not resolved_executable.exists():
        raise BrowserCaptureError(f"bundled node executable not found: {resolved_executable}")
    if not resolved_modules.exists():
        raise BrowserCaptureError(f"bundled node_modules directory not found: {resolved_modules}")

    return resolved_executable, resolved_modules


def _resolve_node_executable_from_env() -> Path:
    explicit = os.environ.get("BRAND_GAP_NODE_EXECUTABLE")
    if explicit:
        return Path(explicit)
    return (
        Path.home()
        / ".cache"
        / "codex-runtimes"
        / "codex-primary-runtime"
        / "dependencies"
        / "node"
        / "bin"
        / "node.exe"
    )


def _resolve_node_modules_from_env() -> Path:
    explicit = os.environ.get("BRAND_GAP_NODE_MODULES")
    if explicit:
        return Path(explicit)
    return (
        Path.home()
        / ".cache"
        / "codex-runtimes"
        / "codex-primary-runtime"
        / "dependencies"
        / "node"
        / "node_modules"
    )


def _coerce_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        return int(value)
    return None


def _coerce_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
