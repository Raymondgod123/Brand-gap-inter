from __future__ import annotations

from dataclasses import dataclass

from .contracts import assert_valid


@dataclass(frozen=True)
class RunManifest:
    run_id: str
    pipeline_version: str
    schema_version: str
    prompt_version: str
    source_snapshot: str
    artifact_root_uri: str
    started_at: str
    status: str
    task_count: int
    completed_at: str | None = None

    @classmethod
    def from_dict(cls, payload: dict) -> "RunManifest":
        assert_valid("run_manifest", payload)
        return cls(**payload)


@dataclass(frozen=True)
class RunTaskEnvelope:
    task_id: str
    run_id: str
    task_type: str
    repo_sha: str
    owner_agent: str
    status: str
    dependencies: list[str]
    input_artifact_uris: list[str]
    acceptance_checks: list[str]
    retry_budget: int
    timeout_seconds: int

    @classmethod
    def from_dict(cls, payload: dict) -> "RunTaskEnvelope":
        assert_valid("run_task_envelope", payload)
        return cls(**payload)
