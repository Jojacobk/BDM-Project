"""RunContext: run identity threaded through every task via XCom."""
from __future__ import annotations
from dataclasses import dataclass, asdict


@dataclass(frozen=True)
class RunContext:
    run_id: str
    dag_run_id: str
    limit: int
    git_sha: str
    trigger_source: str
    clip_version: str
    sbert_version: str
    llm_model: str
    prompt_version: str

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "RunContext":
        return cls(**d)
