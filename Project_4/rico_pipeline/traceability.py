"""Traceability: fingerprints, git_sha, and pipeline_runs lifecycle."""
from __future__ import annotations
import hashlib
import os
import subprocess

from rico_pipeline.context import RunContext


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def current_git_sha() -> str:
    env = os.getenv("GIT_SHA")
    if env:
        return env
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
    except Exception:
        return "unknown"


def start_run_sql(ctx: RunContext):
    sql = """
        INSERT INTO pipeline_runs
          (run_id, dag_run_id, started_at, status, limit_param, git_sha,
           trigger_source, clip_version, sbert_version, llm_model, prompt_version)
        VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (ctx.run_id, ctx.dag_run_id, "running", ctx.limit, ctx.git_sha,
              ctx.trigger_source, ctx.clip_version, ctx.sbert_version,
              ctx.llm_model, ctx.prompt_version)
    return sql, params


def finish_run_sql(run_id: str, status: str):
    sql = "UPDATE pipeline_runs SET status = %s, ended_at = NOW() WHERE run_id = %s"
    return sql, (status, run_id)
