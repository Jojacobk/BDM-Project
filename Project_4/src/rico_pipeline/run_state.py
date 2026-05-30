from __future__ import annotations

import logging
import os
import subprocess
import uuid
from datetime import UTC, datetime

from rico_pipeline.config import Settings
from rico_pipeline.constants import CLIP_MODEL_VERSION, PROMPT_VERSION, SBERT_MODEL_VERSION
from rico_pipeline.db import connect

log = logging.getLogger(__name__)


def current_git_sha() -> str:
    env_sha = os.environ.get("GIT_SHA")
    if env_sha:
        return env_sha
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=3,
        )
        return result.stdout.strip()
    except Exception:  # noqa: BLE001
        return "unknown"


def create_run(
    settings: Settings,
    *,
    dag_run_id: str,
    limit: int,
    trigger_type: str,
) -> str:
    run_id = str(uuid.uuid4())
    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, dag_run_id, started_at, status, limit_param, git_sha,
                clip_model_version, sbert_model_version, llm_model,
                prompt_version, trigger_type
            )
            VALUES (%s, %s, %s, 'running', %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                dag_run_id,
                datetime.now(UTC),
                limit,
                current_git_sha(),
                CLIP_MODEL_VERSION,
                SBERT_MODEL_VERSION,
                settings.ollama_model,
                PROMPT_VERSION,
                trigger_type,
            ),
        )
        conn.commit()
    log.info("run_id=%s created pipeline run limit=%s", run_id, limit)
    return run_id


def finish_run(settings: Settings, run_id: str, status: str) -> None:
    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs
            SET ended_at = %s, status = %s
            WHERE run_id = %s
            """,
            (datetime.now(UTC), status, run_id),
        )
        conn.commit()
    log.info("run_id=%s finished status=%s", run_id, status)
