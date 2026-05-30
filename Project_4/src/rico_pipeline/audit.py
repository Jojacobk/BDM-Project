from __future__ import annotations

import json
import logging

from rico_pipeline.config import Settings
from rico_pipeline.db import connect
from rico_pipeline.errors import AuditError
from rico_pipeline.run_state import finish_run
from rico_pipeline.slack import post_slack

log = logging.getLogger(__name__)


def duplicate_audit(
    settings: Settings,
    run_id: str,
    airflow_log_url: str | None = None,
) -> dict[str, int]:
    details = {"metadata_duplicates": [], "embedding_duplicates": []}
    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT screen_id, count(*)::int
            FROM screens_metadata
            WHERE run_id = %s
            GROUP BY screen_id
            HAVING count(*) > 1
            ORDER BY screen_id
            """,
            (run_id,),
        )
        details["metadata_duplicates"] = [
            {"screen_id": row[0], "count": row[1]} for row in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT screen_id, model_name, model_version, embedding_kind, count(*)::int
            FROM screens_embeddings
            WHERE run_id = %s
            GROUP BY screen_id, model_name, model_version, embedding_kind
            HAVING count(*) > 1
            ORDER BY screen_id, model_name, model_version, embedding_kind
            """,
            (run_id,),
        )
        details["embedding_duplicates"] = [
            {
                "screen_id": row[0],
                "model_name": row[1],
                "model_version": row[2],
                "embedding_kind": row[3],
                "count": row[4],
            }
            for row in cur.fetchall()
        ]

        passed = not details["metadata_duplicates"] and not details["embedding_duplicates"]
        cur.execute(
            """
            INSERT INTO audit_results (run_id, audit_name, passed, details)
            VALUES (%s, 'duplicate_detection', %s, %s::jsonb)
            """,
            (run_id, passed, json.dumps(details)),
        )
        conn.commit()

    if passed:
        log.info("run_id=%s duplicate audit passed", run_id)
        return {"metadata_duplicates": 0, "embedding_duplicates": 0}

    log.error("run_id=%s duplicate audit failed: %s", run_id, details)
    finish_run(settings, run_id, "paused-by-audit")
    log_part = f" airflow_log={airflow_log_url}" if airflow_log_url else ""
    post_slack(
        settings,
        "RICO pipeline audit failed "
        f"run_id={run_id}{log_part} duplicate_keys={json.dumps(details, sort_keys=True)}",
    )
    raise AuditError(f"duplicate audit failed for run_id={run_id}: {details}")
