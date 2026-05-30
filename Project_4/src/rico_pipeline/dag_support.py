from __future__ import annotations

from rico_pipeline.audit import duplicate_audit
from rico_pipeline.config import load_settings
from rico_pipeline.errors import AuditError
from rico_pipeline.metrics import collect_quality_metrics, record_metric, record_task_metrics
from rico_pipeline.run_state import create_run, finish_run
from rico_pipeline.slack import post_slack
from rico_pipeline.stages import (
    embed_image,
    embed_text,
    eval_recall,
    extract,
    ingest,
    load,
    parse,
)


TASK_IDS = [
    "ingest",
    "parse",
    "embed_image",
    "embed_text",
    "extract",
    "load",
    "eval",
]


def limit_from_context(context: dict) -> int:
    settings = load_settings()
    conf = getattr(context.get("dag_run"), "conf", None) or {}
    return int(conf.get("limit", conf.get("LIMIT", settings.default_limit)))


def start_run(**context) -> str:
    settings = load_settings()
    limit = limit_from_context(context)
    dag_run = context["dag_run"]
    trigger_type = "scheduled" if getattr(dag_run, "run_type", "") == "scheduled" else "manual"
    run_id = create_run(
        settings,
        dag_run_id=dag_run.run_id,
        limit=limit,
        trigger_type=trigger_type,
    )
    post_slack(
        settings,
        f"RICO pipeline started run_id={run_id} LIMIT={limit} trigger={trigger_type}",
    )
    return run_id


def ingest_stage(**context):
    settings = load_settings()
    return ingest(settings, _run_id(context), limit_from_context(context))


def parse_stage(**context):
    settings = load_settings()
    return parse(settings, _run_id(context))


def embed_image_stage(**context):
    settings = load_settings()
    return embed_image(settings, _run_id(context))


def embed_text_stage(**context):
    settings = load_settings()
    return embed_text(settings, _run_id(context))


def extract_stage(**context):
    settings = load_settings()
    return extract(settings, _run_id(context))


def load_stage(**context):
    settings = load_settings()
    return load(settings, _run_id(context))


def audit_stage(**context):
    settings = load_settings()
    task_instance = context["task_instance"]
    return duplicate_audit(settings, _run_id(context), airflow_log_url=task_instance.log_url)


def eval_stage(**context):
    settings = load_settings()
    return eval_recall(settings, _run_id(context))


def finish_stage(**context) -> None:
    settings = load_settings()
    ti = context["ti"]
    dag_run = context["dag_run"]
    run_id = _run_id(context)

    for task_id in TASK_IDS:
        result = ti.xcom_pull(task_ids=task_id)
        task_instance = dag_run.get_task_instance(task_id)
        record_task_metrics(settings, run_id, task_id, result, _retries_used(task_instance))

    current_status = _current_run_status(settings, run_id)
    task_states = {
        task_id: getattr(dag_run.get_task_instance(task_id), "state", None)
        for task_id in TASK_IDS + ["audit"]
    }
    if current_status == "paused-by-audit":
        status = "paused-by-audit"
    elif any(state in {"failed", "upstream_failed"} for state in task_states.values()):
        status = "failed"
    else:
        status = "succeeded"

    duration_seconds = finish_run(settings, run_id, status)
    record_metric(settings, run_id, "run.duration_seconds", duration_seconds)
    summary = collect_quality_metrics(settings, run_id)
    post_slack(
        settings,
        "RICO pipeline finished "
        f"run_id={run_id} status={status} duration_seconds={duration_seconds:.1f} "
        f"summary={summary}",
    )
    if status == "paused-by-audit":
        raise AuditError(f"run_id={run_id} paused by audit")
    if status == "failed":
        raise RuntimeError(f"run_id={run_id} failed; task_states={task_states}")


def _run_id(context: dict) -> str:
    return context["ti"].xcom_pull(task_ids="start")


def _current_run_status(settings, run_id: str) -> str:
    from rico_pipeline.db import connect

    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM pipeline_runs WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
    return row[0] if row else "failed"


def _retries_used(task_instance) -> int:
    if task_instance is None:
        return 0
    try_number = int(getattr(task_instance, "try_number", 1) or 1)
    return max(try_number - 1, 0)
