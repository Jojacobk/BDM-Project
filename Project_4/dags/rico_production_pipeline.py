from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from rico_pipeline.audit import duplicate_audit
from rico_pipeline.config import load_settings
from rico_pipeline.errors import AuditError
from rico_pipeline.metrics import collect_quality_metrics, record_task_metrics
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


def _limit_from_context(context: dict) -> int:
    settings = load_settings()
    conf = getattr(context.get("dag_run"), "conf", None) or {}
    return int(conf.get("limit", conf.get("LIMIT", settings.default_limit)))


def _start(**context) -> str:
    settings = load_settings()
    limit = _limit_from_context(context)
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


def _ingest(**context):
    settings = load_settings()
    run_id = context["ti"].xcom_pull(task_ids="start")
    return ingest(settings, run_id, _limit_from_context(context))


def _parse(**context):
    settings = load_settings()
    run_id = context["ti"].xcom_pull(task_ids="start")
    return parse(settings, run_id)


def _embed_image(**context):
    settings = load_settings()
    run_id = context["ti"].xcom_pull(task_ids="start")
    return embed_image(settings, run_id)


def _embed_text(**context):
    settings = load_settings()
    run_id = context["ti"].xcom_pull(task_ids="start")
    return embed_text(settings, run_id)


def _extract(**context):
    settings = load_settings()
    run_id = context["ti"].xcom_pull(task_ids="start")
    return extract(settings, run_id)


def _load(**context):
    settings = load_settings()
    run_id = context["ti"].xcom_pull(task_ids="start")
    return load(settings, run_id)


def _eval(**context):
    settings = load_settings()
    run_id = context["ti"].xcom_pull(task_ids="start")
    return eval_recall(settings, run_id)


def _audit(**context):
    settings = load_settings()
    run_id = context["ti"].xcom_pull(task_ids="start")
    return duplicate_audit(settings, run_id)


def _finish(**context) -> None:
    settings = load_settings()
    ti = context["ti"]
    dag_run = context["dag_run"]
    run_id = ti.xcom_pull(task_ids="start")
    task_ids = [
        "ingest",
        "parse",
        "embed_image",
        "embed_text",
        "extract",
        "load",
        "eval",
    ]
    for task_id in task_ids:
        result = ti.xcom_pull(task_ids=task_id)
        task_instance = dag_run.get_task_instance(task_id)
        # Airflow stores try_number as the next try after a task finishes.
        retries = int(getattr(task_instance, "try_number", 1) or 1) - 2
        record_task_metrics(settings, run_id, task_id, result, max(retries, 0))

    summary = collect_quality_metrics(settings, run_id)
    current_status = _current_run_status(settings, run_id)
    failed_states = {"failed", "upstream_failed"}
    task_states = {
        task_id: getattr(dag_run.get_task_instance(task_id), "state", None)
        for task_id in task_ids + ["audit"]
    }
    has_failed_task = any(state in failed_states for state in task_states.values())
    if current_status == "paused-by-audit":
        status = "paused-by-audit"
    elif has_failed_task:
        status = "failed"
    else:
        status = "succeeded"
    finish_run(settings, run_id, status)
    post_slack(
        settings,
        f"RICO pipeline finished run_id={run_id} status={status} summary={summary}",
    )
    if status == "paused-by-audit":
        raise AuditError(f"run_id={run_id} paused by audit")
    if status == "failed":
        raise RuntimeError(f"run_id={run_id} failed; task_states={task_states}")


def _current_run_status(settings, run_id: str) -> str:
    from rico_pipeline.db import connect

    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM pipeline_runs WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
    return row[0] if row else "failed"


default_args = {
    "owner": "project-4",
    "retries": 0,
}

with DAG(
    dag_id="rico_production_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["project-4", "rico", "pgvector"],
) as dag:
    start = PythonOperator(task_id="start", python_callable=_start)
    ingest_task = PythonOperator(task_id="ingest", python_callable=_ingest)
    parse_task = PythonOperator(task_id="parse", python_callable=_parse)
    embed_image_task = PythonOperator(task_id="embed_image", python_callable=_embed_image)
    embed_text_task = PythonOperator(task_id="embed_text", python_callable=_embed_text)
    extract_task = PythonOperator(task_id="extract", python_callable=_extract)
    load_task = PythonOperator(task_id="load", python_callable=_load)
    audit_task = PythonOperator(task_id="audit", python_callable=_audit)
    eval_task = PythonOperator(task_id="eval", python_callable=_eval)
    finish_task = PythonOperator(
        task_id="finish",
        python_callable=_finish,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> ingest_task >> parse_task
    parse_task >> [embed_image_task, embed_text_task, extract_task]
    [embed_image_task, embed_text_task, extract_task] >> load_task >> audit_task >> eval_task
    eval_task >> finish_task
