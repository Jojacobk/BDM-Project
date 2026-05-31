from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from rico_pipeline.dag_support import (
    audit_stage,
    embed_image_stage,
    embed_text_stage,
    eval_stage,
    extract_stage,
    finish_stage,
    ingest_stage,
    load_stage,
    parse_stage,
    start_run,
)


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
    start = PythonOperator(task_id="start", python_callable=start_run)
    ingest_task = PythonOperator(task_id="ingest", python_callable=ingest_stage)
    parse_task = PythonOperator(task_id="parse", python_callable=parse_stage)
    embed_image_task = PythonOperator(task_id="embed_image", python_callable=embed_image_stage)
    embed_text_task = PythonOperator(task_id="embed_text", python_callable=embed_text_stage)
    extract_task = PythonOperator(task_id="extract", python_callable=extract_stage)
    load_task = PythonOperator(task_id="load", python_callable=load_stage)
    audit_task = PythonOperator(task_id="audit", python_callable=audit_stage)
    eval_task = PythonOperator(task_id="eval", python_callable=eval_stage)
    finish_task = PythonOperator(
        task_id="finish",
        python_callable=finish_stage,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> ingest_task >> parse_task
    parse_task >> [embed_image_task, embed_text_task, extract_task]
    [embed_image_task, embed_text_task, extract_task] >> load_task >> audit_task >> eval_task
    eval_task >> finish_task
