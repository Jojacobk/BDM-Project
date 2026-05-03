"""Project 3 Airflow DAG: CDC plus taxi lakehouse pipeline."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email


PROJECT = "/home/jovyan/project"


def on_failure_email(context):
    """Best-effort failure notification for task errors."""
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    exec_date = context["execution_date"]
    try:
        send_email(
            to="admin@example.com",
            subject=f"[Airflow] FAILED: {dag_id}.{task_id}",
            html_content=f"""
                <p>Task <b>{task_id}</b> in DAG <b>{dag_id}</b> failed.</p>
                <p>Execution date: {exec_date}</p>
                <p>Check the Airflow UI for logs.</p>
            """,
        )
    except Exception as exc:
        print(f"Failure email could not be sent: {exc}")


def on_dag_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"[SLA MISS] DAG {dag.dag_id} exceeded its SLA. Blocking tasks: {blocking_task_list}")


default_args = {
    "owner": "joseph",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_failure_email,
    "sla": timedelta(minutes=8),
}


with DAG(
    dag_id="project3_pipeline",
    default_args=default_args,
    description="CDC + Taxi lakehouse pipeline (Project 3)",
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=on_dag_sla_miss,
    tags=["bdm", "project3", "cdc", "taxi"],
) as dag:

    connector_health = BashOperator(
        task_id="connector_health",
        bash_command=f"docker exec jupyter python {PROJECT}/scripts/health_check.py",
        retries=1,
        retry_delay=timedelta(seconds=30),
    )

    bronze_cdc = BashOperator(
        task_id="bronze_cdc",
        bash_command=f"docker exec jupyter python {PROJECT}/scripts/bronze_cdc.py",
    )

    bronze_taxi = BashOperator(
        task_id="bronze_taxi",
        bash_command=f"docker exec jupyter python {PROJECT}/scripts/bronze_taxi.py",
    )

    silver_cdc = BashOperator(
        task_id="silver_cdc",
        bash_command=f"docker exec jupyter papermill {PROJECT}/notebooks/03_silver_cdc.ipynb /tmp/out_silver_cdc.ipynb",
    )

    silver_taxi = BashOperator(
        task_id="silver_taxi",
        bash_command=f"docker exec jupyter papermill {PROJECT}/notebooks/06_silver_taxi.ipynb /tmp/out_silver_taxi.ipynb",
    )

    gold_taxi = BashOperator(
        task_id="gold_taxi",
        bash_command=f"docker exec jupyter papermill {PROJECT}/notebooks/07_gold_congestion.ipynb /tmp/out_gold_taxi.ipynb",
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=f"docker exec jupyter papermill {PROJECT}/notebooks/04_validation.ipynb /tmp/out_validation.ipynb",
        retries=0,
    )

    connector_health >> [bronze_cdc, bronze_taxi]

    bronze_cdc >> silver_cdc
    bronze_taxi >> silver_taxi >> gold_taxi

    [silver_cdc, gold_taxi] >> validate
