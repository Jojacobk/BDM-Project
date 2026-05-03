"""
pipeline_dag.py — Project 3 Airflow DAG.

Orchestrates both data paths end-to-end:

  Path A (CDC):   health_check → bronze_cdc → silver_cdc → validation
  Path B (Taxi):  health_check → bronze_taxi → silver_taxi → gold_taxi
                                                           → gold_congestion

Full dependency chain:
  health_check
       ├── bronze_cdc  → silver_cdc ──────────────────────┐
       └── bronze_taxi → silver_taxi → gold_taxi           ├── validation
                                     → gold_congestion ───┘

Scheduling: every 15 minutes.
Rationale: supports a ~15-minute data freshness SLA. Debezium streams changes
continuously; this DAG ingests and propagates them in micro-batch windows.
15 min is appropriate for a city transportation authority dashboard that
needs near-real-time congestion data but not sub-minute latency.

Retry policy: 2 retries with 1-minute delay per task.
Failure propagation: all tasks use `trigger_rule=TriggerRule.ALL_SUCCESS`
(default), so a failed silver_cdc stops validation from running.
The health_check failure stops ALL downstream tasks.

Idempotency: every script uses trigger(availableNow=True) with Kafka
checkpoints, so re-running the DAG for the same interval produces the
same Iceberg state — no duplicate rows.

SLA: DAG-level SLA of 10 minutes. If the full run exceeds 10 min, an
sla_miss_callback fires. Individual task SLAs are set below.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email

NOTEBOOKS = "/home/jovyan/project/notebooks"


def on_failure_email(context):
    """Send an email alert when any task fails."""
    task_id   = context["task_instance"].task_id
    dag_id    = context["dag"].dag_id
    exec_date = context["execution_date"]
    try:
        send_email(
            to="your-email@example.com",
            subject=f"[Airflow] FAILED: {dag_id}.{task_id}",
            html_content=f"""
                <p>Task <b>{task_id}</b> in DAG <b>{dag_id}</b> failed.</p>
                <p>Execution date: {exec_date}</p>
                <p>Check the Airflow UI for logs.</p>
            """,
        )
    except Exception:
        pass


def on_dag_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called when the DAG exceeds its SLA of 10 minutes."""
    print(f"[SLA MISS] DAG {dag.dag_id} exceeded its SLA. Blocking tasks: {blocking_task_list}")


default_args = {
    "owner":               "joseph",
    "depends_on_past":     False,
    "start_date":          datetime(2026, 1, 1),
    "email_on_failure":    False,
    "email_on_retry":      False,
    "retries":             2,
    "retry_delay":         timedelta(minutes=1),
    "on_failure_callback": on_failure_email,
    "sla":                 timedelta(minutes=8),
}

with DAG(
    dag_id            = "project3_pipeline",
    default_args      = default_args,
    description       = "CDC + Taxi lakehouse pipeline (Project 3)",
    schedule_interval = "*/15 * * * *",
    catchup           = False,
    max_active_runs   = 1,
    sla_miss_callback = on_dag_sla_miss,
    tags              = ["bdm", "project3", "cdc", "taxi"],
) as dag:

    health_check = BashOperator(
        task_id      = "health_check",
        bash_command = "docker exec jupyter /opt/conda/bin/python /home/jovyan/project/scripts/health_check.py",
        retries      = 1,
        retry_delay  = timedelta(seconds=30),
    )

    bronze_cdc = BashOperator(
        task_id      = "bronze_cdc",
        bash_command = "docker exec -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.9-src.zip jupyter /opt/conda/bin/papermill /home/jovyan/project/notebooks/02_bronze_cdc.ipynb /tmp/out_bronze_cdc.ipynb",
    )

    bronze_taxi = BashOperator(
        task_id      = "bronze_taxi",
        bash_command = "docker exec -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.9-src.zip jupyter /opt/conda/bin/papermill /home/jovyan/project/notebooks/05_bronze_taxi.ipynb /tmp/out_bronze_taxi.ipynb",
    )

    silver_cdc = BashOperator(
        task_id      = "silver_cdc",
        bash_command = "docker exec -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.9-src.zip jupyter /opt/conda/bin/papermill /home/jovyan/project/notebooks/03_silver_cdc.ipynb /tmp/out_silver_cdc.ipynb",
    )

    silver_taxi = BashOperator(
        task_id      = "silver_taxi",
        bash_command = "docker exec -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.9-src.zip jupyter /opt/conda/bin/papermill /home/jovyan/project/notebooks/06_silver_taxi.ipynb /tmp/out_silver_taxi.ipynb",
    )

    gold_taxi = BashOperator(
        task_id      = "gold_taxi",
        bash_command = "docker exec -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.9-src.zip jupyter /opt/conda/bin/papermill /home/jovyan/project/notebooks/07_gold_taxi.ipynb /tmp/out_gold_taxi.ipynb",
    )

    gold_congestion = BashOperator(
        task_id      = "gold_congestion",
        bash_command = "docker exec -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.9-src.zip jupyter /opt/conda/bin/papermill /home/jovyan/project/notebooks/08_gold_congestion.ipynb /tmp/out_gold_congestion.ipynb",
    )

    validation = BashOperator(
        task_id      = "validation",
        bash_command = "docker exec -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.9-src.zip jupyter /opt/conda/bin/papermill /home/jovyan/project/notebooks/04_validation.ipynb /tmp/out_validation.ipynb",
        retries      = 0,
    )

    health_check >> [bronze_cdc, bronze_taxi]

    bronze_cdc  >> silver_cdc
    bronze_taxi >> silver_taxi

    silver_taxi >> [gold_taxi, gold_congestion]

    [silver_cdc, gold_taxi, gold_congestion] >> validation
