import os
from airflow.models import DagBag

def test_dag_loads_and_has_expected_tasks():
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    bag = DagBag(dag_folder="dags", include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    dag = bag.get_dag("rico_pipeline")
    assert dag is not None
    expected = {"start_run", "ingest", "parse", "embed_image", "embed_text",
                "extract", "load", "audit", "eval", "finalize_run"}
    assert expected.issubset(set(dag.task_ids))

def test_middle_three_run_in_parallel():
    bag = DagBag(dag_folder="dags", include_examples=False)
    dag = bag.get_dag("rico_pipeline")
    parse_downstream = dag.get_task("parse").downstream_task_ids
    assert {"embed_image", "embed_text", "extract"}.issubset(parse_downstream)
