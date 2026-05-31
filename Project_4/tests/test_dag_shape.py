from pathlib import Path


def test_dag_has_required_order_and_parallel_middle_tasks():
    src = Path("dags/rico_production_pipeline.py").read_text(encoding="utf-8")

    assert 'task_id="ingest"' in src
    assert 'task_id="parse"' in src
    assert 'task_id="embed_image"' in src
    assert 'task_id="embed_text"' in src
    assert 'task_id="extract"' in src
    assert 'task_id="load"' in src
    assert 'task_id="audit"' in src
    assert 'task_id="eval"' in src
    assert "parse_task >> [embed_image_task, embed_text_task, extract_task]" in src
    assert "[embed_image_task, embed_text_task, extract_task] >> load_task >> audit_task >> eval_task" in src
