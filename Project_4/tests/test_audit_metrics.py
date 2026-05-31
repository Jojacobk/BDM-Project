from pathlib import Path


def test_successful_audit_returns_task_health_metrics():
    audit_src = Path("src/rico_pipeline/audit.py").read_text(encoding="utf-8")
    support_src = Path("src/rico_pipeline/dag_support.py").read_text(encoding="utf-8")

    assert '"rows_in": metadata_count + embedding_count' in audit_src
    assert '"rows_out": 1' in audit_src
    assert '"seconds": time.perf_counter() - started' in audit_src
    assert 'METRIC_TASK_IDS = TASK_IDS + ["audit"]' in support_src
    assert 'record_metric(settings, run_id, "run.status", metric_text=status)' in support_src
