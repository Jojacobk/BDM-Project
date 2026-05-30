from pathlib import Path


def test_schema_contains_required_traceability_and_observability_tables():
    sql = Path("migrations/001_init.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS pipeline_runs" in sql
    assert "CREATE TABLE IF NOT EXISTS pipeline_metrics" in sql
    assert "CREATE TABLE IF NOT EXISTS audit_results" in sql
    assert "run_id" in sql
    assert "source_fingerprint" in sql


def test_schema_does_not_hide_duplicate_audit_with_unique_embedding_constraint():
    sql = Path("migrations/001_init.sql").read_text(encoding="utf-8").lower()

    assert "primary key (screen_id, model_name, model_version, embedding_kind)" not in sql
