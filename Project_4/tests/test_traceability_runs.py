from rico_pipeline.context import RunContext
from rico_pipeline.traceability import start_run_sql, finish_run_sql

CTX = RunContext(run_id="u1", dag_run_id="d1", limit=5, git_sha="abc",
                 trigger_source="manual", clip_version="c", sbert_version="s",
                 llm_model="qwen2.5:3b", prompt_version="v1")

def test_start_run_sql_params():
    sql, params = start_run_sql(CTX)
    assert "INSERT INTO pipeline_runs" in sql
    assert params[0] == "u1" and "running" in params

def test_finish_run_sql_params():
    sql, params = finish_run_sql("u1", "succeeded")
    assert "UPDATE pipeline_runs" in sql
    assert params == ("succeeded", "u1")
