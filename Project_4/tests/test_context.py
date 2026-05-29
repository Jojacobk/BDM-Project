from rico_pipeline.context import RunContext

def test_roundtrip():
    ctx = RunContext(run_id="u1", dag_run_id="d1", limit=5, git_sha="abc",
                     trigger_source="manual", clip_version="c", sbert_version="s",
                     llm_model="qwen2.5:3b", prompt_version="v1")
    d = ctx.to_dict()
    assert d["run_id"] == "u1"
    assert RunContext.from_dict(d) == ctx
