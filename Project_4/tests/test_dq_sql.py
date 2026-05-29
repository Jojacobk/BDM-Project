from rico_pipeline.observability import DQ_QUERIES


def test_dq_queries_present():
    for key in ("meta", "emb", "distinct"):
        assert key in DQ_QUERIES
        assert "%s" in DQ_QUERIES[key]  # all scoped by run_id
