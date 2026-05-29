from rico_pipeline.stages import build_holdout_queries

def test_holdout_query_never_self():
    # query text for screen i comes from screen (i+1)%n — never itself
    reps = {2: "a", 26: "b", 37: "c"}
    order = [2, 26, 37]
    qs = build_holdout_queries(order, reps)
    assert qs == [(2, "b"), (26, "c"), (37, "a")]
