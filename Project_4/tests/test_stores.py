from rico_pipeline.stores import guarded_insert_sql

def test_guarded_insert_builds_not_exists():
    sql = guarded_insert_sql(
        table="screens_embeddings",
        columns=["screen_id", "model_name", "model_version", "embedding_kind",
                 "vector", "run_id", "source_fingerprint"],
        conflict_columns=["screen_id", "model_name", "model_version", "embedding_kind"],
    )
    assert "INSERT INTO screens_embeddings" in sql
    assert "WHERE NOT EXISTS" in sql
    # 7 column placeholders in the SELECT + 4 conflict-key placeholders in WHERE.
    # Caller passes params = column_values + conflict_key_values (11 total).
    assert sql.count("%s") == 11
    select_line = next(l for l in sql.splitlines() if l.strip().startswith("SELECT %s"))
    assert select_line.count("%s") == 7
    assert "screen_id = %s" in sql and "embedding_kind = %s" in sql
