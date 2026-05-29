"""Duplicate-detection audit — the pipeline's circuit breaker."""
from __future__ import annotations

EMB_DUP_SQL = """
    SELECT screen_id, model_name, model_version, embedding_kind, COUNT(*) AS n
    FROM screens_embeddings
    GROUP BY screen_id, model_name, model_version, embedding_kind
    HAVING COUNT(*) > 1
"""
META_DUP_SQL = """
    SELECT screen_id, COUNT(*) AS n
    FROM screens_metadata
    GROUP BY screen_id
    HAVING COUNT(*) > 1
"""


def evaluate_audit(cur):
    """Run both duplicate checks on an open cursor. Returns (passed, details)."""
    cur.execute(EMB_DUP_SQL)
    emb = [
        {"screen_id": r[0], "model_name": r[1], "model_version": r[2],
         "embedding_kind": r[3], "count": r[4]}
        for r in cur.fetchall()
    ]
    cur.execute(META_DUP_SQL)
    meta = [{"screen_id": r[0], "count": r[1]} for r in cur.fetchall()]
    passed = not emb and not meta
    return passed, {"embedding_duplicates": emb, "metadata_duplicates": meta}
