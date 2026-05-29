from rico_pipeline.audit import evaluate_audit

class FakeCur:
    def __init__(self, emb_dupes, meta_dupes):
        self._emb, self._meta, self._q = emb_dupes, meta_dupes, None
    def execute(self, sql, params=None):
        self._q = "embeddings" if "screens_embeddings" in sql else "metadata"
    def fetchall(self):
        return self._emb if self._q == "embeddings" else self._meta

def test_audit_passes_when_no_dupes():
    passed, details = evaluate_audit(FakeCur([], []))
    assert passed is True and details["embedding_duplicates"] == []

def test_audit_fails_on_embedding_dupe():
    dupe = [(2, "open-clip", "v", "image", 2)]
    passed, details = evaluate_audit(FakeCur(dupe, []))
    assert passed is False
    assert details["embedding_duplicates"][0]["count"] == 2
