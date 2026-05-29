import importlib

def test_defaults(monkeypatch):
    for k in ["POSTGRES_DSN", "MINIO_ENDPOINT", "OLLAMA_URL", "MINIO_BUCKET"]:
        monkeypatch.delenv(k, raising=False)
    import rico_pipeline.config as cfg
    importlib.reload(cfg)
    assert cfg.MINIO_BUCKET == "rico-raw"
    assert cfg.CLIP_MODEL_VERSION == "open-clip-ViT-B-32-laion2b-s34b-b79k"
    assert cfg.SBERT_MODEL_VERSION == "sentence-transformers/all-MiniLM-L6-v2"
    assert cfg.PROMPT_VERSION == "v1"

def test_env_override(monkeypatch):
    monkeypatch.setenv("MINIO_BUCKET", "other-bucket")
    import rico_pipeline.config as cfg
    importlib.reload(cfg)
    assert cfg.MINIO_BUCKET == "other-bucket"
