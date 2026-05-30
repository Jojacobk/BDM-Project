SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

\c rico

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              UUID PRIMARY KEY,
    dag_run_id          TEXT NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at            TIMESTAMPTZ,
    status              TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'paused-by-audit')),
    limit_param         INTEGER NOT NULL,
    git_sha             TEXT NOT NULL,
    clip_model_version  TEXT NOT NULL,
    sbert_model_version TEXT NOT NULL,
    llm_model           TEXT NOT NULL,
    prompt_version      TEXT NOT NULL,
    trigger_type        TEXT NOT NULL DEFAULT 'unknown'
);

CREATE TABLE IF NOT EXISTS screens_metadata (
    id                  BIGSERIAL PRIMARY KEY,
    screen_id           BIGINT NOT NULL,
    run_id              UUID NOT NULL REFERENCES pipeline_runs(run_id),
    source_fingerprint  TEXT NOT NULL,
    app_package         TEXT,
    category            TEXT,
    png_path            TEXT NOT NULL,
    hierarchy_json_path TEXT NOT NULL,
    hierarchy_text      TEXT,
    extraction_payload  JSONB,
    prompt_version      TEXT,
    confidence          DOUBLE PRECISION,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_screens_metadata_screen_id
    ON screens_metadata(screen_id);
CREATE INDEX IF NOT EXISTS idx_screens_metadata_run_id
    ON screens_metadata(run_id);

CREATE TABLE IF NOT EXISTS screens_embeddings (
    id                 BIGSERIAL PRIMARY KEY,
    screen_id          BIGINT NOT NULL,
    run_id             UUID NOT NULL REFERENCES pipeline_runs(run_id),
    source_fingerprint TEXT NOT NULL,
    model_name         TEXT NOT NULL,
    model_version      TEXT NOT NULL,
    embedding_kind     TEXT NOT NULL CHECK (embedding_kind IN ('image', 'text')),
    vector             vector NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_screens_embeddings_key
    ON screens_embeddings(screen_id, model_name, model_version, embedding_kind);
CREATE INDEX IF NOT EXISTS idx_screens_embeddings_run_id
    ON screens_embeddings(run_id);

CREATE TABLE IF NOT EXISTS screens_review_queue (
    id                 BIGSERIAL PRIMARY KEY,
    screen_id          BIGINT NOT NULL,
    run_id             UUID NOT NULL REFERENCES pipeline_runs(run_id),
    source_fingerprint TEXT NOT NULL,
    reason             TEXT NOT NULL,
    raw_output         TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS screens_eval (
    id                       BIGSERIAL PRIMARY KEY,
    run_id                   UUID NOT NULL REFERENCES pipeline_runs(run_id),
    embedding_model_version  TEXT NOT NULL,
    n_queries                INTEGER NOT NULL,
    recall_at_5              DOUBLE PRECISION NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES pipeline_runs(run_id),
    audit_name  TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    details     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES pipeline_runs(run_id),
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    metric_text TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
