-- Production additions: traceability, audit, metrics, and run_id/source_fingerprint.
-- Runs alphabetically after 001 on first init of the empty volume.
\c rico

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          UUID PRIMARY KEY,
    dag_run_id      TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ,
    status          TEXT NOT NULL,                 -- running|succeeded|failed|paused_by_audit
    limit_param     INTEGER NOT NULL,
    git_sha         TEXT NOT NULL,
    trigger_source  TEXT,                          -- manual|scheduled|agent
    clip_version    TEXT NOT NULL,
    sbert_version   TEXT NOT NULL,
    llm_model       TEXT NOT NULL,
    prompt_version  TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES pipeline_runs(run_id),
    audit_name  TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    details     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id           BIGSERIAL PRIMARY KEY,
    run_id       UUID NOT NULL REFERENCES pipeline_runs(run_id),
    task_id      TEXT,
    metric_name  TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    labels       JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, task_id, metric_name)
);

-- screens_metadata: surrogate PK, run_id, source_fingerprint
ALTER TABLE screens_metadata DROP CONSTRAINT screens_metadata_pkey;
ALTER TABLE screens_metadata ADD COLUMN id BIGSERIAL PRIMARY KEY;
ALTER TABLE screens_metadata ADD COLUMN run_id UUID NOT NULL REFERENCES pipeline_runs(run_id);
ALTER TABLE screens_metadata ADD COLUMN source_fingerprint TEXT NOT NULL;

-- screens_embeddings: surrogate PK, run_id, source_fingerprint
ALTER TABLE screens_embeddings DROP CONSTRAINT screens_embeddings_pkey;
ALTER TABLE screens_embeddings ADD COLUMN id BIGSERIAL PRIMARY KEY;
ALTER TABLE screens_embeddings ADD COLUMN run_id UUID NOT NULL REFERENCES pipeline_runs(run_id);
ALTER TABLE screens_embeddings ADD COLUMN source_fingerprint TEXT NOT NULL;

-- screens_review_queue: run_id, source_fingerprint
ALTER TABLE screens_review_queue ADD COLUMN run_id UUID NOT NULL REFERENCES pipeline_runs(run_id);
ALTER TABLE screens_review_queue ADD COLUMN source_fingerprint TEXT NOT NULL;
