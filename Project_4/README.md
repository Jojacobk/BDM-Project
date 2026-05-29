# RICO Production Pipeline

A production Airflow DAG that ingests RICO screen data from HuggingFace, embeds each screen with CLIP (image) and SBERT (text), runs an Ollama LLM extractor, loads everything into Postgres+pgvector, runs a duplicate-detection audit, and evaluates recall@k — with every run fully traceable via `run_id`, audited in `audit_results`, and observable through `pipeline_metrics`. Slack notifications fire on run start, audit failure, and completion. The pipeline is idempotent: re-triggering with the same screens inserts no duplicates (guarded INSERTs), and uniqueness is enforced at the audit layer (surrogate PKs + the `duplicate_detection` audit raises `AuditFailed` and halts the DAG before `eval` if duplicates are found).

---

## Architecture

### Task graph

```
start_run → ingest → parse → [embed_image, embed_text, extract] → load → audit → eval → finalize_run
```

The three parallel branches (`embed_image`, `embed_text`, `extract`) fan out after `parse` and rejoin at `load`. `finalize_run` runs with `trigger_rule="all_done"` so it always fires, even if `audit` raises.

### Layout

```
dags/
  rico_pipeline_dag.py    # thin orchestration only — no business logic
rico_pipeline/
  config.py               # connection defaults, model version constants
  stages.py               # ingest, embed_image, embed_text, extract, load, eval_recall
  audit.py                # duplicate-detection circuit breaker
  observability.py        # DQ queries, metric persistence, summary_line
  dag_support.py          # finalize() called by finalize_run task
  traceability.py         # start_run_sql / finish_run_sql / git SHA
  context.py              # RunContext dataclass passed between tasks via XCom
  stores.py               # pg_connect, MinIO client
  slack.py                # Slack webhook helpers
migrations/
  001_init.sql            # pgvector extension + base tables
  002_pipeline.sql        # pipeline_runs, audit_results, pipeline_metrics, run_id/source_fingerprint columns
agent/
  agent.py                # ChatOps agent (Slack Socket Mode → Ollama intent → Airflow REST)
dags/ and rico_pipeline/ are mounted into the Airflow containers at runtime.
```

### Runtime stack

| Component | Image | Port |
|-----------|-------|------|
| Airflow (LocalExecutor) | Custom `Dockerfile.airflow` | :8080 (UI) |
| Postgres 16 + pgvector 0.8 | `pgvector/pgvector:0.8.0-pg16` | :5432 |
| MinIO | `minio/minio:RELEASE.2024-12-13T22-19-12Z` | :9000 (S3 API), :9001 (console) |
| Ollama | `ollama/ollama:0.5.4` | :11434 |

Airflow uses `LocalExecutor` backed by the same Postgres instance (database `airflow`; the pipeline data lives in database `rico`).

**Idempotency and uniqueness.** Every row in `screens_metadata`, `screens_embeddings`, and `screens_review_queue` carries a `run_id` (UUID) and a `source_fingerprint` (SHA-256 of the raw bytes). Surrogate BIGSERIAL PKs replace the original natural-key constraints, so duplicate natural keys trigger the `duplicate_detection` audit rather than a hard DB error.

---

## Prerequisites

- **Docker Desktop** (enable WSL 2 backend if running on Windows + WSL).
- ~3 GB free disk for model weights and the HuggingFace dataset shard.
- No local Python install needed for the pipeline itself; Python 3.11+ is needed only if you want to run the bonus agent or the unit tests locally.

---

## Quickstart

```bash
# 1. Configure environment (only the Slack webhook is required; everything else has defaults)
cp .env.example .env
# Edit .env and set SLACK_WEBHOOK_URL to your incoming webhook URL.
# If you don't have a Slack workspace, leave the value empty — the pipeline still runs.

# 2. Start Postgres, MinIO, and Ollama
make up

# 3. Pull the LLM model into Ollama (one-time, ~1.9 GB)
make pull-models

# 4. Build and start Airflow (init + scheduler + webserver)
make up-airflow

# 5. Open the Airflow UI
#    http://localhost:8080  (username: admin  password: admin)
#    Enable the `rico_pipeline` DAG if it shows as paused.

# 6. Trigger a run with 5 screens
make trigger LIMIT=5
```

MinIO web console is at **http://localhost:9001** (credentials: `minioadmin` / `minioadmin`). Use it to browse the `rico-raw` bucket and inspect raw screen images.

To trigger a larger run: `make trigger LIMIT=50`.

To wipe all pipeline state (pipeline_runs, audit_results, pipeline_metrics, and screen tables) without losing Docker volumes:

```bash
make reset-prod
```

---

## What each metric means

`finalize_run` writes one row per metric into `pipeline_metrics (run_id, task_id, metric_name, metric_value, labels)`. Query with:

```sql
SELECT task_id, metric_name, metric_value, labels
FROM pipeline_metrics
WHERE run_id = '<your-run-id>'
ORDER BY task_id, metric_name;
```

### Run-level metrics (`task_id IS NULL`)

| `metric_name` | Meaning |
|---|---|
| `meta_row_count` | Number of rows written to `screens_metadata` for this run. |
| `extracted_pct` | Percentage of metadata rows where `extraction_payload IS NOT NULL` (LLM returned parseable JSON). |
| `confidence_ge_05_pct` | Percentage of metadata rows where the LLM's reported confidence is ≥ 0.5. |
| `review_queue_count` | Number of rows routed to `screens_review_queue` (low-confidence or parse-failure screens). |
| `zero_norm_pct` | Percentage of embedding vectors whose L2 norm is exactly zero — a signal of a silent embedder bug. Should be 0 %. |
| `total_run_duration_seconds` | Sum of `duration` across all task instances in this DAG run. |
| `final_status` | Stored in `labels` as `{"status": "succeeded" | "failed" | "paused_by_audit"}`. Also mirrored in `pipeline_runs.status`. |

### Per-task metrics (`task_id = <task name>`)

For every task except `finalize_run` itself, two metrics are written:

| `metric_name` | Meaning |
|---|---|
| `duration_seconds` | Airflow-reported wall time for the task instance, in seconds. |
| `retries` | Number of retries beyond the first attempt (`try_number - 1`). |

The `labels` column for `duration_seconds` also carries `{"state": "success" | "failed" | ...}`.

### Embedding quality metrics (from `observability.DQ_QUERIES["emb"]`)

These are not stored as individual `pipeline_metrics` rows but appear in the Slack summary line and the Airflow log. They are computed per `(model_version, embedding_kind)` group:

| Value | Meaning |
|---|---|
| Embedding count | Number of vectors written for that `(model_version, embedding_kind)` pair this run. |
| Avg vector dimensionality | `AVG(vector_dims(vector))` — confirms the model produced the expected dimension (e.g. 512 for CLIP ViT-B/32, 384 for all-MiniLM-L6-v2). |

### Distinct-value counters (in summary line only)

| Value | Meaning |
|---|---|
| `apps=N` | Distinct `app_package` values in `screens_metadata` for this run. |
| `cats=N` | Distinct `category` values for this run. |

### Summary line format

Every run logs and Slack-posts a line in this format:

```
run=<uuid> status=<status> dur=<s>s | meta=<N> (extracted <X>%, conf>=.5 <Y>%, review=<R>) | emb image=<N>/<D>d text=<N>/<D>d zero=<Z>% | apps=<A> cats=<C>
```

---

## How to read an audit failure

The `audit` task runs `duplicate_detection` against the live `screens_embeddings` and `screens_metadata` tables. It checks for any `(screen_id, model_name, model_version, embedding_kind)` combination with more than one row (embedding duplicates), and for any `screen_id` appearing more than once in `screens_metadata` (metadata duplicates).

Results are written to `audit_results`:

```sql
SELECT run_id, audit_name, passed, details
FROM audit_results
WHERE run_id = '<your-run-id>';
```

When `passed = false`, the `details` JSONB column contains:

```json
{
  "embedding_duplicates": [
    {"screen_id": "...", "model_name": "...", "model_version": "...", "embedding_kind": "...", "count": 2}
  ],
  "metadata_duplicates": [
    {"screen_id": "...", "count": 2}
  ]
}
```

The `audit` task raises `AuditFailed`, which marks it as failed in Airflow. Because `eval` depends on `audit`, Airflow skips `eval`. `finalize_run` still runs (it uses `trigger_rule="all_done"`) and sets `pipeline_runs.status = 'paused_by_audit'`. A Slack alert fires with the run ID and a pointer to `audit_results`.

**To reproduce an audit failure:**

```bash
# 1. Run the pipeline normally first
make trigger LIMIT=5

# 2. Manually duplicate an embedding row
make corrupt

# 3. Re-trigger — audit will now detect the duplicate and halt
make trigger LIMIT=5
```

`make corrupt` executes:
```sql
INSERT INTO screens_embeddings (screen_id, model_name, model_version, embedding_kind, vector, run_id, source_fingerprint)
SELECT screen_id, model_name, model_version, embedding_kind, vector, run_id, source_fingerprint
FROM screens_embeddings LIMIT 1;
```

---

## Traceability

Every run writes a row to `pipeline_runs` before any data is processed:

```sql
SELECT run_id, dag_run_id, started_at, ended_at, status,
       limit_param, git_sha, trigger_source,
       clip_version, sbert_version, llm_model, prompt_version
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 5;
```

The `run_id` UUID propagates into every data table via the `RunContext` passed through XCom. To see everything written by one run:

```sql
-- All metadata rows for a run
SELECT * FROM screens_metadata WHERE run_id = '<run-id>';

-- All embedding vectors for a run
SELECT screen_id, model_name, model_version, embedding_kind, source_fingerprint
FROM screens_embeddings WHERE run_id = '<run-id>';

-- Review-queue entries for a run
SELECT * FROM screens_review_queue WHERE run_id = '<run-id>';

-- Audit result for a run
SELECT * FROM audit_results WHERE run_id = '<run-id>';

-- All metrics for a run
SELECT task_id, metric_name, metric_value, labels
FROM pipeline_metrics WHERE run_id = '<run-id>';
```

**`source_fingerprint`** is a SHA-256 hash of the raw bytes fetched from the source (HuggingFace / MinIO). It answers: "Did the model see exactly these bytes?" If the same screen is re-ingested from a different dataset shard or a different fetch, the fingerprint will differ and the two rows are distinguishable even if `screen_id` is the same.

**`git_sha`** in `pipeline_runs` records the exact commit that ran the pipeline, so you can reproduce any run by checking out that commit.

---

## Running the bonus agent

The ChatOps agent listens on Slack via Socket Mode, uses Ollama to parse the user's intent (extracting a `limit` number), then POSTs to the Airflow REST API to trigger the DAG.

### Setup

1. Create a Slack app at https://api.slack.com/apps.
2. Under **OAuth & Permissions**, add the `app_mentions:read` and `chat:write` bot scopes. Install the app to your workspace and copy the **Bot User OAuth Token** (`xoxb-...`).
3. Under **Socket Mode**, enable it and generate an **App-Level Token** (`xapp-...`) with the `connections:write` scope.
4. Invite the bot to a channel: `/invite @<your-bot-name>`.

### Configuration

Add the following to your `.env` (alongside `SLACK_WEBHOOK_URL`):

```
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
AIRFLOW_API_URL=http://localhost:8080
AIRFLOW_API_USER=admin
AIRFLOW_API_PASSWORD=admin
```

### Running

With the pipeline stack already running (`make up` + `make up-airflow`), start the agent in a separate terminal:

```bash
make agent
```

This runs `python agent/agent.py`. The agent connects to Slack via Socket Mode and is ready to receive mentions.

### Usage

In the Slack channel where the bot is installed, mention it with a natural-language request:

```
@DataBot backfill 20 screens
```

The agent sends the message to Ollama (`qwen2.5:3b`) with a structured prompt that extracts `{"intent": "run_pipeline", "limit": 20}`. It then POSTs to `POST /api/v1/dags/rico_pipeline/dagRuns` with `conf={"limit": 20, "trigger_source": "agent"}` and replies in the thread with the `dag_run_id`.

If no number is present in the message, the agent defaults to `limit=5`.

---

## Running tests

Install test dependencies into a local virtual environment:

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Run the unit tests (no Docker required):

```bash
.venv/bin/python -m pytest
```

The unit tests cover `audit.py`, `observability.py`, `dag_support.py` logic, and the agent's `parse_limit_from_llm` helper.

**Note:** `tests/test_dag_import.py` imports `rico_pipeline_dag.py` which requires the Airflow package. Run it inside the Airflow Docker image or in an environment where Airflow is installed:

```bash
docker compose exec airflow-scheduler python -m pytest /opt/airflow/dags/../tests/test_dag_import.py
```

---

## Service endpoints summary

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO S3 API | http://localhost:9000 | minioadmin / minioadmin (bucket: `rico-raw`) |
| Postgres | localhost:5432/rico | rico / rico |
| Ollama API | http://localhost:11434 | — (model: `qwen2.5:3b`) |
