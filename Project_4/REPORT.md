# Project 4 Report - Production RICO Pipeline

## Objective

The Week 7 lab proved the RICO multimodal pipeline in a notebook. This project turns the same primitives into a production-style Airflow DAG that can be rerun, traced, audited, observed, and operated.

The implemented DAG is:

```text
start -> ingest -> parse -> [embed_image, embed_text, extract] -> load -> audit -> eval -> finish
```

`start` and `finish` are operational wrappers for run tracking, metrics, and notifications. The required project stages remain `ingest -> parse -> [embed_image, embed_text, extract] -> load -> audit -> eval`.

## Concepts Used

- Airflow DAG orchestration: Airflow owns scheduling, task dependencies, retries, task logs, and the visible DAG graph. The DAG file is thin and calls functions from `src/rico_pipeline`.
- MinIO object storage: PNG bytes and hierarchy JSON are stored as blob objects, matching the Week 7 notebook pattern.
- Postgres + pgvector: metadata, embeddings, run records, audit records, metrics, and eval rows are persisted in SQL; vectors use pgvector.
- CLIP image embeddings: screenshots are encoded with `open-clip ViT-B-32 laion2b_s34b_b79k` into 512-dimensional normalized vectors.
- SBERT text embeddings: parsed hierarchy text is encoded with `sentence-transformers/all-MiniLM-L6-v2` into 384-dimensional normalized vectors.
- Ollama LLM extraction: `qwen2.5:3b` converts hierarchy text into structured JSON using the versioned `v1` prompt.
- Idempotency: reruns refresh existing destination rows by natural key instead of blindly appending duplicates.
- Traceability: every destination row has `run_id` and `source_fingerprint`, and every run records model versions, prompt version, Airflow run ID, limit, and git SHA.
- Load validation: the `load` task is a destination completeness gate. It verifies metadata, parsed hierarchy text, image embeddings, and text embeddings before audit and eval.
- Circuit breaker audit: duplicate detection is its own visible task. When it fails, eval is skipped and the run is marked `paused-by-audit`.
- Observability: `pipeline_metrics` stores task health and data quality metrics so the pipeline can be evaluated with SQL.
- Slack notifications: start, audit failure, and finish notifications are attempted through `SLACK_WEBHOOK_URL`; notification failure never fails the data pipeline. Audit-failure notifications include duplicate keys and the Airflow audit task log URL when Airflow provides it.

## Definition Of Done Evidence

Evidence collected on May 30, 2026 from the local Docker stack:

- DAG imported successfully in Airflow as `rico_production_pipeline`.
- Final clean run: `final_verify__20260530T172708__limit_20`
- Final clean `pipeline_runs.status`: `succeeded`
- Final clean `pipeline_runs.limit_param`: `20`
- Clean destination counts after repeated runs:
  - `screens_metadata`: 20 rows, 20 traceable
  - `screens_embeddings`: 40 rows, 40 traceable
  - `screens_review_queue`: current-state table keyed by `screen_id`
  - Final run review queue rows: 1
- Object storage proof:
  - MinIO bucket `rico-raw`: 40 objects, matching 20 PNG files plus 20 hierarchy JSON files.
- Idempotency proof:
  - Multiple successful `LIMIT=20` runs were executed.
  - After rerun, destination counts remained 20 metadata rows and 40 embedding rows.
  - Current duplicate natural-key checks returned 0 metadata duplicates and 0 embedding duplicates.
- Audit pass proof:
  - Final `audit_results`: `passed = true`
  - Details: no metadata duplicates and no embedding duplicates.
- Eval proof:
  - Final clean run wrote one `screens_eval` row.
  - `n_queries = 20`
  - `recall_at_5 = 1`
- Metrics proof:
  - `pipeline_metrics` includes task duration, total run duration, rows in/out, retries, data quality percentages, vector dimensions, zero-vector percentages, and a readable `run.summary`.
  - Final summary: `metadata_rows=20 extracted=95.0% confident=95.0% review_queue=5.0% apps=6 categories=6`
  - CLIP image vectors: 20 rows, average dimensionality 512, 0% zero vectors.
  - SBERT text vectors: 20 rows, average dimensionality 384, 0% zero vectors.
  - Extraction produced 19 structured payloads and 1 review queue row, so the 95% extraction metric is honest for the final run.

## Audit Failure Evidence

Manual corruption inserted one duplicate text embedding for screen 2.

The audit-halted run was:

```text
manual__2026-05-29T10:38:42+00:00
```

Observed behavior:

- Airflow DAG state: `failed`
- `pipeline_runs.status`: `paused-by-audit`
- `audit` task state: `failed`
- `eval` task state: `upstream_failed`
- `screens_eval` rows for that failed run: `0`
- `audit_results.passed`: `false`
- Duplicate key logged in `audit_results.details`:

```json
{
  "screen_id": 2,
  "model_name": "sentence-transformers",
  "model_version": "sentence-transformers/all-MiniLM-L6-v2",
  "embedding_kind": "text",
  "count": 2
}
```

This proves the audit is a real circuit breaker, not a warning-only check.

## Post-Feedback Verification Evidence

After reviewing the project against the instructor-facing requirements, the following fixes were made and verified:

- `screens_review_queue` is now idempotent by `screen_id`.
  - Failed extraction updates the existing review queue row instead of blindly appending.
  - Successful extraction deletes any old review queue row for that screen.
  - Migration adds a unique current-state index on `screens_review_queue(screen_id)`.
- `pipeline_runs.git_sha` now records the running code version through `GIT_SHA`.
- `pipeline_metrics` now stores `run.duration_seconds`.
- Audit-failure Slack messages now include the Airflow audit task log URL when Airflow provides it.
- Operational start/finish/status logic was moved from the DAG file into `src/rico_pipeline/dag_support.py`, keeping the DAG focused on orchestration.

Post-fix validation run:

```text
post_fix_verify__20260530T202955__limit_5
```

Observed proof:

- `pipeline_runs.status = 'succeeded'`
- `pipeline_runs.limit_param = 5`
- `pipeline_runs.git_sha` was populated with the deployed short Git SHA, not `unknown`
- `run.duration_seconds = 433.55349`
- `audit_results.passed = true`
- `screens_eval.recall_at_5 = 1`
- Review queue duplicate screen IDs: `0`
- Metadata duplicate keys: `0`
- Embedding duplicate keys: `0`

## How To Reproduce

Start the stack:

```bash
cd Project_4
make up
```

Trigger a clean development run:

```bash
make trigger
```

Check Airflow runs:

```bash
docker compose exec -T airflow-scheduler airflow dags list-runs --dag-id rico_production_pipeline --output table
```

Check SQL evidence:

```bash
docker compose exec -T postgres psql -U rico -d rico
```

Useful SQL:

```sql
SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 5;
SELECT * FROM audit_results ORDER BY created_at DESC LIMIT 5;
SELECT metric_name, metric_value, metric_text
FROM pipeline_metrics
ORDER BY created_at DESC
LIMIT 20;
```

To reproduce the audit failure:

```sql
INSERT INTO screens_embeddings (
    screen_id, run_id, source_fingerprint, model_name,
    model_version, embedding_kind, vector
)
SELECT screen_id, run_id, source_fingerprint, model_name,
       model_version, embedding_kind, vector
FROM screens_embeddings
WHERE embedding_kind = 'text'
LIMIT 1;
```

Then trigger the DAG again. The run should fail at `audit`, skip `eval`, and write a failed `audit_results` row.

## What To Show In The Submission

Use this checklist when preparing screenshots and the bonus video. Each item maps directly to something the instructor can evaluate.

### Main Project Screenshots

1. Airflow DAG graph
   - Show: `rico_production_pipeline`
   - Why: proves the required pipeline order exists.
   - Point out: `ingest -> parse -> [embed_image, embed_text, extract] -> load -> audit -> eval`

2. Airflow successful run
   - Show: run `final_verify__20260530T172708__limit_20`
   - Why: proves the full DAG completed.
   - Point out: all required tasks are green, including `audit` and `eval`.

3. `pipeline_runs` SQL output
   - Show:

     ```sql
     SELECT run_id, dag_run_id, status, limit_param, git_sha, started_at, ended_at
     FROM pipeline_runs
     ORDER BY started_at DESC
     LIMIT 5;
     ```

   - Why: proves run tracking, final status, and `LIMIT`.
   - Point out: `status = succeeded`, `limit_param = 20`, and `git_sha` is the commit that ran.

4. Destination row counts and traceability
   - Show:

     ```sql
     SELECT 'metadata_rows' AS check_name, count(*) AS rows,
            count(*) FILTER (WHERE run_id IS NOT NULL AND source_fingerprint IS NOT NULL) AS traceable
     FROM screens_metadata
     UNION ALL
     SELECT 'embedding_rows', count(*),
            count(*) FILTER (WHERE run_id IS NOT NULL AND source_fingerprint IS NOT NULL)
     FROM screens_embeddings;
     ```

   - Why: proves rows are traceable by `run_id` and `source_fingerprint`.
   - Point out: 20 metadata rows and 40 embedding rows are traceable.

5. `pipeline_metrics` SQL output
   - Show:

     ```sql
     SELECT metric_name, metric_value, metric_text
     FROM pipeline_metrics
     WHERE run_id = (
         SELECT run_id
         FROM pipeline_runs
         WHERE dag_run_id = 'final_verify__20260530T172708__limit_20'
     )
     ORDER BY metric_name;
     ```

   - Why: proves observability.
   - Point out: task durations, row counts, retries, vector dimensions, zero-vector percentages, extraction percentage, and `run.summary`.

6. `audit_results` SQL output
   - Show:

     ```sql
     SELECT audit_name, passed, details
     FROM audit_results
     WHERE run_id = (
         SELECT run_id
         FROM pipeline_runs
         WHERE dag_run_id = 'final_verify__20260530T172708__limit_20'
     );
     ```

   - Why: proves duplicate audit is persisted.
   - Point out: `passed = true` and duplicate lists are empty.

7. `screens_eval` SQL output
   - Show:

     ```sql
     SELECT embedding_model_version, n_queries, recall_at_5
     FROM screens_eval
     WHERE run_id = (
         SELECT run_id
         FROM pipeline_runs
         WHERE dag_run_id = 'final_verify__20260530T172708__limit_20'
     );
     ```

   - Why: proves eval ran after audit.
   - Point out: `n_queries = 20` and `recall_at_5 = 1`.

8. MinIO bucket
   - Show: bucket `rico-raw`
   - Why: proves raw PNG and hierarchy JSON files are stored outside the database.
   - Point out: 40 objects for 20 screens, because each screen has one PNG and one JSON file.

### Bonus Video Flow

Record the bonus as one short end-to-end demo:

1. Start at Slack
   - Show the bot in the channel.
   - Type: `@databot backfill 20 screens`
   - Explain: this is the optional bonus backfill agent entry point.

2. Show the Slack reply
   - Show the bot response with `dag_run_id`.
   - Explain: the agent parsed the command, validated `LIMIT=20`, and triggered Airflow.

3. Show Airflow
   - Open: `http://localhost:8080`
   - Show the new `slack_backfill__...__limit_20` run.
   - Explain: Slack did not run the pipeline itself; it called Airflow REST API.

4. Show SQL proof
   - Run:

     ```sql
     SELECT dag_run_id, status, limit_param
     FROM pipeline_runs
     ORDER BY started_at DESC
     LIMIT 5;
     ```

   - Explain: this proves the Slack-triggered run was recorded in the same production tracking table.

5. Show audit and metrics
   - Show `audit_results` and `pipeline_metrics`.
   - Explain: bonus-triggered runs still use the same audit, traceability, and metrics as normal Airflow runs.

### What To Say During The Demo

Use this short explanation:

The main project productionizes the Week 7 RICO notebook into Airflow. The DAG is intentionally thin, and the business logic is in `src/rico_pipeline`. Each run gets a `run_id`, every destination row gets a `source_fingerprint`, and reruns are idempotent. After loading, the audit checks duplicate metadata and duplicate embedding keys. If the audit fails, eval is skipped and the run is marked `paused-by-audit`. Metrics are stored in `pipeline_metrics` so the run can be inspected after execution.

For the bonus, Slack is only a ChatOps interface. The separate backfill agent listens for a bot mention, uses Ollama to parse the requested limit, validates it, calls the Airflow REST API, and replies with the Airflow `dag_run_id`. The actual pipeline remains in Airflow.

## What Is Needed From The User

- Docker Desktop running.
- Enough disk space for Docker images, PyTorch dependencies, CLIP/SBERT model caches, and the Ollama model.
- Enough memory for Airflow plus model inference; 8 GB available to Docker is the practical minimum.
- Internet access for first-time image/model/dataset downloads.
- Optional: a real Slack incoming webhook in `.env` as `SLACK_WEBHOOK_URL=...` if live Slack screenshots/evidence are required.
- Bonus only: a Slack app with Socket Mode enabled, `SLACK_BOT_TOKEN`, and `SLACK_APP_TOKEN`.

Never commit the Slack webhook URL.

## Bonus Backfill Agent Design

The bonus agent is intentionally separate from the required pipeline:

```text
bonus_backfill_agent/agent.py
```

It is an external client, not DAG logic. It listens to Slack mentions using `slack_bolt`, asks Ollama to parse the natural-language command, validates the extracted `limit`, and calls Airflow's REST API:

```text
POST /api/v1/dags/rico_production_pipeline/dagRuns
```

The payload uses the bonus PDF's expected shape:

```json
{"conf": {"limit": 20}}
```

The DAG accepts both `limit` and `LIMIT`, so the original Project 4 trigger path and the bonus agent both work.

To demo the bonus:

1. Start the core stack with `make up`.
2. Create and configure the Slack app.
3. Add `SLACK_BOT_TOKEN=xoxb-...` and `SLACK_APP_TOKEN=xapp-...` to `.env`.
4. Start the bonus profile: `docker compose --profile agent up -d backfill-agent`.
5. In Slack, write: `@DataBot backfill 20 screens`.
6. Confirm the bot replies with a `dag_run_id`.
7. Confirm Airflow shows a new run and `pipeline_runs.limit_param = 20`.

Bonus validation completed without live Slack credentials:

- Built the separate `backfill-agent` Docker image.
- Tested Ollama parsing of a Slack-style request into `intent='run_pipeline'` and `limit=7`.
- Triggered Airflow from the agent container itself.
- Verified SQL evidence for `slack_backfill__20260530T082231__limit_6`: `status='succeeded'`, `limit_param=6`.

Live Slack validation was completed on May 30, 2026:

- Human Slack message: `@databot backfill 6 screens`
- Bot replied with Airflow run ID: `slack_backfill__20260530T093655__limit_6`
- Airflow DAG run state: `success`
- SQL evidence: `pipeline_runs.status='succeeded'`, `limit_param=6`
- Metrics summary: `metadata_rows=6 extracted=100.0% confident=100.0% review_queue=0.0% apps=1 categories=1`
- Audit result: duplicate detection passed.

Second live Slack validation on May 30, 2026:

- Human Slack message: `@databot backfill 20 screens`
- Agent log recorded the app mention and parsed backfill request.
- Airflow run ID: `slack_backfill__20260530T140326__limit_20`
- SQL evidence: `pipeline_runs.status='succeeded'`, `limit_param=20`
- Destination counts after the run remained idempotent: 20 metadata rows and 40 embedding rows.

Final post-fix validation on May 30, 2026:

- Airflow REST-triggered run: `final_verify__20260530T172708__limit_20`
- SQL evidence: `pipeline_runs.status='succeeded'`, `limit_param=20`
- Metrics summary: `metadata_rows=20 extracted=95.0% confident=95.0% review_queue=5.0% apps=6 categories=6`
- Audit result: duplicate detection passed.
- Eval result: `n_queries=20`, `recall_at_5=1`.
