# Project 4 - Production RICO Airflow Pipeline

This folder implements Project 4 as a self-contained Docker project. It translates the Week 7 RICO notebook into an Airflow DAG with traceability, idempotent writes, a duplicate-detection audit, persisted metrics, and Slack notifications.

The DAG shape is:

```text
ingest -> parse -> [embed_image, embed_text, extract] -> load -> audit -> eval
```

## What It Uses From Week 7

- HuggingFace dataset: `rootsautomation/RICO-Screen2Words`
- MinIO bucket for PNG and hierarchy JSON blobs
- Postgres + pgvector destination tables
- CLIP image embeddings with `open-clip ViT-B-32 laion2b_s34b_b79k`
- SBERT text embeddings with `sentence-transformers/all-MiniLM-L6-v2`
- Ollama extraction with `qwen2.5:3b`
- The same hierarchy parser and reading-order text representation
- Recall@5 evaluation against stored SBERT vectors

## Setup

```bash
cd Project_4
make up
make pull-models
```

You can optionally copy `.env.example` to `.env` if you want to override defaults or configure Slack.

`make up` exports the current short Git commit SHA into the Airflow containers as `GIT_SHA`, so `pipeline_runs.git_sha` records the code version used for each run. If you start Docker Compose manually, set `GIT_SHA` first or leave it as `unknown` for local testing only.

Airflow UI:

- URL: <http://localhost:8080>
- Login: `airflow` / `airflow`

MinIO Console:

- URL: <http://localhost:9001>
- Login: `minioadmin` / `minioadmin`

## Run The DAG

Trigger the DAG with the default development limit:

```bash
make trigger
```

On Windows, this target avoids fragile shell JSON quoting by calling Airflow's Python trigger API inside the scheduler container.

Or trigger manually from Airflow with config:

```json
{"LIMIT": 5}
```

`LIMIT=5` uses the same five Week 7 development screens. Larger limits continue streaming from the same HuggingFace dataset.

## Tables

Core destination tables:

- `screens_metadata`
- `screens_embeddings`
- `screens_review_queue`
- `screens_eval`

Production support tables:

- `pipeline_runs`: one row per DAG run, including model versions, prompt version, git SHA, and final status.
- `pipeline_metrics`: task health and data quality metrics keyed by `run_id`.
- `audit_results`: duplicate audit history keyed by `run_id`.

Every row written to destination tables has:

- `run_id`
- `source_fingerprint`

## Idempotency

The DAG updates existing destination rows by the natural keys from the Week 7 schema:

- `screens_metadata.screen_id`
- `screens_embeddings (screen_id, model_name, model_version, embedding_kind)`
- `screens_review_queue.screen_id`

Re-running with the same `LIMIT` creates a new `pipeline_runs` row and new metrics, but it does not add duplicate destination rows. The updated rows receive the latest `run_id`, so the current run remains traceable. If extraction later succeeds for a screen, its old review queue row is removed. If a duplicate row is manually inserted, the next run refreshes matching rows and the audit catches the duplicate keys.

## Audit Failure

The required audit runs after `load` and before `eval`.

The `load` task is the destination completeness gate. It verifies that the current run has metadata, parsed hierarchy text, image embeddings, and text embeddings before the audit is allowed to run. If any required destination rows are missing, the DAG fails before audit/eval instead of producing misleading downstream evidence.

It fails the DAG if:

- the same `screen_id` appears more than once in `screens_metadata` for the current run
- the same `(screen_id, model_name, model_version, embedding_kind)` appears more than once in `screens_embeddings` for the current run

On failure:

- duplicate keys are logged
- `audit_results.passed` is `false`
- `pipeline_runs.status` becomes `paused-by-audit`
- `eval` is skipped
- Slack is attempted if configured

## Metrics

`pipeline_metrics` stores:

- per-task duration
- total run duration
- rows in and rows out
- retries
- final quality summary
- metadata row count
- percent with extraction payload
- percent with confidence >= 0.5
- percent in review queue
- embedding row count by model version and kind
- average vector dimensionality
- percent zero vectors
- distinct app package and category counts

The final task also logs a one-line summary readable from Airflow logs.

## Slack

Set this in `.env`:

```text
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

Or configure an Airflow connection containing the webhook URL and set:

```text
SLACK_WEBHOOK_CONN_ID=your_connection_id
```

The URL must not be committed. If Slack is missing or posting fails, the DAG logs a warning and continues.

Notifications are sent when:

- a run starts
- the audit fails
- a run finishes

The audit-failure message includes the duplicate keys and the Airflow audit task log URL when Airflow provides it.

## Tests

Local lightweight tests:

```bash
python -m pip install -e .[dev]
python -m pytest
```

Full Docker validation:

```bash
make up
make pull-models
make trigger
```

Then verify with SQL in Postgres:

```sql
SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 5;
SELECT metric_name, metric_value, metric_text FROM pipeline_metrics ORDER BY created_at DESC LIMIT 20;
SELECT * FROM audit_results ORDER BY created_at DESC LIMIT 5;
```

For a fuller explanation of the concepts and the local run evidence, see [REPORT.md](REPORT.md).

## Verified Local Evidence

Final pushed-code validation run:

```text
submission_verify__20260531T192757__limit_5
```

Observed results:

- Airflow run state: `success`
- `pipeline_runs.status`: `succeeded`
- `pipeline_runs.limit_param`: `5`
- `pipeline_runs.git_sha`: `0b2da68`
- `run.duration_seconds`: `161.819`
- Persisted task-health metrics include `audit`, duration, rows in/out, and retries.
- `audit_results.passed`: `true`
- Duplicate metadata keys: `0`
- Duplicate embedding keys: `0`

Verified clean destination state:

- `screens_metadata`: 20 rows, all with `run_id` and `source_fingerprint`
- `screens_embeddings`: 40 rows, all with `run_id` and `source_fingerprint`
- `screens_review_queue`: all rows have `run_id` and `source_fingerprint`
- Review queue duplicate screen IDs: `0`
- MinIO `rico-raw` objects: 40

Verified `LIMIT=20` bonus run:

```text
slack_backfill__20260531T153522__limit_20
```

Observed results:

- Airflow run state: `success`
- `pipeline_runs.status`: `succeeded`
- `pipeline_runs.limit_param`: `20`
- `run.duration_seconds`: `348.438199`
- `audit_results.passed`: `true`
- `run.summary`: `metadata_rows=20 extracted=100.0% confident=100.0% review_queue=0.0% apps=6 categories=6`
- CLIP image vectors: 20 rows, average dimensionality 512, 0% zero vectors
- SBERT text vectors: 20 rows, average dimensionality 384, 0% zero vectors

Verified audit circuit-breaker run:

```text
slack_audit_failure_demo__20260531T182418__limit_5
```

Observed results:

- Manually injected duplicate text embedding was detected.
- `pipeline_runs.status`: `paused-by-audit`
- `audit_results.passed`: `false`
- Duplicate key details were persisted and sent to Slack.
- Airflow task-log URL was included in the Slack message.
- `eval` was skipped.
- The controlled duplicate was deleted after the demo.

The numbered submission screenshots are stored in [`images/`](images/). See [REPORT.md](REPORT.md) for the grading-point mapping and live-presentation guidance.

## Bonus: Backfill Agent

The bonus work is kept separate from the required Project 4 pipeline in [bonus_backfill_agent](bonus_backfill_agent/).

It is a standalone Slack Socket Mode service that:

- listens for bot mentions such as `@DataBot backfill 20 screens`
- uses Ollama to parse the intent and requested screen limit
- calls the Airflow REST API to trigger `rico_production_pipeline`
- sends a Slack thread reply with the Airflow `dag_run_id`

Run it only after creating a Slack app and adding `SLACK_BOT_TOKEN` plus `SLACK_APP_TOKEN` to `.env`:

```bash
docker compose --profile agent up -d backfill-agent
```

Detailed setup is in [bonus_backfill_agent/README.md](bonus_backfill_agent/README.md).

The required bonus demonstration video was recorded separately as:

```text
agent backfills recording.mp4
```

Keep the video outside Git and upload it using the instructor's submission method.
