# Project 4 - Production RICO Airflow Pipeline

This self-contained Docker project converts the Week 7 RICO notebook into a scheduled, idempotent, traceable, auditable, and observable Airflow pipeline.

The required DAG is:

```text
ingest -> parse -> [embed_image, embed_text, extract] -> load -> audit -> eval
```

The visible `start` and `finish` tasks are operational wrappers for run tracking, metrics, and Slack notifications. Business logic lives under `src/rico_pipeline`; the DAG file contains orchestration only.

## Week 7 Modules Reused

- HuggingFace dataset: `rootsautomation/RICO-Screen2Words`
- MinIO storage for PNG and hierarchy JSON blobs
- Postgres + pgvector destination tables
- CLIP image embeddings: `open-clip ViT-B-32 laion2b_s34b_b79k`
- SBERT text embeddings: `sentence-transformers/all-MiniLM-L6-v2`
- Ollama extraction: `qwen2.5:3b`
- Versioned local extraction prompt: `v1`
- Recall@5 evaluation using stored SBERT vectors

## Project Structure

```text
Project_4/
|-- dags/                     # thin Airflow DAG
|-- src/rico_pipeline/        # pipeline business logic
|-- migrations/               # Postgres + pgvector schema
|-- prompts/                  # versioned local LLM prompt
|-- tests/                    # focused project tests
|-- bonus_backfill_agent/     # optional standalone Slack agent
|-- images/                   # verified evidence screenshots
|-- docker-compose.yml
|-- Makefile
|-- README.md
`-- REPORT.md
```

The required pipeline and optional bonus are separate. The Slack backfill agent is an external client and is not part of the Airflow DAG.

## Start The Stack

From the `Project_4` folder:

```bash
make up
make pull-models
```

`make up` exports the current short Git commit SHA into Airflow as `GIT_SHA`. This value is stored in `pipeline_runs.git_sha` for traceability.

Local services:

| Service | URL | Credentials |
| --- | --- | --- |
| Airflow | <http://localhost:8080> | `airflow` / `airflow` |
| MinIO Console | <http://localhost:9001> | `minioadmin` / `minioadmin` |

Stop the stack with:

```bash
make down
```

## Run The DAG

Trigger the development run:

```bash
make trigger
```

Or trigger manually in Airflow with:

```json
{"LIMIT": 5}
```

`LIMIT=5` is suitable for development. A larger limit, such as `LIMIT=20` or `LIMIT=50`, processes more screens from the same Week 7 dataset.

## Destination And Support Tables

Core destination tables:

- `screens_metadata`
- `screens_embeddings`
- `screens_review_queue`
- `screens_eval`

Production support tables:

- `pipeline_runs`: one row per DAG run with run ID, Airflow run ID, timestamps, final status, limit, Git SHA, model versions, and prompt version.
- `pipeline_metrics`: persisted health and destination-quality metrics keyed by `run_id` and `metric_name`.
- `audit_results`: persisted duplicate-audit history.

Rows in `screens_metadata`, `screens_embeddings`, and `screens_review_queue` store:

- `run_id`
- `source_fingerprint`

## Idempotency

The pipeline updates destination rows by natural key:

- `screens_metadata.screen_id`
- `screens_embeddings (screen_id, model_name, model_version, embedding_kind)`
- `screens_review_queue.screen_id`

Re-running the DAG with the same `LIMIT` creates a new `pipeline_runs` row and new metrics, but does not create duplicate destination rows or duplicate MinIO blobs.

## Audit Circuit Breaker

The required audit runs after `load` and before `eval`. It checks the current run for:

- duplicate `screens_metadata.screen_id`
- duplicate `(screen_id, model_name, model_version, embedding_kind)` values in `screens_embeddings`

If duplicates exist:

- the audit logs the complete duplicate keys
- `audit_results.passed` is `false`
- `pipeline_runs.status` becomes `paused-by-audit`
- Slack receives an audit-failed notification with the Airflow task-log URL
- `eval` does not run

## Persisted Metrics

`pipeline_metrics` stores:

- per-task duration
- per-task rows in and rows out
- retries per task
- total run duration
- final run status
- one-line run summary
- metadata row count
- extraction payload percentage
- confidence `>= 0.5` percentage
- review-queue percentage
- embedding row count by model version and kind
- average vector dimensionality
- zero-vector percentage
- distinct application-package and category counts

## Slack Notifications

Set the incoming webhook in `.env`:

```text
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

Alternatively, configure an Airflow connection and set:

```text
SLACK_WEBHOOK_CONN_ID=your_connection_id
```

The webhook URL must never be committed. Missing or failing Slack notifications log a warning and do not fail the pipeline.

Notifications are attempted when:

- a run starts
- an audit fails
- a run finishes

## Optional Bonus: Slack Backfill Agent

The optional bonus service is implemented separately in [`bonus_backfill_agent/`](bonus_backfill_agent/).

It listens for Slack App mentions through Socket Mode:

```text
@DataBot backfill 20 screens
```

The agent uses Ollama to extract intent and `LIMIT`, validates the limit, calls the authenticated Airflow REST API, and replies in the Slack thread with the generated `dag_run_id`.

After configuring `SLACK_BOT_TOKEN` and `SLACK_APP_TOKEN` in `.env`, start it with:

```bash
docker compose --profile agent up -d backfill-agent
```

See [`bonus_backfill_agent/README.md`](bonus_backfill_agent/README.md) for Slack App setup.

## Tests And Validation

Run:

```bash
docker compose exec -T airflow-scheduler python -m pytest /opt/airflow/tests
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

Expected result:

```text
11 passed
No data found
```

Useful SQL:

```sql
SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 5;
SELECT metric_name, metric_value, metric_text
FROM pipeline_metrics
ORDER BY created_at DESC
LIMIT 30;
SELECT * FROM audit_results ORDER BY created_at DESC LIMIT 5;
```

## Submission

Submit the GitHub repository link:

<https://github.com/Jojacobk/BDM-Project/tree/Buland_project_4/Project_4>

For the optional bonus, also upload the recorded demonstration video separately:

```text
agent backfills recording.mp4
```

Do not commit `.env`, Slack credentials, or the Slack webhook URL. Commits made after the instructor deadline will be ignored.

## Evaluation Criteria

The instructor's rubric is:

| Weight | Criterion |
| --- | --- |
| 40% | Correctness and idempotency |
| 20% | Traceability |
| 20% | Audit circuit breaker |
| 15% | Observability |
| 5% | Code quality and README |

Verified results and screenshot evidence are documented in [`REPORT.md`](REPORT.md).
