# E2E Runbook — Definition of Done

This runbook verifies the Project Brief's **Definition of Done (§5)** against a live stack.
It requires **Docker** (Docker Desktop with WSL integration enabled). Each step lists the
command and the expected result. The unit-test suite (`.venv/bin/python -m pytest`) already
passes without Docker; this runbook covers the integration/E2E checks that need the live stack.

> All commands run from `Project_4/`.

## 0. Prerequisites

```bash
cp .env.example .env        # then edit .env: set a real SLACK_WEBHOOK_URL (and bot/app tokens for the agent)
docker --version            # confirm Docker is available
```

## 1. Infra up + DAG loads without error  ✅ DoD: "make up brings up infra; DAG appears in Airflow UI"

```bash
make up                     # postgres+pgvector, minio, ollama (waits healthy)
make pull-models            # pulls qwen2.5:3b (~1.9 GB, one-time)
make up-airflow             # builds the Airflow image, starts scheduler + webserver
```
- Open http://localhost:8080 (admin/admin). The `rico_pipeline` DAG appears with **no import errors**.
- Run the DAG-integrity unit test inside the scheduler container:
  ```bash
  docker compose exec airflow-scheduler python -m pytest /opt/airflow/dags -q || \
  docker compose exec airflow-scheduler bash -c "cd /opt/airflow && python -m pytest tests/test_dag_import.py -q"
  ```
  Expected: `test_dag_loads_and_has_expected_tasks` and `test_middle_three_run_in_parallel` PASS.

## 2. First run populates everything  ✅ DoD: "LIMIT=5 populates all tables + a pipeline_runs row + a pipeline_metrics row"

```bash
make trigger LIMIT=5
```
Wait for the run to succeed in the UI, then:
```bash
docker compose exec postgres psql -U rico -d rico -c \
 "SELECT (SELECT count(*) FROM screens_metadata)  AS meta,
         (SELECT count(*) FROM screens_embeddings) AS emb,
         (SELECT count(*) FROM pipeline_runs)      AS runs,
         (SELECT count(*) FROM pipeline_metrics)   AS metrics;"
```
Expected: **meta=5, emb=10** (5 image + 5 text), **runs=1**, **metrics ≥ 1**.
A Slack **"Run started"** and **"Run finished"** message arrive in your channel.

## 3. Every row is traceable  ✅ DoD: "Every row has non-null run_id and source_fingerprint"

```bash
docker compose exec postgres psql -U rico -d rico -c \
 "SELECT
    (SELECT count(*) FROM screens_metadata   WHERE run_id IS NULL OR source_fingerprint IS NULL) AS bad_meta,
    (SELECT count(*) FROM screens_embeddings WHERE run_id IS NULL OR source_fingerprint IS NULL) AS bad_emb;"
```
Expected: **bad_meta=0, bad_emb=0**. (`run_id`/`source_fingerprint` are `NOT NULL` in the schema, so this is structurally guaranteed — this query is the explicit proof.)

## 4. Re-run is idempotent  ✅ DoD: "Re-triggering LIMIT=5 produces no new rows in any destination table"

```bash
make trigger LIMIT=5        # wait for success
docker compose exec postgres psql -U rico -d rico -c \
 "SELECT (SELECT count(*) FROM screens_metadata) AS meta,
         (SELECT count(*) FROM screens_embeddings) AS emb,
         (SELECT count(*) FROM pipeline_runs) AS runs;"
```
Expected: **meta=5, emb=10 (UNCHANGED)**; **runs=2** (pipeline_runs/metrics are append-only audit logs — one set per run is correct, NOT a duplicate). MinIO objects under `screens/` are unchanged.

## 5. Audit fires on corruption  ✅ DoD: "Manually inserting a duplicate embedding + re-running fails the audit and skips eval"

```bash
make corrupt                # raw INSERT of a duplicate (screen_id, model_name, model_version, embedding_kind) row
make trigger LIMIT=5        # re-run
```
Expected in the UI:
- the **`audit` task fails** (raises `AuditFailed`),
- **`eval` is skipped** (downstream of audit),
- `finalize_run` still runs (trigger_rule `all_done`) and marks the run **`paused_by_audit`**.

Verify:
```bash
docker compose exec postgres psql -U rico -d rico -c \
 "SELECT status FROM pipeline_runs ORDER BY started_at DESC LIMIT 1;"          -- paused_by_audit
docker compose exec postgres psql -U rico -d rico -c \
 "SELECT audit_name, passed, details FROM audit_results ORDER BY created_at DESC LIMIT 1;"  -- passed=f, details lists the dup key
```
A Slack **AUDIT FAILED** message lists the duplicate key. Then clean up:
```bash
make reset-prod             # truncate all destination + traceability tables
```

## 6. One-line health summary  ✅ DoD: "End-of-run log line shows health + data-quality summary, readable in 10s"

In the Airflow UI, open the `finalize_run` task log of any run. Expected: a single `SUMMARY` line like:
```
run=<uuid> status=succeeded dur=Ns | meta=5 (extracted 100%, conf>=.5 80%, review=0) | emb image=5/512d text=5/384d zero=0% | apps=5 cats=5
```

## 7. Slack — three run types  ✅ DoD: "successful / failed / audit-halted runs each post the expected message; webhook not committed"

- Successful run (step 2): "Run started" + "Run finished (succeeded)".
- Audit-halted run (step 5): "Run started" + "AUDIT FAILED" + "Run finished (paused_by_audit)".
- A failed run (e.g. stop Ollama then trigger): "Run finished (failed)".
- Confirm the webhook URL is **not** in git: `git grep -i "hooks.slack.com" -- . ':!docs' ':!.env.example'` returns nothing.

## 8. Bonus agent  ✅ Bonus DoD: "tagging the bot triggers a new DAG run with the dynamic LIMIT; agent replies with dag_run_id"

With `SLACK_BOT_TOKEN` + `SLACK_APP_TOKEN` set in `.env` and the stack up:
```bash
make agent                  # standalone slack_bolt Socket Mode listener
```
In Slack: `@DataBot can you run a backfill for 20 screens?`
Expected: the bot acknowledges in-thread, a new DAG run appears in the UI with **LIMIT=20** (`conf={"limit":20,"trigger_source":"agent"}`), and the bot replies with the `dag_run_id`. Verify the run was agent-triggered:
```bash
docker compose exec postgres psql -U rico -d rico -c \
 "SELECT dag_run_id, limit_param, trigger_source FROM pipeline_runs ORDER BY started_at DESC LIMIT 1;"
```

---

### Notes
- `make reset-prod` truncates destination + traceability tables (keeps the schema). `make clean` wipes all volumes (including Ollama's model cache).
- Per-run traceability query: `SELECT * FROM pipeline_runs WHERE run_id = '<uuid>';` then join `<uuid>` across `screens_metadata`, `screens_embeddings`, `screens_review_queue`, `audit_results`, `pipeline_metrics`.
