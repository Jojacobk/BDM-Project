# Bonus Backfill Agent - Live Presentation Notes

## One-Sentence Explanation

The bonus turns our pipeline from one-way alerting into two-way ChatOps: an engineer can mention a Slack bot in natural language, and the bot safely triggers an Airflow backfill run with the requested `LIMIT`.

## Architecture

```text
Slack mention
  -> Slack Socket Mode
  -> bonus_backfill_agent/agent.py
  -> Ollama intent parser
  -> Airflow REST API
  -> rico_production_pipeline DAG run
  -> Slack thread confirmation
```

The agent is not inside the DAG. This matters because Airflow remains the orchestrator, while the agent is an external client. That separation is exactly what the bonus asks for.

## What Each Part Does

- Slack Socket Mode: lets a local script receive Slack events without a public URL or port forwarding.
- Slack Bot User: the visible identity users mention, for example `@DataBot`.
- `app_mention` event: the event type fired when a human tags the bot in a channel.
- Ollama parser: reads the human sentence and returns structured intent, such as `run_pipeline`, plus `limit`.
- Validation: rejects missing limits, limits below 1, and limits above `BACKFILL_MAX_LIMIT`.
- Airflow REST API: creates a new manual DAG run by calling `/api/v1/dags/{dag_id}/dagRuns`.
- Thread reply: confirms the backfill and gives the `dag_run_id` so the engineer can find it in Airflow.

## Demo Script

1. Show the core DAG running in Airflow:
   `http://localhost:8080/dags/rico_production_pipeline`

2. Explain the bonus folder is separate:
   `Project_4/bonus_backfill_agent/`

3. Start the agent:

   ```bash
   docker compose --profile agent up -d backfill-agent
   ```

4. In Slack, mention the bot:

   ```text
   @DataBot backfill 20 screens
   ```

5. Show the bot reply:

   ```text
   Backfill started for LIMIT=20. Airflow dag_run_id: `slack_backfill__...__limit_20`
   ```

6. Show Airflow has a new manual run.

7. Show SQL proof:

   ```sql
   SELECT dag_run_id, status, limit_param
   FROM pipeline_runs
   ORDER BY started_at DESC
   LIMIT 5;
   ```

## What We Already Validated Locally

Without real Slack tokens, we validated the two core technical paths:

- Brain test: Ollama parsed `@DataBot hey can you run a backfill for 7 screens?` into `intent='run_pipeline', limit=7`.
- Hands test: the agent container triggered Airflow and created `slack_backfill__20260530T082231__limit_6`.
- SQL proof: `pipeline_runs.limit_param = 6` for that agent-triggered run.

With real Slack Socket Mode tokens, we validated the full live loop:

- Slack message: `@databot backfill 6 screens`
- Bot reply: `slack_backfill__20260530T093655__limit_6`
- Airflow state: `success`
- SQL proof: `pipeline_runs.status = 'succeeded'` and `limit_param = 6`
- Audit proof: `audit_results.passed = true`
- Second Slack run: `@databot backfill 20 screens`
- Agent log proof: the container received the app mention for `backfill 20 screens`
- SQL proof for the second run: `slack_backfill__20260530T140326__limit_20` finished with `status = 'succeeded'` and `limit_param = 20`
- Final clean validation after the metrics refresh fix: `final_verify__20260530T172708__limit_20` finished successfully with 20 metadata rows, 40 embedding rows, audit passed, and `recall_at_5 = 1`

## What We Need For The Live Slack Demo

The demo needs real Slack app credentials in `.env`:

- `SLACK_BOT_TOKEN`, starts with `xoxb-`
- `SLACK_APP_TOKEN`, starts with `xapp-`

The Slack app must have:

- Socket Mode enabled
- Event subscription: `app_mention`
- Bot scopes: `app_mentions:read`, `chat:write`
- Bot installed to the workspace
- Bot invited to the test channel

## Good Explanation For The Teacher

This is safer than directly giving humans Airflow admin access. The bot accepts a narrow command, extracts one controlled parameter, validates it, and calls one specific DAG endpoint. The data engineer gets self-service, but the system still keeps guardrails: max limit, authenticated Airflow API, traceability in `pipeline_runs`, and normal audit/metrics behavior inside the DAG.

## Failure Modes To Mention

- If Slack tokens are missing, the agent refuses to start.
- If the LLM cannot parse a limit, the bot asks for a screen count.
- If the requested limit is above `BACKFILL_MAX_LIMIT`, the bot rejects it.
- If Airflow REST fails, the bot replies with the error instead of silently doing nothing.
- If the DAG audit fails, Airflow still halts the pipeline exactly as in the core assignment.
