# Bonus Backfill Agent

This folder is separate from the required Project 4 pipeline. The core Airflow DAG stays in `../dags/`; this service is an external ChatOps client.

## What It Does

The agent listens for Slack mentions through Socket Mode:

```text
@DataBot backfill 20 screens
```

It sends the message to Ollama, extracts the intent and `limit`, then triggers Airflow through the REST API:

```http
POST /api/v1/dags/rico_production_pipeline/dagRuns
```

Payload:

```json
{
  "dag_run_id": "slack_backfill__...",
  "conf": {
    "limit": 20,
    "triggered_by": "slack_backfill_agent"
  }
}
```

## Required Slack App Settings

Create a Slack app with:

- Bot token scopes: `app_mentions:read`, `chat:write`
- Socket Mode enabled
- Event subscription: `app_mention`
- Bot installed into the workspace
- Bot invited to the channel where you will test

Add these values to `../.env`:

```text
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
```

Never commit real Slack tokens.

## Run

Start the core stack first:

```bash
cd Project_4
make up
```

Then start the bonus agent:

```bash
docker compose --profile agent up -d backfill-agent
```

Watch logs:

```bash
docker compose logs -f backfill-agent
```

## Demo Proof

1. Mention the bot in Slack: `@DataBot backfill 20 screens`.
2. Bot replies in the thread with the Airflow `dag_run_id`.
3. Airflow UI shows a new manual run.
4. `pipeline_runs.limit_param` shows `20`.

## Local Validation Already Completed

The bonus service has been validated without real Slack credentials:

- The Docker image builds successfully.
- Ollama parsed `@DataBot hey can you run a backfill for 7 screens?` as `intent='run_pipeline'` and `limit=7`.
- The agent container triggered Airflow and created `slack_backfill__20260530T082231__limit_6`.
- SQL showed `pipeline_runs.limit_param = 6` for that run.

Live Slack validation was also completed:

- Slack mention: `@databot backfill 6 screens`
- Bot replied with `slack_backfill__20260530T093655__limit_6`
- Airflow run succeeded.
- SQL showed `pipeline_runs.limit_param = 6`
- Audit passed with no duplicate metadata or embedding keys.

For live presentation help, see [PRESENTATION_NOTES.md](PRESENTATION_NOTES.md).
