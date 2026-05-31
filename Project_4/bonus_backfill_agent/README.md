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

## Verified Live Demo

Live Slack validation was completed with:

- Slack mention: `@databot backfill 20 screens`
- Bot reply: `slack_backfill__20260531T153522__limit_20`
- Airflow run state: `success`
- `pipeline_runs.status`: `succeeded`
- `pipeline_runs.limit_param`: `20`
- Duplicate audit: passed
- Recorded demo: `agent backfills recording.mp4`, kept outside Git for separate submission

For live presentation help, see [PRESENTATION_NOTES.md](PRESENTATION_NOTES.md).
