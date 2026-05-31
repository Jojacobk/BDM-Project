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

The Slack App configuration is:

- Bot token scopes: `app_mentions:read`, `chat:write`
- Socket Mode enabled
- Event subscription: `app_mention`
- Bot installed into the workspace
- Bot invited to the target channel

Required `../.env` values:

```text
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
```

## Service Commands

Core stack:

```bash
cd Project_4
make up
```

Bonus agent:

```bash
docker compose --profile agent up -d backfill-agent
```

Agent logs:

```bash
docker compose logs -f backfill-agent
```

## Verified Live Demo

Live Slack validation was completed with:

- Slack mention: `@databot backfill 20 screens`
- Bot reply: `slack_backfill__20260531T153522__limit_20`
- Airflow run state: `success`
- `pipeline_runs.status`: `succeeded`
- `pipeline_runs.limit_param`: `20`
- Duplicate audit: passed
- Recorded demo: [`../bonus_video/agent_backfills_recording.mp4`](../bonus_video/agent_backfills_recording.mp4)
