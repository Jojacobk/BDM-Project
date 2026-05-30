from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime

import requests
from requests.auth import HTTPBasicAuth
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("backfill-agent")


@dataclass(frozen=True)
class AgentConfig:
    slack_bot_token: str
    slack_app_token: str
    airflow_base_url: str
    airflow_username: str
    airflow_password: str
    dag_id: str
    ollama_url: str
    ollama_model: str
    max_limit: int


@dataclass(frozen=True)
class ParsedCommand:
    intent: str
    limit: int | None
    explanation: str


def load_config() -> AgentConfig:
    return AgentConfig(
        slack_bot_token=_required_env("SLACK_BOT_TOKEN"),
        slack_app_token=_required_env("SLACK_APP_TOKEN"),
        airflow_base_url=os.environ.get("AIRFLOW_BASE_URL", "http://airflow-webserver:8080"),
        airflow_username=os.environ.get("AIRFLOW_USERNAME", "airflow"),
        airflow_password=os.environ.get("AIRFLOW_PASSWORD", "airflow"),
        dag_id=os.environ.get("AIRFLOW_DAG_ID", "rico_production_pipeline"),
        ollama_url=os.environ.get("OLLAMA_URL", "http://ollama:11434"),
        ollama_model=os.environ.get("OLLAMA_MODEL", "qwen2.5:3b"),
        max_limit=int(os.environ.get("BACKFILL_MAX_LIMIT", "50")),
    )


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required for the Slack backfill agent")
    return value


def parse_command_with_llm(config: AgentConfig, message: str) -> ParsedCommand:
    prompt = f"""\
You are a DataOps command parser for a Slack bot.

Read the user's message and return one JSON object only:
{{
  "intent": "run_pipeline" or "unknown",
  "limit": integer or null,
  "explanation": short string
}}

Rules:
- Use intent "run_pipeline" only when the user asks to run, trigger, backfill, rerun, or load the RICO pipeline.
- Extract the requested number of screens as limit.
- If no number is present, use null.
- Do not include Markdown or commentary.

Message:
{message}
"""
    response = requests.post(
        f"{config.ollama_url}/api/generate",
        json={"model": config.ollama_model, "prompt": prompt, "stream": False},
        timeout=60,
    )
    response.raise_for_status()
    raw = response.json()["response"].strip()
    parsed = json.loads(_json_object(raw))
    limit = parsed.get("limit")
    return ParsedCommand(
        intent=str(parsed.get("intent", "unknown")),
        limit=int(limit) if limit is not None else None,
        explanation=str(parsed.get("explanation", "")),
    )


def _json_object(raw: str) -> str:
    match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
    if not match:
        raise ValueError(f"LLM did not return a JSON object: {raw!r}")
    return match.group(0)


def validate_command(config: AgentConfig, command: ParsedCommand) -> ParsedCommand:
    if command.intent != "run_pipeline":
        return command
    if command.limit is None:
        raise ValueError("I understood the backfill request, but I need a screen count.")
    if command.limit < 1:
        raise ValueError("LIMIT must be at least 1.")
    if command.limit > config.max_limit:
        raise ValueError(
            f"LIMIT {command.limit} is above the configured safety cap "
            f"({config.max_limit})."
        )
    return command


def trigger_airflow_run(config: AgentConfig, limit: int) -> str:
    logical_stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    dag_run_id = f"slack_backfill__{logical_stamp}__limit_{limit}"
    url = f"{config.airflow_base_url.rstrip('/')}/api/v1/dags/{config.dag_id}/dagRuns"
    payload = {
        "dag_run_id": dag_run_id,
        "conf": {"limit": limit, "triggered_by": "slack_backfill_agent"},
    }
    response = requests.post(
        url,
        json=payload,
        auth=HTTPBasicAuth(config.airflow_username, config.airflow_password),
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("dag_run_id", dag_run_id)


def build_app(config: AgentConfig) -> App:
    app = App(token=config.slack_bot_token)

    @app.event("app_mention")
    def handle_mention(event, say):  # type: ignore[no-untyped-def]
        text = event.get("text", "")
        thread_ts = event.get("thread_ts") or event.get("ts")
        channel = event.get("channel")
        log.info("received mention channel=%s thread_ts=%s text=%s", channel, thread_ts, text)

        try:
            command = validate_command(config, parse_command_with_llm(config, text))
            if command.intent != "run_pipeline":
                say(
                    text=(
                        "I can trigger RICO backfills. Try: "
                        "`@DataBot backfill 20 screens`."
                    ),
                    thread_ts=thread_ts,
                )
                return
            assert command.limit is not None
            dag_run_id = trigger_airflow_run(config, command.limit)
            say(
                text=(
                    f"Backfill started for LIMIT={command.limit}. "
                    f"Airflow dag_run_id: `{dag_run_id}`"
                ),
                thread_ts=thread_ts,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("failed to handle Slack mention")
            say(text=f"I could not start the backfill: {exc}", thread_ts=thread_ts)

    return app


def main() -> None:
    config = load_config()
    app = build_app(config)
    log.info("starting Slack Socket Mode backfill agent for dag_id=%s", config.dag_id)
    SocketModeHandler(app, config.slack_app_token).start()


if __name__ == "__main__":
    main()
