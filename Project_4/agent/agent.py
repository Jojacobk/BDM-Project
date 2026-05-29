"""Standalone ChatOps agent: Slack Socket Mode -> Ollama intent -> Airflow REST trigger.
Run with `python agent/agent.py` alongside `make up`. NOT part of the DAG."""
from __future__ import annotations
import json
import os
import re
import requests

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://localhost:8080")
AIRFLOW_API_USER = os.getenv("AIRFLOW_API_USER", "admin")
AIRFLOW_API_PASSWORD = os.getenv("AIRFLOW_API_PASSWORD", "admin")
DAG_ID = "rico_pipeline"

INTENT_PROMPT = """You are a parser. The user wants to trigger a data pipeline.
Read their message and reply with ONLY a JSON object: {"intent":"run_pipeline","limit":<int>}.
If no number is present, use 5. Message: """


def parse_limit_from_llm(raw: str):
    """Extract the integer limit from the LLM's JSON reply. None if unparseable."""
    try:
        return int(json.loads(raw)["limit"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        m = re.search(r'"limit"\s*:\s*(\d+)', raw or "")
        return int(m.group(1)) if m else None


def llm_extract_limit(message: str) -> int:
    resp = requests.post(f"{OLLAMA_URL}/api/generate",
                         json={"model": OLLAMA_MODEL, "prompt": INTENT_PROMPT + message,
                               "stream": False}, timeout=120)
    resp.raise_for_status()
    return parse_limit_from_llm(resp.json()["response"]) or 5


def trigger_dag(limit: int) -> str:
    resp = requests.post(
        f"{AIRFLOW_API_URL}/api/v1/dags/{DAG_ID}/dagRuns",
        auth=(AIRFLOW_API_USER, AIRFLOW_API_PASSWORD),
        json={"conf": {"limit": limit, "trigger_source": "agent"}}, timeout=30)
    resp.raise_for_status()
    return resp.json()["dag_run_id"]


def main():
    from slack_bolt import App
    from slack_bolt.adapter.socket_mode import SocketModeHandler

    app = App(token=os.environ["SLACK_BOT_TOKEN"])

    @app.event("app_mention")
    def handle_mention(event, say):
        text = event.get("text", "")
        thread = event.get("ts")
        limit = llm_extract_limit(text)
        say(text=f":hourglass: Parsed request — triggering `{DAG_ID}` with LIMIT={limit}…",
            thread_ts=thread)
        try:
            run_id = trigger_dag(limit)
            say(text=f":white_check_mark: Triggered! dag_run_id=`{run_id}` (LIMIT={limit})",
                thread_ts=thread)
        except Exception as exc:  # noqa: BLE001
            say(text=f":x: Failed to trigger: {exc}", thread_ts=thread)

    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()


if __name__ == "__main__":
    main()
