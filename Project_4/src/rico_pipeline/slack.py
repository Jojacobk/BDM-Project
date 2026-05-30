from __future__ import annotations

import logging

import requests

from rico_pipeline.config import Settings

log = logging.getLogger(__name__)


def post_slack(settings: Settings, text: str) -> None:
    webhook_url = _webhook_url(settings)
    if not webhook_url:
        log.warning("Slack webhook is not configured; message skipped: %s", text)
        return
    try:
        response = requests.post(webhook_url, json={"text": text}, timeout=10)
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        log.warning("Slack post failed: %s", exc)


def _webhook_url(settings: Settings) -> str | None:
    if settings.slack_webhook_url:
        return settings.slack_webhook_url
    if not settings.slack_webhook_conn_id:
        return None
    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(settings.slack_webhook_conn_id)
        return conn.get_uri()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Slack webhook Airflow connection %s could not be loaded: %s",
            settings.slack_webhook_conn_id,
            exc,
        )
        return None
