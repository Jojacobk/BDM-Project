"""Slack notifications. Every post is best-effort: failures never fail the run."""
from __future__ import annotations
import logging
import requests

log = logging.getLogger(__name__)


def post_slack(webhook_url: str, text: str) -> bool:
    if not webhook_url:
        log.warning("SLACK: no webhook configured; skipping post: %s", text[:120])
        return False
    try:
        resp = requests.post(webhook_url, json={"text": text}, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as exc:  # noqa: BLE001 - notifications must not fail the run
        log.warning("SLACK: post failed (%s); continuing", exc)
        return False


def run_started_message(run_id, limit, trigger) -> str:
    return f":rocket: Run started — run_id=`{run_id}` LIMIT={limit} trigger={trigger}"


def audit_failed_message(run_id, dup_keys, log_url) -> str:
    keys = "\n".join(f"  • {k}" for k in dup_keys) or "  (see logs)"
    return (f":rotating_light: *AUDIT FAILED* run_id=`{run_id}`\n"
            f"Duplicate keys:\n{keys}\nLog: {log_url}")


def run_finished_message(summary_line) -> str:
    return f":checkered_flag: Run finished — {summary_line}"
