from __future__ import annotations

import os
from dataclasses import dataclass

from rico_pipeline.constants import DEFAULT_OLLAMA_MODEL


@dataclass(frozen=True)
class Settings:
    postgres_dsn: str
    minio_url: str
    minio_key: str
    minio_secret: str
    minio_bucket: str
    ollama_url: str
    ollama_model: str
    prompt_path: str
    default_limit: int
    slack_webhook_url: str | None
    slack_webhook_conn_id: str | None = None


def load_settings() -> Settings:
    return Settings(
        postgres_dsn=os.environ.get(
            "POSTGRES_DSN", "postgresql://rico:rico@localhost:5432/rico"
        ),
        minio_url=os.environ.get("MINIO_URL", "http://localhost:9000"),
        minio_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        minio_bucket=os.environ.get("MINIO_BUCKET", "rico-raw"),
        ollama_url=os.environ.get("OLLAMA_URL", "http://localhost:11434"),
        ollama_model=os.environ.get("OLLAMA_MODEL", DEFAULT_OLLAMA_MODEL),
        prompt_path=os.environ.get(
            "RICO_PROMPT_PATH", "/opt/airflow/prompts/extraction_v1.txt"
        ),
        default_limit=int(os.environ.get("RICO_DEFAULT_LIMIT", "5")),
        slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL") or None,
        slack_webhook_conn_id=os.environ.get("SLACK_WEBHOOK_CONN_ID") or None,
    )
