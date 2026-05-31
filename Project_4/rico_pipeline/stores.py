"""Store clients (Postgres, MinIO, Ollama) and the guarded-insert builder."""
from __future__ import annotations
import boto3
import psycopg
import requests
from pgvector.psycopg import register_vector

from rico_pipeline import config


def pg_connect():
    conn = psycopg.connect(config.POSTGRES_DSN)
    register_vector(conn)
    return conn


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_KEY,
        aws_secret_access_key=config.MINIO_SECRET,
    )


def ollama_generate(prompt: str) -> str:
    resp = requests.post(
        f"{config.OLLAMA_URL}/api/generate",
        json={"model": config.OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=600,
    )
    resp.raise_for_status()
    return resp.json()["response"]


def guarded_insert_sql(table: str, columns: list[str], conflict_columns: list[str]) -> str:
    """Idempotent insert: writes the row only if no row matches the natural key.

    INSERT INTO t (cols) SELECT %s,... WHERE NOT EXISTS (
        SELECT 1 FROM t WHERE k1 = %s AND k2 = %s ...)
    Placeholder order: all column values first, then conflict-key values.
    """
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    where = " AND ".join(f"{c} = %s" for c in conflict_columns)
    return (
        f"INSERT INTO {table} ({col_list})\n"
        f"SELECT {placeholders}\n"
        f"WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE {where})"
    )
