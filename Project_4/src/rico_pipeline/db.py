from __future__ import annotations

import psycopg
from pgvector.psycopg import register_vector

from rico_pipeline.config import Settings


def connect(settings: Settings, *, vectors: bool = False):
    conn = psycopg.connect(settings.postgres_dsn)
    if vectors:
        register_vector(conn)
    return conn
