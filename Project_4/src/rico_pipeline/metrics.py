from __future__ import annotations

import logging

from rico_pipeline.config import Settings
from rico_pipeline.db import connect

log = logging.getLogger(__name__)


Metric = tuple[str, float | None, str | None]


def record_metric(
    settings: Settings,
    run_id: str,
    metric_name: str,
    metric_value: float | None = None,
    metric_text: str | None = None,
) -> None:
    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, metric_text)
            VALUES (%s, %s, %s, %s)
            """,
            (run_id, metric_name, metric_value, metric_text),
        )
        conn.commit()


def record_task_metrics(
    settings: Settings,
    run_id: str,
    task_id: str,
    result: dict | None,
    retries: int,
) -> None:
    if not result:
        result = {}
    metrics: list[Metric] = [
        (f"task.{task_id}.duration_seconds", result.get("seconds"), None),
        (f"task.{task_id}.rows_in", result.get("rows_in"), None),
        (f"task.{task_id}.rows_out", result.get("rows_out"), None),
        (f"task.{task_id}.retries", float(retries), None),
    ]
    _insert_metrics(settings, run_id, metrics)


def collect_quality_metrics(settings: Settings, run_id: str) -> str:
    metrics: list[Metric] = []
    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                count(*)::float,
                count(extraction_payload)::float,
                count(*) FILTER (WHERE confidence >= 0.5)::float
            FROM screens_metadata
            WHERE run_id = %s
            """,
            (run_id,),
        )
        metadata_count, extracted_count, confident_count = cur.fetchone()
        metadata_count = metadata_count or 0.0
        metrics.append(("quality.screens_metadata.row_count", metadata_count, None))
        metrics.append(
            (
                "quality.screens_metadata.extraction_payload_non_null_pct",
                _pct(extracted_count, metadata_count),
                None,
            )
        )
        metrics.append(
            (
                "quality.screens_metadata.confidence_gte_0_5_pct",
                _pct(confident_count, metadata_count),
                None,
            )
        )

        cur.execute(
            """
            SELECT count(*)::float
            FROM screens_review_queue
            WHERE run_id = %s
            """,
            (run_id,),
        )
        review_count = cur.fetchone()[0] or 0.0
        metrics.append(
            (
                "quality.screens_metadata.review_queue_pct",
                _pct(review_count, metadata_count),
                None,
            )
        )

        cur.execute(
            """
            SELECT count(DISTINCT app_package)::float, count(DISTINCT category)::float
            FROM screens_metadata
            WHERE run_id = %s
            """,
            (run_id,),
        )
        app_count, category_count = cur.fetchone()
        metrics.append(("quality.screens_metadata.distinct_app_package", app_count, None))
        metrics.append(("quality.screens_metadata.distinct_category", category_count, None))

        cur.execute(
            """
            SELECT model_version, embedding_kind, count(*)::float,
                   avg(vector_dims(vector))::float,
                   min(vector_dims(vector))::float,
                   max(vector_dims(vector))::float,
                   count(*) FILTER (WHERE abs(vector <#> vector) = 0)::float
            FROM screens_embeddings
            WHERE run_id = %s
            GROUP BY model_version, embedding_kind
            ORDER BY model_version, embedding_kind
            """,
            (run_id,),
        )
        for model_version, kind, n_rows, avg_dim, min_dim, max_dim, zero_count in cur.fetchall():
            prefix = f"quality.screens_embeddings.{model_version}.{kind}"
            metrics.append((f"{prefix}.row_count", n_rows, None))
            metrics.append((f"{prefix}.avg_vector_dimensionality", avg_dim, None))
            metrics.append((f"{prefix}.zero_vector_pct", _pct(zero_count, n_rows), None))
            if min_dim != max_dim:
                metrics.append(
                    (
                        f"{prefix}.dimension_warning",
                        None,
                        f"vector dimensions are not constant: min={min_dim}, max={max_dim}",
                    )
                )

    _insert_metrics(settings, run_id, metrics)
    summary = (
        f"metadata_rows={int(metadata_count)} "
        f"extracted={_pct(extracted_count, metadata_count):.1f}% "
        f"confident={_pct(confident_count, metadata_count):.1f}% "
        f"review_queue={_pct(review_count, metadata_count):.1f}% "
        f"apps={int(app_count or 0)} categories={int(category_count or 0)}"
    )
    record_metric(settings, run_id, "run.summary", metric_text=summary)
    log.info("run_id=%s metrics summary: %s", run_id, summary)
    return summary


def _insert_metrics(settings: Settings, run_id: str, metrics: list[Metric]) -> None:
    with connect(settings) as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, metric_text)
            VALUES (%s, %s, %s, %s)
            """,
            [(run_id, name, value, text) for name, value, text in metrics],
        )
        conn.commit()


def _pct(part: float | None, whole: float | None) -> float:
    if not whole:
        return 0.0
    return 100.0 * float(part or 0.0) / float(whole)
