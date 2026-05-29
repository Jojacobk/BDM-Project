"""Observability: data-quality calculators, metric persistence, summary line."""
from __future__ import annotations
import numpy as np


def zero_norm_fraction(vectors) -> float:
    if not len(vectors):
        return 0.0
    zeros = sum(1 for v in vectors if float(np.linalg.norm(v)) == 0.0)
    return zeros / len(vectors)


def summary_line(*, run_id, status, duration_s, meta_count, extracted_pct,
                 conf_pct, review_count, emb_counts, dims, zero_pct, n_apps, n_cats) -> str:
    emb = " ".join(f"{k}={emb_counts.get(k, 0)}/{dims.get(k, '?')}d" for k in ("image", "text"))
    return (
        f"run={run_id} status={status} dur={duration_s:.1f}s | "
        f"meta={meta_count} (extracted {extracted_pct:.0f}%, conf>=.5 {conf_pct:.0f}%, "
        f"review={review_count}) | emb {emb} zero={zero_pct:.0f}% | "
        f"apps={n_apps} cats={n_cats}"
    )


def record_metric_sql(run_id, task_id, metric_name, value, labels=None):
    import json
    sql = ("INSERT INTO pipeline_metrics (run_id, task_id, metric_name, metric_value, labels) "
           "VALUES (%s, %s, %s, %s, %s::jsonb) "
           "ON CONFLICT (run_id, task_id, metric_name) DO UPDATE "
           "SET metric_value = EXCLUDED.metric_value, labels = EXCLUDED.labels")
    return sql, (run_id, task_id, metric_name, value, json.dumps(labels or {}))
