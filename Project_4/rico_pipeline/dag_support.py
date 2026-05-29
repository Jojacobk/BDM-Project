"""Finalize logic invoked by the DAG's finalize_run task. Kept out of the DAG file."""
from __future__ import annotations
import logging

from rico_pipeline import config, observability as obs, slack
from rico_pipeline.context import RunContext
from rico_pipeline.stores import pg_connect
from rico_pipeline.traceability import finish_run_sql

log = logging.getLogger("rico_pipeline")


def finalize(airflow_context):
    ti = airflow_context["ti"]
    raw = ti.xcom_pull(task_ids="start_run")
    if not raw:
        return
    ctx = RunContext.from_dict(raw)
    dag_run = airflow_context["dag_run"]
    # state may be a str-enum (TaskInstanceState) or a plain string depending on
    # the Airflow version; normalise via .value so the comparison is robust.
    failed = [t for t in dag_run.get_task_instances()
              if getattr(t.state, "value", t.state) == "failed"]
    audit_failed = any(t.task_id == "audit" for t in failed)
    status = "paused_by_audit" if audit_failed else ("failed" if failed else "succeeded")

    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(obs.DQ_QUERIES["meta"], (ctx.run_id,))
        meta_count, extracted_pct, conf_pct = cur.fetchone()
        cur.execute(obs.DQ_QUERIES["emb"], (ctx.run_id,))
        emb_rows = cur.fetchall()
        emb_counts = {kind: n for (_, kind, n, _) in emb_rows}
        dims = {kind: int(d) for (_, kind, _, d) in emb_rows if d is not None}
        cur.execute(obs.DQ_QUERIES["distinct"], (ctx.run_id, ctx.run_id))
        n_apps, n_cats, review_count = cur.fetchone()
        # pure-zero vectors are a silent embedder bug — compute the fraction (§3.4).
        cur.execute("SELECT vector FROM screens_embeddings WHERE run_id = %s", (ctx.run_id,))
        vectors = [r[0] for r in cur.fetchall()]
        zero_pct = obs.zero_norm_fraction(vectors) * 100
        for name, val in [("meta_row_count", meta_count or 0),
                          ("extracted_pct", float(extracted_pct or 0)),
                          ("confidence_ge_05_pct", float(conf_pct or 0)),
                          ("review_queue_count", review_count or 0),
                          ("zero_norm_pct", zero_pct)]:
            sql, params = obs.record_metric_sql(ctx.run_id, None, name, val)
            cur.execute(sql, params)
        sql, params = finish_run_sql(ctx.run_id, status)
        cur.execute(sql, params)
        conn.commit()

    line = obs.summary_line(
        run_id=ctx.run_id, status=status, duration_s=0.0, meta_count=meta_count or 0,
        extracted_pct=float(extracted_pct or 0), conf_pct=float(conf_pct or 0),
        review_count=review_count or 0, emb_counts=emb_counts, dims=dims,
        zero_pct=zero_pct, n_apps=n_apps or 0, n_cats=n_cats or 0)
    log.info("[run_id=%s] SUMMARY %s", ctx.run_id, line)
    if status == "paused_by_audit":
        slack.post_slack(config.SLACK_WEBHOOK_URL,
                         slack.audit_failed_message(ctx.run_id, ["see audit_results"], "(airflow log)"))
    slack.post_slack(config.SLACK_WEBHOOK_URL, slack.run_finished_message(line))
