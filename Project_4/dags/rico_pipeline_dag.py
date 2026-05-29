"""Thin orchestration for the RICO pipeline. All logic lives in rico_pipeline/."""
from __future__ import annotations
import logging
import uuid

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from rico_pipeline import config, stages, audit as audit_mod, slack
from rico_pipeline.context import RunContext
from rico_pipeline.stores import pg_connect
from rico_pipeline.traceability import current_git_sha, start_run_sql

log = logging.getLogger("rico_pipeline")


def _ctx_from_xcom(ti) -> RunContext:
    return RunContext.from_dict(ti.xcom_pull(task_ids="start_run"))


@dag(dag_id="rico_pipeline", schedule=None, start_date=pendulum.datetime(2026, 1, 1),
     catchup=False, tags=["rico"])
def rico_pipeline():

    @task
    def start_run() -> dict:
        ctx_air = get_current_context()
        conf = ctx_air["dag_run"].conf or {}
        limit = int(conf.get("limit", config.DEFAULT_LIMIT))
        trigger = conf.get("trigger_source", "manual")
        ctx = RunContext(
            run_id=str(uuid.uuid4()),
            dag_run_id=ctx_air["dag_run"].run_id,
            limit=limit, git_sha=current_git_sha(), trigger_source=trigger,
            clip_version=config.CLIP_MODEL_VERSION, sbert_version=config.SBERT_MODEL_VERSION,
            llm_model=config.OLLAMA_MODEL, prompt_version=config.PROMPT_VERSION,
        )
        with pg_connect() as conn, conn.cursor() as cur:
            sql, params = start_run_sql(ctx)
            cur.execute(sql, params)
            conn.commit()
        slack.post_slack(config.SLACK_WEBHOOK_URL,
                         slack.run_started_message(ctx.run_id, limit, trigger))
        log.info("[run_id=%s] started LIMIT=%s", ctx.run_id, limit)
        return ctx.to_dict()

    @task
    def ingest():
        ctx = _ctx_from_xcom(get_current_context()["ti"]); return stages.ingest(ctx)

    @task
    def parse():
        # parsing is performed inside embed_text/extract from MinIO; this node
        # exists to mirror the lab stage order and gate the parallel fan-out.
        ctx = _ctx_from_xcom(get_current_context()["ti"]); log.info("[run_id=%s] parse ok", ctx.run_id)

    @task
    def embed_image():
        ctx = _ctx_from_xcom(get_current_context()["ti"]); return stages.embed_image(ctx)

    @task
    def embed_text():
        ctx = _ctx_from_xcom(get_current_context()["ti"]); return stages.embed_text(ctx)

    @task
    def extract():
        ctx = _ctx_from_xcom(get_current_context()["ti"]); return stages.extract(ctx)

    @task
    def load():
        ctx = _ctx_from_xcom(get_current_context()["ti"]); return stages.load(ctx)

    @task
    def audit():
        ctx = _ctx_from_xcom(get_current_context()["ti"])
        with pg_connect() as conn, conn.cursor() as cur:
            passed, details = audit_mod.evaluate_audit(cur)
            sql, params = audit_mod.persist_audit_sql(ctx.run_id, "duplicate_detection", passed, details)
            cur.execute(sql, params); conn.commit()
        if not passed:
            log.error("[run_id=%s] AUDIT FAILED: %s", ctx.run_id, details)
            raise audit_mod.AuditFailed(str(details))
        log.info("[run_id=%s] audit passed", ctx.run_id)

    @task(task_id="eval")
    def eval_():
        ctx = _ctx_from_xcom(get_current_context()["ti"]); stages.eval_recall(ctx)

    @task(trigger_rule="all_done")
    def finalize_run():
        from rico_pipeline.dag_support import finalize
        finalize(get_current_context())

    s = start_run()
    i = ingest(); p = parse()
    ei, et, ex = embed_image(), embed_text(), extract()
    l = load(); a = audit(); e = eval_(); f = finalize_run()

    s >> i >> p >> [ei, et, ex] >> l >> a >> e >> f

dag = rico_pipeline()
