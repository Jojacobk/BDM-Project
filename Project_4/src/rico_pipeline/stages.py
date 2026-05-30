from __future__ import annotations

import io
import itertools
import json
import logging
import time
from pathlib import Path

import numpy as np
import requests
from PIL import Image

from rico_pipeline.config import Settings
from rico_pipeline.constants import (
    CLIP_ARCH,
    CLIP_MODEL_NAME,
    CLIP_MODEL_VERSION,
    CLIP_PRETRAINED,
    PROMPT_VERSION,
    SBERT_MODEL_NAME,
    SBERT_MODEL_VERSION,
)
from rico_pipeline.db import connect
from rico_pipeline.fingerprints import sha256_hex
from rico_pipeline.hierarchy import parse_hierarchy, text_representation
from rico_pipeline.prompt import load_prompt
from rico_pipeline.storage import s3_client

log = logging.getLogger(__name__)


def _chosen_ids() -> list[int]:
    path = Path("/opt/airflow/config/chosen_screens.txt")
    if not path.exists():
        path = Path(__file__).resolve().parents[3] / "config" / "chosen_screens.txt"
    return [
        int(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]


def _selected_rows(limit: int) -> list[dict]:
    from datasets import load_dataset

    chosen = _chosen_ids()
    chosen_target = set(chosen[: min(limit, len(chosen))])
    rows_by_id: dict[int, dict] = {}
    ds = load_dataset(
        "rootsautomation/RICO-Screen2Words",
        split="train",
        streaming=True,
        trust_remote_code=True,
    )
    for row in itertools.islice(ds, max(500, limit * 50)):
        sid = int(row["screenId"])
        if sid in chosen_target or (limit > len(chosen) and len(rows_by_id) < limit):
            rows_by_id.setdefault(sid, row)
        if chosen_target.issubset(rows_by_id) and len(rows_by_id) >= limit:
            break
    return [rows_by_id[sid] for sid in sorted(rows_by_id)[:limit]]


def ingest(settings: Settings, run_id: str, limit: int) -> dict[str, int]:
    started = time.perf_counter()
    s3 = s3_client(settings)
    rows = _selected_rows(limit)
    with connect(settings) as conn, conn.cursor() as cur:
        for row in rows:
            sid = int(row["screenId"])
            png_key = f"screens/{sid}.png"
            hier_key = f"screens/{sid}.json"

            png_buf = io.BytesIO()
            row["image"].save(png_buf, format="PNG")
            png_bytes = png_buf.getvalue()
            hierarchy_json = row["view_hierarchy"]

            s3.put_object(Bucket=settings.minio_bucket, Key=png_key, Body=png_bytes)
            s3.put_object(
                Bucket=settings.minio_bucket,
                Key=hier_key,
                Body=hierarchy_json.encode("utf-8"),
            )
            fingerprint = sha256_hex(png_bytes)
            cur.execute(
                """
                UPDATE screens_metadata
                SET run_id = %s,
                    source_fingerprint = %s,
                    app_package = %s,
                    category = %s,
                    png_path = %s,
                    hierarchy_json_path = %s,
                    hierarchy_text = NULL,
                    extraction_payload = NULL,
                    prompt_version = NULL,
                    confidence = NULL,
                    updated_at = NOW()
                WHERE screen_id = %s
                """,
                (
                    run_id,
                    fingerprint,
                    row["app_package_name"],
                    row["category"],
                    png_key,
                    hier_key,
                    sid,
                ),
            )
            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO screens_metadata (
                        screen_id, run_id, source_fingerprint, app_package,
                        category, png_path, hierarchy_json_path
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        sid,
                        run_id,
                        fingerprint,
                        row["app_package_name"],
                        row["category"],
                        png_key,
                        hier_key,
                    ),
                )
            log.info("run_id=%s ingested screen_id=%s", run_id, sid)
        conn.commit()
    return {"rows_in": len(rows), "rows_out": len(rows), "seconds": time.perf_counter() - started}


def parse(settings: Settings, run_id: str) -> dict[str, int]:
    started = time.perf_counter()
    s3 = s3_client(settings)
    rows_out = 0
    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT screen_id, hierarchy_json_path
            FROM screens_metadata
            WHERE run_id = %s
            ORDER BY screen_id
            """,
            (run_id,),
        )
        rows = cur.fetchall()
        for sid, hierarchy_json_path in rows:
            raw_json = (
                s3.get_object(Bucket=settings.minio_bucket, Key=hierarchy_json_path)[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )
            text = text_representation(parse_hierarchy(raw_json))
            cur.execute(
                """
                UPDATE screens_metadata
                SET hierarchy_text = %s, updated_at = NOW()
                WHERE screen_id = %s AND run_id = %s
                """,
                (text, sid, run_id),
            )
            rows_out += 1
        conn.commit()
    return {"rows_in": len(rows), "rows_out": rows_out, "seconds": time.perf_counter() - started}


def embed_image(settings: Settings, run_id: str) -> dict[str, int]:
    started = time.perf_counter()
    import open_clip
    import torch

    s3 = s3_client(settings)
    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT screen_id, png_path
            FROM screens_metadata
            WHERE run_id = %s
            ORDER BY screen_id
            """,
            (run_id,),
        )
        rows = cur.fetchall()

    model, _, preprocess = open_clip.create_model_and_transforms(
        CLIP_ARCH, pretrained=CLIP_PRETRAINED
    )
    model.eval()
    batch = []
    png_bytes_by_sid: dict[int, bytes] = {}
    for sid, png_path in rows:
        blob = s3.get_object(Bucket=settings.minio_bucket, Key=png_path)["Body"].read()
        png_bytes_by_sid[int(sid)] = blob
        batch.append(preprocess(Image.open(io.BytesIO(blob)).convert("RGB")))

    if not batch:
        return {"rows_in": 0, "rows_out": 0, "seconds": time.perf_counter() - started}

    images_tensor = torch.stack(batch)
    with torch.no_grad():
        vectors = model.encode_image(images_tensor)
        vectors = vectors / vectors.norm(dim=-1, keepdim=True)
    vectors_np = vectors.cpu().numpy().astype("float32")

    rows_out = _insert_embeddings(
        settings,
        run_id,
        rows,
        vectors_np,
        CLIP_MODEL_NAME,
        CLIP_MODEL_VERSION,
        "image",
        {sid: sha256_hex(blob) for sid, blob in png_bytes_by_sid.items()},
    )
    return {"rows_in": len(rows), "rows_out": rows_out, "seconds": time.perf_counter() - started}


def embed_text(settings: Settings, run_id: str) -> dict[str, int]:
    started = time.perf_counter()
    from sentence_transformers import SentenceTransformer

    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT screen_id, hierarchy_text
            FROM screens_metadata
            WHERE run_id = %s
            ORDER BY screen_id
            """,
            (run_id,),
        )
        rows = cur.fetchall()

    corpus = [text or "" for _, text in rows]
    if not corpus:
        return {"rows_in": 0, "rows_out": 0, "seconds": time.perf_counter() - started}

    model = SentenceTransformer(SBERT_MODEL_VERSION)
    vectors_np = model.encode(corpus, normalize_embeddings=True).astype("float32")
    rows_out = _insert_embeddings(
        settings,
        run_id,
        rows,
        vectors_np,
        SBERT_MODEL_NAME,
        SBERT_MODEL_VERSION,
        "text",
        {int(sid): sha256_hex(text or "") for sid, text in rows},
    )
    return {"rows_in": len(rows), "rows_out": rows_out, "seconds": time.perf_counter() - started}


def _insert_embeddings(
    settings: Settings,
    run_id: str,
    rows: list[tuple],
    vectors_np: np.ndarray,
    model_name: str,
    model_version: str,
    embedding_kind: str,
    fingerprints: dict[int, str],
) -> int:
    rows_out = 0
    with connect(settings, vectors=True) as conn, conn.cursor() as cur:
        for (sid, _), vec in zip(rows, vectors_np, strict=True):
            cur.execute(
                """
                UPDATE screens_embeddings
                SET run_id = %s,
                    source_fingerprint = %s,
                    vector = %s,
                    created_at = NOW()
                WHERE screen_id = %s
                  AND model_name = %s
                  AND model_version = %s
                  AND embedding_kind = %s
                """,
                (
                    run_id,
                    fingerprints[int(sid)],
                    vec,
                    sid,
                    model_name,
                    model_version,
                    embedding_kind,
                ),
            )
            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO screens_embeddings (
                        screen_id, run_id, source_fingerprint, model_name,
                        model_version, embedding_kind, vector
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        sid,
                        run_id,
                        fingerprints[int(sid)],
                        model_name,
                        model_version,
                        embedding_kind,
                        vec,
                    ),
                )
                rows_out += 1
            else:
                rows_out += cur.rowcount
        conn.commit()
    return rows_out


def extract(settings: Settings, run_id: str) -> dict[str, int]:
    started = time.perf_counter()
    prompt_version, prompt_template = load_prompt(settings.prompt_path)
    rows_out = 0
    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT screen_id, hierarchy_text, source_fingerprint
            FROM screens_metadata
            WHERE run_id = %s
            ORDER BY screen_id
            """,
            (run_id,),
        )
        rows = cur.fetchall()
        for sid, hierarchy_text, source_fingerprint in rows:
            raw_output = ""
            try:
                payload, raw_output = _extract_one(
                    settings, prompt_template, hierarchy_text or ""
                )
                confidence = float(payload.get("confidence", 0.0))
                body = {key: value for key, value in payload.items() if key != "confidence"}
                cur.execute(
                    """
                    UPDATE screens_metadata
                    SET extraction_payload = %s::jsonb,
                        prompt_version = %s,
                        confidence = %s,
                        updated_at = NOW()
                    WHERE screen_id = %s AND run_id = %s
                    """,
                    (json.dumps(body), prompt_version, confidence, sid, run_id),
                )
                cur.execute(
                    "DELETE FROM screens_review_queue WHERE screen_id = %s",
                    (sid,),
                )
                rows_out += 1
            except Exception as exc:  # noqa: BLE001
                cur.execute(
                    """
                    UPDATE screens_review_queue
                    SET run_id = %s,
                        source_fingerprint = %s,
                        reason = %s,
                        raw_output = %s,
                        created_at = NOW()
                    WHERE screen_id = %s
                    """,
                    (run_id, source_fingerprint, str(exc), raw_output, sid),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        """
                        INSERT INTO screens_review_queue (
                            screen_id, run_id, source_fingerprint, reason, raw_output
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (sid, run_id, source_fingerprint, str(exc), raw_output),
                    )
                log.warning("run_id=%s screen_id=%s extraction failed: %s", run_id, sid, exc)
        conn.commit()
    return {"rows_in": len(rows), "rows_out": rows_out, "seconds": time.perf_counter() - started}


def _extract_one(settings: Settings, prompt_template: str, hierarchy_text: str) -> tuple[dict, str]:
    prompt = prompt_template.replace("{hierarchy_text}", hierarchy_text)
    response = requests.post(
        f"{settings.ollama_url}/api/generate",
        json={"model": settings.ollama_model, "prompt": prompt, "stream": False},
        timeout=120,
    )
    response.raise_for_status()
    raw = response.json()["response"]
    return json.loads(raw), raw


def load(settings: Settings, run_id: str) -> dict[str, int]:
    started = time.perf_counter()
    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                count(*)::int,
                count(hierarchy_text)::int
            FROM screens_metadata
            WHERE run_id = %s
            """,
            (run_id,),
        )
        metadata_count, parsed_count = cur.fetchone()
        cur.execute(
            """
            SELECT
                count(*) FILTER (WHERE embedding_kind = 'image')::int,
                count(*) FILTER (WHERE embedding_kind = 'text')::int
            FROM screens_embeddings
            WHERE run_id = %s
            """,
            (run_id,),
        )
        image_count, text_count = cur.fetchone()
        cur.execute(
            """
            SELECT count(*)::int
            FROM screens_review_queue
            WHERE run_id = %s
            """,
            (run_id,),
        )
        review_queue_count = cur.fetchone()[0]

    failures = []
    if metadata_count == 0:
        failures.append("metadata_count=0")
    if parsed_count != metadata_count:
        failures.append(f"parsed_count={parsed_count}, metadata_count={metadata_count}")
    if image_count != metadata_count:
        failures.append(f"image_count={image_count}, metadata_count={metadata_count}")
    if text_count != metadata_count:
        failures.append(f"text_count={text_count}, metadata_count={metadata_count}")
    if failures:
        raise RuntimeError(f"load validation failed for run_id={run_id}: {failures}")

    loaded_count = metadata_count + image_count + text_count + review_queue_count
    return {
        "rows_in": metadata_count + image_count + text_count,
        "rows_out": loaded_count,
        "metadata_rows": metadata_count,
        "image_embedding_rows": image_count,
        "text_embedding_rows": text_count,
        "review_queue_rows": review_queue_count,
        "seconds": time.perf_counter() - started,
    }


def eval_recall(settings: Settings, run_id: str) -> dict[str, float]:
    started = time.perf_counter()
    with connect(settings, vectors=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT screen_id, hierarchy_text
            FROM screens_metadata
            WHERE run_id = %s
            ORDER BY screen_id
            """,
            (run_id,),
        )
        rows = cur.fetchall()
    if not rows:
        recall = 0.0
    else:
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(SBERT_MODEL_VERSION)
        hits = 0
        with connect(settings, vectors=True) as conn, conn.cursor() as cur:
            for expected_id, text in rows:
                qvec = model.encode([text or ""], normalize_embeddings=True).astype("float32")[0]
                cur.execute(
                    """
                    SELECT screen_id
                    FROM screens_embeddings
                    WHERE run_id = %s AND embedding_kind = 'text'
                    ORDER BY vector <-> %s::vector
                    LIMIT 5
                    """,
                    (run_id, qvec),
                )
                top = [row[0] for row in cur.fetchall()]
                if expected_id in top:
                    hits += 1
        recall = hits / len(rows)

    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO screens_eval (
                run_id, embedding_model_version, n_queries, recall_at_5
            )
            VALUES (%s, %s, %s, %s)
            """,
            (run_id, SBERT_MODEL_VERSION, len(rows), recall),
        )
        conn.commit()
    return {
        "rows_in": len(rows),
        "rows_out": 1,
        "recall_at_5": recall,
        "seconds": time.perf_counter() - started,
    }
