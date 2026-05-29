"""Pipeline stages. Each function is pure-ish: takes inputs + RunContext, returns counts."""
from __future__ import annotations
import itertools
import json
from io import BytesIO

from datasets import load_dataset
from PIL import Image

from rico_pipeline import config
from rico_pipeline.context import RunContext
from rico_pipeline.stores import ollama_generate, pg_connect, s3_client, guarded_insert_sql
from rico_pipeline.traceability import sha256_hex


def parse_hierarchy(raw_json: str) -> list[tuple[str, str, tuple[int, int, int, int]]]:
    """Iterative DFS — returns (element_type, text, bounds) for nodes with text or class."""
    tree = json.loads(raw_json)
    root = tree.get("activity", {}).get("root", tree) if isinstance(tree, dict) else None
    elements: list[tuple[str, str, tuple[int, int, int, int]]] = []
    stack = [root]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        text = (node.get("text") or "").strip()
        cls = (node.get("class") or "").strip()
        if text or cls:
            element_type = cls.rsplit(".", 1)[-1] if cls else ""
            raw_bounds = node.get("bounds") or [0, 0, 0, 0]
            bounds = tuple(int(b) for b in raw_bounds) if len(raw_bounds) == 4 else (0, 0, 0, 0)
            elements.append((element_type, text, bounds))
        children = node.get("children")
        if isinstance(children, list):
            stack.extend(reversed(children))
    return elements


def text_representation(elements) -> str:
    """Concatenate texts in reading order: sort by (y_top, x_left), join with spaces."""
    with_text = [e for e in elements if e[1]]
    in_order = sorted(with_text, key=lambda e: (e[2][1], e[2][0]))
    return " ".join(text for _, text, _ in in_order)


PROMPT_VERSION = "v1"
PROMPT_V1 = """\
You are a UI structure extractor for Android app screenshots.

Given the visible text from one screen's view hierarchy, return a single
JSON object with these fields:

- "title": a short string naming the screen (e.g. "Login", "Settings",
  "Search results"). Empty string if unclear.
- "elements": a list of {"type": string, "text": string} objects, one
  per salient interactive or informational element you can identify.
- "confidence": a number in [0.0, 1.0] expressing how confident you are
  in the extraction.

Visible text:
{hierarchy_text}

Respond with valid JSON only — no commentary, no Markdown fences.
"""


def parse_extraction(raw: str):
    """Parse the LLM's raw response. Returns (payload|None, ok: bool)."""
    try:
        return json.loads(raw), True
    except (json.JSONDecodeError, TypeError):
        return None, False


def extract_one(text_rep: str):
    """Ollama call → (payload|None, raw_text, ok)."""
    raw = ollama_generate(PROMPT_V1.replace("{hierarchy_text}", text_rep))
    payload, ok = parse_extraction(raw)
    return payload, raw, ok


def _chosen_ids() -> list[int]:
    with open("chosen_screens.txt") as f:
        return sorted({int(l) for l in f if l.strip() and not l.startswith("#")})


def ingest(ctx: RunContext) -> int:
    """Stream up to ctx.limit screens; PUT PNG+JSON to MinIO (skip if present);
    guarded-insert screens_metadata. Returns rows written."""
    chosen = _chosen_ids()[: ctx.limit]
    target = set(chosen)
    ds = load_dataset(config.DATASET, split="train", streaming=True, trust_remote_code=True)
    rows: dict[int, dict] = {}
    for row in itertools.islice(ds, 500):
        sid = int(row["screenId"])
        if sid in target:
            rows[sid] = row
            if len(rows) == len(target):
                break
    s3 = s3_client()
    existing = {o["Key"] for o in s3.list_objects_v2(
        Bucket=config.MINIO_BUCKET, Prefix="screens/").get("Contents", [])}
    sql = guarded_insert_sql(
        "screens_metadata",
        ["screen_id", "app_package", "category", "png_path", "hierarchy_json_path",
         "run_id", "source_fingerprint"],
        ["screen_id"],
    )
    written = 0
    with pg_connect() as conn, conn.cursor() as cur:
        for sid in chosen:
            row = rows[sid]
            png_key, hier_key = f"screens/{sid}.png", f"screens/{sid}.json"
            buf = BytesIO(); row["image"].save(buf, format="PNG")
            png_bytes = buf.getvalue()
            if png_key not in existing:
                s3.put_object(Bucket=config.MINIO_BUCKET, Key=png_key, Body=png_bytes)
            if hier_key not in existing:
                s3.put_object(Bucket=config.MINIO_BUCKET, Key=hier_key,
                              Body=row["view_hierarchy"].encode("utf-8"))
            fp = sha256_hex(png_bytes)
            cur.execute(sql, (sid, row["app_package_name"], row["category"],
                              png_key, hier_key, ctx.run_id, fp, sid))
            written += cur.rowcount
        conn.commit()
    return written


def embed_image(ctx: RunContext) -> int:
    import torch, open_clip, numpy as np
    chosen = _chosen_ids()[: ctx.limit]
    model, _, preprocess = open_clip.create_model_and_transforms(
        config.CLIP_ARCH, pretrained=config.CLIP_PRETRAINED)
    model.eval()
    s3 = s3_client()
    sql = guarded_insert_sql(
        "screens_embeddings",
        ["screen_id", "model_name", "model_version", "embedding_kind",
         "vector", "run_id", "source_fingerprint"],
        ["screen_id", "model_name", "model_version", "embedding_kind"],
    )
    written = 0
    with pg_connect() as conn, conn.cursor() as cur:
        for sid in chosen:
            blob = s3.get_object(Bucket=config.MINIO_BUCKET,
                                 Key=f"screens/{sid}.png")["Body"].read()
            img = Image.open(BytesIO(blob)).convert("RGB")
            with torch.no_grad():
                v = model.encode_image(torch.stack([preprocess(img)]))
                v = v / v.norm(dim=-1, keepdim=True)
            vec = v.cpu().numpy().astype("float32")[0]
            fp = sha256_hex(blob)
            cur.execute(sql, (sid, config.CLIP_MODEL_NAME, config.CLIP_MODEL_VERSION,
                              "image", vec, ctx.run_id, fp,
                              sid, config.CLIP_MODEL_NAME, config.CLIP_MODEL_VERSION, "image"))
            written += cur.rowcount
        conn.commit()
    return written


def _text_rep_for(sid: int, s3) -> str:
    raw = s3.get_object(Bucket=config.MINIO_BUCKET,
                        Key=f"screens/{sid}.json")["Body"].read().decode("utf-8")
    return text_representation(parse_hierarchy(raw))


def embed_text(ctx: RunContext) -> int:
    from sentence_transformers import SentenceTransformer
    chosen = _chosen_ids()[: ctx.limit]
    sbert = SentenceTransformer(config.SBERT_MODEL_VERSION)
    s3 = s3_client()
    sql = guarded_insert_sql(
        "screens_embeddings",
        ["screen_id", "model_name", "model_version", "embedding_kind",
         "vector", "run_id", "source_fingerprint"],
        ["screen_id", "model_name", "model_version", "embedding_kind"],
    )
    written = 0
    with pg_connect() as conn, conn.cursor() as cur:
        for sid in chosen:
            rep = _text_rep_for(sid, s3)
            vec = sbert.encode([rep], normalize_embeddings=True).astype("float32")[0]
            fp = sha256_hex(rep.encode("utf-8"))
            cur.execute(sql, (sid, config.SBERT_MODEL_NAME, config.SBERT_MODEL_VERSION,
                              "text", vec, ctx.run_id, fp,
                              sid, config.SBERT_MODEL_NAME, config.SBERT_MODEL_VERSION, "text"))
            written += cur.rowcount
        conn.commit()
    return written


def extract(ctx: RunContext) -> int:
    """LLM extraction → UPDATE screens_metadata; bad JSON → screens_review_queue."""
    chosen = _chosen_ids()[: ctx.limit]
    s3 = s3_client()
    rq_sql = guarded_insert_sql(
        "screens_review_queue",
        ["screen_id", "reason", "raw_output", "run_id", "source_fingerprint"],
        ["screen_id", "run_id"],
    )
    written = 0
    with pg_connect() as conn, conn.cursor() as cur:
        for sid in chosen:
            rep = _text_rep_for(sid, s3)
            payload, raw, ok = extract_one(rep)
            fp = sha256_hex(rep.encode("utf-8"))
            if ok:
                cur.execute(
                    "UPDATE screens_metadata SET extraction_payload=%s::jsonb, "
                    "prompt_version=%s, confidence=%s, updated_at=NOW() "
                    "WHERE screen_id=%s AND run_id=%s",
                    (json.dumps(payload), config.PROMPT_VERSION,
                     float(payload.get("confidence", 0.0)), sid, ctx.run_id))
            else:
                cur.execute(rq_sql, (sid, "invalid_json", raw, ctx.run_id, fp, sid, ctx.run_id))
            written += 1
        conn.commit()
    return written


def load(ctx: RunContext) -> int:
    """Consolidation/verification point. Returns total destination rows for this run."""
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM screens_metadata WHERE run_id=%s", (ctx.run_id,))
        meta = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM screens_embeddings WHERE run_id=%s", (ctx.run_id,))
        emb = cur.fetchone()[0]
    return meta + emb
