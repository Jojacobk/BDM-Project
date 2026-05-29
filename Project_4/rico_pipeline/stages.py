"""Pipeline stages. Each function is pure-ish: takes inputs + RunContext, returns counts."""
from __future__ import annotations
import json
from rico_pipeline.stores import ollama_generate


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
