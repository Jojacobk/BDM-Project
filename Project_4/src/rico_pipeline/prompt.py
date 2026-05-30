from __future__ import annotations

from pathlib import Path

from rico_pipeline.constants import PROMPT_VERSION


def load_prompt(path: str) -> tuple[str, str]:
    return PROMPT_VERSION, Path(path).read_text(encoding="utf-8")
