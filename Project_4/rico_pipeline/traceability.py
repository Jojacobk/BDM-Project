"""Traceability: fingerprints, git_sha, and pipeline_runs lifecycle."""
from __future__ import annotations
import hashlib
import os
import subprocess


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def current_git_sha() -> str:
    env = os.getenv("GIT_SHA")
    if env:
        return env
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
    except Exception:
        return "unknown"
