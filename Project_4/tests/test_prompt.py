from pathlib import Path

from rico_pipeline.prompt import load_prompt


def test_load_prompt_returns_version_and_content(tmp_path: Path):
    prompt = tmp_path / "prompt.txt"
    prompt.write_text("Visible text: {hierarchy_text}", encoding="utf-8")

    version, content = load_prompt(str(prompt))

    assert version == "v1"
    assert "{hierarchy_text}" in content
