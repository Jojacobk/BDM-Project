from pathlib import Path


def test_extract_updates_review_queue_instead_of_appending():
    src = Path("src/rico_pipeline/stages.py").read_text(encoding="utf-8")

    assert "UPDATE screens_review_queue" in src
    assert "WHERE screen_id = %s" in src
    assert "DELETE FROM screens_review_queue WHERE screen_id = %s" in src
