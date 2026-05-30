from pathlib import Path


def test_load_validates_required_destination_rows_before_audit():
    src = Path("src/rico_pipeline/stages.py").read_text(encoding="utf-8")

    assert "load validation failed" in src
    assert "parsed_count != metadata_count" in src
    assert "image_count != metadata_count" in src
    assert "text_count != metadata_count" in src
