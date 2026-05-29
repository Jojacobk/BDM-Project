from rico_pipeline.stages import parse_extraction, PROMPT_V1, PROMPT_VERSION


def test_parse_valid_json():
    payload, ok = parse_extraction('{"title":"Login","elements":[],"confidence":0.9}')
    assert ok is True and payload["confidence"] == 0.9


def test_parse_bad_json_routes_to_review():
    payload, ok = parse_extraction("not json{{")
    assert ok is False and payload is None


def test_prompt_version_pinned():
    assert PROMPT_VERSION == "v1"
    assert "{hierarchy_text}" in PROMPT_V1
