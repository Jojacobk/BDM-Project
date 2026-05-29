from agent.agent import parse_limit_from_llm

def test_parse_limit_extracts_int():
    assert parse_limit_from_llm('{"intent":"run_pipeline","limit":20}') == 20

def test_parse_limit_defaults_on_garbage():
    assert parse_limit_from_llm("i could not parse") is None
