from rico_pipeline.stages import parse_hierarchy, text_representation

HIER = '{"activity": {"root": {"class": "android.widget.FrameLayout", "bounds": [0,0,100,100], "children": [{"class": "android.widget.TextView", "text": "Hello", "bounds": [0,10,50,20]}, {"class": "android.widget.TextView", "text": "World", "bounds": [0,0,50,10]}]}}}'

def test_parse_extracts_text_nodes():
    els = parse_hierarchy(HIER)
    texts = [t for _, t, _ in els if t]
    assert "Hello" in texts and "World" in texts

def test_text_representation_reading_order():
    # "World" has smaller y_top (0) than "Hello" (10), so it comes first.
    rep = text_representation(parse_hierarchy(HIER))
    assert rep == "World Hello"
