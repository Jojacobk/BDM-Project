from rico_pipeline.hierarchy import parse_hierarchy, text_representation


def test_parse_hierarchy_and_text_representation_reading_order():
    raw = """
    {
      "activity": {
        "root": {
          "class": "android.widget.LinearLayout",
          "children": [
            {"class": "android.widget.TextView", "text": "Bottom", "bounds": [0, 100, 10, 120]},
            {"class": "android.widget.TextView", "text": "Top", "bounds": [0, 0, 10, 20]}
          ]
        }
      }
    }
    """

    elements = parse_hierarchy(raw)

    assert ("TextView", "Top", (0, 0, 10, 20)) in elements
    assert text_representation(elements) == "Top Bottom"
