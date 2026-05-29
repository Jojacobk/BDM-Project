from rico_pipeline import slack

def test_post_swallows_errors(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("network down")
    monkeypatch.setattr(slack.requests, "post", boom)
    # must NOT raise — notifications are not a pipeline dependency
    assert slack.post_slack("https://hook", "hi") is False

def test_post_noops_without_url():
    assert slack.post_slack("", "hi") is False

def test_audit_failed_message_lists_keys():
    msg = slack.audit_failed_message("u1", [{"screen_id": 2, "count": 2}], "http://log")
    assert "u1" in msg and "screen_id" in msg and "http://log" in msg
