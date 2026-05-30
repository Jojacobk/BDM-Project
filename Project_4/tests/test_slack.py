import logging

from rico_pipeline.config import Settings
from rico_pipeline.slack import post_slack


def test_missing_slack_webhook_does_not_raise(caplog):
    settings = Settings(
        postgres_dsn="postgresql://example",
        minio_url="http://minio",
        minio_key="key",
        minio_secret="secret",
        minio_bucket="bucket",
        ollama_url="http://ollama",
        ollama_model="qwen2.5:3b",
        prompt_path="prompt.txt",
        default_limit=5,
        slack_webhook_url=None,
    )

    with caplog.at_level(logging.WARNING):
        post_slack(settings, "hello")

    assert "Slack webhook is not configured" in caplog.text
