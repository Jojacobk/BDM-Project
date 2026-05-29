from rico_pipeline.traceability import sha256_hex, current_git_sha


def test_sha256_stable():
    assert sha256_hex(b"abc") == sha256_hex(b"abc")
    assert sha256_hex(b"abc") != sha256_hex(b"abd")
    assert len(sha256_hex(b"abc")) == 64


def test_git_sha_from_env(monkeypatch):
    monkeypatch.setenv("GIT_SHA", "deadbeef")
    assert current_git_sha() == "deadbeef"
