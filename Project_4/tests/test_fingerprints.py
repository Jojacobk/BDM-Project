from rico_pipeline.fingerprints import sha256_hex


def test_sha256_hex_accepts_bytes_and_text():
    assert sha256_hex("abc") == sha256_hex(b"abc")
    assert sha256_hex("abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
