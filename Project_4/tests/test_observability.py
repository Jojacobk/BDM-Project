import numpy as np
from rico_pipeline.observability import zero_norm_fraction, summary_line


def test_zero_norm_fraction():
    vecs = [np.zeros(4), np.array([1.0, 0, 0, 0]), np.zeros(4)]
    assert zero_norm_fraction(vecs) == 2 / 3


def test_summary_line_is_one_line():
    line = summary_line(run_id="u1", status="succeeded", duration_s=12.0,
                        meta_count=5, extracted_pct=100.0, conf_pct=80.0,
                        review_count=0, emb_counts={"image": 5, "text": 5},
                        dims={"image": 512, "text": 384}, zero_pct=0.0,
                        n_apps=5, n_cats=5)
    assert "\n" not in line
    assert "run=u1" in line and "status=succeeded" in line
