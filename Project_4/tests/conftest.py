import numpy as np
import pytest


@pytest.fixture
def zero_vec():
    return np.zeros(512, dtype="float32")


@pytest.fixture
def unit_vec():
    v = np.zeros(512, dtype="float32")
    v[0] = 1.0
    return v
