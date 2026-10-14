import random
import pytest

@pytest.fixture(autouse=True)
def set_random_seed():
    random.seed(42)
    yield