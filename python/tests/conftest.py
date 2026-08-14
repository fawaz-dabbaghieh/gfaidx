import os
from pathlib import Path

import pytest


@pytest.fixture
def indexed_graph_path():
    value = os.environ.get("GFAIDX_TEST_GRAPH")
    if not value:
        pytest.skip("GFAIDX_TEST_GRAPH is not set")
    return Path(value)
