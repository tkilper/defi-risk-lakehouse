"""
Root conftest.py — shared pytest fixtures and configuration.
"""

import pytest

# ---------------------------------------------------------------------------
# Environment setup for unit tests
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _set_default_env(monkeymodule):
    """
    Set minimal environment variables so unit tests don't require a running
    Docker environment.  Individual tests can override these via monkeymodule.
    """
    monkeymodule.setenv("GRAPH_API_KEY", "")
    monkeymodule.setenv("MINIO_ENDPOINT", "http://localhost:9000")
    monkeymodule.setenv("MINIO_ACCESS_KEY", "minioadmin")
    monkeymodule.setenv("MINIO_SECRET_KEY", "minioadmin")
    monkeymodule.setenv("NESSIE_URI", "http://localhost:19120/api/v2")


@pytest.fixture(scope="module")
def monkeymodule():
    with pytest.MonkeyPatch.context() as mp:
        yield mp
