"""
Root conftest.py — shared pytest fixtures and configuration.
"""

import pytest

# ---------------------------------------------------------------------------
# Environment setup for unit tests
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _set_default_env(monkeypatch):
    """
    Set minimal environment variables so unit tests don't require a running
    Docker environment.  Individual tests can override these via monkeypatch.
    """
    monkeypatch.setenv("GRAPH_API_KEY", "")
    monkeypatch.setenv("MINIO_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("NESSIE_URI", "http://localhost:19120/api/v2")
