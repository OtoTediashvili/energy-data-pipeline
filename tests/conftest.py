"""Shared fixtures.

Every test gets its own temp data directory, so tests never see each other's
files and can run in parallel.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import SecretStr

from pipeline.config import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Settings pointed at a throwaway directory."""
    cfg = Settings(
        env="test",
        source_base_url="https://source.test/api",
        source_api_key=SecretStr("test-key"),
        data_dir=tmp_path / "data",
        duckdb_path=tmp_path / "data" / "warehouse" / "test.duckdb",
        max_retries=1,
    )
    cfg.ensure_dirs()
    return cfg


@pytest.fixture
def sample_payload() -> bytes:
    """A small newline-delimited JSON payload shaped like a real extract."""
    return (
        b'{"id": 1, "country": "GE", "value": 41.5}\n'
        b'{"id": 2, "country": "DE", "value": 87.2}\n'
        b'{"id": 3, "country": "FR", "value": 55.0}\n'
    )


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Stop a developer's real .env leaking into the test run."""
    for key in [k for k in os.environ if k.startswith("PIPELINE_")]:
        monkeypatch.delenv(key, raising=False)
    yield
