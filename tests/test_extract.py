"""Extraction tests.

The interesting cases are not "does a 200 work" but the failure modes:
retryable vs permanent errors, and whether a rerun duplicates data.
"""

from __future__ import annotations

from datetime import date

import httpx
import pytest
import respx

from pipeline.config import Settings
from pipeline.extract import (
    PermanentSourceError,
    extract_to_landing,
    fetch,
    land,
    landing_path,
)

LOGICAL_DATE = date(2026, 9, 13)


def test_landing_path_is_deterministic(settings: Settings) -> None:
    first = landing_path("prices", LOGICAL_DATE, settings)
    second = landing_path("prices", LOGICAL_DATE, settings)
    assert first == second
    assert "year=2026" in str(first)
    assert "month=09" in str(first)


@respx.mock
def test_fetch_returns_body(settings: Settings) -> None:
    route = respx.get("https://source.test/api/prices").mock(
        return_value=httpx.Response(200, content=b'{"ok": true}')
    )
    body = fetch("prices", settings=settings)
    assert body == b'{"ok": true}'
    assert route.called


@respx.mock
def test_fetch_sends_auth_header(settings: Settings) -> None:
    route = respx.get("https://source.test/api/prices").mock(
        return_value=httpx.Response(200, content=b"{}")
    )
    fetch("prices", settings=settings)
    assert route.calls.last.request.headers["Authorization"] == "Bearer test-key"


@respx.mock
def test_fetch_retries_on_503_then_succeeds(settings: Settings) -> None:
    respx.get("https://source.test/api/prices").mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(503),
            httpx.Response(200, content=b"recovered"),
        ]
    )
    assert fetch("prices", settings=settings) == b"recovered"


@respx.mock
def test_fetch_does_not_retry_on_404(settings: Settings) -> None:
    route = respx.get("https://source.test/api/missing").mock(
        return_value=httpx.Response(404, text="no such dataset")
    )
    with pytest.raises(PermanentSourceError):
        fetch("missing", settings=settings)
    assert route.call_count == 1, "a 4xx must fail fast, not burn the retry budget"


def test_land_writes_payload(settings: Settings, sample_payload: bytes) -> None:
    path = land(sample_payload, "prices", LOGICAL_DATE, settings)
    assert path.exists()
    assert path.read_bytes() == sample_payload


def test_land_leaves_no_temp_files(settings: Settings, sample_payload: bytes) -> None:
    land(sample_payload, "prices", LOGICAL_DATE, settings)
    assert list(settings.landing_dir.rglob("*.tmp")) == []


def test_rerunning_the_same_date_overwrites(settings: Settings) -> None:
    """The core idempotency guarantee: a backfill must not duplicate."""
    land(b"first run", "prices", LOGICAL_DATE, settings)
    land(b"second run", "prices", LOGICAL_DATE, settings)

    files = list(settings.landing_dir.rglob("*.json"))
    assert len(files) == 1
    assert files[0].read_bytes() == b"second run"


@respx.mock
def test_extract_to_landing_end_to_end(settings: Settings, sample_payload: bytes) -> None:
    respx.get("https://source.test/api/prices").mock(
        return_value=httpx.Response(200, content=sample_payload)
    )
    path = extract_to_landing("prices", "prices", LOGICAL_DATE, settings=settings)
    assert path.read_bytes() == sample_payload
