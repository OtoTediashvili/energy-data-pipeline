"""Extraction tests.

The interesting cases are not "does a 200 work" but the failure modes:
retryable vs permanent errors, and whether a rerun duplicates data.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import httpx
import pytest
import respx
from structlog.testing import capture_logs

from pipeline.config import Settings
from pipeline.extract import (
    TOKEN_PARAM,
    PermanentSourceError,
    TransientSourceError,
    extract_to_landing,
    fetch,
    land,
    landing_path,
)
from pipeline.parse import parse_prices

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
def test_fetch_sends_token_as_query_param(settings: Settings) -> None:
    """ENTSO-E reads securityToken from the query string and ignores headers."""
    route = respx.get("https://source.test/api/prices").mock(
        return_value=httpx.Response(200, content=b"{}")
    )
    fetch("prices", settings=settings)
    request = route.calls.last.request
    assert request.url.params[TOKEN_PARAM] == "test-key"
    assert "Authorization" not in request.headers


@respx.mock
def test_fetch_with_empty_endpoint_calls_base_url(settings: Settings) -> None:
    """ENTSO-E is one endpoint: nothing may be appended to the base URL."""
    route = respx.get("https://source.test/api").mock(
        return_value=httpx.Response(200, content=b"<ok/>")
    )
    assert fetch(settings=settings, params={"documentType": "A44"}) == b"<ok/>"
    assert route.calls.last.request.url.path == "/api"


@respx.mock
def test_fetch_never_logs_the_token(settings: Settings) -> None:
    """The token travels in the URL, so it must stay out of every log event.

    Airflow stores task logs and shows them in its UI: a token logged once is
    exposed to anyone who can read that run.
    """
    respx.get("https://source.test/api").mock(return_value=httpx.Response(200, content=b"<ok/>"))
    with capture_logs() as events:
        fetch(settings=settings, params={"documentType": "A44"})
    assert events, "expected fetch to emit log events"
    assert all("test-key" not in repr(event) for event in events)


@respx.mock
def test_no_library_log_line_carries_the_token(
    settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    """The test above sees only our structured events. httpx writes through
    standard logging, at INFO, with the full URL: query string and token. Under
    Airflow that lands in task logs. Capture every logger, at every level."""
    respx.get("https://source.test/api").mock(return_value=httpx.Response(200, content=b"<ok/>"))
    with caplog.at_level(logging.DEBUG):
        fetch(settings=settings, params={"documentType": "A44"})
    assert "test-key" not in caplog.text


@respx.mock
def test_a_failed_request_cannot_leak_the_token_through_a_traceback(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Airflow 3 prints tracebacks with each frame's local variables, and inside
    the HTTP call those locals hold the token. Run the real retry path to its
    final re-raise, then walk every frame a renderer could reach, chained
    exceptions included, and check every local."""
    monkeypatch.setattr("time.sleep", lambda _seconds: None)  # skip backoff waits
    route = respx.get("https://source.test/api").mock(
        side_effect=httpx.ConnectError("Connection refused")
    )
    with pytest.raises(TransientSourceError) as caught:
        fetch(settings=settings)
    assert route.call_count == 5, "a transport failure must still be retried"

    exc: BaseException | None = caught.value
    assert "Connection refused" in str(caught.value), "the cause must stay diagnosable"
    while exc is not None:
        frame = exc.__traceback__
        while frame is not None:
            for name, value in frame.tb_frame.f_locals.items():
                assert "test-key" not in repr(value), f"token in local '{name}'"
            frame = frame.tb_next
        exc = exc.__cause__ or exc.__context__


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


@pytest.mark.integration
def test_live_entsoe_day_ahead_prices_parse() -> None:
    """Hits the real ENTSO-E API with the token from .env.

    Excluded from CI by its marker and skipped when no token is configured. Run
    locally with `make test-all`. The only test that checks the real contract
    (parameter names, endpoint shape, auth) rather than our model of it.
    """
    cfg = Settings(source_base_url="https://web-api.tp.entsoe.eu/api")
    if not cfg.source_api_key.get_secret_value():
        pytest.skip("no ENTSO-E token in .env")
    zone = "10YNL----------L"
    body = fetch(
        settings=cfg,
        params={
            "documentType": "A44",
            "in_Domain": zone,
            "out_Domain": zone,
            "periodStart": "202609240000",
            "periodEnd": "202609242300",
        },
    )
    rows = parse_prices(body)
    assert len(rows) >= 96
    assert {r.bidding_zone for r in rows} == {zone}


def test_landing_path_with_a_zone(settings: Settings) -> None:
    path = landing_path("prices", LOGICAL_DATE, settings, suffix=".xml", zone="NL")
    assert path.parent.parts[-4:] == ("dataset=prices", "zone=NL", "year=2026", "month=09")
    assert path.name == "prices_NL_2026-09-13.xml"


def test_landing_path_suffix_records_format(settings: Settings) -> None:
    path = landing_path("prices", LOGICAL_DATE, settings, suffix=".xml")
    assert path.name == "prices_2026-09-13.xml"


ACK = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b"<Acknowledgement_MarketDocument"
    b' xmlns="urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0">'
    b"<Reason><code>999</code><text>No matching data found</text></Reason>"
    b"</Acknowledgement_MarketDocument>"
)


def _landed_files(settings: Settings) -> list[Path]:
    return [p for p in settings.landing_dir.rglob("*") if p.is_file()]


def test_land_refuses_an_acknowledgement(settings: Settings) -> None:
    with pytest.raises(PermanentSourceError, match="Acknowledgement"):
        land(ACK, "prices", LOGICAL_DATE, settings, suffix=".xml")
    assert _landed_files(settings) == []


def test_an_acknowledgement_says_why(settings: Settings) -> None:
    """ENTSO-E's reason sits after the sender and receiver headers. The error
    must carry it, or every refusal reads the same in the task log."""
    padded = ACK.replace(
        b"<Reason>",
        b"<mRID>00487115-7699-4</mRID><createdDateTime>2026-10-10T02:25:28Z</createdDateTime>"
        + b"<sender_MarketParticipant.mRID codingScheme='A01'>10X1001A1001A450</sender_MarketParticipant.mRID>"
        * 4
        + b"<Reason>",
    )
    assert len(padded) > 400, "the reason must sit beyond the old 400-byte preview"
    with pytest.raises(PermanentSourceError, match="999: No matching data found"):
        land(padded, "prices", LOGICAL_DATE, settings, suffix=".xml")


def test_an_unparseable_acknowledgement_still_fails_loudly(settings: Settings) -> None:
    broken = b"<Acknowledgement_MarketDocument><Reason><code>999"
    with pytest.raises(PermanentSourceError, match="Acknowledgement_MarketDocument"):
        land(broken, "prices", LOGICAL_DATE, settings, suffix=".xml")


def test_acknowledgement_cannot_overwrite_good_landed_data(settings: Settings) -> None:
    """The failure this guard exists for: a rerun on a bad day must never
    destroy the one raw copy of a good day."""
    good = b"<Publication_MarketDocument>real data</Publication_MarketDocument>"
    path = land(good, "prices", LOGICAL_DATE, settings, suffix=".xml")
    with pytest.raises(PermanentSourceError):
        land(ACK, "prices", LOGICAL_DATE, settings, suffix=".xml")
    assert path.read_bytes() == good


@respx.mock
def test_extract_does_not_land_a_200_acknowledgement(settings: Settings) -> None:
    """ENTSO-E can answer 'no data' with HTTP 200, so fetch() itself succeeds."""
    respx.get("https://source.test/api").mock(return_value=httpx.Response(200, content=ACK))
    with pytest.raises(PermanentSourceError):
        extract_to_landing("", "prices", LOGICAL_DATE, settings=settings, suffix=".xml")
    assert _landed_files(settings) == []
