"""Parser tests.

The real fixture proves the parser handles what ENTSO-E actually sends. The
synthetic documents cover what the real sample happened not to contain: gaps
in an A03 curve, a 25-hour DST day, hourly resolution, negative prices,
acknowledgements, a zone with two auctions, and the malformed cases that must
fail loudly.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pipeline.config import Settings
from pipeline.extract import land
from pipeline.parse import (
    PARQUET_SCHEMA,
    EntsoeAcknowledgementError,
    EntsoeParseError,
    PricePoint,
    parse_landed_file,
    parse_prices,
    parse_resolution,
    parsed_path,
    write_parquet,
)

FIXTURE = Path(__file__).parent / "fixtures" / "entsoe_a44_nl_20260924.xml"
TWO_AUCTIONS = Path(__file__).parent / "fixtures" / "entsoe_a44_at_two_auctions_20250930.xml"
NS = "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"
NL = "10YNL----------L"


def utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


def build_doc(
    points: list[tuple[int, float]],
    *,
    start: str = "2026-09-23T22:00Z",
    end: str = "2026-09-23T23:00Z",
    resolution: str = "PT15M",
    curve_type: str = "A03",
    namespace: str = NS,
    revision: int = 1,
    sequence: str | None = None,
) -> bytes:
    """A minimal A44 document with one series and one period."""
    point_xml = "".join(
        f"<Point><position>{pos}</position><price.amount>{price}</price.amount></Point>"
        for pos, price in points
    )
    tag = "classificationSequence_AttributeInstanceComponent.position"
    sequence_xml = f"<{tag}>{sequence}</{tag}>" if sequence is not None else ""
    return f"""<?xml version="1.0" encoding="utf-8"?>
<Publication_MarketDocument xmlns="{namespace}">
  <mRID>synthetic</mRID>
  <revisionNumber>{revision}</revisionNumber>
  <type>A44</type>
  <createdDateTime>2026-09-26T10:00:00Z</createdDateTime>
  <TimeSeries>
    <mRID>1</mRID>
    <in_Domain.mRID codingScheme="A01">{NL}</in_Domain.mRID>
    <out_Domain.mRID codingScheme="A01">{NL}</out_Domain.mRID>
    <currency_Unit.name>EUR</currency_Unit.name>
    <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
    {sequence_xml}
    <curveType>{curve_type}</curveType>
    <Period>
      <timeInterval><start>{start}</start><end>{end}</end></timeInterval>
      <resolution>{resolution}</resolution>
      {point_xml}
    </Period>
  </TimeSeries>
</Publication_MarketDocument>""".encode()


# ------------------------------------------------------------- real fixture


@pytest.fixture(scope="module")
def real_rows() -> list[PricePoint]:
    return parse_prices(FIXTURE.read_bytes())


def test_real_fixture_has_two_market_days_of_quarter_hours(real_rows: list[PricePoint]) -> None:
    assert len(real_rows) == 192
    assert {r.resolution_minutes for r in real_rows} == {15}
    assert {r.bidding_zone for r in real_rows} == {NL}


def test_real_fixture_first_points_match_source(real_rows: list[PricePoint]) -> None:
    by_start = {r.interval_start_utc: r for r in real_rows}
    assert by_start[utc(2026, 9, 23, 22, 0)].price_eur_mwh == 176.03
    assert by_start[utc(2026, 9, 24, 22, 0)].price_eur_mwh == 197.01


def test_real_fixture_natural_key_is_unique_and_contiguous(real_rows: list[PricePoint]) -> None:
    """(zone, interval_start) is the business key. It must be unique, and the
    two market days must tile the window with no gap or overlap."""
    starts = sorted(r.interval_start_utc for r in real_rows)
    assert len(starts) == len(set(starts))
    assert starts[0] == utc(2026, 9, 23, 22, 0)
    assert all(b - a == timedelta(minutes=15) for a, b in pairwise(starts))
    assert max(r.interval_end_utc for r in real_rows) == utc(2026, 9, 25, 22, 0)


def test_real_fixture_carries_document_lineage(real_rows: list[PricePoint]) -> None:
    """Every row traces back to exactly one source document.

    ENTSO-E regenerates the document mRID on every response, even for identical
    data, so it identifies a delivery rather than the data. Assert the invariant
    (one document, revision 1, a creation time), never a specific mRID value:
    that breaks the moment the fixture is re-fetched.
    """
    mrids = {r.document_mrid for r in real_rows}
    assert len(mrids) == 1
    assert mrids.pop()
    assert {r.revision_number for r in real_rows} == {1}
    assert all(
        r.document_created_utc is not None and r.document_created_utc.utcoffset() == timedelta(0)
        for r in real_rows
    )


def test_real_fixture_timestamps_are_utc_aware(real_rows: list[PricePoint]) -> None:
    assert all(r.interval_start_utc.utcoffset() == timedelta(0) for r in real_rows)


# ------------------------------------------------------------ two auctions


@pytest.fixture(scope="module")
def austria_rows() -> list[PricePoint]:
    return parse_prices(TWO_AUCTIONS.read_bytes())


def test_every_row_records_its_auction(austria_rows: list[PricePoint]) -> None:
    """Austria's document holds the main European auction (1), hourly until
    30 September, and EXAA's (2). Both are kept and labelled; staging decides."""
    counts = Counter((r.auction_sequence, r.resolution_minutes) for r in austria_rows)
    assert counts == {(1, 60): 24, (1, 15): 96, (2, 15): 192}


def test_only_the_label_tells_two_quarter_hourly_auctions_apart(
    austria_rows: list[PricePoint],
) -> None:
    """From 1 October both auctions price the same 96 quarter-hours, at
    different prices. By (zone, interval) they collide, which is how they
    mixed silently before the label was recorded."""
    first_15_minute_day = [r for r in austria_rows if r.interval_start_utc >= utc(2025, 9, 30, 22)]
    prices = {
        seq: {
            r.interval_start_utc: r.price_eur_mwh
            for r in first_15_minute_day
            if r.auction_sequence == seq
        }
        for seq in (1, 2)
    }
    assert prices[1].keys() == prices[2].keys()
    assert len(prices[1]) == 96
    assert all(prices[1][start] != prices[2][start] for start in prices[1])


@pytest.mark.parametrize("auction", [1, 2])
def test_each_auction_alone_tiles_the_window(austria_rows: list[PricePoint], auction: int) -> None:
    """Within one auction every interval is unique and the intervals neither
    overlap nor leave a gap, whatever the resolution."""
    rows = sorted(
        (r for r in austria_rows if r.auction_sequence == auction),
        key=lambda r: r.interval_start_utc,
    )
    assert all(a.interval_end_utc == b.interval_start_utc for a, b in pairwise(rows))
    assert rows[0].interval_start_utc == utc(2025, 9, 29, 22)
    assert rows[-1].interval_end_utc == utc(2025, 10, 1, 22)


def test_a_zone_with_one_auction_has_no_sequence(real_rows: list[PricePoint]) -> None:
    assert {r.auction_sequence for r in real_rows} == {None}


def test_a_labelled_series_records_its_number() -> None:
    rows = parse_prices(build_doc([(1, 10.0)], sequence="2"))
    assert {r.auction_sequence for r in rows} == {2}


def test_a_non_numeric_auction_sequence_is_an_error() -> None:
    with pytest.raises(EntsoeParseError, match="auction sequence"):
        parse_prices(build_doc([(1, 10.0)], sequence="first"))


# ------------------------------------------------------------ curve types


def test_a03_gaps_are_forward_filled_and_flagged() -> None:
    """Positions 2 and 4 omitted: ENTSO-E does this when the price is unchanged."""
    rows = parse_prices(build_doc([(1, 10.0), (3, 30.0)]))
    assert [(r.position, r.price_eur_mwh, r.is_filled) for r in rows] == [
        (1, 10.0, False),
        (2, 10.0, True),
        (3, 30.0, False),
        (4, 30.0, True),  # trailing gap is filled up to the period end too
    ]


def test_a01_gap_is_an_error_not_a_silent_fill() -> None:
    with pytest.raises(EntsoeParseError, match="must be complete"):
        parse_prices(build_doc([(1, 10.0), (3, 30.0)], curve_type="A01"))


def test_a03_missing_first_position_is_an_error() -> None:
    with pytest.raises(EntsoeParseError, match="nothing to forward-fill"):
        parse_prices(build_doc([(2, 10.0)]))


# ------------------------------------------------------------ time handling


def test_dst_fall_back_day_has_100_quarter_hours() -> None:
    """25 Oct 2026: clocks go back, so the CET market day lasts 25 hours."""
    doc = build_doc(
        [(1, 50.0)], start="2026-10-24T22:00Z", end="2026-10-25T23:00Z", curve_type="A03"
    )
    rows = parse_prices(doc)
    assert len(rows) == 100
    assert rows[-1].interval_end_utc == utc(2026, 10, 25, 23, 0)


def test_hourly_resolution() -> None:
    doc = build_doc(
        [(p, float(p)) for p in range(1, 25)],
        start="2026-09-23T22:00Z",
        end="2026-09-24T22:00Z",
        resolution="PT60M",
        curve_type="A01",
    )
    rows = parse_prices(doc)
    assert len(rows) == 24
    assert {r.resolution_minutes for r in rows} == {60}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("PT15M", 15), ("PT30M", 30), ("PT60M", 60), ("PT1H", 60)],
)
def test_parse_resolution(raw: str, expected: int) -> None:
    assert parse_resolution(raw) == timedelta(minutes=expected)


@pytest.mark.parametrize("raw", ["P1D", "PT", "15M", "PT15S"])
def test_parse_resolution_rejects_unsupported(raw: str) -> None:
    with pytest.raises(EntsoeParseError):
        parse_resolution(raw)


def test_negative_prices_parse() -> None:
    """Common in NL and DE on sunny, windy days."""
    rows = parse_prices(build_doc([(1, -12.5), (2, -0.01), (3, 0.0), (4, 4.2)], curve_type="A01"))
    assert [r.price_eur_mwh for r in rows] == [-12.5, -0.01, 0.0, 4.2]


# ------------------------------------------------------ malformed documents


def test_position_beyond_period_is_an_error() -> None:
    with pytest.raises(EntsoeParseError, match="outside"):
        parse_prices(build_doc([(1, 10.0), (5, 50.0)]))


def test_duplicate_position_is_an_error() -> None:
    with pytest.raises(EntsoeParseError, match="duplicate"):
        parse_prices(build_doc([(1, 10.0), (1, 11.0)]))


def test_interval_not_divisible_by_resolution_is_an_error() -> None:
    with pytest.raises(EntsoeParseError, match="whole number"):
        parse_prices(build_doc([(1, 10.0)], end="2026-09-23T22:50Z"))


def test_unexpected_root_is_an_error() -> None:
    with pytest.raises(EntsoeParseError, match="unexpected root"):
        parse_prices(b"<SomethingElse/>")


def test_namespace_version_bump_still_parses() -> None:
    """ENTSO-E versions its namespaces. A bump must not break ingestion."""
    rows = parse_prices(
        build_doc([(1, 1.0)], namespace="urn:iec62325.351:tc57wg16:451-3:publicationdocument:8:0")
    )
    assert len(rows) == 4


# -------------------------------------------------------- acknowledgements


ACK = b"""<?xml version="1.0" encoding="UTF-8"?>
<Acknowledgement_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0">
  <mRID>ack</mRID>
  <Reason><code>999</code><text>No matching data found</text></Reason>
</Acknowledgement_MarketDocument>"""


def test_acknowledgement_raises_with_reason() -> None:
    """The failure mode that matters most: a 200 response with no data in it."""
    with pytest.raises(EntsoeAcknowledgementError) as caught:
        parse_prices(ACK)
    assert caught.value.code == "999"
    assert caught.value.text == "No matching data found"


# ------------------------------------------------------------------ parquet


def test_write_parquet_round_trip(tmp_path: Path, real_rows: list[PricePoint]) -> None:
    target = write_parquet(real_rows, tmp_path / "parsed" / "prices.parquet")
    table = pq.read_table(target)
    assert table.schema.equals(PARQUET_SCHEMA)
    assert table.num_rows == 192
    assert not list(target.parent.glob("*.tmp"))


def test_write_parquet_empty_keeps_schema(tmp_path: Path) -> None:
    target = write_parquet([], tmp_path / "empty.parquet")
    table = pq.read_table(target)
    assert table.num_rows == 0
    assert table.schema.equals(PARQUET_SCHEMA)


# -------------------------------------------------------------- parsed zone

MARKET_DAY = date(2026, 9, 24)


def _parsed_files(settings: Settings) -> list[Path]:
    return [p for p in settings.parsed_dir.rglob("*") if p.is_file()]


def test_parsed_path_mirrors_landing_layout(settings: Settings) -> None:
    path = parsed_path("prices", MARKET_DAY, settings)
    assert path.is_relative_to(settings.parsed_dir)
    assert path.parent.parts[-3:] == ("dataset=prices", "year=2026", "month=09")
    assert path.name == "prices_2026-09-24.parquet"


def test_parse_landed_file_writes_parquet(settings: Settings) -> None:
    landed = land(FIXTURE.read_bytes(), "prices", MARKET_DAY, settings, suffix=".xml")
    target, rows = parse_landed_file(landed, "prices", MARKET_DAY, settings)
    assert rows == 192
    assert target == parsed_path("prices", MARKET_DAY, settings)
    assert pq.read_table(target).num_rows == 192


def test_parse_never_modifies_the_landed_file(settings: Settings) -> None:
    """The landing zone is the source of truth. Parsing only ever reads it."""
    original = FIXTURE.read_bytes()
    landed = land(original, "prices", MARKET_DAY, settings, suffix=".xml")
    parse_landed_file(landed, "prices", MARKET_DAY, settings)
    assert landed.read_bytes() == original


def test_reparsing_replaces_rather_than_accumulates(settings: Settings) -> None:
    landed = land(FIXTURE.read_bytes(), "prices", MARKET_DAY, settings, suffix=".xml")
    for _ in range(3):
        parse_landed_file(landed, "prices", MARKET_DAY, settings)
    outputs = _parsed_files(settings)
    assert len(outputs) == 1
    assert pq.read_table(outputs[0]).num_rows == 192


def test_failed_parse_writes_nothing(settings: Settings) -> None:
    """Defence in depth: extract refuses to land acknowledgements, but if one
    is ever on disk, parsing must fail without producing any output."""
    bad = settings.landing_dir / "bad.xml"
    bad.write_bytes(ACK)
    with pytest.raises(EntsoeAcknowledgementError):
        parse_landed_file(bad, "prices", MARKET_DAY, settings)
    assert _parsed_files(settings) == []


def test_parse_rejects_a_file_from_the_wrong_zone(settings: Settings) -> None:
    """A delivery stored under zone=BE must contain Belgian prices. A mapping
    mistake that fetched another zone fails here, before anything is written."""
    landed = land(FIXTURE.read_bytes(), "prices", MARKET_DAY, settings, suffix=".xml", zone="BE")
    with pytest.raises(EntsoeParseError, match="expected bidding zone"):
        parse_landed_file(
            landed,
            "prices",
            MARKET_DAY,
            settings,
            zone="BE",
            expected_bidding_zone="10YBE----------2",
        )
    assert _parsed_files(settings) == []


def test_parse_with_the_right_zone_writes_into_its_partition(settings: Settings) -> None:
    landed = land(FIXTURE.read_bytes(), "prices", MARKET_DAY, settings, suffix=".xml", zone="NL")
    target, rows = parse_landed_file(
        landed, "prices", MARKET_DAY, settings, zone="NL", expected_bidding_zone=NL
    )
    assert rows == 192
    assert target.parent.parts[-4:] == ("dataset=prices", "zone=NL", "year=2026", "month=09")
