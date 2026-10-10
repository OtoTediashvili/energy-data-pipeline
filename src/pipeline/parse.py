"""Parse: ENTSO-E Publication_MarketDocument XML -> flat, typed price rows.

This is the step between the landing zone and the warehouse. Its core,
parse_prices, is a pure transformation: bytes in, rows out. It never fetches,
so a parser bug is fixed by replaying files already on disk, never by
re-hitting a rate-limited API. parse_landed_file applies it to one landed file
and writes the result into the parsed zone.

Five properties of the real data drive the design:

1. Market days run on Central European time, the API speaks UTC. One UTC
   request window overlaps two market days, so every row carries its own
   absolute timestamp. The natural key is (bidding_zone, interval_start_utc),
   never the run date that happened to deliver it.
2. The number of points per period is derived from the period's time interval
   and resolution, never assumed. A DST day has 92 or 100 quarter-hours.
3. curveType A03 omits a point whenever the price is unchanged, so gaps are
   forward-filled and flagged. A01 must be complete, and a gap there is an error.
4. Errors and "no data" arrive as an Acknowledgement_MarketDocument, sometimes
   with HTTP 200. Those raise, so an empty result can never pass for success.
5. Some zones have two day-ahead auctions in one document. Austria has the
   main European auction (SDAC) and EXAA's separate 10:15 auction, labelled
   with classificationSequence position 1 and 2. From October 2025 both are
   quarter-hourly, so their rows share every timestamp. Every row records its
   auction; staging, not the parser, decides which one counts.

Namespaces are matched with a wildcard because ENTSO-E versions them
(e.g. publicationdocument:7:3), and a version bump must not break parsing.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from xml.etree.ElementTree import Element

import pyarrow as pa
import pyarrow.parquet as pq
from defusedxml import ElementTree as SafeET

from pipeline.config import Settings, get_settings
from pipeline.logging_config import get_logger
from pipeline.zones import partition_path

log = get_logger(__name__)

PUBLICATION_ROOT = "Publication_MarketDocument"
ACKNOWLEDGEMENT_ROOT = "Acknowledgement_MarketDocument"

_RESOLUTION = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?$")


class EntsoeParseError(ValueError):
    """The document is malformed or violates an assumption we rely on."""


class EntsoeAcknowledgementError(RuntimeError):
    """The API answered with an Acknowledgement document instead of data."""

    def __init__(self, code: str | None, text: str | None) -> None:
        self.code = code
        self.text = text
        super().__init__(f"ENTSO-E acknowledgement (code={code}): {text}")


@dataclass(frozen=True, slots=True)
class PricePoint:
    """One market time unit for one bidding zone."""

    bidding_zone: str
    interval_start_utc: datetime
    interval_end_utc: datetime
    resolution_minutes: int
    price_eur_mwh: float
    currency: str
    price_unit: str
    position: int
    is_filled: bool
    document_mrid: str
    revision_number: int
    document_created_utc: datetime | None
    # classificationSequence position: 1 = the main European auction, 2 = a
    # second auction (EXAA for Austria). None when a zone has only one.
    auction_sequence: int | None


PARQUET_SCHEMA = pa.schema(
    [
        ("bidding_zone", pa.string()),
        ("interval_start_utc", pa.timestamp("us", tz="UTC")),
        ("interval_end_utc", pa.timestamp("us", tz="UTC")),
        ("resolution_minutes", pa.int32()),
        ("price_eur_mwh", pa.float64()),
        ("currency", pa.string()),
        ("price_unit", pa.string()),
        ("position", pa.int32()),
        ("is_filled", pa.bool_()),
        ("document_mrid", pa.string()),
        ("revision_number", pa.int32()),
        ("document_created_utc", pa.timestamp("us", tz="UTC")),
        ("auction_sequence", pa.int32()),
    ]
)


# ------------------------------------------------------------------ helpers


def _local(tag: str) -> str:
    """Strip the XML namespace: '{urn:...}TimeSeries' -> 'TimeSeries'."""
    return tag.rsplit("}", 1)[-1]


def _text(parent: Element, path: str) -> str:
    """Required child text, namespace-agnostic. Raises if absent or empty."""
    node = parent.find(_ns(path))
    if node is None or node.text is None or not node.text.strip():
        raise EntsoeParseError(f"missing required element: {path}")
    return node.text.strip()


def _opt_text(parent: Element, path: str) -> str | None:
    node = parent.find(_ns(path))
    if node is None or node.text is None:
        return None
    return node.text.strip() or None


def _ns(path: str) -> str:
    """'Period/timeInterval/start' -> '{*}Period/{*}timeInterval/{*}start'."""
    return "/".join(f"{{*}}{part}" for part in path.split("/"))


def parse_timestamp(value: str) -> datetime:
    """ENTSO-E timestamps look like '2026-09-23T22:00Z'. Always tz-aware UTC."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise EntsoeParseError(f"timestamp without timezone: {value!r}")
    return parsed


def parse_resolution(value: str) -> timedelta:
    """'PT15M' -> 15 minutes, 'PT60M' -> 60 minutes, 'PT1H' -> 1 hour."""
    match = _RESOLUTION.match(value)
    if not match or not (match.group(1) or match.group(2)):
        raise EntsoeParseError(f"unsupported resolution: {value!r}")
    hours, minutes = int(match.group(1) or 0), int(match.group(2) or 0)
    return timedelta(hours=hours, minutes=minutes)


# -------------------------------------------------------------------- parse


def _raise_acknowledgement(root: Element) -> None:
    reason = root.find("{*}Reason")
    code = _opt_text(reason, "code") if reason is not None else None
    text = _opt_text(reason, "text") if reason is not None else None
    raise EntsoeAcknowledgementError(code, text)


def _expand_period(
    period: Element, curve_type: str
) -> tuple[datetime, timedelta, list[tuple[int, float, bool]]]:
    """Return (period_start, step, [(position, price, is_filled), ...])."""
    start = parse_timestamp(_text(period, "timeInterval/start"))
    end = parse_timestamp(_text(period, "timeInterval/end"))
    step = parse_resolution(_text(period, "resolution"))

    span = end - start
    if span <= timedelta(0) or span % step != timedelta(0):
        raise EntsoeParseError(f"interval {start}..{end} is not a whole number of {step}")
    expected = span // step

    observed: dict[int, float] = {}
    for point in period.findall("{*}Point"):
        position = int(_text(point, "position"))
        if not 1 <= position <= expected:
            raise EntsoeParseError(f"position {position} outside 1..{expected}")
        if position in observed:
            raise EntsoeParseError(f"duplicate position {position}")
        observed[position] = float(_text(point, "price.amount"))

    if not observed:
        return start, step, []

    missing = expected - len(observed)
    if missing and curve_type != "A03":
        raise EntsoeParseError(
            f"curveType {curve_type} must be complete but {missing} of {expected} points are missing"
        )
    if 1 not in observed:
        # A03 fills forward, so there must be a value to fill from.
        raise EntsoeParseError("first position is missing; nothing to forward-fill from")

    expanded: list[tuple[int, float, bool]] = []
    last_price = observed[1]
    for position in range(1, expected + 1):
        if position in observed:
            last_price = observed[position]
            expanded.append((position, last_price, False))
        else:
            expanded.append((position, last_price, True))
    return start, step, expanded


def _auction_sequence(series: Element) -> int | None:
    """Which auction a series belongs to, or None if the zone has only one."""
    value = _opt_text(series, "classificationSequence_AttributeInstanceComponent.position")
    if value is None:
        return None
    if not value.isdigit():
        raise EntsoeParseError(f"auction sequence is not a number: {value!r}")
    return int(value)


def parse_prices(payload: bytes) -> list[PricePoint]:
    """Parse one A44 day-ahead price document into rows.

    Raises EntsoeAcknowledgementError if the API returned an acknowledgement
    (error or no data) and EntsoeParseError if the document is malformed.
    """
    root = SafeET.fromstring(payload)
    kind = _local(root.tag)

    if kind == ACKNOWLEDGEMENT_ROOT:
        _raise_acknowledgement(root)
    if kind != PUBLICATION_ROOT:
        raise EntsoeParseError(f"unexpected root element: {kind}")

    document_mrid = _text(root, "mRID")
    revision_number = int(_text(root, "revisionNumber"))
    created_raw = _opt_text(root, "createdDateTime")
    document_created = parse_timestamp(created_raw) if created_raw else None

    rows: list[PricePoint] = []
    for series in root.findall("{*}TimeSeries"):
        zone = _text(series, "in_Domain.mRID")
        currency = _text(series, "currency_Unit.name")
        unit = _text(series, "price_Measure_Unit.name")
        curve_type = _opt_text(series, "curveType") or "A01"
        auction_sequence = _auction_sequence(series)

        for period in series.findall("{*}Period"):
            start, step, points = _expand_period(period, curve_type)
            minutes = int(step.total_seconds() // 60)
            for position, price, filled in points:
                interval_start = start + step * (position - 1)
                rows.append(
                    PricePoint(
                        bidding_zone=zone,
                        interval_start_utc=interval_start,
                        interval_end_utc=interval_start + step,
                        resolution_minutes=minutes,
                        price_eur_mwh=price,
                        currency=currency,
                        price_unit=unit,
                        position=position,
                        is_filled=filled,
                        document_mrid=document_mrid,
                        revision_number=revision_number,
                        document_created_utc=document_created,
                        auction_sequence=auction_sequence,
                    )
                )
    return rows


# ------------------------------------------------------------------- output


def write_parquet(rows: list[PricePoint], target: Path) -> Path:
    """Write rows to Parquet with a pinned schema, atomically."""
    table = pa.Table.from_pylist([asdict(row) for row in rows], schema=PARQUET_SCHEMA)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    pq.write_table(table, tmp)
    tmp.replace(target)
    return target


# -------------------------------------------------------------- parsed zone


def parsed_path(
    dataset: str, logical_date: date, settings: Settings | None = None, zone: str | None = None
) -> Path:
    """Deterministic parsed-zone location, mirroring the landing layout 1:1.

    One landed file maps to exactly one Parquet file, so reparsing a date and
    zone replaces its output and never accumulates duplicates.
    """
    cfg = settings or get_settings()
    return partition_path(cfg.parsed_dir, dataset, logical_date, ".parquet", zone)


def parse_landed_file(
    landed: Path,
    dataset: str,
    logical_date: date,
    settings: Settings | None = None,
    zone: str | None = None,
    expected_bidding_zone: str | None = None,
) -> tuple[Path, int]:
    """Parse one landed file into its parsed-zone Parquet. Returns (path, rows).

    Only ever reads the landed file. Parsing completes before anything is
    written, so a document that fails to parse writes nothing, and any earlier
    output for that date is left exactly as it was.

    ``expected_bidding_zone`` (an EIC code) guards the zone mapping: a file
    stored under zone=BE must contain Belgian prices, never another zone's.
    """
    rows = parse_prices(landed.read_bytes())
    if expected_bidding_zone is not None:
        found = {row.bidding_zone for row in rows}
        if found - {expected_bidding_zone}:
            raise EntsoeParseError(
                f"{landed.name}: expected bidding zone {expected_bidding_zone}, found {sorted(found)}"
            )
    target = write_parquet(rows, parsed_path(dataset, logical_date, settings, zone))
    log.info("parse.ok", source=str(landed), target=str(target), rows=len(rows))
    return target, len(rows)
