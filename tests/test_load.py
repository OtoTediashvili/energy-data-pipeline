"""Loader tests against a real DuckDB file in a temp directory.

Mocking the warehouse would test nothing worth testing. DuckDB is fast enough
that these stay in the unit suite.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.config import Settings
from pipeline.extract import land
from pipeline.load import (
    RAW_SCHEMA,
    load_json_partition,
    load_parquet_partition,
    row_count,
    warehouse,
)
from pipeline.parse import PARQUET_SCHEMA, parse_landed_file

DAY_ONE = date(2026, 9, 13)
DAY_TWO = date(2026, 9, 14)


def test_load_inserts_rows(settings: Settings, sample_payload: bytes) -> None:
    path = land(sample_payload, "prices", DAY_ONE, settings)
    with warehouse(settings) as conn:
        rows = load_json_partition(conn, path, "prices", DAY_ONE)
        assert rows == 3
        assert row_count(conn, "prices") == 3


def test_load_adds_lineage_columns(settings: Settings, sample_payload: bytes) -> None:
    path = land(sample_payload, "prices", DAY_ONE, settings)
    with warehouse(settings) as conn:
        load_json_partition(conn, path, "prices", DAY_ONE)
        columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [RAW_SCHEMA, "prices"],
            ).fetchall()
        }
    assert {"_logical_date", "_ingested_at", "_source_file"} <= columns


def test_reloading_same_partition_does_not_duplicate(
    settings: Settings, sample_payload: bytes
) -> None:
    """Rerun the same date three times; row count must not move."""
    path = land(sample_payload, "prices", DAY_ONE, settings)
    with warehouse(settings) as conn:
        for _ in range(3):
            load_json_partition(conn, path, "prices", DAY_ONE)
        assert row_count(conn, "prices") == 3


def test_loading_a_second_date_appends(settings: Settings, sample_payload: bytes) -> None:
    """Replacing one partition must leave neighbouring partitions alone."""
    first = land(sample_payload, "prices", DAY_ONE, settings)
    second = land(b'{"id": 9, "country": "IT", "value": 12.0}\n', "prices", DAY_TWO, settings)

    with warehouse(settings) as conn:
        load_json_partition(conn, first, "prices", DAY_ONE)
        load_json_partition(conn, second, "prices", DAY_TWO)
        assert row_count(conn, "prices") == 4

        load_json_partition(conn, first, "prices", DAY_ONE)
        assert row_count(conn, "prices") == 4


def test_missing_file_raises(settings: Settings) -> None:
    with warehouse(settings) as conn, pytest.raises(FileNotFoundError):
        load_json_partition(conn, settings.landing_dir / "nope.json", "prices", DAY_ONE)


def test_row_count_of_unknown_table_is_zero(settings: Settings) -> None:
    with warehouse(settings) as conn:
        assert row_count(conn, "does_not_exist") == 0


def test_hive_path_does_not_leak_partition_columns(
    settings: Settings, sample_payload: bytes
) -> None:
    """Regression: DuckDB parses dataset=/year=/month= paths into columns.

    With hive_partitioning left on, read_json_auto silently widens the table by
    three fields duplicating _logical_date. Pin the exact schema.
    """
    path = land(sample_payload, "prices", DAY_ONE, settings)
    with warehouse(settings) as conn:
        load_json_partition(conn, path, "prices", DAY_ONE)
        columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [RAW_SCHEMA, "prices"],
            ).fetchall()
        }
    assert columns == {"id", "country", "value", "_logical_date", "_ingested_at", "_source_file"}
    assert not {"dataset", "year", "month"} & columns


# -------------------------------------------------------------- parsed zone

FIXTURE = Path(__file__).parent / "fixtures" / "entsoe_a44_nl_20260924.xml"
TABLE = "day_ahead_prices"
LINEAGE = {"_logical_date", "_ingested_at", "_source_file"}


def _parsed(settings: Settings, logical_date: date) -> Path:
    """Land the real ENTSO-E fixture and parse it, as a run for that date would."""
    landed = land(FIXTURE.read_bytes(), "prices", logical_date, settings, suffix=".xml")
    target, _ = parse_landed_file(landed, "prices", logical_date, settings)
    return target


def _columns(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ?",
        [RAW_SCHEMA, table],
    ).fetchall()
    return {row[0] for row in rows}


def test_load_parquet_partition_loads_every_interval(settings: Settings) -> None:
    source = _parsed(settings, DAY_ONE)
    with warehouse(settings) as conn:
        assert load_parquet_partition(conn, source, TABLE, DAY_ONE) == 192


def test_parquet_raw_schema_is_pinned(settings: Settings) -> None:
    """Regression guard, Parquet edition. The parsed zone uses the same
    dataset=/year=/month= layout, and read_parquet turns those directory names
    into columns unless told not to. Pin the exact schema."""
    source = _parsed(settings, DAY_ONE)
    with warehouse(settings) as conn:
        load_parquet_partition(conn, source, TABLE, DAY_ONE)
        columns = _columns(conn, TABLE)
    assert columns == set(PARQUET_SCHEMA.names) | LINEAGE
    assert not {"dataset", "year", "month"} & columns


def test_reloading_a_parquet_partition_is_idempotent(settings: Settings) -> None:
    source = _parsed(settings, DAY_ONE)
    with warehouse(settings) as conn:
        for _ in range(3):
            load_parquet_partition(conn, source, TABLE, DAY_ONE)
        assert row_count(conn, TABLE) == 192


def test_timestamps_arrive_as_utc_instants(settings: Settings) -> None:
    """Compared inside DuckDB, so the check depends neither on how Python
    converts timestamps nor on the machine's local time zone."""
    source = _parsed(settings, DAY_ONE)
    with warehouse(settings) as conn:
        load_parquet_partition(conn, source, TABLE, DAY_ONE)
        checks = conn.execute(
            "SELECT min(interval_start_utc) = TIMESTAMPTZ '2026-09-23 22:00:00+00', "
            "max(interval_end_utc) = TIMESTAMPTZ '2026-09-25 22:00:00+00' "
            f"FROM {RAW_SCHEMA}.{TABLE}"
        ).fetchone()
    assert checks == (True, True)


def test_a_reordered_file_cannot_swap_columns(settings: Settings, tmp_path: Path) -> None:
    """Inserts match columns by name. By position, a file whose two timestamp
    columns were merely reordered would swap interval start and end, silently."""
    original = _parsed(settings, DAY_ONE)
    table = pq.read_table(original)
    names = table.column_names
    i, j = names.index("interval_start_utc"), names.index("interval_end_utc")
    names[i], names[j] = names[j], names[i]
    reordered = tmp_path / "reordered.parquet"
    pq.write_table(table.select(names), reordered)
    with warehouse(settings) as conn:
        load_parquet_partition(conn, original, TABLE, DAY_ONE)
        load_parquet_partition(conn, reordered, TABLE, DAY_TWO)
        inverted = conn.execute(
            f"SELECT count(*) FROM {RAW_SCHEMA}.{TABLE} "
            "WHERE interval_end_utc <= interval_start_utc"
        ).fetchone()
    assert inverted == (0,)


def test_a_failed_load_leaves_the_previous_data_intact(settings: Settings) -> None:
    """Delete and insert share one transaction. When a delivery's file is
    replaced by a broken one, the reload deletes the old rows and then fails to
    insert; the rollback must restore them, so a bad file never empties a delivery.

    The broken file sits at the same path as the original on purpose. Deletes
    are scoped to one delivery, so a broken file anywhere else would delete
    nothing, and this test would pass even with the transaction removed.
    """
    original = _parsed(settings, DAY_ONE)
    table = pq.read_table(original)
    with warehouse(settings) as conn:
        load_parquet_partition(conn, original, TABLE, DAY_ONE, settings)
        pq.write_table(table.append_column("surprise", pa.array([1] * table.num_rows)), original)
        with pytest.raises(duckdb.Error):
            load_parquet_partition(conn, original, TABLE, DAY_ONE, settings)
        assert row_count(conn, TABLE) == 192


def test_raw_keeps_every_delivery_of_an_interval(settings: Settings) -> None:
    """The partition key is not the business key.

    Consecutive runs overlap by one market day, so the same interval arrives
    under two logical dates. Loading one delivery under two dates stands in for
    that: raw keeps both, and only staging collapses them to one row per
    (bidding_zone, interval_start_utc). Replacing one date leaves the other alone.
    """
    with warehouse(settings) as conn:
        load_parquet_partition(conn, _parsed(settings, DAY_ONE), TABLE, DAY_ONE)
        load_parquet_partition(conn, _parsed(settings, DAY_TWO), TABLE, DAY_TWO)
        assert row_count(conn, TABLE) == 384
        distinct = conn.execute(
            "SELECT count(*) FROM (SELECT DISTINCT bidding_zone, interval_start_utc "
            f"FROM {RAW_SCHEMA}.{TABLE})"
        ).fetchone()
        assert distinct == (192,)

        load_parquet_partition(conn, _parsed(settings, DAY_ONE), TABLE, DAY_ONE)
        assert row_count(conn, TABLE) == 384


# ------------------------------------------------------------ several zones


def _parsed_for_zone(settings: Settings, logical_date: date, zone: str, eic: str) -> Path:
    """The real fixture relabelled as another bidding zone, landed and parsed."""
    xml = FIXTURE.read_bytes().replace(b"10YNL----------L", eic.encode())
    landed = land(xml, "prices", logical_date, settings, suffix=".xml", zone=zone)
    target, _ = parse_landed_file(
        landed, "prices", logical_date, settings, zone=zone, expected_bidding_zone=eic
    )
    return target


def test_two_zones_on_the_same_day_keep_each_others_rows(settings: Settings) -> None:
    """The bug this layout fixes: with deletes scoped to the date, the second
    zone's load wiped the first zone's rows for that day."""
    nl = _parsed_for_zone(settings, DAY_ONE, "NL", "10YNL----------L")
    be = _parsed_for_zone(settings, DAY_ONE, "BE", "10YBE----------2")
    with warehouse(settings) as conn:
        load_parquet_partition(conn, nl, TABLE, DAY_ONE, settings)
        load_parquet_partition(conn, be, TABLE, DAY_ONE, settings)
        zones = conn.execute(
            f"SELECT bidding_zone, count(*) FROM {RAW_SCHEMA}.{TABLE} GROUP BY 1 ORDER BY 1"
        ).fetchall()
    assert zones == [("10YBE----------2", 192), ("10YNL----------L", 192)]


def test_delivery_key_is_relative_to_the_data_directory(settings: Settings) -> None:
    source = _parsed_for_zone(settings, DAY_ONE, "NL", "10YNL----------L")
    with warehouse(settings) as conn:
        load_parquet_partition(conn, source, TABLE, DAY_ONE, settings)
        keys = conn.execute(f"SELECT DISTINCT _source_file FROM {RAW_SCHEMA}.{TABLE}").fetchall()
    assert keys == [
        ("parsed/dataset=prices/zone=NL/year=2026/month=09/prices_NL_2026-09-13.parquet",)
    ]


def test_the_same_delivery_from_another_environment_replaces_it(
    settings: Settings, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Airflow loads /opt/airflow/data/...; your terminal loads data/... . Both
    are one delivery, so the second load must replace the first, not add to it."""
    source = _parsed_for_zone(settings, DAY_ONE, "NL", "10YNL----------L")
    monkeypatch.chdir(tmp_path)
    as_seen_from_the_host = source.relative_to(tmp_path)
    with warehouse(settings) as conn:
        load_parquet_partition(conn, source.resolve(), TABLE, DAY_ONE, settings)
        load_parquet_partition(conn, as_seen_from_the_host, TABLE, DAY_ONE, settings)
        assert row_count(conn, TABLE) == 192
