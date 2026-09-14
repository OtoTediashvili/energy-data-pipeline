"""Loader tests against a real DuckDB file in a temp directory.

Mocking the warehouse would test nothing worth testing. DuckDB is fast enough
that these stay in the unit suite.
"""

from __future__ import annotations

from datetime import date

import pytest

from pipeline.config import Settings
from pipeline.extract import land
from pipeline.load import RAW_SCHEMA, load_json_partition, row_count, warehouse

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
