"""Load: files -> warehouse raw schema.

Still no business logic. The only job here is to get rows into queryable
tables with lineage columns attached, so dbt has something to build on.

Idempotency strategy: delete-then-insert scoped to the partition key. Rerunning
a date replaces exactly that date and touches nothing else.

The partition key is the run's logical date, and it is not the business key.
ENTSO-E answers a UTC window with every market day that window overlaps, so
consecutive runs deliver the same market day twice, under two logical dates.
Raw keeps both deliveries on purpose, as a faithful log of what each run
received. Staging deduplicates on (bidding_zone, interval_start_utc).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from pipeline.config import Settings, get_settings
from pipeline.logging_config import get_logger

log = get_logger(__name__)

RAW_SCHEMA = "raw"


@contextmanager
def warehouse(settings: Settings | None = None) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open a DuckDB connection, guaranteeing it is closed."""
    cfg = settings or get_settings()
    cfg.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(cfg.duckdb_path))
    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        yield conn
    finally:
        conn.close()


def _replace_partition(
    conn: duckdb.DuckDBPyConnection,
    reader: str,
    source_file: Path,
    table: str,
    logical_date: date,
) -> int:
    """Replace one logical date's rows in raw.<table> with the rows of a file.

    ``reader`` is a DuckDB table function whose single parameter is the file
    path, e.g. "read_parquet(?, hive_partitioning=false)". Returns the number
    of rows now present for that logical date.
    """
    if not source_file.exists():
        raise FileNotFoundError(f"source file missing: {source_file}")

    qualified = f"{RAW_SCHEMA}.{table}"
    ingested_at = datetime.now(UTC)
    staged = f"""
        SELECT
            *,
            ?::DATE      AS _logical_date,
            ?::TIMESTAMP AS _ingested_at,
            ?            AS _source_file
        FROM {reader}
    """
    args = [logical_date, ingested_at, str(source_file), str(source_file)]

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {qualified} AS {staged} LIMIT 0", args)
        conn.execute(f"DELETE FROM {qualified} WHERE _logical_date = ?", [logical_date])
        # BY NAME, never by position. Interval start and end are both
        # timestamps, so a file that arrived with those columns reordered would
        # swap them silently. By name, a reorder is harmless and an unknown
        # column is a loud error.
        conn.execute(f"INSERT INTO {qualified} BY NAME {staged}", args)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        log.exception("load.failed", table=qualified, logical_date=str(logical_date))
        raise

    result = conn.execute(
        f"SELECT count(*) FROM {qualified} WHERE _logical_date = ?", [logical_date]
    ).fetchone()
    rows = int(result[0]) if result else 0

    log.info("load.ok", table=qualified, logical_date=str(logical_date), rows=rows)
    return rows


def load_json_partition(
    conn: duckdb.DuckDBPyConnection,
    source_file: Path,
    table: str,
    logical_date: date,
) -> int:
    """Load one landed JSON file into raw.<table>, replacing any prior run.

    The demo path. read_json_auto infers the schema; in production you would
    pin an explicit columns={...} spec so an upstream schema change fails
    loudly here rather than silently reshaping downstream models.
    """
    return _replace_partition(
        conn, "read_json_auto(?, hive_partitioning=false)", source_file, table, logical_date
    )


def load_parquet_partition(
    conn: duckdb.DuckDBPyConnection,
    source_file: Path,
    table: str,
    logical_date: date,
) -> int:
    """Load one parsed-zone Parquet file into raw.<table>, replacing any prior run.

    The schema arrives pinned from parse.PARQUET_SCHEMA. hive_partitioning=false
    for the same reason as the JSON path: the parsed zone uses dataset=/year=/
    month= directories, which DuckDB would otherwise turn into three columns.
    """
    return _replace_partition(
        conn, "read_parquet(?, hive_partitioning=false)", source_file, table, logical_date
    )


def row_count(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    """Total rows in a raw table, or 0 if it does not exist yet."""
    exists = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [RAW_SCHEMA, table],
    ).fetchone()
    if not exists or exists[0] == 0:
        return 0
    result = conn.execute(f"SELECT count(*) FROM {RAW_SCHEMA}.{table}").fetchone()
    return int(result[0]) if result else 0
