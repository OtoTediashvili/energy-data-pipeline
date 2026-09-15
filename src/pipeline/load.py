"""Load: landing zone -> warehouse raw schema.

Still no business logic. The only job here is to get bytes into queryable
tables with lineage columns attached, so dbt has something to build on.

Idempotency strategy: delete-then-insert scoped to the partition key. Rerunning
a date replaces exactly that date and touches nothing else.
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


def load_json_partition(
    conn: duckdb.DuckDBPyConnection,
    source_file: Path,
    table: str,
    logical_date: date,
) -> int:
    """Load one JSON partition into raw.<table>, replacing any prior run.

    Returns the number of rows now present for that logical date.
    """
    if not source_file.exists():
        raise FileNotFoundError(f"landing file missing: {source_file}")

    qualified = f"{RAW_SCHEMA}.{table}"
    ingested_at = datetime.now(UTC)

    # read_json_auto infers the schema. In production you would pin an explicit
    # columns={...} spec so an upstream schema change fails loudly here rather
    # than silently reshaping downstream models.
    staged = """
        SELECT
            *,
            ?::DATE      AS _logical_date,
            ?::TIMESTAMP AS _ingested_at,
            ?            AS _source_file
        FROM read_json_auto(?, hive_partitioning=false)
    """
    args = [logical_date, ingested_at, str(source_file), str(source_file)]

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {qualified} AS {staged} LIMIT 0", args)
        conn.execute(f"DELETE FROM {qualified} WHERE _logical_date = ?", [logical_date])
        conn.execute(f"INSERT INTO {qualified} {staged}", args)
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
