"""Day-ahead electricity prices from ENTSO-E: extract -> parse -> load -> dbt.

Runs daily at 13:00 UTC, after the day-ahead auction publishes tomorrow's
prices (around 12:45 CET). Each run requests its UTC day, which returns two
market days: today's, and tomorrow's freshly published one. Consecutive runs
therefore overlap by a market day; raw keeps both deliveries and staging
collapses them on (bidding_zone, interval_start_utc).

What makes it safe to rerun and backfill:

* Every file and partition is keyed on the run's date, so a rerun replaces
  that date's output instead of duplicating it.
* catchup=True with a real start_date backfills history, and max_active_runs=1
  keeps a backfill to one request at a time, well inside ENTSO-E's rate limit.
* Each stage is its own task. A parser bug is fixed by clearing parse and
  rerunning from there, never by re-fetching.
* The zero-row gate fails the run rather than publishing an empty day.

Pipeline imports happen inside tasks, so the DAG processor parses this file
without loading DuckDB or PyArrow every few seconds.
"""

from __future__ import annotations

import os
from datetime import date

import pendulum
from airflow.sdk import dag, get_current_context, task

BIDDING_ZONE = "10YNL----------L"
DATASET = "prices"
RAW_TABLE = "day_ahead_prices"

# dbt runs from its own virtualenv inside the image (see Dockerfile), so its
# dependencies can never conflict with Airflow's.
DBT_BIN = os.environ.get("DBT_BIN", "dbt")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_TARGET = os.environ.get("DBT_TARGET", "prod")


def _run_day() -> date:
    """The UTC day this run is for.

    Airflow 3 manual runs can carry no logical date; fall back to run_after.
    """
    context = get_current_context()
    moment = context.get("logical_date") or context["dag_run"].run_after
    return moment.date()


@dag(
    dag_id="day_ahead_prices",
    schedule="0 13 * * *",
    start_date=pendulum.datetime(2026, 9, 20, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=30),
    },
    tags=["entsoe", "elt"],
    doc_md=__doc__,
)
def day_ahead_prices() -> None:
    @task
    def extract() -> str:
        """Fetch this run's UTC day and land the raw XML. Returns the landed path."""
        from pipeline.extract import extract_to_landing

        day = _run_day()
        stamp = day.strftime("%Y%m%d")
        landed = extract_to_landing(
            endpoint="",
            dataset=DATASET,
            logical_date=day,
            suffix=".xml",
            params={
                "documentType": "A44",
                "in_Domain": BIDDING_ZONE,
                "out_Domain": BIDDING_ZONE,
                "periodStart": f"{stamp}0000",
                "periodEnd": f"{stamp}2300",
            },
        )
        return str(landed)

    @task
    def parse(landed: str) -> str:
        """Parse the landed XML into the parsed zone. Returns the Parquet path."""
        from pathlib import Path

        from pipeline.parse import parse_landed_file

        target, _ = parse_landed_file(Path(landed), DATASET, _run_day())
        return str(target)

    @task
    def load(parsed: str) -> int:
        """Replace this run's partition in raw. Returns the rows loaded."""
        from pathlib import Path

        from pipeline.load import load_parquet_partition, warehouse

        with warehouse() as conn:
            return load_parquet_partition(conn, Path(parsed), RAW_TABLE, _run_day())

    @task
    def validate(rows: int) -> None:
        """Fail loudly rather than publishing an empty day.

        A silent zero-row load is the most common way a pipeline lies to its
        consumers for a week before anyone notices.
        """
        if rows == 0:
            raise ValueError(f"{RAW_TABLE}: zero rows loaded; refusing to publish")

    @task.bash
    def transform() -> str:
        """Build and test every dbt model. Any failing dbt test fails the run."""
        return f"cd {DBT_PROJECT_DIR} && {DBT_BIN} deps && {DBT_BIN} build --target {DBT_TARGET}"

    rows = load(parse(extract()))
    validate(rows) >> transform()


day_ahead_prices()
