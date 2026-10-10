"""Day-ahead electricity prices from ENTSO-E: extract -> parse -> load -> dbt.

Runs daily at 13:00 UTC, after the day-ahead auction publishes tomorrow's
prices (around 12:45 CET). Each run requests its UTC day for every bidding zone,
which returns two market days per zone: today's, and tomorrow's freshly
published one. Consecutive runs therefore overlap by a market day; raw keeps
both deliveries and staging collapses them on (bidding_zone, interval_start_utc).

Extract, parse and load fan out: one task per bidding zone, labelled with the
zone in the UI. The zone list is the dbt reference seed, so adding a zone is one
row in dbt/seeds/bidding_zones.csv; the DAG, the dimension and its tests follow.

What makes it safe to rerun and backfill:

* Every file and raw delivery is keyed on zone and date, so a rerun replaces
  exactly that zone's day and never touches another zone's.
* Catch-up after an outage replays the missed days one run at a time
  (max_active_runs=1). A backfill ignores that limit and sets its own, which
  `make backfill` keeps small. Either way requests stay far inside ENTSO-E's
  rate limit.
* Backfill runs skip dbt. `make backfill` ends with one dbt build, and every
  later scheduled run builds too: the fact table rebuilds whichever days
  received data since its last build, however old they are.
* Each stage is its own task. A parser bug is fixed by clearing parse and
  rerunning from there, never by re-fetching.
* Loads into the warehouse run one at a time: DuckDB allows a single writer.
* The validate gate fails the run if any zone loaded nothing, naming the zone.

Pipeline imports happen inside tasks, so the DAG processor parses this file
without loading DuckDB or PyArrow every few seconds.
"""

from __future__ import annotations

import csv
import os
from datetime import date
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task

DATASET = "prices"
RAW_TABLE = "day_ahead_prices"

# The zone list lives in one place, the dbt reference seed.
ZONES_SEED = Path(__file__).resolve().parent.parent / "dbt" / "seeds" / "bidding_zones.csv"

# dbt runs from its own virtualenv inside the image (see Dockerfile), so its
# dependencies can never conflict with Airflow's.
DBT_BIN = os.environ.get("DBT_BIN", "dbt")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_TARGET = os.environ.get("DBT_TARGET", "prod")


def _zones() -> dict[str, str]:
    """Zone key (the seed's country_code) -> ENTSO-E EIC code."""
    with ZONES_SEED.open(newline="") as handle:
        return {row["country_code"]: row["bidding_zone_eic"] for row in csv.DictReader(handle)}


ZONES = _zones()


def _run_day() -> date:
    """The UTC day this run is for.

    Airflow 3 manual runs can carry no logical date; fall back to run_after.
    """
    context = get_current_context()
    moment = context.get("logical_date") or context["dag_run"].run_after
    return moment.date()


def _label(zone: str) -> None:
    """Show the zone, not a bare map index, next to this task in the UI."""
    get_current_context()["zone_key"] = zone


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
    @task(map_index_template="{{ zone_key }}")
    def extract(zone: str) -> dict[str, str]:
        """Fetch this run's UTC day for one zone and land the raw XML."""
        from pipeline.entsoe import day_ahead_params
        from pipeline.extract import extract_to_landing

        _label(zone)
        day = _run_day()
        landed = extract_to_landing(
            endpoint="",
            dataset=DATASET,
            logical_date=day,
            suffix=".xml",
            zone=zone,
            params=day_ahead_params(ZONES[zone], day),
        )
        return {"zone": zone, "path": str(landed)}

    @task(map_index_template="{{ zone_key }}")
    def parse(delivery: dict[str, str]) -> dict[str, str]:
        """Parse one zone's landed XML into the parsed zone."""
        from pipeline.parse import parse_landed_file

        zone = delivery["zone"]
        _label(zone)
        target, _ = parse_landed_file(
            Path(delivery["path"]),
            DATASET,
            _run_day(),
            zone=zone,
            expected_bidding_zone=ZONES[zone],
        )
        return {"zone": zone, "path": str(target)}

    # Extract and parse fan out freely: each zone touches only its own files.
    # Loads all write to one DuckDB file, and DuckDB allows one writer at a time,
    # so they run one after another. In parallel, all but one were refused with
    # "Could not set lock on file".
    @task(map_index_template="{{ zone_key }}", max_active_tis_per_dag=1)
    def load(delivery: dict[str, str]) -> dict[str, str | int]:
        """Replace this zone's delivery for the day in raw."""
        from pipeline.load import load_parquet_partition, warehouse

        _label(delivery["zone"])
        with warehouse() as conn:
            rows = load_parquet_partition(conn, Path(delivery["path"]), RAW_TABLE, _run_day())
        return {"zone": delivery["zone"], "rows": rows}

    @task
    def validate(loaded: list[dict[str, str | int]]) -> None:
        """Fail loudly rather than publishing a day with a zone missing.

        A silent zero-row load is the most common way a pipeline lies to its
        consumers for a week before anyone notices.
        """
        results = list(loaded)
        missing = sorted(set(ZONES) - {str(r["zone"]) for r in results})
        empty = sorted(str(r["zone"]) for r in results if r["rows"] == 0)
        if missing or empty:
            raise ValueError(f"{RAW_TABLE}: zones not loaded {missing}, zones empty {empty}")

    @task.bash
    def transform() -> str:
        """Build and test every dbt model. Any failing dbt test fails the run.

        Skipped in backfill runs. A backfill is hundreds of runs, and dbt after
        each one would cost an hour and fight the backfill's own loads for
        DuckDB's single writer lock. `make backfill` builds once at the end
        (scripts/build_marts.sh), bringing every backfilled day in at once.
        """
        from airflow.exceptions import AirflowSkipException

        if get_current_context()["dag_run"].run_type == "backfill":
            raise AirflowSkipException(
                "backfill run: dbt runs once after the backfill, not per day"
            )
        return f"cd {DBT_PROJECT_DIR} && {DBT_BIN} deps && {DBT_BIN} build --target {DBT_TARGET}"

    deliveries = extract.expand(zone=sorted(ZONES))
    loaded = load.expand(delivery=parse.expand(delivery=deliveries))
    validate(loaded) >> transform()


day_ahead_prices()
