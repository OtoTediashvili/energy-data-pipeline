"""Reference DAG: extract -> land -> load -> transform -> validate.

Swap the endpoint and dataset names for your real source. The structure is the
point, and it demonstrates the things interviewers ask about:

* ``catchup=True`` with a real ``start_date``, so backfills work
* ``max_active_runs=1``, so a backfill cannot stampede the source API
* Every task keyed on ``logical_date``, so reruns replace rather than duplicate
* Transform runs as a separate task from load, so a broken model never costs
  you a re-fetch
* A data-quality gate that fails the run instead of publishing bad numbers
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from pipeline.extract import extract_to_landing
from pipeline.load import load_json_partition, warehouse

DATASET = "prices"
ENDPOINT = "prices"


@dag(
    dag_id="example_pipeline",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=30),
    },
    tags=["portfolio", "batch", "elt"],
    doc_md=__doc__,
)
def example_pipeline() -> None:
    @task
    def extract(logical_date: pendulum.DateTime | None = None) -> str:
        """Fetch one day and land it raw. Returns the landed path."""
        assert logical_date is not None
        day = logical_date.date()
        path = extract_to_landing(
            endpoint=ENDPOINT,
            dataset=DATASET,
            logical_date=day,
            params={"date": day.isoformat()},
        )
        return str(path)

    @task
    def load(path: str, logical_date: pendulum.DateTime | None = None) -> int:
        """Replace this date's partition in the raw schema."""
        from pathlib import Path

        assert logical_date is not None
        with warehouse() as conn:
            return load_json_partition(conn, Path(path), DATASET, logical_date.date())

    @task.bash
    def transform() -> str:
        """Run dbt. Models are incremental and keyed on the same logical date."""
        return "cd /opt/airflow/dbt && dbt build --target prod"

    @task
    def validate(rows: int) -> None:
        """Fail loudly rather than publishing an empty day.

        A silent zero-row load is the most common way a pipeline lies to its
        consumers for a week before anyone notices.
        """
        if rows == 0:
            raise ValueError(f"{DATASET}: zero rows landed — refusing to publish")

    landed = extract()
    row_count = load(landed)
    validate(row_count) >> transform()


example_pipeline()
