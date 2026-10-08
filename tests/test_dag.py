"""DAG integrity: the structure the pipeline's guarantees depend on.

Needs Airflow, which the project venv deliberately does not install: Airflow
pins its dependencies hard and would fight the analytics stack. Skipped there;
CI's DAG job runs it with Airflow present.
"""

from __future__ import annotations

import csv
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag

DAGS = Path(__file__).parent.parent / "dags"
ZONES_SEED = Path(__file__).parent.parent / "dbt" / "seeds" / "bidding_zones.csv"
PIPELINE = ["extract", "parse", "load", "validate", "transform"]


@pytest.fixture(scope="module")
def dag() -> Any:
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    # bag.dags is what parsing produced, in memory. bag.get_dag() would query
    # Airflow's metadata database, which a test environment doesn't have.
    found = bag.dags.get("day_ahead_prices")
    assert found is not None
    return found


def test_tasks_run_in_pipeline_order(dag: Any) -> None:
    assert set(dag.task_ids) == set(PIPELINE)
    for upstream, downstream in pairwise(PIPELINE):
        assert downstream in dag.get_task(upstream).downstream_task_ids


def test_backfills_are_safe(dag: Any) -> None:
    """catchup replays history; one run at a time keeps it inside the rate limit."""
    assert dag.catchup is True
    assert dag.max_active_runs == 1


def test_runs_after_the_auction_publishes(dag: Any) -> None:
    """Day-ahead results publish around 12:45 CET: 13:00 UTC is after that all year."""
    assert "0 13 * * *" in str(dag.timetable.summary)


def test_every_task_retries(dag: Any) -> None:
    assert all(task.retries >= 1 for task in dag.tasks)


def test_fans_out_one_task_per_zone_in_the_seed(dag: Any) -> None:
    """The zone list is the dbt reference seed. Adding a bidding zone is one
    row there; the DAG must pick it up without any code change."""
    with ZONES_SEED.open(newline="") as handle:
        seed_zones = sorted(row["country_code"] for row in csv.DictReader(handle))
    for task_id in ("extract", "parse", "load"):
        assert dag.get_task(task_id).is_mapped, f"{task_id} should fan out per zone"
    assert sorted(dag.get_task("extract").op_kwargs_expand_input.value["zone"]) == seed_zones


def test_warehouse_writes_are_serialised(dag: Any) -> None:
    """DuckDB allows one writer. Parallel loads locked each other out."""
    assert dag.get_task("load").max_active_tis_per_dag == 1
