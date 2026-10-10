#!/usr/bin/env bash
# Build and test every dbt model once, outside any DAG run.
#
#   make build-marts
#
# The same dbt build the DAG's transform task runs, without the download in
# front of it. make backfill ends with it: backfill runs skip dbt, and a
# triggered run would first download today's prices, which ENTSO-E refuses
# until the day's auction is published, so the build behind it never ran.
#
# Runs inside the scheduler container, where dbt and the warehouse settings
# live. DuckDB allows one writer, so it refuses while a load or dbt task is
# writing.
set -euo pipefail

DAG_ID=day_ahead_prices

sql() { docker compose exec -T postgres psql -U airflow -AtX -v ON_ERROR_STOP=1 -c "$1"; }

writing=$(sql "select count(*) from task_instance where dag_id = '$DAG_ID'
               and task_id in ('load', 'transform') and state in ('queued', 'running')")
if [[ $writing -gt 0 ]]; then
    echo "build-marts: a load or dbt task is writing to the warehouse right now, and DuckDB" >&2
    echo "allows one writer at a time. Wait for it to finish, then run make build-marts." >&2
    exit 1
fi

log=$(mktemp)
trap 'rm -f "$log"' EXIT
echo "Building and testing every dbt model..."
# Single quotes: the variables are the container's, not this shell's.
# shellcheck disable=SC2016
if docker compose exec -T airflow-scheduler bash -c \
    'cd "$DBT_PROJECT_DIR" && "$DBT_BIN" deps && "$DBT_BIN" build --target "$DBT_TARGET"' \
    >"$log" 2>&1; then
    sed 's/\x1b\[[0-9;]*m//g' "$log" | grep -E "Done\. PASS=" | sed 's/^[0-9:]* *//'
else
    echo "dbt failed. The end of its output:" >&2
    sed 's/\x1b\[[0-9;]*m//g' "$log" | tail -n 30 >&2
    exit 1
fi
