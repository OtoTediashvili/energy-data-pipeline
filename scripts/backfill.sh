#!/usr/bin/env bash
# Backfill market history, then build the marts once.
#
#   make backfill FROM=2025-10-01 TO=2026-09-20
#
# Runs the DAG for every day from FROM up to, not including, TO, waits for the
# runs to finish, then builds the marts once (make build-marts), bringing every
# backfilled day in. Days that already have a run are left alone. Set
# REPROCESS=failed to rerun the days a previous backfill failed.
#
# Each guard below is a behaviour of Airflow 3.0 found by testing, not assumed:
#
#  * Backfill runs never start while the DAG is paused, so it is unpaused.
#  * The backfill must end on or before the newest scheduled run. If a
#    backfill run is the newest run, the scheduler resumes catch-up from it:
#    one "successful" run with no tasks for every day up to the start date,
#    and each of them then blocks any later backfill of its day.
#  * A backfill ignores the DAG's max_active_runs (one at a time) and defaults
#    to 10 runs at once. MAX_ACTIVE_RUNS sets it explicitly.
#  * Backfill runs skip dbt (see the DAG), so the build at the end is what
#    brings the backfilled days into the marts. It runs dbt directly: a
#    triggered run would first download today's prices, which ENTSO-E refuses
#    until the day's auction is published.
set -euo pipefail

DAG_ID=day_ahead_prices
USAGE="usage: make backfill FROM=YYYY-MM-DD TO=YYYY-MM-DD  (TO is not included)"
MAX_ACTIVE_RUNS=${MAX_ACTIVE_RUNS:-4}
REPROCESS=${REPROCESS:-none}

fail() {
    echo "backfill: $*" >&2
    exit 1
}

[[ $# -eq 2 && -n $1 && -n $2 ]] || fail "$USAGE"
FROM=$(date -d "$1" +%F 2>/dev/null) || fail "FROM is not a date: $1"
TO=$(date -d "$2" +%F 2>/dev/null) || fail "TO is not a date: $2"
[[ $FROM < $TO ]] || fail "FROM must be before TO ($FROM, $TO)"
LAST_DAY=$(date -d "$TO - 1 day" +%F)

airflow() { docker compose exec -T airflow-scheduler airflow "$@"; }
# One value per line, columns separated by |, no headers.
sql() { docker compose exec -T postgres psql -U airflow -AtX -v ON_ERROR_STOP=1 -c "$1"; }

newest=$(sql "select coalesce(max(logical_date at time zone 'UTC')::date::text, 'none')
              from dag_run where dag_id = '$DAG_ID' and run_type = 'scheduled'")
if [[ $newest == none ]]; then
    fail "there are no scheduled runs yet. Unpause the DAG (make unpause), let it
catch up to today, then backfill the days before its start date."
fi
if [[ $LAST_DAY > $newest ]]; then
    fail "the backfill would end on $LAST_DAY, after the newest scheduled run ($newest).
The scheduler would carry on from $LAST_DAY, filling every day after it with
empty runs. Choose TO no later than $(date -d "$newest + 1 day" +%F)."
fi

busy=$(sql "select count(*) from dag_run where dag_id = '$DAG_ID'
            and run_type <> 'backfill' and state in ('queued', 'running')")
[[ $busy -eq 0 ]] || fail "$busy run(s) still queued or running. Wait for them: their dbt
build and the backfill's loads would take turns at the warehouse."

# Airflow allows one backfill per DAG at a time, and marks one complete up to
# 30 seconds after its last run finishes.
running=$(sql "select count(*) from backfill where dag_id = '$DAG_ID' and completed_at is null")
[[ $running -eq 0 ]] || fail "another backfill is still running. Wait for it to finish."

echo "Backfilling $FROM to $LAST_DAY, $MAX_ACTIVE_RUNS days at a time (reprocess: $REPROCESS)"
airflow dags unpause "$DAG_ID" >/dev/null
airflow backfill create --dag-id "$DAG_ID" --from-date "$FROM" --to-date "$TO" \
    --max-active-runs "$MAX_ACTIVE_RUNS" --reprocess-behavior "$REPROCESS" >/dev/null
backfill_id=$(sql "select max(id) from backfill where dag_id = '$DAG_ID'")

# The scheduler does the work; this only watches until Airflow marks the
# backfill complete. Ctrl-C stops the watching, not the backfill.
echo "  (Ctrl-C stops this progress display, not the backfill)"
skipped=$(sql "select count(*) from backfill_dag_run
               where backfill_id = $backfill_id and dag_run_id is null")
while :; do
    progress=$(sql "
        select count(*),
               count(*) filter (where r.state in ('success', 'failed')),
               count(*) filter (where r.state = 'failed'),
               (select completed_at is not null from backfill where id = $backfill_id)
        from backfill_dag_run b join dag_run r on r.id = b.dag_run_id
        where b.backfill_id = $backfill_id")
    IFS='|' read -r total finished failed complete <<<"$progress"
    printf '\r  %s of %s days finished, %s failed, %s skipped (already had a run)' \
        "$finished" "$total" "$failed" "$skipped"
    [[ $complete == t ]] && break
    sleep 15
done
echo

if [[ $failed -gt 0 ]]; then
    echo "Failed days (open them in the UI to see why):"
    sql "select (r.logical_date at time zone 'UTC')::date
         from backfill_dag_run b join dag_run r on r.id = b.dag_run_id
         where b.backfill_id = $backfill_id and r.state = 'failed' order by 1" | sed 's/^/  /'
    echo "To retry just those: make backfill FROM=$FROM TO=$TO REPROCESS=failed"
fi

if [[ $finished -gt $failed ]]; then
    "$(dirname "$0")/build_marts.sh"
fi
