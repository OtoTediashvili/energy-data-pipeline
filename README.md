# Batch ELT Pipeline

> Template repo. Replace this README with your real one when you pick a data source — the structure below is the structure your portfolio README should follow.

A daily batch pipeline: source API → immutable landing zone → warehouse → dbt star schema, orchestrated by Airflow. Reruns of any date replace that date's data rather than duplicating it, so backfills are safe.

## Architecture

```
  Source API
      │  httpx + tenacity (retry 429/5xx, fail fast on 4xx)
      ▼
  Landing zone            data/landing/dataset=…/year=…/month=…/
      │                   raw bytes, never mutated, atomic writes
      │  DuckDB read_json_auto + lineage columns
      ▼
  raw schema              delete+insert scoped to _logical_date
      │
      │  dbt
      ▼
  staging schema          rename, cast, dedupe source revisions
      │
      ▼
  marts schema            dim_country · fct_daily_prices (incremental)
      │
      ▼
  Quality gates           17 dbt tests + a zero-row guard in the DAG
```

Orchestration: Airflow 3, `LocalExecutor`, `catchup=True`, `max_active_runs=1`.

## Quickstart

```bash
bash scripts/bootstrap.sh    # installs uv, Python 3.12, deps, hooks
source .venv/bin/activate
make check                   # lint + typecheck + tests
make up                      # Airflow at localhost:8080 (admin / admin)
```

## Design decisions

**Landing zone holds raw bytes, not parsed records.** Parsing happens downstream. When a transform bug appears three weeks later, the fix is a replay from local disk rather than a re-fetch from an API that may have rate limits or a retention window.

**Landing paths are a pure function of (dataset, date).** Rerunning an interval overwrites one file. This is what makes `catchup=True` safe — without it, a 90-day backfill produces 90 duplicate loads.

**Writes are atomic.** Payloads go to a `.tmp` file and are then renamed. A crash mid-write leaves no half-written partition for a downstream task to read as complete.

**Loads use delete+insert on the partition key, not append.** Same reasoning. `_logical_date`, `_ingested_at`, and `_source_file` are attached at load time so every row is traceable to the file it came from.

**Staging deduplicates on `_ingested_at`.** The source publishes revisions — the same record can arrive again with a corrected value. `row_number()` keeps the most recent version rather than letting both flow into the fact table.

**`generate_schema_name` is overridden.** dbt's default produces `main_raw`, `main_staging`, `main_marts`. Each developer has their own DuckDB file here, so the collision-avoidance prefix buys nothing. Revert this if you move to a shared warehouse.

**LocalExecutor, not Celery.** Celery adds Redis and worker containers for throughput this workload will never need. Fewer moving parts, faster local startup.

**Airflow is installed via Docker, not `pyproject.toml`.** Airflow pins its transitive dependencies aggressively and will fight your analytics libraries. CI installs it separately against the official constraints file to verify DAGs import.

## Testing

| Layer | Tool | What it covers |
|---|---|---|
| Unit | pytest + respx | Retry on 5xx, fail fast on 4xx, idempotent paths, no temp leftovers |
| Integration | pytest + real DuckDB | Partition replacement, lineage columns, cross-partition isolation |
| Data | dbt tests | Uniqueness, referential integrity, accepted values, accepted ranges |
| Types | mypy strict | Whole `src/` and `tests/` tree |
| Secrets | gitleaks | Pre-commit and CI |

```bash
make test        # unit
make dbt-build   # models + data tests
make check       # everything CI runs
```

## Repo layout

```
├── dags/                  Airflow DAGs — orchestration only, no business logic
├── src/pipeline/          Importable package: config, logging, extract, load
├── dbt/
│   ├── models/staging/    Rename, cast, dedupe. No joins.
│   ├── models/marts/      Star schema. Business grain.
│   └── macros/            Schema naming override
├── tests/                 pytest suite
├── scripts/bootstrap.sh   One-command workstation setup
└── docker-compose.yml     Airflow 3 + Postgres
```

Business logic lives in `src/`, not in DAG files. DAGs stay thin so the logic is unit-testable without an Airflow runtime.

## What's next

- [ ] Swap the placeholder endpoint for a real source
- [ ] Source freshness checks (`dbt source freshness`)
- [ ] Terraform the cloud deployment
- [ ] Alerting on DAG failure
- [ ] Column-level lineage in dbt docs
