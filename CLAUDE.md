# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A template/portfolio repo for a daily batch ELT pipeline: source API → immutable landing zone → DuckDB warehouse → dbt star schema, orchestrated by Airflow 3. The README (`README.md`) explicitly frames this as a template — the placeholder source is ENTSO-E-shaped (`prices`/`country` fields) but points at `https://example.com/api` until a real source is wired in.

## Commands

```bash
bash scripts/bootstrap.sh    # one-time: installs uv, Python 3.12, deps, pre-commit hooks (run inside WSL2 on Windows, not PowerShell)
source .venv/bin/activate

make check                   # lint + typecheck + test — everything CI runs
make fmt                     # ruff format + ruff check --fix
make lint                    # ruff check + ruff format --check (no fixing)
make typecheck               # mypy (strict, whole src/ + tests/ tree)
make test                    # pytest, unit only (-m "not integration")
make test-all                # pytest including integration tests (hits real services)

make dbt-deps                # dbt deps (installs dbt_utils)
make dbt-build               # dbt build: models + data tests, against dev target
make dbt-docs                # dbt docs generate && serve

make up / make down / make logs   # docker compose Airflow stack — http://localhost:8080 (admin / admin)
make reset-data              # wipe data/landing/* and data/warehouse/* for a clean local rerun
make clean                   # remove .venv, caches, coverage artifacts
```

Run a single test: `pytest tests/test_load.py::test_name -m "not integration"` (standard pytest node-id selection; the `-m` filter still applies).

There is no `npm`/JS tooling — this is a pure Python + dbt + Airflow stack.

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
    │  dbt
    ▼
staging schema          rename, cast, dedupe source revisions
    ▼
marts schema            dim_country · fct_daily_prices (incremental)
    ▼
Quality gates           dbt tests + a zero-row guard in the DAG
```

Orchestration: Airflow 3, `LocalExecutor`, `catchup=True`, `max_active_runs=1`.

### Layer boundaries (`src/pipeline/`)

- **`config.py`** — `Settings` (pydantic-settings, env prefix `PIPELINE_`, loads `.env`). Every module reads config through `get_settings()`; nothing reads `os.environ` directly. Config errors surface at startup, not mid-DAG-run.
- **`extract.py`** — fetches from the source API and writes raw, unparsed bytes to the landing zone. Nothing here parses or reshapes payloads — that's intentional, so a downstream transform bug only costs a replay from disk, not a re-fetch against a rate-limited API. `landing_path(dataset, logical_date)` is a **pure function**; the same (dataset, date) always resolves to the same file, which is what makes `catchup=True` backfills idempotent. Writes are atomic (`.tmp` then `rename`). Retries: `tenacity` retries `TransientSourceError`/`httpx.TransportError` (429, 5xx) with exponential backoff, fails immediately on other 4xx via `PermanentSourceError`.
- **`load.py`** — moves one landing-zone file into `raw.<table>` in DuckDB. Idempotency strategy is **delete+insert scoped to the partition key** (`_logical_date`), not append — rerunning a date replaces exactly that date. Every row gets lineage columns attached at load time: `_logical_date`, `_ingested_at`, `_source_file`. `read_json_auto` infers the schema; a real deployment should pin an explicit `columns={...}` spec so upstream schema drift fails loudly here instead of silently reshaping dbt models downstream.
- **`logging_config.py`** — structlog, human-readable console locally / JSON when `PIPELINE_LOG_JSON=true` (set in prod/Docker). Call `configure_logging()` once at process entry; get loggers via `get_logger(__name__)`.

Business logic lives in `src/`, never in DAG files — DAGs stay thin (`dags/example_pipeline.py`: extract → load → validate → transform) so logic is unit-testable without an Airflow runtime. The `validate` task raises on zero landed rows *before* the dbt `transform` task runs, so a silently-empty extract never gets published downstream.

### dbt (`dbt/`)

- Each developer works against their own local DuckDB file (`data/warehouse/warehouse.duckdb`), so `generate_schema_name` (in `dbt/macros/`) is overridden to use the custom schema verbatim (`raw`, `staging`, `marts`) instead of dbt's default `<target>_<schema>` prefixing. **Revert this if the project moves to a shared warehouse** (Snowflake/BigQuery), where the collision-avoidance prefix matters again.
- `models/staging/` — rename, cast, dedupe only, no joins. `stg_prices.sql` deduplicates source revisions by `row_number() over (partition by id, _logical_date order by _ingested_at desc)`, keeping the latest `_ingested_at` — the source can republish a corrected value for the same record.
- `models/marts/` — star schema at business grain. `dim_country` has a `dbt_utils.generate_surrogate_key` surrogate key so the fact table never depends on a source-controlled natural key. `fct_daily_prices` is `materialized='incremental'` with `incremental_strategy='delete+insert'` keyed on `[country_sk, observed_date]` — mirrors the same "replace-a-partition" idempotency pattern used in `load.py`.
- `vars.logical_date` in `dbt_project.yml` is overridden by Airflow at run time so the loader and dbt agree on which partition is being processed.
- Profile targets (`profiles.yml`): `dev` (local DuckDB file), `ci` (`ci.duckdb`, used by the CI dbt job), `prod` (in-container path used by the Airflow `transform` task).
- CI seeds before building (`dbt seed --target ci` then `dbt build --target ci --exclude "resource_type:seed"`) because a `source()` has no DAG edge to a seed, so a single `dbt build` would race the staging model against the seed load.

### Airflow (`docker-compose.yml`)

Deliberately slimmer than Apache's official compose file: `LocalExecutor`, no Redis/Celery/Flower — this workload doesn't need Celery's throughput and it's two fewer moving parts locally. Airflow is installed via Docker image, **not** as a `pyproject.toml` dependency, because Airflow pins transitive deps aggressively and fights analytics libraries (duckdb/polars/pandas etc.); CI verifies DAGs import by installing Airflow separately against the official constraints file for the pinned `AIRFLOW_VERSION`. `PYTHONPATH=/opt/airflow/src` is set so `pipeline.*` is importable inside DAG tasks; `src/`, `dbt/`, `data/`, `dags/` are all bind-mounted.

## Design decisions worth knowing before changing behavior

- **Landing zone holds raw bytes, not parsed records.** Parsing happens only downstream (in `load.py`/dbt).
- **Everything keyed on `logical_date`, never "today."** This is what makes backfills and reruns safe/idempotent throughout the stack (landing path → raw table → incremental mart).
- **Staging dedup key is `_ingested_at`, not source ordering** — always keep the most recently *ingested* revision, not the highest ID or similar.

## Testing layout

| Layer | Tool | Covers |
|---|---|---|
| Unit | pytest + respx | Retry on 5xx, fail fast on 4xx, idempotent paths, no temp leftovers |
| Integration | pytest + real DuckDB | Partition replacement, lineage columns, cross-partition isolation |
| Data | dbt tests | Uniqueness, referential integrity, accepted values/ranges |
| Types | mypy strict | Whole `src/` and `tests/` tree |
| Secrets | gitleaks | Pre-commit and CI |

Tests requiring real external services are marked `@pytest.mark.integration` and excluded by default (`make test`); `make test-all` includes them. `tests/conftest.py` gives every test an isolated `tmp_path`-scoped `Settings` fixture and auto-clears `PIPELINE_*` env vars so a developer's real `.env` never leaks into a test run.
