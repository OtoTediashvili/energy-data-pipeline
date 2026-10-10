.DEFAULT_GOAL := help
SHELL := /bin/bash

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Create venv and install everything
	uv venv --python 3.12 .venv
	. .venv/bin/activate && uv pip install -e ".[dev,dbt]"
	. .venv/bin/activate && pre-commit install

fmt: ## Auto-format and auto-fix
	. .venv/bin/activate && ruff format . && ruff check --fix .

lint: ## Lint without fixing
	. .venv/bin/activate && ruff check . && ruff format --check .

typecheck: ## Static type check
	. .venv/bin/activate && mypy

test: ## Run unit tests with coverage
	. .venv/bin/activate && pytest -m "not integration"

test-all: ## Include integration tests (hits real services)
	. .venv/bin/activate && pytest

check: lint typecheck test ## Everything CI runs

dbt-deps: ## Install dbt packages
	. .venv/bin/activate && cd dbt && dbt deps

dbt-build: ## Run dbt models and tests against the local warehouse
	. .venv/bin/activate && cd dbt && dbt build

dbt-docs: ## Generate and serve dbt docs
	. .venv/bin/activate && cd dbt && dbt docs generate && dbt docs serve

up: ## Start Airflow locally (builds the image on first run)
	mkdir -p logs data config
	# Containers run as you (AIRFLOW_UID = your user id, the official guidance
	# on Linux), so everything they write under ./data and ./logs stays yours to
	# edit and delete. The chown repairs directories an earlier setup left owned
	# by another user, and runs inside a container so no sudo is needed.
	AIRFLOW_UID=$$(id -u) docker compose build
	AIRFLOW_UID=$$(id -u) docker compose run --rm --no-deps --entrypoint chown -u root airflow-init -R $$(id -u):0 /opt/airflow/logs /opt/airflow/data /opt/airflow/config
	AIRFLOW_UID=$$(id -u) docker compose up -d
	@echo "Airflow: http://localhost:8081  user: admin  password: make password"

password: ## Print the Airflow admin password (generated on first start)
	@python3 -c "import json; print(json.load(open('config/simple_auth_manager_passwords.json'))['admin'])" 2>/dev/null \
		|| echo "No password yet: run make up and give the API server a minute to start."

down: ## Stop Airflow
	docker compose down

logs: ## Tail Airflow logs
	docker compose logs -f

clean: ## Remove generated artefacts
	rm -rf .venv .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

DAG_ID := day_ahead_prices

unpause: ## Unpause the DAG; the scheduler catches up every day since its start date
	docker compose exec -T airflow-scheduler airflow dags unpause $(DAG_ID)

backfill: ## Backfill days FROM up to, not including, TO; e.g. make backfill FROM=2025-10-01 TO=2026-09-20
	./scripts/backfill.sh "$(FROM)" "$(TO)"

build-marts: ## Build and test every dbt model now, without a DAG run
	./scripts/build_marts.sh

# `airflow dags delete` removes a DAG's backfills before the runs that point at
# them, which a foreign key refuses once any backfill exists. Unlinking the
# runs first lets it work.
reset-history: ## Delete the DAG's runs from Airflow (data files stay); it reappears paused
	docker compose exec -T airflow-scheduler airflow dags pause $(DAG_ID)
	docker compose exec -T postgres psql -U airflow -X -v ON_ERROR_STOP=1 \
		-c "update dag_run set backfill_id = null where dag_id = '$(DAG_ID)'"
	docker compose exec -T airflow-scheduler airflow dags delete $(DAG_ID) --yes
	@# Airflow re-registers the DAG on its next scan of the dags folder, paused.
	@# Commands run before that find no DAG, so wait for it here.
	@echo "Run history deleted. Waiting for Airflow to find the DAG again..."
	@for i in $$(seq 1 36); do \
		found=$$(docker compose exec -T postgres psql -U airflow -AtX \
			-c "select count(*) from dag where dag_id = '$(DAG_ID)'"); \
		if [ "$$found" = 1 ]; then echo "The DAG is back, paused, with no runs."; exit 0; fi; \
		sleep 5; \
	done; \
	echo "Still missing after 3 minutes: check make logs for the dag processor."; exit 1

reset-data: ## Wipe the landing zone, parsed files and warehouse (pause the DAG first)
	rm -rf data/landing/* data/parsed/* data/warehouse/*

.PHONY: help install fmt lint typecheck test test-all check dbt-deps dbt-build dbt-docs up password down logs clean unpause backfill build-marts reset-history reset-data
