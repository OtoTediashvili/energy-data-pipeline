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
	mkdir -p logs data
	# Containers run as you (AIRFLOW_UID = your user id, the official guidance
	# on Linux), so everything they write under ./data and ./logs stays yours to
	# edit and delete. The chown repairs directories an earlier setup left owned
	# by another user, and runs inside a container so no sudo is needed.
	AIRFLOW_UID=$$(id -u) docker compose build
	AIRFLOW_UID=$$(id -u) docker compose run --rm --no-deps --entrypoint chown -u root airflow-init -R $$(id -u):0 /opt/airflow/logs /opt/airflow/data
	AIRFLOW_UID=$$(id -u) docker compose up -d
	@echo "Airflow: http://localhost:8081 (admin / admin)"

down: ## Stop Airflow
	docker compose down

logs: ## Tail Airflow logs
	docker compose logs -f

clean: ## Remove generated artefacts
	rm -rf .venv .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

reset-data: ## Wipe local landing zone and warehouse
	rm -rf data/landing/* data/warehouse/*

.PHONY: help install fmt lint typecheck test test-all check dbt-deps dbt-build dbt-docs up down logs clean reset-data
