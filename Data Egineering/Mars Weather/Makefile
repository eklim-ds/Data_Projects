.DEFAULT_GOAL := help

# Optional, untracked file with NASA_API_KEY=... (see .env.example)
-include .env

PYTHON ?= python3
VENV   := .venv
BIN    := $(VENV)/bin

export PYTHONPATH := src
export LAKE_PATH ?= $(CURDIR)/data/lake
export WAREHOUSE_PATH ?= $(CURDIR)/data/warehouse/mars_weather.duckdb
export DBT_SEND_ANONYMOUS_USAGE_STATS := false
export NASA_API_KEY

SAMPLE := sample_data/insight_weather_sample.json

.PHONY: help setup ingest ingest-sample dbt-build pipeline demo dashboard test docs clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-14s %s\n", $$1, $$2}'

setup: ## Create .venv and install dependencies (Python 3.10+)
	$(PYTHON) -m venv $(VENV)
	$(BIN)/python -m pip install --quiet --upgrade pip
	$(BIN)/python -m pip install -r requirements.txt

ingest: ## Pull the live NASA API -> raw JSON in the object store (needs NASA_API_KEY in .env)
	$(BIN)/python -m mars_weather.ingest

ingest-sample: ## Land the bundled sample response instead (no key, no network)
	$(BIN)/python -m mars_weather.ingest --from-file $(SAMPLE)

dbt-build: ## Build all dbt models and run all dbt tests
	@mkdir -p $(dir $(WAREHOUSE_PATH))
	cd dbt && ../$(BIN)/dbt build --profiles-dir .

pipeline: ingest dbt-build ## Full batch run with live data: ingest -> dbt build

demo: ingest-sample dbt-build ## Full batch run with the bundled sample (no API key needed)

dashboard: ## Open the dashboard at http://localhost:8501
	$(BIN)/streamlit run dashboard/app.py

test: ## Run the Python tests (they also run `dbt build` on test data)
	$(BIN)/python -m pytest

docs: ## Generate and serve dbt docs (lineage graph) at http://localhost:8080
	@mkdir -p $(dir $(WAREHOUSE_PATH))
	cd dbt && ../$(BIN)/dbt docs generate --profiles-dir . && ../$(BIN)/dbt docs serve --profiles-dir . --port 8080

clean: ## Delete the lake, the warehouse and dbt artifacts (re-create them with `make demo`)
	rm -rf data/lake data/warehouse dbt/target dbt/logs
