# =============================================================================
# DeFi Risk Lakehouse — Makefile
# =============================================================================
# Usage: make <target>
# Run `make help` to list all available targets.

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
AIRFLOW_SCHEDULER := $(COMPOSE) exec airflow-scheduler
DBT_DIR := /opt/airflow/dbt/defi_risk

# Colour helpers
BOLD  := \033[1m
RESET := \033[0m
GREEN := \033[32m

##@ Setup

.PHONY: init
init: ## First-time project setup: copy .env, build images, start services
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example — add your GRAPH_API_KEY")
	$(COMPOSE) build --pull
	$(MAKE) up
	@echo "Waiting 45s for services to become healthy..."
	@sleep 45
	@echo "$(GREEN)$(BOLD)Environment ready!$(RESET)"
	@echo "  Airflow UI  →  http://localhost:8080  (admin / admin)"
	@echo "  Trino UI    →  http://localhost:8081"
	@echo "  MinIO UI    →  http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  Spark UI    →  http://localhost:8082"
	@echo "  Nessie API  →  http://localhost:19120/api/v2/config"

.PHONY: build
build: ## Rebuild custom Airflow Docker image
	$(COMPOSE) build --no-cache airflow-webserver airflow-scheduler airflow-init

##@ Services

.PHONY: up
up: ## Start all services (detached)
	$(COMPOSE) up -d

.PHONY: down
down: ## Stop and remove containers (keeps volumes)
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop containers AND remove all volumes (destructive)
	$(COMPOSE) down -v
	@echo "All volumes removed."

.PHONY: restart
restart: down up ## Restart all services

.PHONY: ps
ps: ## Show status of running containers
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail logs for all services
	$(COMPOSE) logs -f

.PHONY: logs-scheduler
logs-scheduler: ## Tail Airflow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

.PHONY: logs-spark
logs-spark: ## Tail Spark master + worker logs
	$(COMPOSE) logs -f spark-master spark-worker

##@ Linting & Formatting

.PHONY: lint
lint: ## Run ruff linter (check only, no changes)
	ruff check .
	ruff format --check .

.PHONY: format
format: ## Auto-fix lint issues and reformat code
	ruff check --fix .
	ruff format .

##@ Testing

.PHONY: test
test: test-unit ## Run unit tests (default)

.PHONY: test-unit
test-unit: ## Run unit tests (no Docker required)
	pytest tests/unit/ -v --tb=short -m "not integration"

.PHONY: test-integration
test-integration: up ## Start services then run integration tests
	@echo "Waiting 45s for services to be ready..."
	@sleep 45
	$(AIRFLOW_SCHEDULER) pytest tests/integration/ -v -m integration --tb=short
	$(MAKE) down

.PHONY: test-dag
test-dag: ## Validate Airflow DAG syntax (no Airflow running needed)
	AIRFLOW__CORE__LOAD_EXAMPLES=false \
	  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/test_airflow.db \
	  python -c "from airflow.models import DagBag; \
	    db = DagBag(dag_folder='airflow/dags', include_examples=False); \
	    print(f'DAGs loaded: {len(db.dags)}, Errors: {db.import_errors}'); \
	    exit(1 if db.import_errors else 0)"

##@ dbt

.PHONY: dbt-deps
dbt-deps: ## Install dbt packages
	$(AIRFLOW_SCHEDULER) dbt deps --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

.PHONY: dbt-parse
dbt-parse: ## Parse and validate dbt project (no DB connection needed)
	$(AIRFLOW_SCHEDULER) dbt parse --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

.PHONY: dbt-compile
dbt-compile: ## Compile dbt SQL without executing
	$(AIRFLOW_SCHEDULER) dbt compile --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

.PHONY: dbt-run
dbt-run: ## Run all dbt models
	$(AIRFLOW_SCHEDULER) dbt run --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

.PHONY: dbt-test
dbt-test: ## Run all dbt schema + custom tests
	$(AIRFLOW_SCHEDULER) dbt test --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

.PHONY: dbt-docs
dbt-docs: ## Generate and serve dbt docs
	$(AIRFLOW_SCHEDULER) bash -c "\
	  dbt docs generate --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) && \
	  dbt docs serve --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) --port 8084"

##@ Pipeline

.PHONY: trigger-ingest
trigger-ingest: ## Trigger the DeFi ingestion DAG in Airflow
	$(AIRFLOW_SCHEDULER) airflow dags trigger defi_ingest

.PHONY: trigger-transform
trigger-transform: ## Trigger the Spark + dbt transform DAG in Airflow
	$(AIRFLOW_SCHEDULER) airflow dags trigger defi_transform

.PHONY: spark-bronze
spark-bronze: ## Manually run the bronze Spark loader job
	$(COMPOSE) exec spark-master \
	  spark-submit \
	    --master spark://spark-master:7077 \
	    --conf spark.driver.memory=1g \
	    /opt/spark/jobs/bronze_loader.py

.PHONY: spark-silver
spark-silver: ## Manually run the silver Spark transformer job
	$(COMPOSE) exec spark-master \
	  spark-submit \
	    --master spark://spark-master:7077 \
	    --conf spark.driver.memory=1g \
	    /opt/spark/jobs/silver_transformer.py

##@ Querying

.PHONY: trino-shell
trino-shell: ## Open a Trino SQL shell
	$(COMPOSE) exec trino trino --catalog lakehouse

##@ Help

.PHONY: help
help: ## Show this help message
	@python -c "\
import re, sys; \
lines = open('Makefile').readlines(); \
print('\nUsage: make <target>\n'); \
[\
  print('\n' + l[4:].rstrip()) if l.startswith('##@') \
  else print('  {:<22} {}'.format(*m.groups())) \
  for l in lines \
  for m in [re.match(r'^([a-zA-Z_-]+):.*?##\s*(.*)', l)] if m or l.startswith('##@') \
]"
