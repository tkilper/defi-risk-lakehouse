# =============================================================================
# DeFi Risk Lakehouse — Makefile
# =============================================================================
# Usage: make <target>
# Run `make help` to list all available targets.

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
	@python -c "import os, shutil; (shutil.copy('.env.example', '.env'), print('Created .env from .env.example — add your GRAPH_API_KEY')) if not os.path.exists('.env') else None"
	$(COMPOSE) build --pull
	$(MAKE) up
	@echo Waiting 45s for services to become healthy...
	@python -c "import time; time.sleep(45)"
	@echo Environment ready!
	@echo   Airflow UI  -^>  http://localhost:8080  (admin / admin)
	@echo   Trino UI    -^>  http://localhost:8081
	@echo   MinIO UI    -^>  http://localhost:9001  (minioadmin / minioadmin)
	@echo   Spark UI    -^>  http://localhost:8082
	@echo   Nessie API  -^>  http://localhost:19120/api/v2/config

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
	@echo Waiting 45s for services to be ready...
	@python -c "import time; time.sleep(45)"
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
	  /opt/spark/bin/spark-submit \
	    --master spark://spark-master:7077 \
	    --conf spark.driver.memory=1g \
	    /opt/spark/jobs/bronze_loader.py

.PHONY: spark-silver
spark-silver: ## Manually run the silver Spark transformer job
	$(COMPOSE) exec spark-master \
	  /opt/spark/bin/spark-submit \
	    --master spark://spark-master:7077 \
	    --conf spark.driver.memory=1g \
	    /opt/spark/jobs/silver_transformer.py

##@ Querying

.PHONY: trino-shell
trino-shell: ## Open a Trino SQL shell
	$(COMPOSE) exec trino trino --catalog lakehouse

##@ Spark Jobs (ML Extension)

.PHONY: spark-bronze-liq
spark-bronze-liq: ## Run bronze liquidations Spark job
	$(COMPOSE) exec spark-master \
	  /opt/spark/bin/spark-submit \
	    --master spark://spark-master:7077 \
	    --conf spark.driver.memory=1g \
	    /opt/spark/jobs/bronze_liquidations.py

.PHONY: spark-silver-liq
spark-silver-liq: ## Run silver liquidations Spark job
	$(COMPOSE) exec spark-master \
	  /opt/spark/bin/spark-submit \
	    --master spark://spark-master:7077 \
	    --conf spark.driver.memory=1g \
	    /opt/spark/jobs/silver_liquidations.py

.PHONY: spark-snapshots
spark-snapshots: ## Run position snapshots Spark job
	$(COMPOSE) exec spark-master \
	  /opt/spark/bin/spark-submit \
	    --master spark://spark-master:7077 \
	    --conf spark.driver.memory=1g \
	    /opt/spark/jobs/silver_position_snapshots.py

##@ ML Pipeline

.PHONY: backfill
backfill: ## Backfill 90 days of historical liquidation events
	python scripts/backfill_labels.py --days 90

.PHONY: ingest-liquidations
ingest-liquidations: ## Trigger the liquidation labels Airflow DAG
	$(AIRFLOW_SCHEDULER) airflow dags trigger liquidation_labels

.PHONY: features
features: ## Trigger the feature engineering Airflow DAG
	$(AIRFLOW_SCHEDULER) airflow dags trigger feature_engineering

.PHONY: train
train: ## Train the XGBoost liquidation predictor
	python training/train.py

.PHONY: tune
tune: ## Run Optuna hyperparameter search (50 trials)
	python training/hyperparameter_search.py --n-trials 50

.PHONY: evaluate
evaluate: ## Evaluate Production model and generate SHAP plots
	python training/evaluate.py

.PHONY: score
score: ## Trigger batch scoring Airflow DAG
	$(AIRFLOW_SCHEDULER) airflow dags trigger batch_score

.PHONY: retrain
retrain: ## Trigger model retraining Airflow DAG
	$(AIRFLOW_SCHEDULER) airflow dags trigger model_retrain

.PHONY: monitor
monitor: ## Run Evidently drift monitoring report
	python monitoring/drift_report.py

##@ Model Serving

.PHONY: serve
serve: ## Start the prediction API container
	$(COMPOSE) up -d api

.PHONY: predict
predict: ## Score a sample position via the API (uses sample_position.json)
	curl -s -X POST http://localhost:8000/predict \
	  -H "Content-Type: application/json" \
	  -d '{"health_factor":1.08,"collateral_usd":10000,"debt_usd":8500,"ltv_ratio":0.85,"liquidation_threshold":0.80,"protocol_encoded":0,"num_collateral_assets":1,"largest_collateral_pct":1.0,"is_eth_dominant_collateral":1,"user_prior_liquidations":0}' \
	  | python -m json.tool

.PHONY: reload-model
reload-model: ## Hot-reload the model in the running API container
	curl -s -X POST http://localhost:8000/reload | python -m json.tool

.PHONY: api-health
api-health: ## Check the API health endpoint
	curl -s http://localhost:8000/health | python -m json.tool

##@ Data Versioning (DVC)

.PHONY: dvc-push
dvc-push: ## Push data artifacts to MinIO DVC remote
	dvc push

.PHONY: dvc-pull
dvc-pull: ## Pull data artifacts from MinIO DVC remote
	dvc pull

.PHONY: dvc-init
dvc-init: ## Initialize DVC with MinIO as remote (run once)
	dvc init
	dvc remote add -d minio s3://dvc-artifacts/
	dvc remote modify minio endpointurl http://localhost:9000
	dvc remote modify minio access_key_id minioadmin
	dvc remote modify minio secret_access_key minioadmin

##@ UI Links

.PHONY: mlflow-ui
mlflow-ui: ## Open MLflow UI in browser
	@echo "MLflow UI: http://localhost:5000"
	@python -c "import webbrowser; webbrowser.open('http://localhost:5000')" 2>/dev/null || true

.PHONY: airflow-ui
airflow-ui: ## Open Airflow UI in browser
	@echo "Airflow UI: http://localhost:8080 (admin / admin)"
	@python -c "import webbrowser; webbrowser.open('http://localhost:8080')" 2>/dev/null || true

.PHONY: minio-ui
minio-ui: ## Open MinIO console in browser
	@echo "MinIO UI: http://localhost:9001 (minioadmin / minioadmin)"
	@python -c "import webbrowser; webbrowser.open('http://localhost:9001')" 2>/dev/null || true

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
