.PHONY: help up down clean pull-models reset logs up-airflow trigger corrupt reset-prod agent

COMPOSE := docker compose

OLLAMA_MODEL     ?= qwen2.5:3b
POSTGRES_USER    ?= rico
POSTGRES_DB      ?= rico
MINIO_ACCESS_KEY ?= minioadmin
MINIO_SECRET_KEY ?= minioadmin
MINIO_BUCKET     ?= rico-raw

help:
	@echo "Lab targets:"
	@echo "  up           start Postgres+pgvector, MinIO, Ollama (waits until healthy)"
	@echo "  pull-models  pull qwen2.5:3b into the Ollama container (run once)"
	@echo "  down         stop services (volumes preserved)"
	@echo "  clean        stop services and wipe volumes (full reset)"
	@echo "  reset        truncate Postgres tables + clear MinIO bucket (lighter than clean)"
	@echo "  logs         tail compose logs"

up:
	$(COMPOSE) up -d --wait postgres minio ollama
	$(COMPOSE) up -d minio-init ollama-init

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

pull-models:
	$(COMPOSE) exec ollama ollama pull $(OLLAMA_MODEL)

# Wipe lab data without re-pulling Ollama or rebuilding volumes.
# Use this between notebook re-runs (after `Kernel → Restart`).
reset:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
	  "TRUNCATE TABLE screens_metadata, screens_embeddings, screens_review_queue, screens_eval RESTART IDENTITY;"
	$(COMPOSE) exec minio mc alias set local http://minio:9000 $(MINIO_ACCESS_KEY) $(MINIO_SECRET_KEY) >/dev/null
	$(COMPOSE) exec minio mc rm --recursive --force local/$(MINIO_BUCKET)/ >/dev/null 2>&1 || true
	@echo "lab state truncated"

logs:
	$(COMPOSE) logs -f --tail=100

AIRFLOW_DAG ?= rico_pipeline
LIMIT       ?= 5

up-airflow:
	GIT_SHA=$$(git rev-parse HEAD) $(COMPOSE) up -d --build airflow-init airflow-scheduler airflow-webserver

trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger $(AIRFLOW_DAG) --conf '{"limit": $(LIMIT)}'

corrupt:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
	  "INSERT INTO screens_embeddings (screen_id, model_name, model_version, embedding_kind, vector, run_id, source_fingerprint) \
	   SELECT screen_id, model_name, model_version, embedding_kind, vector, run_id, source_fingerprint \
	   FROM screens_embeddings LIMIT 1;"

reset-prod:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
	  "TRUNCATE screens_metadata, screens_embeddings, screens_review_queue, screens_eval, \
	   audit_results, pipeline_metrics, pipeline_runs RESTART IDENTITY CASCADE;"

agent:
	python agent/agent.py
