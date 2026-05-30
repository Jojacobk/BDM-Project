.PHONY: up down logs pull-models migrate trigger test clean reset-db

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs --tail=200 -f

pull-models:
	docker compose exec ollama ollama pull $${OLLAMA_MODEL:-qwen2.5:3b}

migrate:
	docker compose exec -T postgres psql -U $${POSTGRES_USER:-rico} -d $${POSTGRES_DB:-rico} -f /migrations/001_init.sql

trigger:
	docker compose exec -T airflow-scheduler python -c "from airflow.api.common.trigger_dag import trigger_dag; trigger_dag(dag_id='rico_production_pipeline', conf={'LIMIT': 5})"

test:
	docker compose exec -T airflow-scheduler python -m pytest /opt/airflow/tests

reset-db:
	docker compose down -v
	docker compose up -d postgres storage mc ollama

clean:
	docker compose down -v
