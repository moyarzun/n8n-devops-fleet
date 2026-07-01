.PHONY: build up down logs shell test status

build:
	docker compose build

up:
	docker compose up -d
	@echo ""
	@echo "  DevOps Fleet arriba:"
	@echo "  N8N Dashboard    → http://localhost:5679"
	@echo "  Fleet API        → http://localhost:8010"
	@echo "  MCP Server       → http://localhost:8011"
	@echo "  Fleet Dashboard  → http://localhost:8010/"
	@echo ""

down:
	docker compose down

logs:
	docker compose logs -f devops-fleet-api

logs-all:
	docker compose logs -f

shell:
	docker compose exec devops-fleet-api sh

status:
	@curl -s http://localhost:8010/health | python3 -m json.tool || echo "API no disponible"

test-deploy:
	@curl -s -X POST http://localhost:8010/pipeline/deploy \
	  -H "Content-Type: application/json" \
	  -d '{"environment":"staging","triggered_by":"makefile-test"}' \
	  | python3 -m json.tool

test-metrics:
	@curl -s -X POST http://localhost:8010/pipeline/metrics \
	  -H "Content-Type: application/json" \
	  -d '{"window_days":7,"report_type":"dora"}' \
	  | python3 -m json.tool

dora:
	@curl -s http://localhost:8010/metrics/dora | python3 -m json.tool

jobs:
	@curl -s http://localhost:8010/jobs | python3 -m json.tool

slo-define:
	@curl -s -X POST http://localhost:8010/slo \
	  -H "Content-Type: application/json" \
	  -d '{"name":"api-availability","target_pct":99.9,"window_days":30}' \
	  | python3 -m json.tool

test:
	docker compose exec devops-fleet-api python3 -m pytest /data/scripts/../tests/ -v

restart:
	docker compose restart devops-fleet-api
