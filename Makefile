# Convenience targets. Everything here is a thin wrapper around docker compose / uv / mvn,
# so each command also works on its own (see README).
SHELL := bash
PY_PROJECTS := services/collector services/query-api tools/platform-init tools/load-generator
MAVEN_IMAGE := maven:3.9.16-eclipse-temurin-17
FLINK_REST := http://localhost:8082
JOB_ID := 00000000000000000000000000000000

.PHONY: help up down clean ps logs wait test test-python test-java lint e2e dashboards savepoint

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

up: ## Build and start the whole platform
	docker compose up -d --build

down: ## Stop the platform (keeps data)
	docker compose down

clean: ## Stop the platform and delete all data volumes
	docker compose down -v

ps: ## Show service status
	docker compose ps

logs: ## Follow logs of all services
	docker compose logs -f --tail=100

wait: ## Block until data flows end to end
	bash scripts/wait-for-stack.sh

test: test-python test-java ## Run all unit and integration tests

test-python: ## Lint, type-check and test every Python project
	@set -e; for p in $(PY_PROJECTS); do \
		echo "== $$p"; \
		(cd $$p && uv sync --locked -q && uv run ruff check . && uv run ruff format --check . \
			&& uv run mypy src tests && uv run pytest -q); \
	done

test-java: ## Build and test the Flink job (uses a local mvn, or Maven in Docker)
	@if command -v mvn > /dev/null; then \
		mvn -B -ntp -f processing/stream-processor/pom.xml verify; \
	else \
		docker run --rm -v "$(CURDIR)":/build -v pulse-m2:/root/.m2 -w /build $(MAVEN_IMAGE) \
			mvn -B -ntp -f processing/stream-processor/pom.xml verify; \
	fi

e2e: wait ## Run the end-to-end suite against the running platform
	cd tests/e2e && uv sync --locked -q && uv run pytest -v

dashboards: ## Regenerate Grafana dashboards from code
	python3 infra/grafana/generate_dashboards.py

savepoint: ## Take a savepoint of the running Flink job (before an upgrade)
	curl -fsS -X POST -H 'Content-Type: application/json' -d '{"cancel-job": false}' \
		$(FLINK_REST)/jobs/$(JOB_ID)/savepoints
