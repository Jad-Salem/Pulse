#!/usr/bin/env bash
# Block until the platform is serving end to end, or fail after a timeout (default 300 s).
#   ./scripts/wait-for-stack.sh [timeout_seconds]
set -euo pipefail

timeout="${1:-300}"
deadline=$(( $(date +%s) + timeout ))

check() {
  local name="$1" cmd="$2"
  until eval "$cmd" > /dev/null 2>&1; do
    if (( $(date +%s) > deadline )); then
      echo "timed out waiting for ${name}" >&2
      docker compose ps >&2 || true
      exit 1
    fi
    sleep 3
  done
  echo "ready: ${name}"
}

check "collector"      "curl -fsS http://localhost:8000/readyz"
check "query-api"      "curl -fsS http://localhost:8001/readyz"
check "flink job"      "curl -fsS http://localhost:8082/jobs/overview | grep -q '\"state\":\"RUNNING\"'"
check "first events in ClickHouse" \
  "curl -fsS 'http://localhost:8123/?user=query_api&password=${CLICKHOUSE_QUERY_API_PASSWORD:-query-api-dev}' \
     --data-binary 'SELECT count() FROM pulse.events' | grep -qv '^0$'"
check "grafana"        "curl -fsS http://localhost:3000/api/health"
echo "platform is up"
