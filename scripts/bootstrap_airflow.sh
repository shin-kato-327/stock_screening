#!/usr/bin/env bash
# One-time setup: load .env values into Airflow Variables/Connections.
# Run this *inside* the airflow webserver/scheduler container, or with
# AIRFLOW_HOME pointed at the same metadata DB.
#
# Usage: bash scripts/bootstrap_airflow.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${ROOT}/.env"

if [ ! -f "${ENV_FILE}" ]; then
    echo ".env not found at ${ENV_FILE}" >&2
    exit 1
fi

# shellcheck disable=SC1090
set -a; source "${ENV_FILE}"; set +a

require() {
    local var="$1"
    if [ -z "${!var:-}" ]; then
        echo "Missing required env var: ${var}" >&2
        exit 1
    fi
}

require EDINET_KEY
require JQUANTS_REFRESH_TOKEN
require POSTGRES_USER
require POSTGRES_PASSWORD
require POSTGRES_HOST
require POSTGRES_PORT
require POSTGRES_DB

echo "Setting Airflow Variables..."
airflow variables set EDINET_KEY "${EDINET_KEY}"
airflow variables set JQUANTS_REFRESH_TOKEN "${JQUANTS_REFRESH_TOKEN}"
airflow variables set INITIAL_CAPITAL "${INITIAL_CAPITAL:-10000000}"
airflow variables set MAX_POSITIONS "${MAX_POSITIONS:-20}"
airflow variables set TRANSACTION_COST_BPS "${TRANSACTION_COST_BPS:-10}"
airflow variables set BENCHMARK_TICKER "${BENCHMARK_TICKER:-1306}"

echo "Setting Airflow Connection: financial_data..."
airflow connections delete financial_data 2>/dev/null || true
airflow connections add financial_data \
    --conn-type postgres \
    --conn-login "${POSTGRES_USER}" \
    --conn-password "${POSTGRES_PASSWORD}" \
    --conn-host "${POSTGRES_HOST}" \
    --conn-port "${POSTGRES_PORT}" \
    --conn-schema "${POSTGRES_DB}"

echo "Done. Variables and Connection are set."
