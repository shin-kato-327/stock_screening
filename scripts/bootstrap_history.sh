#!/usr/bin/env bash
# Backfill 12 months of EDINET filings, JQuants prices, screen results,
# and (optionally) sim NAV. Same DAGs as the daily schedule — Airflow
# `dags backfill` uses data_interval_start to drive each run.
#
# Usage:
#     bash scripts/bootstrap_history.sh             # default: 1y back to yesterday
#     bash scripts/bootstrap_history.sh 2025-04-25  # custom start
#     bash scripts/bootstrap_history.sh 2025-04-25 2026-04-24
#
# Run this *inside* the airflow scheduler container.

set -euo pipefail

if [ $# -ge 1 ]; then
    START="$1"
else
    START=$(date -v-1y +%Y-%m-%d 2>/dev/null || date -d "1 year ago" +%Y-%m-%d)
fi
if [ $# -ge 2 ]; then
    END="$2"
else
    END=$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)
fi

echo "Backfilling ${START} → ${END}"

# Order matters: doclist precedes xbrl_ingest (xbrl_ingest reads t_doc_list);
# prices precede screen (screen reads t_daily_stock_perf); screen precedes sim.
DAGS=(
    edinet_doclist_dag
    edinet_xbrl_ingest_dag
    jquants_daily_prices_dag
    value_screen_dag
    paper_trading_sim_dag   # comment this line to skip the backtest replay
)

for dag in "${DAGS[@]}"; do
    echo "==> ${dag}"
    airflow dags backfill -s "${START}" -e "${END}" -y "${dag}"
done

echo "Backfill complete. NAV equity curve is in t_sim_portfolio_nav."
