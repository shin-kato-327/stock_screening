# stock_screening

Daily Japan-equity value screener and paper-trading simulator.

The pipeline pulls financial reports from EDINET, parses XBRL, ingests
into Postgres, pulls daily prices from JQuants, ranks stocks by the
**net-cash ratio** `(流動資産 − 負債合計 + 0.7 × 投資有価証券) ÷ 時価総額`
(a market-cap-based, conservative form — uses total liabilities, not
just interest-bearing debt; see `screening/metrics.py` for the
empirical justification), and runs an equal-weight top-N paper portfolio
that rebalances daily with a strict-improvement swap rule against a
TOPIX (`1306.T`) buy-and-hold benchmark.

## Pipeline

```
EDINET API ──► t_doc_list ──► XBRL parse ──► t_financials (raw EAV)
                                                   │
                                          build_annual_mart
                                                   ▼
JQuants v2 API ──► t_daily_stock_perf ◄──► t_financials_annual
                          │                       │
                          └──── value screen ─────┘
                                     │
                                     ▼
                            t_screen_results
                                     │
                                     ▼
                          paper trading sim ──► t_sim_positions
                                                t_sim_trades
                                                t_sim_portfolio_nav
```

Five Airflow DAGs run the daily cadence; the same logic is also
exposed as `scripts/backfill_window.py` for historical replays.

## Repo layout

```
src/stock_screening/        Python package (importable from DAGs and scripts)
  config.py                 Variable / env accessors
  db.py                     SQLAlchemy engine factory
  edinet/{client,xbrl_parser}.py
  jquants/client.py         v2 x-api-key client
  financials/{concepts,build_annual_mart}.py
  screening/metrics.py      net-cash ratio screen
  simulation/{rebalance,portfolio}.py

airflow/
  Dockerfile                custom image: Airflow 2.11 + uv-installed deps
  docker-compose.yaml       webserver + scheduler + Airflow metadata pg16
  dags/                     5 DAGs (edinet_doclist, edinet_xbrl_ingest,
                            jquants_daily_prices, value_screen, paper_trading_sim)

alembic/versions/           Schema migrations 0001–0006

scripts/
  bootstrap_airflow.sh      load .env into Airflow Variables / Connection
  bootstrap_history.sh      multi-month backfill via airflow dags backfill
  backfill_window.py        single-process backfill driver

tests/                      pytest unit tests

notebooks/
  archived/                 prototype notebooks now superseded by src/
  active/                   reference notebooks for unique logic ports
```

## Running locally

### Prerequisites

- Docker Desktop
- Python 3.12 (for local lint/test/scripts)
- [uv](https://docs.astral.sh/uv/) (`brew install uv`)
- Active EDINET API key + JQuants v2 paid-tier API key

### Setup

```bash
cp .env.example .env
chmod 600 .env
# Fill in EDINET_KEY, JQUANTS_API_KEY, POSTGRES_PASSWORD, AIRFLOW_FERNET_KEY
# (generate Fernet: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")

uv sync --extra dev

docker compose up -d                                                   # pg16 (financial_data)
docker compose -f airflow/docker-compose.yaml --env-file .env up -d    # Airflow 2.11 stack

uv run alembic upgrade head                                            # apply schema migrations

docker compose -f airflow/docker-compose.yaml --env-file .env exec scheduler \
    bash -c 'airflow variables set EDINET_KEY "$EDINET_KEY" && \
             airflow variables set JQUANTS_API_KEY "$JQUANTS_API_KEY"'
```

Airflow UI: http://localhost:8083 (admin / admin).
pgAdmin: http://localhost:8080 (admin@admin.com / admin).

### Backfill historical data

`scripts/backfill_window.py` runs the full pipeline (doclist → XBRL →
prices → screen → sim) over a date range in dependency order. Ideal
for first-time population:

```bash
PYTHONUNBUFFERED=1 uv run python scripts/backfill_window.py 2025-06-01 2026-04-25
```

The window has to cross **June–July** to pick up the bulk of TSE
annual reports (FY ends March 31).

### Daily operation

Once the DAGs are unpaused in the Airflow UI they run on a JST cron:

| DAG | Schedule | Purpose |
|---|---|---|
| `edinet_doclist_dag` | 22:00 JST | Pull the day's filing list |
| `edinet_xbrl_ingest_dag` | 22:30 JST | Download + parse XBRL → mart |
| `jquants_daily_prices_dag` | 17:00 JST (weekdays) | Daily quotes + computed marketCap |
| `value_screen_dag` | 18:00 JST (weekdays) | Net-cash-ratio screen |
| `paper_trading_sim_dag` | 19:00 JST (weekdays) | Rebalance paper portfolio |

## Sim semantics

- **Universe**: all stocks in `t_screen_results` for the run date with `qualifies = TRUE`.
- **Target**: top `MAX_POSITIONS` (default 20) by ratio descending.
- **Swap rule**: a held name `H` is replaced by candidate `C` only when `C.ratio > H.ratio` strictly. Equality does not swap — anti-thrashing.
- **Sizing**: equal weight on the day's NAV across `MAX_POSITIONS` slots; shares rounded down to lot size 100 (TSE board lot); residual sits in cash.
- **Costs**: `TRANSACTION_COST_BPS` (default 10) on traded notional, each side.
- **Mark-to-market**: previous day's positions × today's close from `t_daily_stock_perf`.
- **Benchmark**: parallel buy-and-hold of `BENCHMARK_TICKER` (default `1306` — TOPIX ETF) using the same initial capital from sim start.
- **Bootstrap**: on first run with no prior snapshot, allocates `INITIAL_CAPITAL` (default 10,000,000 JPY) to that day's qualifying top-N.
- **Path-dependent**: the sim DAG runs strictly serial (`max_active_runs=1`, `depends_on_past=True`). The first task deletes its own outputs for the run date so re-runs are idempotent.

## Schema highlights

- `t_doc_list` — EDINET filing metadata. PK on `docID`. `is_latest_for_period` flag set at ingest to handle amended/superseded filings.
- `t_financials` — raw EAV of XBRL facts. PK `(docID, itemName, periodEnd, categoryID)`. Stores `concept_id` (e.g. `jpcrp_cor:CurrentAssets`) and `currency_code` (`JPY` or `SHR`).
- `t_financials_annual` — typed mart with one row per `(secCode, period_end)`. `interest_bearing_debt` is a `GENERATED ALWAYS AS … STORED` sum of `short_term_borrowings + long_term_borrowings + bonds + lease_obligations`, so the total is always consistent with its components and auditable per company.
- `t_daily_stock_perf` — daily quotes + `marketCap` (computed from close × `t_financials_annual.issued_shares`). PK `(Date, ShokenCode)`.
- `t_screen_results` — daily screen output. FK to `t_financials_annual` so inputs are reconstructable without duplicating them here.
- `t_sim_positions` / `t_sim_trades` / `t_sim_portfolio_nav` — paper portfolio state. Trades use a `bigserial trade_id` PK with a unique constraint on the natural key, so partial fills aren't blocked.

## Tests

```bash
uv run pytest          # 24 unit tests covering rebalance rules, concepts, formula constants
uv run ruff check src/ airflow/dags/ tests/
```

Migrations are roundtrip-tested with `uv run alembic upgrade head --sql`.

## Secrets

Stored in `.env` locally (gitignored, `chmod 600`). Loaded into Airflow
Variables and an Airflow Connection (`financial_data`, Postgres) via
`scripts/bootstrap_airflow.sh`. Inside Airflow tasks, `config.py` reads
Variables; outside Airflow, it falls back to env vars from `.env`.

Required variables (see `.env.example`):

- `EDINET_KEY` — EDINET API subscription key
- `JQUANTS_API_KEY` — JQuants v2 x-api-key
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- `AIRFLOW_FERNET_KEY` — encrypts Variables / Connections at rest

Optional tunables (also exposed as Airflow Variables):

- `INITIAL_CAPITAL` (default 10,000,000)
- `MAX_POSITIONS` (default 20)
- `TRANSACTION_COST_BPS` (default 10)
- `BENCHMARK_TICKER` (default `1306`)
