# Home server migration runbook

Move the screening stack (Postgres + Airflow) to a Linux home server,
scheduled as a long-running data pipeline. After this, the Mac runs
the analysis scripts (`generate_trade_plan.py` etc.) against the
server's DB instead of a local one.

This runbook reflects the actual 2026-05-02 deployment to
`192.168.68.52`. Every gotcha encountered then is documented here.

## Prereqs on the home server

```bash
# Docker + Docker Compose plugin (Linux package names — adjust per distro)
sudo apt update && sudo apt install -y docker.io docker-compose-plugin git
sudo usermod -aG docker $USER
# **Re-login (or close the SSH session)** so the group change takes
# effect — `groups` should show `docker`. Without this, every docker
# command needs sudo and the runbook below won't work.

# Verify
docker --version
docker compose version
```

Disk: ~5 GB free (DB volume + airflow images). Dump itself is ~155 MB
compressed; restored size is ~2.7 GB.

## Step 1: SSH access from the Mac

On the Mac, add the home server's host key to known_hosts (one-time
TOFU on the LAN):

```bash
ssh-keyscan -t ed25519 192.168.68.52 >> ~/.ssh/known_hosts
ssh-copy-id <USER>@192.168.68.52   # pushes your pubkey
ssh <USER>@192.168.68.52 'echo ok'  # should print "ok" with no password
```

Replace `<USER>` with your account on the home server.

## Step 2: Clone the repo on the server

```bash
ssh <USER>@192.168.68.52
mkdir -p ~/workspace && cd ~/workspace
git clone https://github.com/shin-kato-327/stock_screening.git
cd stock_screening
```

## Step 3: Configure .env on the server

Copy the .env from the Mac:

```bash
# On the Mac:
scp /Users/skat/workspace/stock_screening/.env <USER>@192.168.68.52:~/workspace/stock_screening/.env
```

Two values to verify in the server's `.env`:

```
POSTGRES_HOST=pgdatabase   # internal docker hostname; leave as-is
POSTGRES_USER=root
POSTGRES_PASSWORD=<...>
```

The `.env` typically does **not** set `POSTGRES_DB`, `POSTGRES_PORT`,
`POSTGRES_HOST` explicitly — they fall back to docker-compose defaults
(`pgdatabase`, `5432`, `financial_data`). Step 8 needs these defaults
spelled out for `airflow connections add`, so make a mental note.

To expose postgres to the LAN (so the Mac can query the server DB
remotely), the docker-compose maps `5433:5432` already — just allow
the port through the firewall:

```bash
sudo ufw allow from 192.168.68.0/24 to any port 5433
```

## Step 4: Generate + transfer the database dump

On the Mac (where the canonical DB lives):

```bash
mkdir -p /tmp/screening_migration
docker exec stock_screening-pgdatabase-1 pg_dump \
    -U root -d financial_data -F c \
    -f /tmp/financial_data.dump
docker cp stock_screening-pgdatabase-1:/tmp/financial_data.dump \
    /tmp/screening_migration/financial_data.dump

# Transfer
scp /tmp/screening_migration/financial_data.dump \
    <USER>@192.168.68.52:/tmp/financial_data.dump

# Checksum verification
md5 /tmp/screening_migration/financial_data.dump
ssh <USER>@192.168.68.52 md5sum /tmp/financial_data.dump
# Both should match.
```

## Step 5: Pre-empt known network/permission gotchas

Before starting docker-compose, two things often pre-exist on a
shared server and break the first run.

**Stale `financial_network` network.** If the home server has been
used for other compose stacks, an unlabeled `financial_network` may
exist and conflict with our compose. Check and remove if empty:

```bash
docker network inspect financial_network 2>/dev/null \
    | jq '.[0].Containers'      # should be {} if safe to remove
docker network rm financial_network 2>/dev/null || true
```

**Airflow `logs/` directory ownership.** The compose mounts `./logs`
into the airflow container which runs as UID 50000. If the directory
is created by docker (or root) before airflow runs, airflow can't
write to it and init fails with `Unable to configure handler 'processor'`.
Fix proactively:

```bash
mkdir -p ~/workspace/stock_screening/airflow/logs
docker run --rm -v ~/workspace/stock_screening/airflow/logs:/logs \
    --user 0 alpine chown -R 50000:0 /logs
```

(The `docker run` workaround avoids needing sudo on the host.)

## Step 6: Start the data postgres and restore

```bash
cd ~/workspace/stock_screening
docker compose up -d pgdatabase

# Wait for healthy
until docker exec stock_screening-pgdatabase-1 pg_isready -U root -d financial_data > /dev/null 2>&1; do
    sleep 2
done

# Restore
docker cp /tmp/financial_data.dump stock_screening-pgdatabase-1:/tmp/financial_data.dump
docker exec stock_screening-pgdatabase-1 pg_restore \
    -U root -d financial_data --no-owner --no-acl /tmp/financial_data.dump

# Verify row counts
docker exec stock_screening-pgdatabase-1 psql -U root -d financial_data -c \
    "SELECT 'prices' tbl, COUNT(*) n, MAX(\"Date\") latest FROM t_daily_stock_perf
     UNION ALL SELECT 'docs',  COUNT(*), MAX(\"submitDateTime\")::date FROM t_doc_list
     UNION ALL SELECT 'mart',  COUNT(*), MAX(period_end) FROM t_financials_annual;"
# Expect: prices ~4.1M, docs ~357k, mart ~19k (as of the 2026-05-01 dump)
```

If the dump was created from an older repo state and the server's
repo has newer alembic migrations, run them:

```bash
docker compose exec pgdatabase psql -U root -d financial_data -c "SELECT version_num FROM alembic_version;"
# If repo HEAD has further migrations, run from the airflow image
# (which has python + alembic):
cd airflow
docker compose run --rm -e POSTGRES_HOST=pgdatabase -e POSTGRES_PORT=5432 \
    -e POSTGRES_DB=financial_data -e POSTGRES_USER=root \
    -e POSTGRES_PASSWORD="$(grep ^POSTGRES_PASSWORD ../.env | cut -d= -f2)" \
    --entrypoint bash scheduler -c "cd /opt/airflow && alembic upgrade head"
cd ..
```

## Step 7: Symlink .env into the airflow directory

Docker Compose looks for `.env` in the same directory as the compose
file. The airflow stack lives in `airflow/` but our env file is at
the repo root. Without this symlink, `docker compose up` for airflow
errors out with `AIRFLOW_FERNET_KEY must be set`.

```bash
cd ~/workspace/stock_screening/airflow
ln -sf ../.env .env
```

## Step 8: Initialize and start the Airflow stack

```bash
# Still in airflow/
docker compose up -d postgres
# Wait a few seconds for the metadata postgres
docker compose run --rm airflow-init   # creates admin/admin user
docker compose up -d                    # webserver + scheduler
```

Wait for healthy:

```bash
until curl -sf http://localhost:8083/health > /dev/null; do sleep 5; done
curl -s http://localhost:8083/health
```

The webserver is now at `http://192.168.68.52:8083`. Login `admin` /
`admin` (change in production).

## Step 9: Bootstrap Airflow Variables and the financial_data Connection

The DAGs read API keys via `airflow.models.Variable` and connect to
the data DB via `PostgresHook(postgres_conn_id="financial_data")`.
Both must be registered in airflow's metadata DB before any DAG can
run a task.

`scripts/bootstrap_airflow.sh` exists for this, but the airflow
docker-compose mounts only `dags`, `logs`, `src`, `alembic`,
`alembic.ini` — **not `scripts/`**. So either mount `scripts/` or run
the commands inline as below.

Critically: `airflow connections add` needs explicit `--conn-host`,
`--conn-port`, `--conn-schema` because the .env doesn't set
POSTGRES_HOST / POSTGRES_PORT / POSTGRES_DB (they rely on
docker-compose defaults). If you pass `$POSTGRES_HOST` from the host
shell, it'll be empty and airflow will reject the empty values.

```bash
cd ~/workspace/stock_screening
set -a && . ./.env && set +a
cd airflow

docker compose exec -T -e EDINET_KEY="$EDINET_KEY" \
    -e JQUANTS_API_KEY="$JQUANTS_API_KEY" scheduler bash -c '
  airflow variables set EDINET_KEY        "$EDINET_KEY"
  airflow variables set JQUANTS_API_KEY   "$JQUANTS_API_KEY"
  airflow variables set INITIAL_CAPITAL   10000000
  airflow variables set MAX_POSITIONS     20
  airflow variables set TRANSACTION_COST_BPS 10
  airflow variables set BENCHMARK_TICKER  1306
'

docker compose exec -T scheduler bash -c "
  airflow connections delete financial_data 2>/dev/null || true
  airflow connections add financial_data \
    --conn-type postgres \
    --conn-login   '$POSTGRES_USER' \
    --conn-password '$POSTGRES_PASSWORD' \
    --conn-host    pgdatabase \
    --conn-port    5432 \
    --conn-schema  financial_data
"
```

**Avoid `airflow connections get financial_data`** — it prints the
password in cleartext to stdout. Use the UI's masked view instead.

## Step 10: Confirm the DAG `start_date` matches deployment day

The DAGs ship with `start_date=datetime(2026, 5, 1, tzinfo=JST)` which
was the original cutover. If you're deploying on a *different* day,
update the start_date in all 5 DAG files to the deployment date so
airflow creates only one historical run (the deployment day) before
firing forward.

If you leave it at an old date and the airflow metadata DB is empty,
the scheduler will try to **catchup** from `start_date` to today,
creating a DagRun for every missed schedule period. That can be
hundreds of runs of needlessly re-fetching APIs. Even though the data
is idempotent, it wastes hours. Don't skip this step.

```bash
# In the repo, on the server (or do it on Mac and git push, then pull):
sed -i 's/start_date=datetime([0-9]\+, [0-9]\+, [0-9]\+, tzinfo=JST)/start_date=datetime(YEAR, MONTH, DAY, tzinfo=JST)/' airflow/dags/*.py
# Then restart the scheduler so it re-parses
cd airflow && docker compose restart scheduler
```

## Step 11: Unpause the DAGs

```bash
cd ~/workspace/stock_screening/airflow
for dag in edinet_doclist_dag edinet_xbrl_ingest_dag \
           jquants_daily_prices_dag value_screen_dag paper_trading_sim_dag; do
    docker compose exec -T scheduler airflow dags unpause "$dag"
done

# Verify all 5 show is_paused: False
docker compose exec -T scheduler airflow dags list
```

DAG schedules (JST):
- 17:00 weekdays — `jquants_daily_prices_dag`
- 18:00 weekdays — `value_screen_dag` (depends on prices)
- 19:00 weekdays — `paper_trading_sim_dag`
- 22:00 daily — `edinet_doclist_dag`
- 22:30 daily — `edinet_xbrl_ingest_dag`

## Step 12: Smoke test

Trigger the heaviest DAG manually to confirm the connection +
variables + image all work end-to-end:

```bash
docker compose exec -T scheduler airflow dags trigger edinet_xbrl_ingest_dag
# Wait ~10 minutes (it has retries=2 with retry_delay=10min in case
# of transient errors), then check
docker exec airflow-postgres-1 psql -U airflow -d airflow -c \
    "SELECT execution_date, state FROM dag_run
     WHERE dag_id='edinet_xbrl_ingest_dag' AND run_type='manual'
     ORDER BY execution_date DESC LIMIT 1;"
```

State should land on `success`. If `failed`, check the task log:

```bash
ls airflow/logs/dag_id=edinet_xbrl_ingest_dag/run_id=manual__*/task_id=*/
tail -30 airflow/logs/dag_id=edinet_xbrl_ingest_dag/run_id=manual__*/task_id=select_pending_docs/attempt=*.log | tail -30
```

Common failure: `AirflowNotFoundException: The conn_id 'financial_data'
isn't defined` → step 9 didn't run or the connection was added with
empty host/port/schema.

## Step 13 (optional): Point Mac analysis scripts at the server DB

To run `generate_trade_plan.py` and friends from the Mac against the
server's data:

```bash
# On the Mac, edit .env:
POSTGRES_HOST=192.168.68.52
POSTGRES_PORT=5433
# Also stop the local postgres so there's no confusion:
cd ~/workspace/stock_screening && docker compose down
```

If you want to keep both: leave the Mac .env alone, and use a
separate `.env.server` for analysis scripts that should hit the
server. The scripts read POSTGRES_HOST from the environment.

## Rollback

The Mac's local DB is untouched. Worst case, keep using the Mac.

To wipe the server and start over:

```bash
# On the server:
cd ~/workspace/stock_screening
docker compose down -v
cd airflow && docker compose down -v && cd ..
rm -rf ~/workspace/stock_screening/financial_data_pg16
rm -rf ~/workspace/stock_screening/airflow/logs
# Then start over from Step 5.
```

## Things that bit us in the 2026-05-02 deployment

For posterity, in the order they hit:

1. **`shinkato` not in `docker` group** → docker commands needed sudo
   which prompts for a password. Fixed by adding to `docker` group
   and re-logging in (Step 0).
2. **Stale `financial_network` from a 2024 stack** → docker compose
   refused to attach. Fixed by `docker network rm` (Step 5).
3. **Logs dir created by root** → airflow init failed with cryptic
   "Unable to configure handler 'processor'". Fixed by chown'ing to
   50000:0 via a throwaway docker container (Step 5).
4. **`AIRFLOW_FERNET_KEY` missing** → docker compose in `airflow/`
   couldn't find `.env`. Fixed by symlinking `airflow/.env -> ../.env`
   (Step 7).
5. **`scripts/` not mounted in airflow image** → bootstrap script
   couldn't be run. Fixed by inlining the commands (Step 9).
6. **`airflow connections add` with empty host/port/schema** → because
   POSTGRES_HOST etc. aren't in the .env (compose-default values).
   Fixed by passing `pgdatabase`, `5432`, `financial_data` literally
   (Step 9).
7. **`start_date=2025-04-01` + `catchup=True` + empty airflow metadata
   DB** → scheduler tried to backfill 13 months. `airflow dags
   backfill --mark-success` works for static-task DAGs but **not**
   for DAGs with dynamic task mapping (e.g. `edinet_xbrl_ingest_dag`).
   The clean fix is bumping `start_date` to deployment day before
   unpause (Step 10).
8. **`airflow connections get` printed the password in cleartext** →
   to logs and chat history. Don't run it.
