# Home server migration runbook

Move the screening stack (Postgres + Airflow) from the Mac dev box to
`192.168.68.52`, scheduled as a long-running data pipeline. After
this, the Mac runs the analysis scripts (`generate_trade_plan.py`
etc.) against the server's DB instead of a local one.

## Prereqs on the home server

```bash
# Docker + Docker Compose plugin (Linux package names — adjust per distro)
sudo apt update && sudo apt install -y docker.io docker-compose-plugin git
sudo usermod -aG docker $USER  # so you don't need sudo for `docker`
# Re-login so the group change takes effect.

# Verify
docker --version
docker compose version
```

Disk: ~5 GB free for the DB volume + airflow images. The dump itself
is 155 MB compressed; restored size is ~2.7 GB.

## Step 1: SSH access from the Mac

On the Mac, add the home server's host key to known_hosts (one-time
TOFU on the LAN):

```bash
ssh-keyscan -t ed25519 192.168.68.52 >> ~/.ssh/known_hosts
```

Then push your public key to enable passwordless ssh:

```bash
ssh-copy-id <USER>@192.168.68.52   # replaces password prompt going forward
ssh <USER>@192.168.68.52 'echo ok'  # should print "ok" without password
```

Replace `<USER>` with your account name on the home server.

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

Then on the server, edit it. Two values to consider changing:

```bash
# In ~/workspace/stock_screening/.env on the server:
POSTGRES_HOST=pgdatabase   # internal docker hostname; leave as-is
# The IBKR token & EDINET key & JQuants key carry over unchanged.
```

If you want the postgres exposed to the LAN (so the Mac can query it
remotely), the docker-compose already maps `5433:5432` — verify the
server's firewall allows port 5433 from your LAN subnet:

```bash
sudo ufw allow from 192.168.68.0/24 to any port 5433  # adjust for your subnet
```

## Step 4: Transfer the database dump

The dump file is at `/tmp/screening_migration/financial_data.dump` on
the Mac (155 MB compressed, MD5 `367eaf4d30e7f4a2e0c25f0aa6454df6`).

```bash
# On the Mac:
scp /tmp/screening_migration/financial_data.dump \
    <USER>@192.168.68.52:/tmp/financial_data.dump
```

Verify checksum on the server:

```bash
# On the server:
md5sum /tmp/financial_data.dump
# Expect: 367eaf4d30e7f4a2e0c25f0aa6454df6
```

## Step 5: Start postgres and restore

```bash
# On the server:
cd ~/workspace/stock_screening
docker compose up -d pgdatabase   # starts only the data postgres + creates volume

# Wait for it to be healthy (5-10s)
until docker compose exec pgdatabase pg_isready -U "$(grep POSTGRES_USER .env | cut -d= -f2)"; do
    sleep 2
done

# Copy the dump into the container and restore
docker cp /tmp/financial_data.dump stock_screening-pgdatabase-1:/tmp/financial_data.dump
docker compose exec pgdatabase pg_restore \
    -U root -d financial_data --no-owner --no-acl --verbose \
    /tmp/financial_data.dump 2>&1 | tail -20

# Verify row counts
docker compose exec pgdatabase psql -U root -d financial_data -c \
    "SELECT 'prices' table_name, COUNT(*) n FROM t_daily_stock_perf
     UNION ALL SELECT 'docs', COUNT(*) FROM t_doc_list
     UNION ALL SELECT 'mart', COUNT(*) FROM t_financials_annual;"
# Expect: prices ~4.1M, docs ~357k, mart ~19k (as of 2026-05-01 dump)
```

## Step 6: Run any pending alembic migrations

```bash
# On the server, in the repo root:
docker compose run --rm -e PYTHONPATH=src \
    pgdatabase alembic upgrade head
# (We're using pgdatabase as a generic python-capable container; if
# this fails because pgdatabase doesn't have python, run alembic
# from the airflow image instead — see step 7.)
```

If the repo state is already at HEAD on the Mac, `alembic upgrade
head` is a no-op. The dump preserves all schema and the stamped
revision, so this is just a safety check.

## Step 7: Start the Airflow stack

```bash
# On the server:
cd ~/workspace/stock_screening/airflow

# First-time only: initialize airflow metadata DB
docker compose up -d postgres
docker compose run --rm airflow-init

# Bring up the rest
docker compose up -d
```

The webserver will be at `http://192.168.68.52:8083` (login admin /
admin per the existing setup; change in production).

## Step 8: Unpause the DAGs

By default DAGs are paused at creation. Unpause from the UI or CLI:

```bash
docker compose exec airflow-webserver airflow dags unpause edinet_doclist_dag
docker compose exec airflow-webserver airflow dags unpause edinet_xbrl_ingest_dag
docker compose exec airflow-webserver airflow dags unpause jquants_daily_prices_dag
docker compose exec airflow-webserver airflow dags unpause value_screen_dag
docker compose exec airflow-webserver airflow dags unpause paper_trading_sim_dag
```

The DAGs are configured with `catchup=True` and `start_date` of
2025-04-01. With the data already restored through 2026-05-01, you'll
want to `mark success` for the historical window so the DAGs only run
forward from today, not re-process every past day:

```bash
# Mark all historical runs as success up through 2026-05-01
for dag in edinet_doclist_dag edinet_xbrl_ingest_dag jquants_daily_prices_dag value_screen_dag paper_trading_sim_dag; do
    docker compose exec airflow-webserver airflow dags backfill \
        -s 2025-04-01 -e 2026-05-01 --mark-success "$dag"
done
```

DAG schedules already configured (JST):
- 17:00 weekdays — `jquants_daily_prices_dag`
- 18:00 weekdays — `value_screen_dag` (depends on prices)
- 19:00 weekdays — `paper_trading_sim_dag`
- 22:00 daily — `edinet_doclist_dag`
- 22:30 daily — `edinet_xbrl_ingest_dag`

## Step 9: Verify next-day pipeline

The next trading day, watch the airflow UI around 17:00 JST to see
the prices DAG fire, and 22:00 for the EDINET DAGs. Or check via DB:

```bash
# On the server (or remotely from Mac):
docker compose exec pgdatabase psql -U root -d financial_data -c \
    "SELECT MAX(\"Date\") FROM t_daily_stock_perf;"
# Should advance one day per weekday.
```

## Step 10 (optional): Point Mac analysis scripts at the server DB

To run `generate_trade_plan.py` and friends from the Mac against the
server's data:

```bash
# On the Mac, edit .env:
POSTGRES_HOST=192.168.68.52
POSTGRES_PORT=5433
# Also stop the local postgres so there's no confusion:
cd ~/workspace/stock_screening && docker compose down
```

The Mac's analysis scripts (no Airflow needed locally) will then
query the server. If you want to keep a local DB for development,
leave POSTGRES_HOST=pgdatabase locally and only point at the server
when you want fresh data.

## Rollback

If anything goes wrong on the server, the Mac's local DB is
untouched. Just keep using the Mac as before.

To wipe the server's data and try again:

```bash
# On the server:
docker compose down -v   # removes the postgres volume too
rm -rf ~/workspace/stock_screening/financial_data_pg16
rm -rf ~/workspace/stock_screening/airflow/logs
# Then start over from Step 5.
```

## What I can do for you on the next turn

Once you've finished Step 1 (SSH access), I can drive Steps 2-9
remotely from this Claude session — clone the repo, scp the dump,
restore, start the stack, unpause the DAGs, and verify. Let me know
when SSH is set up and I'll proceed.
