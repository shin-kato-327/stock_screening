# Signal alerts + Claude-driven diagnosis — setup

## Overview

Outbound-only Signal integration for two purposes:

1. **Daily report** (19:30 JST weekdays): position snapshot + new screen entrants
2. **Failure alerts**: when a DAG task fails terminally, send a message; if Claude Code is installed on the host, follow up with a read-only diagnosis (root cause + suggested fix as a unified diff)

No inbound channel. The bot never accepts commands. The signal-cli REST container is bound to `127.0.0.1:8090` (loopback only) so nothing on the LAN can talk to it directly.

## One-time setup on the home server

### 1. Bring up the signal-cli container

```bash
cd ~/workspace/stock_screening/signal
docker compose up -d
docker compose logs -f signal-api &  # tail logs in another terminal if you want
```

### 2. Link the bot to your Signal account

Linking adds the bot as a "secondary device" on your Signal account, similar to Signal Desktop. It can send/receive on your behalf for as long as the link is active. Revokable from your phone (Settings → Linked devices).

```bash
# Generate a QR code link URI and display it as ASCII art:
curl -s "http://localhost:8090/v1/qrcodelink?device_name=stock_screening_bot" -o /tmp/signal_link.png
# Display the QR code on the terminal — pick whichever works on your distro:
which qrencode > /dev/null && qrencode -t ANSIUTF8 -r /tmp/signal_link.png \
    || echo "Open /tmp/signal_link.png and scan it with Signal on your phone."
```

Or do it via signal-cli-rest-api's web UI: open `http://localhost:8090/v1/qrcodelink?device_name=stock_screening_bot` in a browser — it returns a PNG. Scan with Signal app: Settings → Linked devices → Link new device.

After scanning, the API will reply 201 and the bot is linked. Verify:

```bash
curl -s http://localhost:8090/v1/accounts | jq
# Should show your phone number registered.
```

### 3. Set the env vars

Append to `~/workspace/stock_screening/.env`:

```
# Signal sender (your phone number in E.164 format with no spaces)
SIGNAL_SENDER_NUMBER=+819012345678
# Comma-separated recipients (yourself, and/or others)
SIGNAL_RECIPIENT_NUMBERS=+819012345678
# Where the airflow worker can reach the signal-cli REST API.
# Note: airflow runs in a docker container; from inside the container,
# the host's loopback is reachable as host.docker.internal:8090 OR
# you can expose signal-api on the docker network (see below).
SIGNAL_API_URL=http://host.docker.internal:8090

# Day 2: enable Claude-driven failure diagnosis (default off)
SIGNAL_DIAGNOSIS_ENABLED=false
```

### 4. Wire signal-api into the airflow docker network

The airflow stack uses `financial_network`. Either add signal-api to that network too, or rely on `host.docker.internal` from inside airflow containers. The simpler option:

```yaml
# in signal/docker-compose.yaml, add:
networks:
  default:
    name: financial_network
    external: true
```

Then airflow containers can reach `http://signal-api:8080` (no port-mapping needed).

After editing, recreate:

```bash
cd ~/workspace/stock_screening/signal
docker compose down && docker compose up -d
```

And update `SIGNAL_API_URL=http://signal-api:8080` in `.env`.

### 5. Test outbound

```bash
cd ~/workspace/stock_screening
PYTHONPATH=src python3 -c "
from stock_screening.signal_alerts.client import send
send('🧪 hello from the screening bot — outbound link OK')
"
```

You should see the message on your Signal app within a few seconds.

### 6. Restart airflow scheduler

The new on_failure_callback wiring requires the scheduler to re-parse the DAGs:

```bash
cd ~/workspace/stock_screening/airflow
docker compose restart scheduler
```

### 7. Unpause the new daily report DAG

```bash
docker compose exec scheduler airflow dags unpause daily_signal_report_dag
```

## Day 2 — enable Claude-driven diagnosis

This step is opt-in. Set `SIGNAL_DIAGNOSIS_ENABLED=true` in `.env` only after:

1. **Install Claude Code on the home server**
   ```bash
   # See https://docs.claude.com/en/docs/claude-code/quickstart
   curl -fsSL https://claude.ai/install.sh | bash
   ```

2. **OAuth Claude Code to your subscription** — interactive, one-time:
   ```bash
   claude  # opens a URL; complete the OAuth in your browser
   /exit   # exit the interactive session once authenticated
   ```
   The token persists in `~/.claude/`. Subsequent `claude --print` calls use it.

3. **Verify headless mode works:**
   ```bash
   claude --print --output-format text "say hello in one sentence"
   ```

4. **Mount the script + repo into the airflow scheduler container.**
   The script `scripts/diagnose_and_notify.sh` runs OUTSIDE the airflow worker (fire-and-forget subprocess) on the host. It needs:
   - `claude` on PATH
   - Read access to `~/workspace/stock_screening/airflow/logs/`
   - Read access to the repo source

   The airflow `on_failure_callback` is what fires the script. Because subprocess is invoked from inside the airflow worker container, the script path `/opt/airflow/scripts/diagnose_and_notify.sh` must be mounted in. **Add to `airflow/docker-compose.yaml`:**

   ```yaml
   volumes:
     - ./logs:/opt/airflow/logs
     - ../scripts:/opt/airflow/scripts:ro    # NEW
   ```

   But the script also needs `claude` available, which is on the *host* not in the worker. Two options:
   a. Install `claude` inside the airflow image (extend Dockerfile).
   b. Have the script `ssh` back to the host (simpler but adds an SSH dep).
   c. Have the script `docker exec` into a separate `claude-runner` container that has Claude Code installed.

   For v1, **option (a)** is cleanest: extend `airflow/Dockerfile` to install Claude Code, OAuth via the host token mounted as a volume, and run the diagnosis directly from the worker.

5. **Set `SIGNAL_DIAGNOSIS_ENABLED=true`** in `.env`, restart scheduler, manually fail a DAG to test.

## Trust model

- The signal-cli container runs on loopback only. The LAN can't reach it.
- The bot is a secondary device on your account. Revoke from your phone any time (Settings → Linked devices).
- No inbound message handling — the bot never reads incoming messages, never executes commands. If your phone is stolen, the worst case is the attacker sees the daily reports and failure alerts (information disclosure), not pipeline manipulation.
- Claude diagnosis is read-only: the headless invocation uses `--allowedTools "Read,..."` and `--disallowedTools "Edit,Write,..."`. Even a misbehaving Claude can't modify code or push.
- Cost cap: `--max-turns 30` per session, max 5 diagnoses per 24h (file-based rate limit).

## Revoking access

```bash
# Option 1: unlink the bot device from your phone
# Signal app → Settings → Linked devices → tap → Remove

# Option 2: kill the container
docker compose -f signal/docker-compose.yaml down
# (data persists in the volume; can re-link later)

# Option 3: nuke completely
docker compose -f signal/docker-compose.yaml down -v
# (volume + linkage gone; need to re-scan QR to re-link)
```
