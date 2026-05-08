# Telegram alerts + Claude-driven diagnosis — setup

## Overview

Outbound-only Telegram integration:

1. **Daily report** (19:30 JST weekdays) — position snapshot + new screen entrants
2. **Failure alerts** — when a DAG task fails terminally, send a message; if Claude Code is installed on the host, follow up with a read-only diagnosis (root cause + suggested fix as a unified diff)

No inbound channel. The bot only sends; it never reads incoming messages, never accepts commands. If the bot token leaks, the worst case is impersonation in that chat (information disclosure), not pipeline manipulation.

## One-time setup (~3 minutes)

### 1. Create the bot via BotFather

In Telegram on your phone or desktop:

1. Open chat with **@BotFather**
2. Send `/newbot`
3. Provide a display name (e.g. `Stock Screening Bot`)
4. Provide a username ending in `bot` (e.g. `skat_screening_bot` — must be globally unique)
5. BotFather replies with your token, looks like `123456789:ABCdefGHIjklMNOpqrSTUvwxYZ...`

Save this token — you'll paste it into `.env`. Treat it like a password (don't commit).

### 2. Get your chat_id

Open the bot you just created (search its username), tap **Start**, and send any message (e.g. `hi`). This makes the bot aware of you.

Then on the home server (or from your Mac):

```bash
TOKEN="<paste your bot token>"
curl -s "https://api.telegram.org/bot${TOKEN}/getUpdates" | jq '.result[].message.chat'
```

You'll see something like:

```json
{
  "id": 987654321,
  "first_name": "...",
  "type": "private"
}
```

The `id` is your `TELEGRAM_CHAT_ID`. Copy it (positive integer for direct chats; negative for groups).

### 3. Set the env vars

Append to `~/workspace/stock_screening/.env` on the server (and your local Mac):

```
TELEGRAM_BOT_TOKEN=123456789:ABCdef...
TELEGRAM_CHAT_ID=987654321
TELEGRAM_DIAGNOSIS_ENABLED=false   # toggle to true after Day 2 setup
```

### 4. Test outbound

From inside the airflow stack on the server (so the airflow worker can reach api.telegram.org):

```bash
cd ~/workspace/stock_screening/airflow
docker compose exec -T -e TELEGRAM_BOT_TOKEN -e TELEGRAM_CHAT_ID scheduler python -c "
from stock_screening.telegram_alerts.client import send
send('🧪 hello from the screening bot')
"
```

A message should land in your Telegram chat with the bot within a few seconds.

If you want to test from your Mac:

```bash
cd ~/workspace/stock_screening
set -a && . ./.env && set +a
PYTHONPATH=src python3 -c "
from stock_screening.telegram_alerts.client import send
send('🧪 hello from local')
"
```

### 5. Restart airflow scheduler

So the on_failure_callback wiring picks up:

```bash
cd ~/workspace/stock_screening/airflow
docker compose restart scheduler
```

### 6. Unpause the daily report DAG

```bash
docker compose exec scheduler airflow dags unpause daily_telegram_report_dag
```

That DAG runs at 19:30 JST weekdays after the paper_trading_sim DAG. The first message will land tomorrow evening.

## Day 2 — enable Claude-driven diagnosis

Opt-in. Set `TELEGRAM_DIAGNOSIS_ENABLED=true` in `.env` only after:

1. **Install Claude Code on the home server**
   ```bash
   # See https://docs.claude.com/en/docs/claude-code/quickstart
   curl -fsSL https://claude.ai/install.sh | bash
   ```

2. **OAuth Claude Code to your subscription** — interactive, one-time:
   ```bash
   claude  # opens a URL; complete OAuth in your browser
   /exit
   ```
   Token persists in `~/.claude/`. Headless calls use it.

3. **Verify headless mode:**
   ```bash
   claude --print --output-format text "say hello in one sentence"
   ```

4. **Mount scripts/ into the airflow scheduler container.**
   The script `scripts/diagnose_and_notify.sh` runs OUTSIDE the airflow worker (fire-and-forget subprocess). Add to `airflow/docker-compose.yaml`:

   ```yaml
   volumes:
     - ./logs:/opt/airflow/logs
     - ../scripts:/opt/airflow/scripts:ro    # NEW
   ```

   But the script also needs `claude` available, which is on the *host* not in the worker. Two clean options:
   - **(a)** Install `claude` inside the airflow image (extend `airflow/Dockerfile`).
   - **(b)** Run the script via `docker exec` into a separate `claude-runner` container that has Claude Code installed.

5. **Set `TELEGRAM_DIAGNOSIS_ENABLED=true`** in `.env`, restart scheduler, manually fail a DAG to test.

## Trust model

- Bot can only send messages. It does not poll for incoming messages, does not accept commands.
- If the token leaks: an attacker can impersonate the bot in your chat. They cannot read pipeline data, cannot trigger jobs, cannot modify code.
- Claude diagnosis is read-only by allowlist: `--allowedTools "Read,Bash(git:*,grep:*,find:*,head:*,tail:*,wc:*,ls:*,cat:*)"` and an explicit disallow list for Edit, Write, NotebookEdit, and destructive Bash patterns. Even a misbehaving Claude can't modify code or push.
- Cost guard: `--max-turns 30` per session + 5-diagnoses-per-day file-based rate limit.

## Telegram vs Signal — what we picked, what we gave up

We started this integration with Signal but hit Signal's anti-abuse rate limit during the QR linking flow (24h cooldown). Switched to Telegram for these reasons:

- **No daemon to run.** Telegram Bot API is just HTTPS calls; no signal-cli container, no QR scanning, no linked-device session management.
- **No setup rate limits.** BotFather creates the bot instantly.
- **Simpler trust model.** A bot token in `.env` is easier to rotate than a linked-device session.

Tradeoff: Telegram's regular chats are server-side encrypted (Telegram has the key), where Signal is end-to-end encrypted. For our content — failure alerts and diagnostic info — server-side encryption is acceptable. We never transmit credentials or user data through this channel.

## Revoking access

```bash
# Option 1: revoke the bot token via BotFather
#   /revoke → pick the bot → BotFather replies with a new token
#   The old token immediately stops working.
#
# Option 2: delete the bot via BotFather
#   /deletebot → pick the bot → confirm
#   Bot is gone, all messages remain in your chat history.
#
# Option 3: clear the env vars locally
#   Removes our ability to send. Bot still exists on Telegram's side.
```
