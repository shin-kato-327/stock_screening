# `screening-bot.service` — Telegram Q&A Bot Setup

Read-only natural-language Q&A over the screening postgres DB, accessible from your phone via the existing alerts Telegram bot. See `/Users/skat/.claude/plans/harmonic-scribbling-spring.md` for the full design + threat model.

## Prerequisites on the home server

- The screening repo cloned at `/home/shinkato/workspace/stock_screening`.
- Python venv at `.venv/` with project deps (`uv sync` or equivalent).
- `claude` CLI on `shinkato`'s PATH and OAuth-authenticated (already true if Day 2 diagnosis is set up).
- `psql` client installed (`sudo apt install postgresql-client`).
- The data postgres running (`docker compose up -d pgdatabase` from the repo root).

## One-time install

### 1. Generate a strong password for the read-only DB role

```bash
python -c "import secrets; print(secrets.token_urlsafe(24))"
# example: x9VQ2cAyhJN_qPg-bP8JxXAKlz7pTzkN
```

### 2. Apply the alembic migration

```bash
cd /home/shinkato/workspace/stock_screening
export POSTGRES_BOT_READONLY_PASSWORD='<the password from step 1>'
# Run as superuser (default `root` per docker-compose):
export POSTGRES_USER=root POSTGRES_PASSWORD=<root pw from .env>
.venv/bin/alembic upgrade head
```

The migration creates the `bot_readonly` role with SELECT-only grants on six tables (`t_doc_list`, `t_edinet_code_mappings`, `t_financials`, `t_financials_annual`, `t_daily_stock_perf`, `t_screen_results`) and three role-level GUCs (`default_transaction_read_only=on`, `statement_timeout=15s`, `idle_in_transaction_session_timeout=30s`).

Smoke-test the role:
```bash
PGPASSWORD='<password>' psql -h localhost -p 5433 -U bot_readonly -d financial_data \
    -c "SELECT count(*) FROM t_screen_results"   # should succeed
PGPASSWORD='<password>' psql -h localhost -p 5433 -U bot_readonly -d financial_data \
    -c "DELETE FROM t_screen_results"            # should fail with read-only-transaction error
```

### 3. Install the qa-psql shim

```bash
sudo install -m 755 systemd/qa-psql /usr/local/bin/qa-psql
```

The shim hard-codes host/port/user/db and rejects override flags. Claude's tool whitelist permits `Bash(qa-psql:*)` but explicitly disallows `Bash(psql:*)`.

### 4. Create the `qa_sessions` directory

```bash
mkdir -p ~/qa_sessions
```

systemd's `ProtectHome=read-only` makes `$HOME` read-only for the bot process EXCEPT for the explicit `ReadWritePaths` carve-outs (`~/qa_sessions` and `~/.claude`). Creating the dir in advance avoids first-run errors.

### 5. Create `.env.bot`

Copy `.env.bot.example` to `.env.bot` at the repo root and fill in the values. Required keys:

- `TELEGRAM_BOT_TOKEN` — the existing alerts bot token (we reuse it for inbound).
- `TELEGRAM_CHAT_ID` — your numeric chat id (already set in `.env`).
- `BOT_POSTGRES_HOST=localhost`, `BOT_POSTGRES_PORT=5433`, `BOT_POSTGRES_DB=financial_data`, `BOT_POSTGRES_USER=bot_readonly`, `BOT_POSTGRES_PASSWORD=<from step 1>`.
- `MAX_PER_DAY=10` (override if you want).
- `PATH=/usr/local/bin:/usr/bin:/bin:/home/shinkato/.local/bin` so systemd's reduced PATH still finds `claude` and `qa-psql`.

**Do not set bare `POSTGRES_USER` / `POSTGRES_PASSWORD` here.** If any code path in the bot's import chain ever calls `db.get_engine()`, it must fail loudly rather than silently connect as `bot_readonly`. The `BOT_` prefix prevents that collision.

### 6. Install the systemd unit

```bash
sudo cp systemd/screening-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now screening-bot.service
sudo systemctl status screening-bot.service
journalctl -u screening-bot.service -f
```

## Verifying

Send a few messages from your phone:

```
hello                                 → silently ignored (no /q prefix)
/q how many qualifying stocks today?  → reply within 60s with Answer/SQL/Rows
/q DROP TABLE t_screen_results        → REFUSED (system prompt) or
                                         "cannot execute DELETE in a read-only transaction"
                                         (postgres role)
```

Check session artifacts:
```bash
ls -la ~/qa_sessions/                            # one dir per question
cat ~/qa_sessions/<latest>/answer.txt            # what got sent to Telegram
cat ~/qa_sessions/<latest>/claude.log            # claude stderr (if any)
cat ~/qa_sessions/rate_limit.txt                 # one line per accepted question
```

## Operational notes

### `getUpdates` token contention
Telegram allows **only one** `getUpdates` consumer per bot token. While `screening-bot.service` is running, a manual `curl https://api.telegram.org/bot$TOKEN/getUpdates` (e.g. for debugging chat_id flow) will race the listener — one side gets the update, the other doesn't.

**Always stop the service before any raw `getUpdates` debugging:**
```bash
sudo systemctl stop screening-bot.service
# do your debugging
sudo systemctl start screening-bot.service
```

### Backlog discard on stale restart
On startup the listener checks `~/qa_sessions/.offset`:
- Missing → discard backlog (cold start; advance to current tip).
- Older than 1 hour → also discard backlog (long outage; don't replay 24h of queued questions).
- Less than 1 hour → resume from saved offset (clean restart).

This avoids the "phone-in-pocket overnight" replay hazard.

### Daily cap reset
The cap (`MAX_PER_DAY=10` by default) counts entries in `~/qa_sessions/rate_limit.txt` whose date prefix matches today's UTC date. Hitting the cap returns "Daily cap reached … Resets at UTC midnight." No automatic file pruning; you can `rm` it manually if you want to reset earlier.

### Updating Claude Code
When `claude` is updated (its bundle path changes), the systemd unit's `PATH` entry should still work because we list `/home/shinkato/.local/bin` (where the install symlink lives). If not, find the new path with `bash -lc 'which claude'` and adjust `PATH` in `.env.bot`.

### Stopping the bot
```bash
sudo systemctl stop screening-bot.service        # stop now, will start on reboot
sudo systemctl disable screening-bot.service     # don't start on reboot
sudo systemctl disable --now screening-bot.service   # both at once
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Service hot-loops on `Restart=on-failure` | `claude` not on PATH | Check `journalctl -u screening-bot -n 20`; ensure `PATH` in `.env.bot` includes `~/.local/bin` |
| Bot silent on `/q ...` | Wrong chat_id, or `getUpdates` racing another consumer | `journalctl -u screening-bot -f` shows the rejection reason |
| Reply: "Sorry, that query failed. Session: …" | claude crashed or 90s timeout | `cat ~/qa_sessions/<session>/claude.log` for stderr |
| Reply: "Daily cap reached" but you only sent 2 | Past sessions for today already counted | `cat ~/qa_sessions/rate_limit.txt`; clear if unintended |
| psql says "permission denied for table X" | Migration grant list omitted that table | Add to `GRANTED_TABLES` in `alembic/versions/0012_…`, write a follow-up migration |
| Migration fails at upgrade with "must be superuser" | Running `alembic upgrade` as a non-superuser DB role | Set `POSTGRES_USER=root POSTGRES_PASSWORD=<root pw>` and re-run |
