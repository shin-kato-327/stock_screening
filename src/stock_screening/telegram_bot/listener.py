"""Long-poll Telegram listener that forwards `/q ...` questions to a
restricted Claude Code subprocess and replies via the existing
telegram_alerts client.

Sequential by design: one question at a time. systemd handles
restarts; the loop only needs to NOT crash. Every uncaught exception
inside the per-update handler logs + replies "internal error" and
advances the offset so we don't replay the bad update.

See /Users/skat/.claude/plans/harmonic-scribbling-spring.md for the
full architecture and threat model.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from stock_screening.telegram_alerts.client import send as telegram_send
from stock_screening.telegram_bot.filters import (
    Accept,
    Reject,
    classify_update,
)
from stock_screening.telegram_bot.rate_limit import at_cap, record

logger = logging.getLogger("telegram_bot")

# Paths — overridable for testing.
QA_SESSIONS_DIR = Path(os.environ.get("QA_SESSIONS_DIR", str(Path.home() / "qa_sessions")))
OFFSET_FILE = QA_SESSIONS_DIR / ".offset"
RATE_LIMIT_FILE = QA_SESSIONS_DIR / "rate_limit.txt"

# Backlog stale threshold: if .offset is older than this when we boot,
# discard the queue and resume from current tip.
STALE_OFFSET_HOURS = 1.0

# Long-poll timeout. Telegram supports up to 50s; we use 30 to keep
# the loop responsive to systemd shutdown signals.
LONG_POLL_TIMEOUT_S = 30

# Backoff schedule for getUpdates errors.
BACKOFF_SCHEDULE_S = [5, 15, 60, 300]


def _api_base() -> str:
    return f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN']}"


def _allowed_chat_id() -> int:
    return int(os.environ["TELEGRAM_CHAT_ID"])


def _daily_cap() -> int:
    return int(os.environ.get("MAX_PER_DAY", "10"))


# --- offset persistence ---


def _load_offset() -> int | None:
    """Returns the saved offset, or None if .offset is absent or stale.

    None means caller should advance to the current tip.
    """
    if not OFFSET_FILE.exists():
        return None
    age_hours = (time.time() - OFFSET_FILE.stat().st_mtime) / 3600
    if age_hours > STALE_OFFSET_HOURS:
        logger.warning(
            "offset file %s is %.1fh stale; discarding backlog", OFFSET_FILE, age_hours
        )
        return None
    try:
        return int(OFFSET_FILE.read_text().strip())
    except (OSError, ValueError):
        return None


def _save_offset(offset: int) -> None:
    """Atomic write to avoid half-updated offsets on crash."""
    OFFSET_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = OFFSET_FILE.with_suffix(".offset.tmp")
    tmp.write_text(str(offset))
    tmp.replace(OFFSET_FILE)


def _discard_backlog() -> int:
    """Return current tip + 1 by calling getUpdates with a dummy
    offset of -1 (Telegram returns the latest update if any). The
    caller will save tip+1 so the next poll only sees new messages.
    """
    resp = requests.get(
        f"{_api_base()}/getUpdates",
        params={"offset": -1, "limit": 1, "timeout": 0},
        timeout=10,
    )
    resp.raise_for_status()
    payload = resp.json()
    results = payload.get("result", [])
    if not results:
        return 0  # no updates ever — start from offset 0
    return int(results[-1]["update_id"]) + 1


# --- claude invocation ---


def _session_dir(update_id: int) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    d = QA_SESSIONS_DIR / f"{ts}_{update_id}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _runner_path() -> Path:
    """Resolve runner.sh next to this module."""
    return Path(__file__).parent / "runner.sh"


def _run_claude(question: str, session_dir: Path) -> tuple[bool, str, Path]:
    """Invoke runner.sh, capture analysis. Returns (success, text, answer_file)."""
    answer_file = session_dir / "answer.txt"
    claude_log = session_dir / "claude.log"
    question_file = session_dir / "question.txt"
    question_file.write_text(question)

    env = {
        **os.environ,
        "QA_QUESTION": question,
        "QA_ANSWER_FILE": str(answer_file),
        "QA_CLAUDE_LOG": str(claude_log),
    }

    try:
        # The wall-clock timeout is enforced inside runner.sh via the
        # `timeout` command (180s); we add a generous backstop here.
        completed = subprocess.run(
            [str(_runner_path())],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=210,
        )
    except subprocess.TimeoutExpired:
        return False, "Claude timed out (>120s wall clock).", answer_file

    if completed.returncode != 0 or not answer_file.exists() or not answer_file.read_text().strip():
        return False, f"Claude exited rc={completed.returncode}; see {claude_log.name}.", answer_file

    return True, answer_file.read_text(), answer_file


# --- per-update handler ---


def _handle(update: dict) -> int | None:
    """Process one update. Returns the new offset value (update_id+1)
    on processed/rejected; returns None only on parse failure (caller
    will advance offset to skip)."""
    classification = classify_update(update, _allowed_chat_id())

    if isinstance(classification, Reject):
        logger.info("rejected: %s", classification.reason)
        # Still advance offset so it doesn't replay.
        return int(update.get("update_id", -1)) + 1

    accept: Accept = classification

    # Rate limit gate.
    if at_cap(RATE_LIMIT_FILE, _daily_cap()):
        try:
            telegram_send(
                f"Daily cap reached ({_daily_cap()}/day). Resets at UTC midnight."
            )
        except Exception as e:
            logger.exception("rate-limit refusal send failed: %s", e)
        return accept.update_id + 1

    record(RATE_LIMIT_FILE, accept.update_id)

    # Acknowledge.
    try:
        telegram_send("Working on it… (~30-180s; longer for multi-table joins)")
    except Exception as e:
        logger.exception("ack send failed (continuing anyway): %s", e)

    # Run claude.
    session_dir = _session_dir(accept.update_id)
    ok, body, answer_file = _run_claude(accept.text, session_dir)

    # Reply.
    if ok:
        try:
            from stock_screening.telegram_alerts.client import MAX_MSG_CHARS

            if len(body) > MAX_MSG_CHARS:
                telegram_send(body, attachments=[answer_file])
            else:
                telegram_send(body)
        except Exception as e:
            logger.exception("reply send failed: %s", e)
    else:
        try:
            telegram_send(
                f"Sorry, that query failed. Session: {session_dir.name}. ({body})"
            )
        except Exception as e:
            logger.exception("failure-reply send failed: %s", e)

    return accept.update_id + 1


# --- main loop ---


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info("listener starting (qa_sessions=%s)", QA_SESSIONS_DIR)

    # Sanity: required env present.
    for name in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        if not os.environ.get(name):
            logger.error("required env var %s not set", name)
            return 2

    # Boot: pick offset.
    offset = _load_offset()
    if offset is None:
        try:
            offset = _discard_backlog()
            logger.info("cold/stale start: advanced offset to %d", offset)
            _save_offset(offset)
        except requests.RequestException as e:
            logger.error("cold-start tip fetch failed: %s; defaulting to 0", e)
            offset = 0

    backoff_idx = 0

    while True:
        try:
            resp = requests.get(
                f"{_api_base()}/getUpdates",
                params={
                    "offset": offset,
                    "timeout": LONG_POLL_TIMEOUT_S,
                    "allowed_updates": json.dumps(["message"]),
                },
                timeout=LONG_POLL_TIMEOUT_S + 10,
            )
            resp.raise_for_status()
            payload = resp.json()
            backoff_idx = 0  # reset on success
        except requests.RequestException as e:
            wait = BACKOFF_SCHEDULE_S[min(backoff_idx, len(BACKOFF_SCHEDULE_S) - 1)]
            logger.warning("getUpdates failed (%s); backoff %ds", e, wait)
            time.sleep(wait)
            backoff_idx += 1
            continue

        if not payload.get("ok"):
            logger.error("getUpdates ok=false: %s", payload)
            time.sleep(5)
            continue

        for update in payload.get("result", []):
            try:
                new_offset = _handle(update)
            except Exception:
                logger.exception("uncaught exception in handler; advancing offset to skip")
                new_offset = int(update.get("update_id", -1)) + 1
                try:
                    telegram_send("Internal error. Skipping this question.")
                except Exception:
                    logger.exception("failure-of-failure send")
            if new_offset is not None and new_offset > offset:
                offset = new_offset
                _save_offset(offset)


if __name__ == "__main__":
    sys.exit(main())
