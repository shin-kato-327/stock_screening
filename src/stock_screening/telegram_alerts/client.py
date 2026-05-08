"""Send Telegram messages via the Bot API.

Outbound-only by design. We never poll for incoming messages, never
accept commands. The bot's only capability is sending; even if its
token leaks, the worst an attacker can do is impersonate the bot in
the chat (information disclosure), not control the pipeline.

Setup is one-time and ~2 minutes:
  1. Open Telegram → @BotFather → /newbot → choose name/username
  2. Receive token like 123456:ABCdef...
  3. Open the new bot, send it any message (e.g. "hi") so it sees you
  4. Find your chat_id: curl https://api.telegram.org/bot<TOKEN>/getUpdates
     → look for "chat":{"id":<CHAT_ID>,...}
  5. Set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID in .env

Telegram's per-message limit is 4096 chars; we cap a bit lower to
leave room for formatting and let the rest go as a file attachment.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

API_BASE = "https://api.telegram.org"
MAX_MSG_CHARS = 3800  # Telegram limit is 4096; leave headroom


class TelegramConfigError(RuntimeError):
    pass


def _config() -> tuple[str, str]:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token:
        raise TelegramConfigError("TELEGRAM_BOT_TOKEN not set in environment")
    if not chat_id:
        raise TelegramConfigError("TELEGRAM_CHAT_ID not set in environment")
    return token, chat_id


def send(message: str, attachments: list[Path] | None = None,
         timeout_s: float = 15.0) -> None:
    """Send `message` (truncated if needed) plus optional file attachments
    to the configured chat. Raises on HTTP error so the caller knows it
    didn't reach Telegram."""
    token, chat_id = _config()

    body = message
    if len(body) > MAX_MSG_CHARS:
        body = body[: MAX_MSG_CHARS - 30] + "\n…(truncated; see attachment)"

    url = f"{API_BASE}/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": body,
        "disable_web_page_preview": True,
    }
    resp = requests.post(url, json=payload, timeout=timeout_s)
    if resp.status_code >= 300 or not resp.json().get("ok", False):
        raise RuntimeError(f"telegram send failed: {resp.status_code} {resp.text[:300]}")
    logger.info("telegram: sent %d chars", len(body))

    if attachments:
        for attachment in attachments:
            _send_document(token, chat_id, attachment, timeout_s)


def _send_document(token: str, chat_id: str, path: Path, timeout_s: float) -> None:
    url = f"{API_BASE}/bot{token}/sendDocument"
    with path.open("rb") as f:
        resp = requests.post(
            url,
            data={"chat_id": chat_id},
            files={"document": (path.name, f)},
            timeout=timeout_s,
        )
    if resp.status_code >= 300 or not resp.json().get("ok", False):
        raise RuntimeError(
            f"telegram sendDocument failed for {path.name}: {resp.status_code} {resp.text[:300]}"
        )
    logger.info("telegram: sent attachment %s", path.name)
