"""Send Signal messages via the local signal-cli-rest-api container.

This is intentionally outbound-only and read-no-config. We never poll
for incoming messages, never accept commands. The container is bound
to 127.0.0.1:8090 so nothing on the LAN can talk to it directly —
only services on the same host (e.g. airflow worker) can call it.

Linking the bot to a Signal account is a one-time interactive step
done outside this code; see signal/SETUP.md.
"""

from __future__ import annotations

import logging
import os
from base64 import b64encode
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# 2000-char Signal soft limit. Keep messages comfortably under this
# and split with attachments for long content (e.g. diffs).
MAX_MSG_CHARS = 1900


class SignalConfigError(RuntimeError):
    pass


def _config() -> tuple[str, str, list[str]]:
    """Read configuration from env. Raises if not set so failures are
    obvious (don't silently swallow alerts)."""
    base = os.environ.get("SIGNAL_API_URL", "http://localhost:8090")
    sender = os.environ.get("SIGNAL_SENDER_NUMBER")
    recipients_raw = os.environ.get("SIGNAL_RECIPIENT_NUMBERS")
    if not sender:
        raise SignalConfigError("SIGNAL_SENDER_NUMBER not set in environment")
    if not recipients_raw:
        raise SignalConfigError("SIGNAL_RECIPIENT_NUMBERS not set in environment")
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]
    return base, sender, recipients


def send(message: str, attachments: list[Path] | None = None,
         timeout_s: float = 15.0) -> None:
    """Send `message` (truncated if needed) plus optional file attachments
    to all configured recipients. Raises on HTTP error so the caller knows
    the alert didn't reach Signal."""
    base, sender, recipients = _config()

    body = message
    if len(body) > MAX_MSG_CHARS:
        body = body[: MAX_MSG_CHARS - 25] + "\n…(truncated, see attachment)"

    payload: dict = {
        "number": sender,
        "recipients": recipients,
        "message": body,
    }
    if attachments:
        encoded: list[str] = []
        for a in attachments:
            data = a.read_bytes()
            mime_hint = "text/plain" if a.suffix in (".txt", ".log", ".diff", ".md") else "application/octet-stream"
            encoded.append(f"data:{mime_hint};filename={a.name};base64,{b64encode(data).decode()}")
        payload["base64_attachments"] = encoded

    url = f"{base.rstrip('/')}/v2/send"
    resp = requests.post(url, json=payload, timeout=timeout_s)
    if resp.status_code >= 300:
        raise RuntimeError(f"signal-cli send failed: {resp.status_code} {resp.text[:300]}")
    logger.info("signal: sent %d chars to %d recipients", len(body), len(recipients))
