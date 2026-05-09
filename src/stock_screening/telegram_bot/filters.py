"""Pure filter logic for incoming Telegram updates.

Split out from listener.py so the filter chain is unit-testable
without mocking HTTP. Each function returns either:
  - the message text to forward, or
  - a (rejected, reason) tuple where rejected=True and reason
    is a short human-readable string for logs.

Decision philosophy: fail-closed at every stage. An update has to
clear ALL filters to be processed.
"""

from __future__ import annotations

from dataclasses import dataclass

REQUIRED_PREFIX = "/q "
MAX_QUESTION_CHARS = 500
DAILY_CAP_DEFAULT = 10


@dataclass(frozen=True)
class Accept:
    text: str  # the question content with prefix stripped
    update_id: int


@dataclass(frozen=True)
class Reject:
    reason: str  # short, log-friendly


def classify_update(update: dict, allowed_chat_id: int) -> Accept | Reject:
    """Apply the fail-closed filter chain.

    Order is important — earlier checks must not depend on fields
    later checks would establish.
    """
    # 1. Must be a fresh message (skip edited_message, channel_post,
    #    callback_query, inline_query, my_chat_member, etc.).
    msg = update.get("message")
    if not isinstance(msg, dict):
        return Reject("not a fresh message (e.g. edited_message or callback)")

    # 2. Strict chat_id allowlist.
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    if chat_id != allowed_chat_id:
        return Reject(f"chat_id mismatch: got {chat_id!r}")

    # 3. Private chat only — sender must equal chat (rejects groups
    #    where the bot was added with the user).
    sender = (msg.get("from") or {}).get("id")
    if sender != chat_id:
        return Reject("not a private chat (from != chat)")

    # 4. Must have a text body (reject photo/voice/document/etc.).
    text = msg.get("text")
    if not isinstance(text, str):
        return Reject("non-text payload")

    # 5. Length cap pre-Claude (cheap prompt-injection mitigation).
    if len(text) > MAX_QUESTION_CHARS:
        return Reject(f"text too long: {len(text)} > {MAX_QUESTION_CHARS}")

    # 6. Magic prefix.
    if not text.startswith(REQUIRED_PREFIX):
        return Reject(f"missing {REQUIRED_PREFIX!r} prefix")

    question = text[len(REQUIRED_PREFIX):].strip()
    if not question:
        return Reject("empty question after prefix")

    update_id = update.get("update_id")
    if not isinstance(update_id, int):
        return Reject("missing update_id")

    return Accept(text=question, update_id=update_id)
