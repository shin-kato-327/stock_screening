"""Filter-chain tests for the Telegram Q&A bot listener.

Pure-function tests — no HTTP, no subprocess, no DB. The filter is
the security boundary; covering every reject path makes accidental
regression loud.
"""

from __future__ import annotations

from stock_screening.telegram_bot.filters import (
    Accept,
    Reject,
    REQUIRED_PREFIX,
    classify_update,
)

OWNER_CHAT_ID = 7753955053


def _msg(text: str | None, **chat_overrides) -> dict:
    """Build a minimal-shape Telegram update for a private text message."""
    chat_id = chat_overrides.pop("chat_id", OWNER_CHAT_ID)
    from_id = chat_overrides.pop("from_id", chat_id)  # private chat default
    message = {
        "message_id": 1,
        "from": {"id": from_id, "is_bot": False},
        "chat": {"id": chat_id, "type": "private"},
        "date": 1715000000,
    }
    if text is not None:
        message["text"] = text
    return {"update_id": 100, "message": message}


def test_accept_basic():
    res = classify_update(_msg("/q how many qualifying stocks today?"), OWNER_CHAT_ID)
    assert isinstance(res, Accept)
    assert res.text == "how many qualifying stocks today?"
    assert res.update_id == 100


def test_reject_foreign_chat_id():
    res = classify_update(_msg("/q hi", chat_id=9999), OWNER_CHAT_ID)
    assert isinstance(res, Reject)
    assert "chat_id mismatch" in res.reason


def test_reject_missing_prefix():
    res = classify_update(_msg("hi"), OWNER_CHAT_ID)
    assert isinstance(res, Reject)
    assert REQUIRED_PREFIX.strip() in res.reason


def test_reject_prefix_without_question():
    res = classify_update(_msg("/q "), OWNER_CHAT_ID)
    assert isinstance(res, Reject)
    assert "empty" in res.reason


def test_reject_too_long():
    res = classify_update(_msg("/q " + "x" * 1000), OWNER_CHAT_ID)
    assert isinstance(res, Reject)
    assert "too long" in res.reason


def test_reject_non_text_payload():
    update = _msg(None)
    update["message"]["photo"] = [{"file_id": "abc"}]  # photo payload, no text
    res = classify_update(update, OWNER_CHAT_ID)
    assert isinstance(res, Reject)
    assert "non-text" in res.reason


def test_reject_edited_message_skipped():
    # Telegram delivers edits as edited_message, not message. We MUST
    # skip them — otherwise an attacker could edit a benign message
    # to a malicious one and bypass our offset-tracking.
    update = {"update_id": 200, "edited_message": _msg("/q rewritten")["message"]}
    res = classify_update(update, OWNER_CHAT_ID)
    assert isinstance(res, Reject)


def test_reject_callback_query_skipped():
    update = {"update_id": 300, "callback_query": {"data": "/q hi"}}
    res = classify_update(update, OWNER_CHAT_ID)
    assert isinstance(res, Reject)


def test_reject_group_chat_via_from_mismatch():
    # A group chat would have chat.id of a group AND from.id of an
    # individual user. We require from == chat for private chats.
    update = _msg("/q hi", from_id=OWNER_CHAT_ID + 1)
    res = classify_update(update, OWNER_CHAT_ID)
    assert isinstance(res, Reject)
    assert "private chat" in res.reason


def test_strips_prefix_keeps_internal_whitespace():
    # We strip leading/trailing whitespace of the question but keep
    # internal spacing.
    res = classify_update(_msg("/q  what    about   5363?  "), OWNER_CHAT_ID)
    assert isinstance(res, Accept)
    assert res.text == "what    about   5363?"


def test_reject_missing_update_id():
    update = {"message": _msg("/q hi")["message"]}  # no update_id
    res = classify_update(update, OWNER_CHAT_ID)
    assert isinstance(res, Reject)
