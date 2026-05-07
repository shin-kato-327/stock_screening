"""Tests for Telegram client config + truncation logic. All network
calls mocked via `responses` — no real Telegram traffic."""

from __future__ import annotations

import pytest
import responses

from stock_screening.telegram_alerts import client as tc


@pytest.fixture
def telegram_env(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "111:fake-test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")


def test_config_raises_when_token_missing(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    with pytest.raises(tc.TelegramConfigError, match="TELEGRAM_BOT_TOKEN"):
        tc._config()


def test_config_raises_when_chat_id_missing(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "x")
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    with pytest.raises(tc.TelegramConfigError, match="TELEGRAM_CHAT_ID"):
        tc._config()


@responses.activate
def test_send_basic(telegram_env):
    responses.add(
        responses.POST,
        "https://api.telegram.org/bot111:fake-test-token/sendMessage",
        json={"ok": True, "result": {"message_id": 1}},
        status=200,
    )
    tc.send("hello world")
    assert len(responses.calls) == 1
    body = responses.calls[0].request.body
    assert b'"hello world"' in body
    assert b'"chat_id": "12345"' in body
    assert b'"disable_web_page_preview": true' in body


@responses.activate
def test_send_truncates_long_message(telegram_env):
    responses.add(
        responses.POST,
        "https://api.telegram.org/bot111:fake-test-token/sendMessage",
        json={"ok": True, "result": {}},
        status=200,
    )
    long = "x" * (tc.MAX_MSG_CHARS + 1000)
    tc.send(long)
    body = responses.calls[0].request.body.decode()
    assert "truncated" in body
    assert body.count("x") <= tc.MAX_MSG_CHARS


@responses.activate
def test_send_with_attachment(telegram_env, tmp_path):
    responses.add(
        responses.POST,
        "https://api.telegram.org/bot111:fake-test-token/sendMessage",
        json={"ok": True, "result": {}},
        status=200,
    )
    responses.add(
        responses.POST,
        "https://api.telegram.org/bot111:fake-test-token/sendDocument",
        json={"ok": True, "result": {}},
        status=200,
    )
    f = tmp_path / "diag.diff"
    f.write_text("--- a/foo\n+++ b/foo\n@@ -1 +1 @@\n-old\n+new\n")
    tc.send("see attached", attachments=[f])
    # Two requests: sendMessage + sendDocument
    assert len(responses.calls) == 2
    assert responses.calls[0].request.url.endswith("/sendMessage")
    assert responses.calls[1].request.url.endswith("/sendDocument")


@responses.activate
def test_send_raises_on_http_error(telegram_env):
    responses.add(
        responses.POST,
        "https://api.telegram.org/bot111:fake-test-token/sendMessage",
        json={"ok": False, "error_code": 401, "description": "Unauthorized"},
        status=401,
    )
    with pytest.raises(RuntimeError, match="telegram send failed"):
        tc.send("payload")


@responses.activate
def test_send_raises_when_ok_false_with_200(telegram_env):
    """Telegram returns HTTP 200 with `ok: false` for some errors. Catch that."""
    responses.add(
        responses.POST,
        "https://api.telegram.org/bot111:fake-test-token/sendMessage",
        json={"ok": False, "error_code": 400, "description": "chat not found"},
        status=200,
    )
    with pytest.raises(RuntimeError, match="telegram send failed"):
        tc.send("payload")
