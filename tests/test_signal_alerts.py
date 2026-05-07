"""Tests for Signal client config + truncation logic. Network calls are
mocked — no real Signal traffic."""

from __future__ import annotations

from base64 import b64encode
from pathlib import Path

import pytest
import responses

from stock_screening.signal_alerts import client as sc


@pytest.fixture
def signal_env(monkeypatch):
    monkeypatch.setenv("SIGNAL_API_URL", "http://test-signal:8080")
    monkeypatch.setenv("SIGNAL_SENDER_NUMBER", "+819000000000")
    monkeypatch.setenv("SIGNAL_RECIPIENT_NUMBERS", "+819000000001,+819000000002")


def test_config_raises_when_sender_missing(monkeypatch):
    monkeypatch.delenv("SIGNAL_SENDER_NUMBER", raising=False)
    monkeypatch.setenv("SIGNAL_RECIPIENT_NUMBERS", "+1")
    with pytest.raises(sc.SignalConfigError):
        sc._config()


def test_config_raises_when_recipients_missing(monkeypatch):
    monkeypatch.setenv("SIGNAL_SENDER_NUMBER", "+1")
    monkeypatch.delenv("SIGNAL_RECIPIENT_NUMBERS", raising=False)
    with pytest.raises(sc.SignalConfigError):
        sc._config()


def test_config_strips_recipient_whitespace(monkeypatch):
    monkeypatch.setenv("SIGNAL_API_URL", "http://x:1")
    monkeypatch.setenv("SIGNAL_SENDER_NUMBER", "+1")
    monkeypatch.setenv("SIGNAL_RECIPIENT_NUMBERS", " +2 , +3,, +4 ")
    base, sender, recipients = sc._config()
    assert base == "http://x:1"
    assert sender == "+1"
    assert recipients == ["+2", "+3", "+4"]


@responses.activate
def test_send_basic(signal_env):
    responses.add(
        responses.POST,
        "http://test-signal:8080/v2/send",
        json={"timestamp": 12345},
        status=201,
    )
    sc.send("hello world")
    assert len(responses.calls) == 1
    body = responses.calls[0].request.body
    assert b'"hello world"' in body
    assert b'"+819000000000"' in body
    assert b'"+819000000001"' in body and b'"+819000000002"' in body
    assert b"base64_attachments" not in body  # no attachments path


@responses.activate
def test_send_truncates_long_message(signal_env):
    responses.add(
        responses.POST,
        "http://test-signal:8080/v2/send",
        json={},
        status=201,
    )
    long = "x" * (sc.MAX_MSG_CHARS + 500)
    sc.send(long)
    body = responses.calls[0].request.body.decode()
    # Truncation marker present, payload no larger than the cap
    assert "truncated" in body
    # Roughly: payload contains the truncated body, not the original 2400 chars
    assert body.count("x") <= sc.MAX_MSG_CHARS


@responses.activate
def test_send_with_attachment(signal_env, tmp_path):
    responses.add(
        responses.POST,
        "http://test-signal:8080/v2/send",
        json={},
        status=201,
    )
    f = tmp_path / "diag.diff"
    f.write_text("--- a/foo\n+++ b/foo\n@@ -1 +1 @@\n-old\n+new\n")
    sc.send("see attached", attachments=[f])
    body = responses.calls[0].request.body.decode()
    assert "base64_attachments" in body
    expected_b64 = b64encode(f.read_bytes()).decode()
    assert expected_b64 in body
    assert "filename=diag.diff" in body


@responses.activate
def test_send_raises_on_http_error(signal_env):
    responses.add(
        responses.POST,
        "http://test-signal:8080/v2/send",
        json={"error": "linked device removed"},
        status=400,
    )
    with pytest.raises(RuntimeError, match="signal-cli send failed"):
        sc.send("payload")
