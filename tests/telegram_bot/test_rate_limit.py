"""Rate-limit tests for the Telegram Q&A bot."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from stock_screening.telegram_bot import rate_limit as rl


def _write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + ("\n" if lines else ""))


def test_count_today_empty(tmp_path: Path):
    p = tmp_path / "rl.txt"
    assert rl.count_today(p) == 0


def test_count_today_only_today(tmp_path: Path):
    today = rl.today_utc()
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    p = tmp_path / "rl.txt"
    _write_lines(p, [
        f"{yesterday} 23:50:00 1",
        f"{today} 00:01:00 2",
        f"{today} 09:30:00 3",
        f"{yesterday} 09:30:00 99",  # not today
    ])
    assert rl.count_today(p) == 2


def test_record_appends(tmp_path: Path):
    p = tmp_path / "qa_sessions/rate_limit.txt"
    rl.record(p, 42)
    rl.record(p, 43)
    assert p.exists()
    lines = p.read_text().strip().splitlines()
    assert len(lines) == 2
    assert lines[0].endswith(" 42")
    assert lines[1].endswith(" 43")


def test_at_cap_threshold(tmp_path: Path):
    p = tmp_path / "rl.txt"
    today = rl.today_utc()
    _write_lines(p, [f"{today} 00:00:0{i} {i}" for i in range(9)])
    assert not rl.at_cap(p, 10)
    rl.record(p, 99)
    assert rl.at_cap(p, 10)
