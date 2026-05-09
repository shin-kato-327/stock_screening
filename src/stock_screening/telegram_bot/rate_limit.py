"""File-based per-day rate limiter.

Mirrors the pattern used by scripts/diagnose_and_notify.sh — a flat
text file at ~/qa_sessions/rate_limit.txt where each accepted question
appends one line `YYYY-MM-DD HH:MM:SS <update_id>`. We count lines
starting with today's UTC date and refuse if at cap.

Racy on concurrent writers, but the listener is single-threaded by
design, so it doesn't matter in practice.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


def today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def count_today(path: Path) -> int:
    if not path.exists():
        return 0
    prefix = today_utc() + " "
    return sum(1 for line in path.read_text().splitlines() if line.startswith(prefix))


def record(path: Path, update_id: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with path.open("a") as f:
        f.write(f"{ts} {update_id}\n")


def at_cap(path: Path, cap: int) -> bool:
    return count_today(path) >= cap
