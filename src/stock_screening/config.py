"""Typed accessors for runtime configuration.

Inside Airflow tasks, secrets and tunable parameters live in Airflow
Variables; outside Airflow (notebooks, ad-hoc scripts), they fall back
to environment variables loaded from .env. Same names in both places
so behavior matches.
"""

from __future__ import annotations

import os


def _get(name: str, default: str | None = None) -> str | None:
    try:
        from airflow.models import Variable  # type: ignore

        v = Variable.get(name, default_var=None)
        if v is not None:
            return v
    except Exception:
        pass
    return os.environ.get(name, default)


def edinet_key() -> str:
    v = _get("EDINET_KEY")
    if not v:
        raise RuntimeError("EDINET_KEY not set (Airflow Variable or env)")
    return v


def jquants_refresh_token() -> str:
    v = _get("JQUANTS_REFRESH_TOKEN")
    if not v:
        raise RuntimeError("JQUANTS_REFRESH_TOKEN not set")
    return v


def initial_capital() -> int:
    return int(_get("INITIAL_CAPITAL", "10000000"))


def max_positions() -> int:
    return int(_get("MAX_POSITIONS", "20"))


def transaction_cost_bps() -> int:
    return int(_get("TRANSACTION_COST_BPS", "10"))


def benchmark_ticker() -> str:
    return _get("BENCHMARK_TICKER", "1306")
