"""Daily Telegram report: position snapshot + new screen entrants.

Runs after the paper_trading_sim DAG has settled for the day so the
NAV/positions tables reflect the latest screen.

What gets sent:
  1. Today's IBKR position snapshot (fetched via the Flex client)
     with current ratio per holding and EXIT/WATCH/HOLD classification.
  2. New entrants in today's screen vs yesterday's: stocks that meet
     the sweet-spot criteria today but didn't yesterday.

Send-only. No commands accepted from the receiving end.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

import pandas as pd
import pendulum
from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/dags")
from _alerts import _hydrate_env_from_variables, alert_on_failure  # noqa: E402
sys.path.insert(0, "/opt/airflow/src")
from stock_screening import db  # noqa: E402
from stock_screening.telegram_alerts.client import send as telegram_send  # noqa: E402

JST = pendulum.timezone("Asia/Tokyo")
logger = logging.getLogger(__name__)

HAIRCUT = 0.7


@dag(
    dag_id="daily_telegram_report_dag",
    description="Daily Telegram message: positions + new screen entrants.",
    start_date=datetime(2026, 5, 1, tzinfo=JST),
    # 19:30 JST weekdays — after paper_trading_sim_dag at 19:00.
    schedule="30 19 * * 1-5",
    catchup=False,                # never want to backfill yesterday's report
    max_active_runs=1,
    default_args={
        "owner": "stock_screening",
        "on_failure_callback": alert_on_failure,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["telegram"],
)
def daily_telegram_report_dag():

    @task
    def build_and_send(data_interval_end=None):
        from stock_screening.screening.metrics import compute_strict_cohort

        _hydrate_env_from_variables()
        run_date = (data_interval_end or pendulum.now(JST)).date()
        engine = db.get_engine()

        # The strict strategy cohort (cap ¥3-30B + ratio>1.5 + PER<=10
        # + lowest-float-33% overlay, per docs/STRATEGY.md). This is
        # the set the user actually buys from — much narrower than the
        # loose "qualifies" pre-filter in t_screen_results.
        cohort_today = compute_strict_cohort(engine, run_date)

        # Compare to the most recent prior session that produced a
        # non-empty cohort. Look back up to a week to skip Golden Week
        # / weekend holes.
        prev_cohort = pd.DataFrame()
        prev_d = None
        for back in range(1, 8):
            d = run_date - timedelta(days=back)
            c = compute_strict_cohort(engine, d)
            if not c.empty:
                prev_cohort = c
                prev_d = d
                break

        today_codes = set(cohort_today["sec_code"]) if not cohort_today.empty else set()
        prev_codes = set(prev_cohort["sec_code"]) if not prev_cohort.empty else set()
        new_entrants = sorted(today_codes - prev_codes)
        dropped = sorted(prev_codes - today_codes)

        # Optional: positions snapshot (skipped if IBKR creds aren't
        # configured; the DAG still serves as the daily heartbeat).
        positions_block = _try_positions_snapshot()

        body_parts = [
            f"📊 Daily report — {run_date.isoformat()}",
            "",
            positions_block,
            "",
            f"🎯 Strategy cohort ({len(cohort_today)}): "
            f"sweet-spot ∩ lowest-float-33%",
        ]
        if cohort_today.empty:
            body_parts.append("  (none — universe didn't produce any picks today)")
        else:
            for _, r in cohort_today.iterrows():
                marker = " 🆕" if r["sec_code"] in new_entrants else ""
                body_parts.append(
                    f"  {int(r['rank_in_cohort'])}. {r['sec_code']} "
                    f"{(r['name'] or '')[:24]:<24} "
                    f"ratio={float(r['ratio']):.2f} "
                    f"PER={float(r['per']):.1f} "
                    f"MC=¥{float(r['market_cap'])/1e9:.1f}B"
                    f"{marker}"
                )

        if dropped:
            body_parts.append("")
            body_parts.append(
                f"📤 Dropped from cohort vs {prev_d.isoformat() if prev_d else '?'}: "
                f"{', '.join(dropped)}"
            )

        message = "\n".join(p for p in body_parts if p is not None)
        telegram_send(message)
        logger.info(
            "daily report sent (cohort=%d, new=%d, dropped=%d, prev_d=%s)",
            len(cohort_today), len(new_entrants), len(dropped), prev_d,
        )

    build_and_send()


def _try_positions_snapshot() -> str:
    """Best-effort positions block. If IBKR creds aren't configured or
    the call fails, returns a placeholder. We never want this to take
    down the daily report — Signal still gets the screen update."""
    import os

    token = os.environ.get("IBKR_FLEX_TOKEN")
    query_id = os.environ.get("IBKR_POSITIONS_QUERY_ID")
    if not token or not query_id:
        return "💼 Positions: (IBKR Flex not configured on this host)"

    try:
        from stock_screening.ibkr.flex_client import FlexClient
        from stock_screening.ibkr.positions import enrich_with_screen, parse_open_positions

        engine = db.get_engine()
        resp = FlexClient(token).fetch_query(query_id)
        positions = parse_open_positions(resp.root)
        if not positions:
            return "💼 Positions: (none)"
        df = enrich_with_screen(engine, positions)
        lines = ["💼 Positions:"]
        total_value = 0.0
        for _, r in df.iterrows():
            value = (
                float(r["mark_price"]) * float(r["quantity"])
                if r.get("mark_price") is not None and r.get("quantity") is not None
                else 0.0
            )
            total_value += value
            ratio_s = f"{r['ratio']:.2f}" if r.get("ratio") is not None and not _is_nan(r["ratio"]) else " ?  "
            ret_s = ""
            if r.get("cost_basis") and r.get("mark_price") and r["cost_basis"]:
                ret_s = f"{(float(r['mark_price']) / float(r['cost_basis']) - 1) * 100:+.1f}%"
            lines.append(
                f"  • {r['symbol']} ratio={ratio_s} ret={ret_s} val=¥{value:,.0f}"
            )
        lines.append(f"  Total: ¥{total_value:,.0f}")
        return "\n".join(lines)
    except Exception as e:
        logger.warning("positions snapshot failed: %s", e)
        return f"💼 Positions: (snapshot failed: {type(e).__name__})"


def _is_nan(x) -> bool:
    try:
        return x != x  # NaN is the only value that doesn't equal itself
    except Exception:
        return False


daily_telegram_report_dag()
