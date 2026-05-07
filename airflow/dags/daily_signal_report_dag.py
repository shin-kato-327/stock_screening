"""Daily Signal report: position snapshot + new screen entrants.

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

import pendulum
from airflow.decorators import dag, task
from sqlalchemy import text

sys.path.insert(0, "/opt/airflow/dags")
from _alerts import alert_on_failure  # noqa: E402
sys.path.insert(0, "/opt/airflow/src")
from stock_screening import db  # noqa: E402
from stock_screening.signal_alerts.client import send as signal_send  # noqa: E402

JST = pendulum.timezone("Asia/Tokyo")
logger = logging.getLogger(__name__)

HAIRCUT = 0.7


@dag(
    dag_id="daily_signal_report_dag",
    description="Daily Signal message: positions + new screen entrants.",
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
    tags=["signal"],
)
def daily_signal_report_dag():

    @task
    def build_and_send(data_interval_end=None):
        run_date = (data_interval_end or pendulum.now(JST)).date()
        engine = db.get_engine()

        # New entrants = sweet-spot today minus sweet-spot yesterday.
        # Use the t_screen_results table that the value_screen_dag writes.
        with engine.connect() as conn:
            today_codes = {
                r[0]
                for r in conn.execute(
                    text(
                        'SELECT "secCode" FROM t_screen_results '
                        'WHERE run_date = :d AND qualifies = TRUE'
                    ),
                    {"d": run_date},
                ).fetchall()
            }
            prev_codes = set()
            for back in (1, 2, 3):
                prev_d = run_date - timedelta(days=back)
                rows = conn.execute(
                    text(
                        'SELECT "secCode" FROM t_screen_results '
                        'WHERE run_date = :d AND qualifies = TRUE'
                    ),
                    {"d": prev_d},
                ).fetchall()
                if rows:
                    prev_codes = {r[0] for r in rows}
                    break
            new_entrants = sorted(today_codes - prev_codes)

            entrant_details = []
            if new_entrants:
                rows = conn.execute(
                    text(
                        'SELECT "secCode", ratio, market_cap '
                        'FROM t_screen_results WHERE run_date = :d '
                        'AND "secCode" = ANY(:codes) '
                        'ORDER BY ratio DESC'
                    ),
                    {"d": run_date, "codes": list(new_entrants)},
                ).fetchall()
                for sc, ratio, mc in rows:
                    name_row = conn.execute(
                        text(
                            'SELECT "filerName" FROM t_doc_list '
                            'WHERE "secCode" = :c '
                            'ORDER BY "submitDateTime" DESC LIMIT 1'
                        ),
                        {"c": sc},
                    ).fetchone()
                    name = name_row[0] if name_row else ""
                    entrant_details.append(
                        f"  • {sc} {name[:24]:<24}  ratio={float(ratio):.2f}  "
                        f"MC=¥{float(mc) / 1e9:.1f}B"
                    )

        # Optional: positions snapshot (skipped if IBKR creds aren't set,
        # since this DAG also serves as the daily heartbeat even without
        # broker integration).
        positions_block = _try_positions_snapshot()

        body_parts = [
            f"📊 Daily report — {run_date.isoformat()}",
            "",
            positions_block,
            "",
            f"🆕 New screen entrants ({len(new_entrants)}):",
        ]
        if entrant_details:
            body_parts.extend(entrant_details)
        else:
            body_parts.append("  (none — universe unchanged from prior session)")

        body_parts.append("")
        body_parts.append(f"Total qualifying today: {len(today_codes)}")
        message = "\n".join(p for p in body_parts if p is not None)
        signal_send(message)
        logger.info("daily report sent (%d new entrants, %d total)",
                    len(new_entrants), len(today_codes))

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


daily_signal_report_dag()
