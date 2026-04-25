"""Daily paper-trading simulation: rebalance the portfolio on the run
date using that day's screen output.

Sim is path-dependent (day N reads day N-1 positions), so this DAG is
strictly serial: depends_on_past=True, max_active_runs=1. The first
task deletes the run date's outputs so re-runs are idempotent.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task

JST = pendulum.timezone("Asia/Tokyo")

logger = logging.getLogger(__name__)


@dag(
    dag_id="paper_trading_sim_dag",
    description="Equal-weight top-N net-cash-ratio paper portfolio.",
    start_date=datetime(2025, 4, 1, tzinfo=JST),
    schedule="0 19 * * 1-5",  # 19:00 JST weekdays — after value_screen
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "stock_screening",
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sim"],
)
def paper_trading_sim_dag():
    @task
    def rebalance(data_interval_start=None) -> dict:
        import pandas as pd
        from sqlalchemy import text

        from stock_screening import config, db
        from stock_screening.simulation import portfolio
        from stock_screening.simulation.rebalance import (
            compute_target_shares,
            generate_trades,
            select_target_names,
        )

        run_date = data_interval_start.date()
        engine = db.get_engine()

        portfolio.delete_outputs_for_date(engine, run_date)

        prev = portfolio.load_previous_snapshot(engine, run_date)

        with engine.connect() as conn:
            qual = pd.read_sql(
                text(
                    'SELECT "secCode" AS sec_code, ratio FROM t_screen_results '
                    "WHERE run_date = :d AND qualifies = TRUE "
                    "ORDER BY ratio DESC"
                ),
                conn,
                params={"d": run_date},
            )

        if qual.empty:
            logger.warning("no qualifying names on %s; carrying prior state", run_date)
            qual = pd.DataFrame(columns=["sec_code", "ratio"])

        if prev is None:
            cash = float(config.initial_capital())
            current_positions = pd.DataFrame(
                columns=["secCode", "shares", "avg_cost", "last_price", "market_value"]
            )
            sim_start_date = run_date
        else:
            current_positions = prev.positions
            cash = prev.cash
            sim_start_date = _sim_start_date(engine) or run_date

        all_names = sorted(set(qual["sec_code"]) | set(current_positions["secCode"]))
        prices = portfolio.fetch_prices(engine, run_date, all_names)
        bench = config.benchmark_ticker()
        if bench not in prices:
            prices.update(portfolio.fetch_prices(engine, run_date, [bench]))

        marked = portfolio.mark_to_market(current_positions, prices)
        nav_pre = cash + (
            float(marked["market_value"].sum()) if not marked.empty else 0.0
        )

        target_names = select_target_names(
            qual, list(marked["secCode"]) if not marked.empty else [], config.max_positions()
        )
        target_shares = compute_target_shares(
            target_names, prices, nav_pre, config.max_positions()
        )

        current_shares = (
            {row.secCode: int(row.shares) for row in marked.itertuples()}
            if not marked.empty
            else {}
        )
        trades = generate_trades(
            current_shares, target_shares, prices, config.transaction_cost_bps()
        )

        new_positions, new_cash = portfolio.apply_trades(marked, trades, cash)
        portfolio.persist_positions(engine, run_date, new_positions)
        portfolio.persist_trades(engine, run_date, trades)
        bench_nav = portfolio.benchmark_nav_for(
            engine, sim_start_date, run_date, float(config.initial_capital()), bench
        )
        portfolio.persist_nav(engine, run_date, new_cash, new_positions, bench_nav)

        result = {
            "run_date": str(run_date),
            "n_positions": int(len(new_positions)),
            "n_trades": int(len(trades)),
            "cash": round(new_cash, 2),
            "nav": round(
                new_cash
                + (
                    float(new_positions["market_value"].sum())
                    if not new_positions.empty
                    else 0.0
                ),
                2,
            ),
            "benchmark_nav": round(bench_nav, 2) if bench_nav else None,
        }
        logger.info("sim result: %s", result)
        return result

    rebalance()


def _sim_start_date(engine):
    """Earliest as_of_date in t_sim_portfolio_nav — the day the sim
    started, used as the benchmark anchor."""
    from sqlalchemy import text

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT MIN(as_of_date) FROM t_sim_portfolio_nav")
        ).first()
    return row[0] if row else None


paper_trading_sim_dag()
