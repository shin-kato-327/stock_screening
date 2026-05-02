"""Daily multi-strategy paper-trading simulation. Each registered
Strategy maintains its own portfolio against the same screen output.

Sim is path-dependent (day N reads day N-1 positions), so this DAG is
strictly serial: depends_on_past=True, max_active_runs=1. The first
task deletes the run date's outputs (per-strategy) so re-runs are
idempotent.
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
    description="Multi-strategy paper portfolio against the net-cash-ratio screen.",
    start_date=datetime(2026, 5, 1, tzinfo=JST),
    schedule="0 19 * * 1-5",
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
    def rebalance_all(data_interval_start=None) -> dict:
        import pandas as pd
        from sqlalchemy import text

        from stock_screening import config, db
        from stock_screening.simulation import portfolio
        from stock_screening.simulation.rebalance import (
            compute_target_shares,
            generate_trades,
            select_target_names,
        )
        from stock_screening.simulation.strategies import STRATEGIES

        run_date = data_interval_start.date()
        engine = db.get_engine()

        with engine.connect() as conn:
            qrows = conn.execute(
                text(
                    'SELECT "secCode" AS sec_code, ratio FROM t_screen_results '
                    "WHERE run_date = :d AND qualifies = TRUE ORDER BY ratio DESC"
                ),
                {"d": run_date},
            ).fetchall()
        qual = pd.DataFrame(qrows, columns=["sec_code", "ratio"])
        if not qual.empty:
            qual["ratio"] = qual["ratio"].astype(float)
        ratios = dict(zip(qual["sec_code"], qual["ratio"], strict=True)) if not qual.empty else {}

        bench = config.benchmark_ticker()
        results: dict[str, dict] = {}

        for strategy in STRATEGIES:
            portfolio.delete_outputs_for_date(engine, run_date, strategy.name)
            prev = portfolio.load_previous_snapshot(engine, run_date, strategy.name)

            if prev is None:
                cash = float(strategy.initial_capital)
                current_positions = pd.DataFrame(
                    columns=["secCode", "shares", "avg_cost", "last_price", "market_value"]
                )
                sim_start_date = run_date
            else:
                current_positions = prev.positions
                cash = prev.cash
                sim_start_date = _sim_start_date(engine, strategy.name) or run_date

            all_names = sorted(set(qual["sec_code"]) | set(current_positions["secCode"]))
            prices = portfolio.fetch_prices(engine, run_date, all_names)
            if bench not in prices:
                prices.update(portfolio.fetch_prices(engine, run_date, [bench]))

            marked = portfolio.mark_to_market(current_positions, prices)
            nav_pre = cash + (
                float(marked["market_value"].sum()) if not marked.empty else 0.0
            )

            target_names = select_target_names(
                qual,
                list(marked["secCode"]) if not marked.empty else [],
                strategy.max_positions,
                swap_rule=strategy.swap_rule,
            )
            target_shares = compute_target_shares(
                target_names,
                prices,
                nav_pre,
                strategy.max_positions,
                weighting=strategy.weighting,
                ratios=ratios,
            )
            current_shares = (
                {row.secCode: int(row.shares) for row in marked.itertuples()}
                if not marked.empty
                else {}
            )
            trades = generate_trades(
                current_shares, target_shares, prices, strategy.transaction_cost_bps
            )
            new_positions, new_cash = portfolio.apply_trades(marked, trades, cash)
            portfolio.persist_positions(engine, run_date, new_positions, strategy.name)
            portfolio.persist_trades(engine, run_date, trades, strategy.name)
            bench_nav = portfolio.benchmark_nav_for(
                engine, sim_start_date, run_date, float(strategy.initial_capital), bench
            )
            portfolio.persist_nav(
                engine, run_date, new_cash, new_positions, bench_nav, strategy.name
            )

            pos_value = (
                float(new_positions["market_value"].sum()) if not new_positions.empty else 0.0
            )
            results[strategy.name] = {
                "n_positions": int(len(new_positions)),
                "n_trades": len(trades),
                "nav": round(new_cash + pos_value, 2),
                "benchmark_nav": round(bench_nav, 2) if bench_nav else None,
            }
            logger.info("[%s] %s", strategy.name, results[strategy.name])

        return {"run_date": str(run_date), "results": results}

    rebalance_all()


def _sim_start_date(engine, strategy_name: str):
    """Earliest as_of_date for this strategy — used as benchmark anchor."""
    from sqlalchemy import text

    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT MIN(as_of_date) FROM t_sim_portfolio_nav WHERE strategy_name = :s"
            ),
            {"s": strategy_name},
        ).first()
    return row[0] if row else None


paper_trading_sim_dag()
