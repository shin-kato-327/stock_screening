"""Run the net-cash-ratio screen for the run date and persist results.

Reads t_financials_annual (typed columns; interest_bearing_debt is the
GENERATED sum of components) joined with t_daily_stock_perf for the
run date's marketCap. Days are independent — catchup parallelizes.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task

JST = pendulum.timezone("Asia/Tokyo")

logger = logging.getLogger(__name__)


@dag(
    dag_id="value_screen_dag",
    description="Net-cash-ratio screen → t_screen_results.",
    start_date=datetime(2025, 4, 1, tzinfo=JST),
    schedule="0 18 * * 1-5",  # 18:00 JST weekdays — after prices DAG
    catchup=True,
    max_active_runs=4,
    default_args={
        "owner": "stock_screening",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["screen"],
)
def value_screen_dag():
    @task
    def compute_and_persist(data_interval_start=None) -> int:
        from stock_screening import db
        from stock_screening.screening.metrics import (
            compute_screen,
            persist_screen_results,
        )

        run_date = data_interval_start.date()
        engine = db.get_engine()
        df = compute_screen(engine, run_date)
        n = persist_screen_results(engine, run_date, df)
        if not df.empty:
            n_qual = int(df["qualifies"].sum())
            logger.info("%s: %d names scored, %d qualify", run_date, len(df), n_qual)
        return n

    compute_and_persist()


value_screen_dag()
