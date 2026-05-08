"""TEMPORARY test DAG — deliberately fails to verify the Telegram
on_failure_callback path. Delete this file once verified.

retries=0 + raise so the failure callback fires immediately on the
first attempt (no waiting for retry delays).
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/dags")
from _alerts import alert_on_failure  # noqa: E402

JST = pendulum.timezone("Asia/Tokyo")


@dag(
    dag_id="test_failure_dag",
    description="TEMPORARY — verifies Telegram failure-alert + Claude diagnosis path. Delete after testing.",
    start_date=datetime(2026, 5, 1, tzinfo=JST),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "stock_screening",
        "on_failure_callback": alert_on_failure,
        "retries": 0,
        "retry_delay": timedelta(seconds=1),
    },
    tags=["test", "telegram"],
)
def test_failure_dag():

    @task
    def will_fail():
        # Match a real-looking failure shape so Claude diagnosis has
        # something to reason about beyond an empty traceback.
        rows = [{"close": float("nan"), "volume": float("nan")}]
        if rows:
            raise RuntimeError(
                "Simulated failure: NaN close/volume rejected by bigint volume column. "
                "Intentional test for Telegram alert + Claude diagnosis end-to-end."
            )

    will_fail()


test_failure_dag()
