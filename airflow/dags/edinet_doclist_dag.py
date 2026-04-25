"""Fetch the EDINET document list for one logical date and upsert it
into t_doc_list.

Date-parameterized via data_interval_start, so the same DAG runs daily
(1-day window) and historically (via `airflow dags backfill`).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from sqlalchemy import text

JST = pendulum.timezone("Asia/Tokyo")

logger = logging.getLogger(__name__)


@dag(
    dag_id="edinet_doclist_dag",
    description="Fetch EDINET filings list for the run date; upsert into t_doc_list.",
    start_date=datetime(2025, 4, 1, tzinfo=JST),
    schedule="0 22 * * *",  # 22:00 JST — well after typical filing windows
    catchup=True,
    max_active_runs=4,
    default_args={
        "owner": "stock_screening",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["edinet"],
)
def edinet_doclist_dag():
    @task
    def fetch_and_upsert(data_interval_start=None) -> int:
        from stock_screening import config, db
        from stock_screening.edinet.client import (
            DOC_LIST_COLUMNS,
            list_documents,
        )

        target_date = data_interval_start.date()
        entries = list_documents(target_date, config.edinet_key())
        if not entries:
            logger.info("no filings on %s", target_date)
            return 0

        rows = [e.to_row() for e in entries]
        engine = db.get_engine()

        cols = ", ".join(f'"{c}"' for c in DOC_LIST_COLUMNS)
        placeholders = ", ".join(f":{c}" for c in DOC_LIST_COLUMNS)
        update_set = ", ".join(
            f'"{c}" = EXCLUDED."{c}"' for c in DOC_LIST_COLUMNS if c != "docID"
        )
        sql = text(
            f"INSERT INTO t_doc_list ({cols}) VALUES ({placeholders}) "
            f'ON CONFLICT ("docID") DO UPDATE SET {update_set}'
        )
        with engine.begin() as conn:
            conn.execute(sql, rows)
        logger.info("upserted %d filings for %s", len(rows), target_date)
        return len(rows)

    fetch_and_upsert()


edinet_doclist_dag()
