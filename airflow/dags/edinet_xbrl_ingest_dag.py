"""Download + parse XBRL bundles for filings submitted on the run date,
upsert facts into t_financials, mark latest-for-period, and build/refresh
the annual mart.

Dynamic task mapping fans out the parse step across docIDs so a single
slow filing doesn't block the rest of the day's batch.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from sqlalchemy import text

# Alerting hook for the on_failure_callback (Day 1 of Signal alerts).
import sys as _sys
_sys.path.insert(0, '/opt/airflow/dags')
from _alerts import alert_on_failure  # noqa: E402

JST = pendulum.timezone("Asia/Tokyo")

logger = logging.getLogger(__name__)


@dag(
    dag_id="edinet_xbrl_ingest_dag",
    description=(
        "For filings submitted on the run date: download XBRL bundles, "
        "parse facts → t_financials, refresh latest-flag and annual mart."
    ),
    start_date=datetime(2026, 5, 1, tzinfo=JST),
    schedule="30 22 * * *",  # 30 min after doclist
    catchup=True,
    max_active_runs=4,
    default_args={
        "owner": "stock_screening",
        "on_failure_callback": alert_on_failure,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["edinet"],
)
def edinet_xbrl_ingest_dag():
    @task
    def select_pending_docs(data_interval_start=None) -> list[str]:
        """Securities reports submitted on the run date that haven't
        already been ingested into t_financials.
        """
        from stock_screening import db

        target_date = data_interval_start.date()
        engine = db.get_engine()
        sql = text(
            """
            SELECT dl."docID"
            FROM t_doc_list dl
            WHERE dl."formCode" = '030000'
              AND dl."ordinanceCode" = '010'
              AND dl."submitDateTime"::date = :d
              AND NOT EXISTS (
                  SELECT 1 FROM t_financials f WHERE f."docID" = dl."docID"
              )
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(sql, {"d": target_date}).all()
        ids = [r[0] for r in rows]
        logger.info("%d pending filings for %s", len(ids), target_date)
        return ids

    @task(max_active_tis_per_dag=4)
    def parse_and_upsert(doc_id: str) -> int:
        """Download a single bundle, parse, upsert into t_financials."""
        from stock_screening import config, db
        from stock_screening.edinet.client import download_xbrl_bundle
        from stock_screening.edinet.xbrl_parser import extract_facts_many

        engine = db.get_engine()

        with tempfile.TemporaryDirectory(prefix=f"xbrl-{doc_id}-") as tmp:
            paths = download_xbrl_bundle(doc_id, config.edinet_key(), Path(tmp))
            xbrl_files = [p for p in paths if p.suffix == ".xbrl"]
            if not xbrl_files:
                logger.warning("no .xbrl in bundle for %s", doc_id)
                return 0
            facts = extract_facts_many(doc_id, xbrl_files)

        if not facts:
            return 0

        rows = [
            {
                "docID": f.doc_id,
                "itemName": f.item_name,
                "amount": f.amount,
                "periodStart": f.period_start,
                "periodEnd": f.period_end,
                "categoryID": f.category_id,
                "concept_id": f.concept_id,
                "currency_code": f.currency_code,
            }
            for f in facts
        ]
        # The PK is (docID, itemName, periodEnd, categoryID) — migration
        # 0005 switched off periodStart when it became nullable for
        # Instant facts. scripts/backfill_window.py and
        # scripts/reparse_for_prior_year.py already use this key; this
        # DAG had drifted to the old (periodStart-based) version.
        sql = text(
            """
            INSERT INTO t_financials
                ("docID", "itemName", amount, "periodStart", "periodEnd",
                 "categoryID", concept_id, currency_code)
            VALUES (:docID, :itemName, :amount, :periodStart, :periodEnd,
                    :categoryID, :concept_id, :currency_code)
            ON CONFLICT ("docID", "itemName", "periodEnd", "categoryID")
            DO UPDATE SET
                amount = EXCLUDED.amount,
                "periodStart" = EXCLUDED."periodStart",
                concept_id = EXCLUDED.concept_id,
                currency_code = EXCLUDED.currency_code
            """
        )
        with engine.begin() as conn:
            conn.execute(sql, rows)
        return len(rows)

    @task
    def mark_latest_for_period(doc_ids: list[str]) -> int:
        """For each (secCode, periodEnd, formCode) touched by this run,
        set is_latest_for_period=TRUE on the row with the latest
        submitDateTime, FALSE on the rest.
        """
        if not doc_ids:
            return 0
        from stock_screening import db

        engine = db.get_engine()
        sql = text(
            """
            WITH affected AS (
                SELECT DISTINCT "secCode", "periodEnd", "formCode"
                FROM t_doc_list WHERE "docID" = ANY(:ids)
            ),
            ranked AS (
                SELECT dl."docID",
                       ROW_NUMBER() OVER (
                           PARTITION BY dl."secCode", dl."periodEnd", dl."formCode"
                           ORDER BY dl."submitDateTime" DESC
                       ) AS rn
                FROM t_doc_list dl
                JOIN affected a USING ("secCode", "periodEnd", "formCode")
            )
            UPDATE t_doc_list dl
            SET is_latest_for_period = (r.rn = 1)
            FROM ranked r
            WHERE dl."docID" = r."docID"
            """
        )
        with engine.begin() as conn:
            conn.execute(sql, {"ids": doc_ids})
        return len(doc_ids)

    @task
    def build_mart(doc_ids: list[str]) -> int:
        """Refresh t_financials_annual rows for the affected filings."""
        if not doc_ids:
            return 0
        from stock_screening import db
        from stock_screening.financials.build_annual_mart import build_for_docs

        engine = db.get_engine()
        n = build_for_docs(engine, doc_ids)
        logger.info("annual mart rows written: %d (from %d docs)", n, len(doc_ids))
        return n

    pending = select_pending_docs()
    parsed = parse_and_upsert.expand(doc_id=pending)
    marked = mark_latest_for_period(doc_ids=pending)
    built = build_mart(doc_ids=pending)

    parsed >> marked >> built


edinet_xbrl_ingest_dag()
