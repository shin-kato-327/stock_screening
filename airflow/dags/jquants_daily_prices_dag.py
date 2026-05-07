"""Pull daily quotes from JQuants v2 for the run date and upsert into
t_daily_stock_perf. Computes marketCap via close × latest known
issued_shares from t_financials_annual.

v2 API doesn't have a single "listed_info" endpoint, so sector / company
metadata columns on t_daily_stock_perf are populated only when EDINET
mappings supply them; the screen itself reads only close + marketCap.
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
    dag_id="jquants_daily_prices_dag",
    description="JQuants v2 daily_quotes → t_daily_stock_perf.",
    start_date=datetime(2026, 5, 1, tzinfo=JST),
    schedule="0 17 * * 1-5",
    catchup=True,
    max_active_runs=4,
    default_args={
        "owner": "stock_screening",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["jquants"],
)
def jquants_daily_prices_dag():
    @task
    def fetch_and_upsert(data_interval_start=None) -> int:
        import pandas as pd

        from stock_screening import config, db
        from stock_screening.jquants.client import JQuantsClient

        run_date = data_interval_start.date()
        client = JQuantsClient(config.jquants_api_key())

        quotes = client.daily_quotes(target_date=run_date)
        if quotes.empty:
            logger.info("no quotes for %s (likely non-trading day)", run_date)
            return 0

        # v2 uses C / Vo for close / volume; collapse 5-digit market codes
        # to 4-digit secCode (last digit is a check digit added by JPX).
        quotes = quotes.rename(columns={"Code": "ShokenCode", "C": "close", "Vo": "volume"})
        quotes["ShokenCode"] = quotes["ShokenCode"].astype(str)
        quotes_subset = quotes[["ShokenCode", "close", "volume"]].copy()

        engine = db.get_engine()
        # pd.read_sql with a SQLAlchemy text() expression is broken under
        # SA 1.4 + pandas 2.2 ("Query must be a string"). Use the
        # connection-execute-fetchall path so the dependency matrix
        # doesn't matter.
        with engine.connect() as conn:
            share_rows = conn.execute(
                text(
                    'SELECT DISTINCT ON ("secCode") "secCode" AS "ShokenCode", issued_shares '
                    "FROM t_financials_annual "
                    "WHERE issued_shares IS NOT NULL AND period_end <= :d "
                    'ORDER BY "secCode", period_end DESC'
                ),
                {"d": run_date},
            ).fetchall()
        shares = pd.DataFrame(share_rows, columns=["ShokenCode", "issued_shares"])

        merged = quotes_subset.merge(shares, on="ShokenCode", how="left")
        merged["Date"] = run_date
        merged["marketCap"] = (
            merged["close"].astype("float64") * merged["issued_shares"].astype("float64")
        ).round().astype("Int64")

        cols = ("Date", "ShokenCode", "close", "volume", "marketCap")
        # Drop rows where close or volume is NaN — JQuants returns NaN
        # for halted / non-trading issues on the date, and bigint columns
        # reject NaN. Casting to object first lets us replace NaN with
        # None reliably for the marketCap column (Int64 with <NA>).
        upsert_df = merged[list(cols)].dropna(subset=["close", "volume"])
        rows = upsert_df.astype(object).where(pd.notna(upsert_df), None).to_dict("records")

        col_list = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(f":{c}" for c in cols)
        update_set = ", ".join(
            f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in ("Date", "ShokenCode")
        )
        sql = text(
            f"INSERT INTO t_daily_stock_perf ({col_list}) VALUES ({placeholders}) "
            f'ON CONFLICT ("Date", "ShokenCode") DO UPDATE SET {update_set}'
        )
        with engine.begin() as conn:
            conn.execute(sql, rows)
        logger.info("upserted %d quotes for %s", len(rows), run_date)
        return len(rows)

    fetch_and_upsert()


jquants_daily_prices_dag()
