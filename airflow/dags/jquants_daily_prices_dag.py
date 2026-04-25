"""Pull daily prices + listed-info from JQuants for the run date and
upsert into t_daily_stock_perf. Computes marketCap via close × latest
known issued_shares from t_financials_annual (no yfinance dependency).
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
    description="JQuants listed_info + daily_quotes → t_daily_stock_perf.",
    start_date=datetime(2025, 4, 1, tzinfo=JST),
    schedule="0 17 * * 1-5",  # 17:00 JST weekdays — after market close
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
        from stock_screening.jquants.client import (
            PRIME_MARKET,
            STANDARD_MARKET,
            JQuantsClient,
        )

        run_date = data_interval_start.date()
        client = JQuantsClient(config.jquants_refresh_token())

        info = client.listed_info(run_date)
        if info.empty:
            logger.info("no listed_info for %s (likely non-trading day)", run_date)
            return 0

        info = info[info["MarketCode"].isin([PRIME_MARKET, STANDARD_MARKET])].copy()
        info["MarketCodeCleansed"] = info["Code"].str[:4]
        info = info.rename(columns={"Code": "ShokenCode"})

        quotes = client.daily_quotes(target_date=run_date)
        if quotes.empty:
            logger.info("no quotes for %s", run_date)
            return 0
        quotes = quotes.rename(columns={"Code": "ShokenCode", "Close": "close"})
        quotes_subset = quotes[["ShokenCode", "close", "Volume"]].rename(
            columns={"Volume": "volume"}
        )

        engine = db.get_engine()
        shares = pd.read_sql(
            text(
                'SELECT DISTINCT ON ("secCode") "secCode" AS ShokenCode, issued_shares '
                "FROM t_financials_annual "
                "WHERE issued_shares IS NOT NULL AND period_end <= :d "
                'ORDER BY "secCode", period_end DESC'
            ),
            engine,
            params={"d": run_date},
        )

        merged = info.merge(quotes_subset, on="ShokenCode", how="inner").merge(
            shares, on="ShokenCode", how="left"
        )
        if merged.empty:
            return 0

        merged["Date"] = run_date
        merged["marketCap"] = (
            merged["close"].astype("float64") * merged["issued_shares"].astype("float64")
        ).round().astype("Int64")

        cols = (
            "Date", "ShokenCode", "CompanyName", "CompanyNameEnglish",
            "Sector17Code", "Sector17CodeName", "Sector33Code", "Sector33CodeName",
            "ScaleCategory", "MarketCode", "MarketCodeName", "MarketCodeCleansed",
            "close", "volume", "marketCap",
        )
        for c in cols:
            if c not in merged.columns:
                merged[c] = None
        rows = merged[list(cols)].where(pd.notna(merged[list(cols)]), None).to_dict("records")

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
