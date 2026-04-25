"""Net-cash ratio screen.

Formula (canonical Japanese ネットキャッシュ比率, market-cap based):
    ratio = (current_assets - interest_bearing_debt + 0.7*investment_securities)
            / market_cap
qualifies = ratio >= 1.0

Reads from t_financials_annual (typed columns; debt is the GENERATED
sum of components) joined with t_daily_stock_perf for the run date's
market_cap. For each company, picks the most recent annual filing
with period_end <= run_date.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

QUALIFY_THRESHOLD = 1.0
INVESTMENT_SECURITIES_HAIRCUT = 0.7


@dataclass
class ScreenRow:
    sec_code: str
    ratio: float
    qualifies: bool
    source_period_end: date
    market_cap: int


_SCREEN_SQL = text(
    """
    WITH latest AS (
        SELECT DISTINCT ON (fa."secCode")
            fa."secCode" AS sec_code,
            fa.period_end AS source_period_end,
            fa.current_assets,
            fa.interest_bearing_debt,
            fa.investment_securities
        FROM t_financials_annual fa
        WHERE fa.period_end <= :run_date
        ORDER BY fa."secCode", fa.period_end DESC
    )
    SELECT
        l.sec_code,
        l.source_period_end,
        l.current_assets,
        l.interest_bearing_debt,
        l.investment_securities,
        d."marketCap" AS market_cap
    FROM latest l
    JOIN t_daily_stock_perf d
      ON d."ShokenCode" = l.sec_code
     AND d."Date" = :run_date
    WHERE d."marketCap" IS NOT NULL
      AND d."marketCap" > 0
    """
)


def compute_screen(engine: Engine, run_date: date) -> pd.DataFrame:
    """Run the screen for `run_date`, return one row per scoreable
    company with the ratio and qualifies flag.
    """
    with engine.connect() as conn:
        df = pd.read_sql(_SCREEN_SQL, conn, params={"run_date": run_date})

    if df.empty:
        return df.assign(ratio=pd.Series(dtype=float), qualifies=pd.Series(dtype=bool))

    df["ratio"] = (
        df["current_assets"].fillna(0)
        - df["interest_bearing_debt"].fillna(0)
        + INVESTMENT_SECURITIES_HAIRCUT * df["investment_securities"].fillna(0)
    ) / df["market_cap"]

    df["qualifies"] = df["ratio"] >= QUALIFY_THRESHOLD
    return df


def persist_screen_results(engine: Engine, run_date: date, df: pd.DataFrame) -> int:
    """Upsert today's screen into t_screen_results (PK run_date, secCode).
    Returns row count written.
    """
    if df.empty:
        return 0
    rows = [
        {
            "run_date": run_date,
            "secCode": r["sec_code"],
            "ratio": float(r["ratio"]),
            "qualifies": bool(r["qualifies"]),
            "source_period_end": r["source_period_end"],
            "market_cap": int(r["market_cap"]),
        }
        for _, r in df.iterrows()
    ]
    sql = text(
        """
        INSERT INTO t_screen_results
            (run_date, "secCode", ratio, qualifies, source_period_end, market_cap)
        VALUES (:run_date, :secCode, :ratio, :qualifies, :source_period_end, :market_cap)
        ON CONFLICT (run_date, "secCode") DO UPDATE SET
            ratio = EXCLUDED.ratio,
            qualifies = EXCLUDED.qualifies,
            source_period_end = EXCLUDED.source_period_end,
            market_cap = EXCLUDED.market_cap,
            computed_at = now()
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)
