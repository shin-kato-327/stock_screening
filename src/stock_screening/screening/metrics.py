"""Net-cash ratio screen.

Formula (market-cap based, conservative form):
    ratio = (current_assets - total_liabilities + 0.7*investment_securities)
            / market_cap
qualifies = ratio >= 1.0

NOTE on liabilities: we use `total_liabilities` (負債合計) rather than
`interest_bearing_debt` (有利子負債) — even though the canonical
ネットキャッシュ比率 textbook uses interest-bearing debt — because
empirical quintile analysis on our data and a 1,800-stock 3-year
study (https://zenn.dev/morim34/articles/ff991f32187d96) both show
that total_liabilities produces a cleaner monotonic relationship
between ratio and forward return. The interest-bearing-debt form has
a Q5-collapse artifact: companies with no bank debt but huge trade
payables / accruals (operating distress) score artificially high.

The mart still stores both — interest_bearing_debt remains as a
GENERATED column from short_term_borrowings + long_term_borrowings +
bonds + lease_obligations — so it's available for diagnostic queries
and alternative strategies. The screen here uses total_liabilities.

Reads from t_financials_annual joined with t_daily_stock_perf for
the run date's market_cap. For each company, picks the most recent
annual filing with period_end <= run_date.
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
            fa.total_liabilities,
            fa.investment_securities
        FROM t_financials_annual fa
        WHERE fa.period_end <= :run_date
        ORDER BY fa."secCode", fa.period_end DESC
    )
    SELECT
        l.sec_code,
        l.source_period_end,
        l.current_assets,
        l.total_liabilities,
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
        rows = conn.execute(_SCREEN_SQL, {"run_date": run_date}).fetchall()

    cols = [
        "sec_code", "source_period_end", "current_assets",
        "total_liabilities", "investment_securities", "market_cap",
    ]
    df = pd.DataFrame(rows, columns=cols)

    if df.empty:
        return df.assign(ratio=pd.Series(dtype=float), qualifies=pd.Series(dtype=bool))

    # SA1.4 returns Decimal for NUMERIC columns; cast to float for arithmetic.
    for col in ("current_assets", "total_liabilities", "investment_securities", "market_cap"):
        df[col] = df[col].astype(float)

    df["ratio"] = (
        df["current_assets"].fillna(0)
        - df["total_liabilities"].fillna(0)
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
