"""Net-cash-plus-quality screen.

A stock qualifies if all three are true:

1. **Net-cash ratio ≥ 1.0** — `(current_assets − total_liabilities +
   0.7 × investment_securities) / market_cap`. Conservative form of the
   ネットキャッシュ比率: uses total liabilities (負債合計) rather than
   interest-bearing debt (有利子負債). Empirically — see quintile
   analysis on our data and a 1,800-stock 3-year study at
   https://zenn.dev/morim34/articles/ff991f32187d96 — total_liabilities
   produces a cleaner monotonic relationship between ratio and forward
   return. The interest-bearing-debt form has a Q5-collapse artifact:
   companies with no bank debt but huge trade payables / accruals
   (operating distress) score artificially high.

2. **Operating-income yield > 5%** — `operating_income / market_cap`.
   Catches value traps with structurally net cash but stagnant or
   declining operations (e.g. companies whose earnings power is a tiny
   fraction of their cash hoard). Threshold tuned from the 2022-cohort
   3-year analysis: > 5% catches 3 of 5 worst-trap cases at the cost
   of only 1 turnaround winner.

3. **6-month price momentum > 0%** — adj-close change vs the closest
   trading day ~180 days before run_date. Adds a "market is starting
   to wake up to this" check. Modest improvement over net-cash alone.

The mart still stores `interest_bearing_debt` as a GENERATED column;
it's diagnostic, not used by this screen.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

QUALIFY_THRESHOLD = 1.0
INVESTMENT_SECURITIES_HAIRCUT = 0.7
OP_YIELD_THRESHOLD = 0.05            # operating_income / market_cap > 5%
MOMENTUM_THRESHOLD = 0.0             # 6-month price return > 0%
MOMENTUM_LOOKBACK_DAYS = 180


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
            fa.investment_securities,
            fa.operating_income
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
        l.operating_income,
        d."marketCap" AS market_cap,
        d.adj_close AS close_now,
        prior.adj_close AS close_180d_ago
    FROM latest l
    JOIN t_daily_stock_perf d
      ON d."ShokenCode" = l.sec_code
     AND d."Date" = :run_date
    LEFT JOIN LATERAL (
        SELECT adj_close
        FROM t_daily_stock_perf p
        WHERE p."ShokenCode" = l.sec_code
          AND p."Date" BETWEEN :lookback_start AND :lookback_end
          AND p.adj_close IS NOT NULL
        ORDER BY p."Date" DESC LIMIT 1
    ) prior ON TRUE
    WHERE d."marketCap" IS NOT NULL
      AND d."marketCap" > 0
    """
)


def compute_screen(engine: Engine, run_date: date) -> pd.DataFrame:
    """Run the screen for `run_date`. Returns one row per scoreable
    company with all three filter components computed and a combined
    `qualifies` flag.
    """
    params = {
        "run_date": run_date,
        "lookback_start": run_date - timedelta(days=MOMENTUM_LOOKBACK_DAYS + 20),
        "lookback_end": run_date - timedelta(days=MOMENTUM_LOOKBACK_DAYS - 20),
    }
    with engine.connect() as conn:
        rows = conn.execute(_SCREEN_SQL, params).fetchall()

    cols = [
        "sec_code", "source_period_end", "current_assets",
        "total_liabilities", "investment_securities", "operating_income",
        "market_cap", "close_now", "close_180d_ago",
    ]
    df = pd.DataFrame(rows, columns=cols)

    if df.empty:
        return df.assign(
            ratio=pd.Series(dtype=float),
            op_yield=pd.Series(dtype=float),
            momentum_6m=pd.Series(dtype=float),
            qualifies=pd.Series(dtype=bool),
        )

    # SA1.4 returns Decimal for NUMERIC columns; cast for arithmetic.
    for col in (
        "current_assets", "total_liabilities", "investment_securities",
        "operating_income", "market_cap", "close_now", "close_180d_ago",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["ratio"] = (
        df["current_assets"].fillna(0)
        - df["total_liabilities"].fillna(0)
        + INVESTMENT_SECURITIES_HAIRCUT * df["investment_securities"].fillna(0)
    ) / df["market_cap"]

    df["op_yield"] = df["operating_income"] / df["market_cap"]
    df["momentum_6m"] = (df["close_now"] / df["close_180d_ago"]) - 1

    df["qualifies"] = (
        (df["ratio"] >= QUALIFY_THRESHOLD)
        & (df["op_yield"] > OP_YIELD_THRESHOLD)
        & (df["momentum_6m"] > MOMENTUM_THRESHOLD)
    ).fillna(False)

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
