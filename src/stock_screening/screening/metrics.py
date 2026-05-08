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


# --- Turnaround signal --------------------------------------------------
#
# A turnaround candidate is a stock whose operations are *recovering* —
# distinct from a value trap (cheap and stagnant). Three companion
# measurements off the multi-year mart:
#
#   op_income_yoy  = (op_income_now - op_income_prev) / |op_income_prev|
#   sales_yoy      = (net_sales_now - net_sales_prev) / net_sales_prev
#   margin_change  = (op_income_now / net_sales_now)
#                  - (op_income_prev / net_sales_prev)
#
# Combined judgment:
#   - op_income_yoy > 0.20 AND sales_yoy > 0:  real recovery
#   - op_income_yoy > 0.20 AND sales_yoy <= 0: cost-cut driven (often fragile)
#   - op_income_yoy <= 0:                       not turning around
#
# Returns NaN for companies with fewer than 2 visible filings — common
# for our pre-2024 backtests since financials backfill starts at 2022-04-25
# (most TSE companies have only one filing visible until 2024 when their
# FY2023 reports land).

_TURNAROUND_SQL = text(
    """
    WITH visible AS (
        SELECT fa."secCode", fa.period_end,
               fa.operating_income, fa.net_sales
        FROM t_financials_annual fa
        WHERE fa.period_end <= :run_date
    ),
    ranked AS (
        SELECT "secCode", period_end, operating_income, net_sales,
               LAG(operating_income) OVER w AS op_income_prev,
               LAG(net_sales) OVER w AS net_sales_prev,
               LAG(period_end) OVER w AS period_end_prev
        FROM visible
        WINDOW w AS (PARTITION BY "secCode" ORDER BY period_end)
    )
    SELECT DISTINCT ON ("secCode")
        "secCode" AS sec_code, period_end, period_end_prev,
        operating_income, op_income_prev,
        net_sales, net_sales_prev
    FROM ranked
    WHERE op_income_prev IS NOT NULL OR net_sales_prev IS NOT NULL
    ORDER BY "secCode", period_end DESC
    """
)


def compute_turnaround_score(engine: Engine, run_date: date) -> pd.DataFrame:
    """For each company with ≥2 visible annual filings by run_date,
    return YoY changes in op income, sales, and operating margin.
    Companies with insufficient history are absent from the output.
    """
    with engine.connect() as conn:
        rows = conn.execute(_TURNAROUND_SQL, {"run_date": run_date}).fetchall()

    cols = [
        "sec_code", "period_end", "period_end_prev",
        "operating_income", "op_income_prev",
        "net_sales", "net_sales_prev",
    ]
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df.assign(
            op_income_yoy=pd.Series(dtype=float),
            sales_yoy=pd.Series(dtype=float),
            margin_change=pd.Series(dtype=float),
        )

    for c in ("operating_income", "op_income_prev", "net_sales", "net_sales_prev"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["op_income_yoy"] = (df["operating_income"] - df["op_income_prev"]) / df[
        "op_income_prev"
    ].abs()
    df["sales_yoy"] = (df["net_sales"] - df["net_sales_prev"]) / df["net_sales_prev"]

    margin_now = df["operating_income"] / df["net_sales"]
    margin_prev = df["op_income_prev"] / df["net_sales_prev"]
    df["margin_change"] = margin_now - margin_prev

    return df


# --- Strict strategy cohort (per docs/STRATEGY.md) ----------------------
#
# The looser `compute_screen()` above is a Q-flag-shaped pre-filter
# (ratio>=1.0 + op_yield>5% + mom_6m>0). The strategy proper layers on
# tighter cuts: a small-cap band, a higher ratio threshold, a PER cap,
# and the validated lowest-float-33% overlay. This function returns the
# final cohort the daily report and trade-plan generator should use.

# Strategy parameters from STRATEGY.md (kept in lock-step).
STRICT_CAP_MIN = 3_000_000_000
STRICT_CAP_MAX = 30_000_000_000
STRICT_RATIO_THRESHOLD = 1.5
STRICT_PER_MAX = 10.0
STRICT_FLOAT_PCT = 0.33


def compute_strict_cohort(engine: Engine, run_date: date) -> pd.DataFrame:
    """Return the strict strategy cohort for run_date.

    Columns: sec_code, name, ratio, per, market_cap, issued_shares,
             rank_in_cohort (1-N by float).
    The result is the lowest-float STRICT_FLOAT_PCT subset of the
    sweet-spot universe (cap STRICT_CAP_MIN..MAX + ratio>STRICT_RATIO
    + 0<PER<=STRICT_PER_MAX), sorted by issued_shares ascending.
    """
    sql = text(
        """
        WITH latest AS (
            SELECT DISTINCT ON (fa."secCode")
                fa."secCode" AS sec_code,
                fa.current_assets,
                fa.total_liabilities,
                fa.investment_securities,
                fa.net_income,
                fa.issued_shares
            FROM t_financials_annual fa
            JOIN t_doc_list dl ON dl."docID" = fa.source_doc_id
            WHERE dl."submitDateTime"::date <= :run_date
              AND fa.current_assets IS NOT NULL
              AND fa.issued_shares IS NOT NULL
            ORDER BY fa."secCode", fa.period_end DESC
        )
        SELECT
            l.sec_code,
            l.current_assets,
            l.total_liabilities,
            l.investment_securities,
            l.net_income,
            l.issued_shares,
            d."marketCap" AS market_cap,
            (
                SELECT "filerName" FROM t_doc_list
                WHERE "secCode" = l.sec_code
                ORDER BY "submitDateTime" DESC LIMIT 1
            ) AS name
        FROM latest l
        JOIN t_daily_stock_perf d
          ON d."ShokenCode" = l.sec_code
         AND d."Date" = :run_date
         AND d."marketCap" IS NOT NULL
         AND d."marketCap" > 0
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"run_date": run_date}).fetchall()

    cols = [
        "sec_code", "current_assets", "total_liabilities",
        "investment_securities", "net_income", "issued_shares",
        "market_cap", "name",
    ]
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df.assign(ratio=pd.Series(dtype=float), per=pd.Series(dtype=float))

    for c in (
        "current_assets", "total_liabilities", "investment_securities",
        "net_income", "issued_shares", "market_cap",
    ):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["ratio"] = (
        df["current_assets"].fillna(0)
        - df["total_liabilities"].fillna(0)
        + INVESTMENT_SECURITIES_HAIRCUT * df["investment_securities"].fillna(0)
    ) / df["market_cap"]
    df["per"] = df["market_cap"] / df["net_income"]

    sweet = df[
        df["market_cap"].between(STRICT_CAP_MIN, STRICT_CAP_MAX)
        & (df["ratio"] > STRICT_RATIO_THRESHOLD)
        & (df["per"] > 0)
        & (df["per"] <= STRICT_PER_MAX)
    ].copy()

    if sweet.empty:
        return sweet

    sweet = sweet.sort_values("issued_shares").reset_index(drop=True)
    n_picks = max(1, int(len(sweet) * STRICT_FLOAT_PCT))
    cohort = sweet.head(n_picks).copy()
    cohort["rank_in_cohort"] = range(1, len(cohort) + 1)
    return cohort[
        ["sec_code", "name", "ratio", "per", "market_cap",
         "issued_shares", "rank_in_cohort"]
    ]
