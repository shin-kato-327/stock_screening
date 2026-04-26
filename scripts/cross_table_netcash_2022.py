"""3-year per-stock holding study, entered at end of 2022.

Each stock with a high net-cash ratio at entry is "bought" at its
2022-12-30 close and "sold" when EITHER:
  (a) the daily-recomputed net-cash ratio falls below 1.0, OR
  (b) the 3-year holding window ends (2025-12-30).

The ratio re-evaluates daily because:
  - market_cap shifts every trading day → denominator changes
  - new annual reports may arrive during the holding window →
    numerator changes (we use the latest filing visible as-of
    each day, with submitDateTime <= that day)

Output: a cross-table by (initial_net_cash_ratio bucket × initial
market_cap bucket) showing count, mean return, median return.

This is NOT a portfolio simulation — no initial capital, no shares,
no commissions, no equal-weight. We compute one price-return number
per stock and aggregate.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stock_screening import db

ENTRY_DATE = date(2022, 12, 30)
EXIT_DEADLINE = date(2025, 12, 30)  # 3 years from entry

MIN_MARKET_CAP = 15_000_000_000   # ¥15B
ENTRY_THRESHOLD = 1.5              # tighter qualifying bar (was 1.0)
EXIT_THRESHOLD = 1.0               # still hold until full re-rating
INVESTMENT_SECURITIES_HAIRCUT = 0.7

# Finer ratio buckets within the new universe.
RATIO_BUCKETS = [
    ("1.5–1.75",  1.5,  1.75),
    ("1.75–2.0",  1.75, 2.0),
    ("2.0–2.5",   2.0,  2.5),
    ("2.5–3.0",   2.5,  3.0),
    ("3.0+",      3.0,  float("inf")),
]

# Market cap buckets in JPY
MC_BUCKETS = [
    ("¥15-30B",   15e9,  30e9),
    ("¥30-100B",  30e9,  100e9),
    ("¥100-500B", 100e9, 500e9),
    ("¥500B+",    500e9, float("inf")),
]


def bucket(value: float, buckets: list[tuple[str, float, float]]) -> str:
    for label, lo, hi in buckets:
        if lo <= value < hi:
            return label
    return "out-of-range"


def main():
    engine = db.get_engine()

    # Step 1: identify the universe at ENTRY_DATE.
    # Latest mart row per company that was visible (filed) by ENTRY_DATE,
    # joined to that day's price + market cap.
    print(f"Building entry universe as of {ENTRY_DATE}...")
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH visible AS (
                    SELECT fa."secCode", fa.period_end,
                           fa.current_assets, fa.total_liabilities, fa.investment_securities,
                           fa.source_doc_id
                    FROM t_financials_annual fa
                    JOIN t_doc_list dl ON dl."docID" = fa.source_doc_id
                    WHERE dl."submitDateTime"::date <= :entry
                      AND fa.current_assets IS NOT NULL
                      AND fa.total_liabilities IS NOT NULL
                ),
                latest AS (
                    SELECT DISTINCT ON ("secCode") *
                    FROM visible ORDER BY "secCode", period_end DESC
                )
                SELECT l."secCode", l.current_assets, l.total_liabilities,
                       l.investment_securities, l.period_end,
                       d.adj_close AS entry_price,
                       d."marketCap" AS entry_mc
                FROM latest l
                JOIN t_daily_stock_perf d
                  ON d."ShokenCode" = l."secCode" AND d."Date" = :entry
                WHERE d.adj_close IS NOT NULL
                  AND d."marketCap" IS NOT NULL
                  AND d."marketCap" >= :min_mc
                """
            ),
            {"entry": ENTRY_DATE, "min_mc": MIN_MARKET_CAP},
        ).fetchall()

    universe = pd.DataFrame(
        rows,
        columns=[
            "sec_code", "current_assets", "total_liabilities", "investment_securities",
            "period_end", "entry_price", "entry_mc",
        ],
    )
    for c in ("current_assets", "total_liabilities", "investment_securities", "entry_price", "entry_mc"):
        universe[c] = universe[c].astype(float)

    universe["net_cash_numerator"] = (
        universe["current_assets"]
        - universe["total_liabilities"]
        + INVESTMENT_SECURITIES_HAIRCUT * universe["investment_securities"].fillna(0)
    )
    universe["entry_ratio"] = universe["net_cash_numerator"] / universe["entry_mc"]

    # Filter to "high net-cash" at entry
    qualifying = universe[universe["entry_ratio"] >= ENTRY_THRESHOLD].reset_index(drop=True)
    print(f"  {len(universe)} stocks ≥ ¥15B mc with mart data at entry")
    print(f"  {len(qualifying)} of those have entry net-cash ratio ≥ {ENTRY_THRESHOLD}")

    # Step 2: for each qualifying stock, walk daily prices and find exit.
    # Exit = first day ratio < 1.0 (using latest visible mart numerator
    # divided by that day's market cap), else EXIT_DEADLINE.
    print(f"\nWalking forward to {EXIT_DEADLINE} for each stock...")

    sec_codes = qualifying["sec_code"].tolist()

    # Pre-fetch all the daily data for these stocks in the holding window.
    with engine.connect() as conn:
        price_rows = conn.execute(
            text(
                'SELECT "ShokenCode" AS sec_code, "Date", adj_close, "marketCap" '
                "FROM t_daily_stock_perf "
                'WHERE "ShokenCode" = ANY(:codes) '
                'AND "Date" BETWEEN :start AND :end '
                "AND adj_close IS NOT NULL AND \"marketCap\" IS NOT NULL "
                'ORDER BY "ShokenCode", "Date"'
            ),
            {"codes": sec_codes, "start": ENTRY_DATE, "end": EXIT_DEADLINE},
        ).fetchall()
    prices = pd.DataFrame(price_rows, columns=["sec_code", "date", "adj_close", "market_cap"])
    prices["adj_close"] = prices["adj_close"].astype(float)
    prices["market_cap"] = prices["market_cap"].astype(float)

    # Pre-fetch all visible mart rows that could become "latest" during the
    # holding window — i.e. filings submitted between ENTRY_DATE and
    # EXIT_DEADLINE (older ones are already in the entry universe).
    with engine.connect() as conn:
        mart_rows = conn.execute(
            text(
                """
                SELECT fa."secCode" AS sec_code,
                       dl."submitDateTime"::date AS submit_date,
                       fa.current_assets, fa.total_liabilities, fa.investment_securities,
                       fa.period_end
                FROM t_financials_annual fa
                JOIN t_doc_list dl ON dl."docID" = fa.source_doc_id
                WHERE fa."secCode" = ANY(:codes)
                  AND dl."submitDateTime"::date BETWEEN :start AND :end
                  AND fa.current_assets IS NOT NULL
                  AND fa.total_liabilities IS NOT NULL
                ORDER BY fa."secCode", dl."submitDateTime"
                """
            ),
            {"codes": sec_codes, "start": ENTRY_DATE, "end": EXIT_DEADLINE},
        ).fetchall()
    new_filings = pd.DataFrame(
        mart_rows,
        columns=["sec_code", "submit_date", "current_assets", "total_liabilities",
                 "investment_securities", "period_end"],
    )
    for c in ("current_assets", "total_liabilities", "investment_securities"):
        new_filings[c] = new_filings[c].astype(float)

    # Compute per-stock exit
    results = []
    initial_state = qualifying.set_index("sec_code")[
        ["entry_price", "entry_mc", "entry_ratio", "current_assets",
         "total_liabilities", "investment_securities", "net_cash_numerator"]
    ]

    new_filings_by_stock = {
        code: g.sort_values("submit_date") for code, g in new_filings.groupby("sec_code")
    }
    prices_by_stock = {code: g.sort_values("date") for code, g in prices.groupby("sec_code")}

    for sec_code, init in initial_state.iterrows():
        stock_prices = prices_by_stock.get(sec_code)
        if stock_prices is None or stock_prices.empty:
            continue

        numerator = init["net_cash_numerator"]
        stock_filings = new_filings_by_stock.get(sec_code, pd.DataFrame())

        exit_date = None
        exit_price = None
        exit_reason = "deadline"

        for _, row in stock_prices.iterrows():
            d = row["date"]
            mc = row["market_cap"]
            adj = row["adj_close"]

            # Refresh numerator if a new filing has landed by this date
            if not stock_filings.empty:
                applicable = stock_filings[stock_filings["submit_date"] <= d]
                if not applicable.empty:
                    latest = applicable.iloc[-1]
                    numerator = (
                        latest["current_assets"]
                        - latest["total_liabilities"]
                        + INVESTMENT_SECURITIES_HAIRCUT * (latest["investment_securities"] or 0)
                    )

            ratio = numerator / mc if mc > 0 else 0
            if d >= ENTRY_DATE and ratio < EXIT_THRESHOLD:
                exit_date = d
                exit_price = adj
                exit_reason = "ratio_below_exit"
                break

        if exit_date is None:
            last = stock_prices.iloc[-1]
            exit_date = last["date"]
            exit_price = last["adj_close"]

        period_return = (exit_price / init["entry_price"] - 1) * 100
        days_held = (exit_date - ENTRY_DATE).days
        results.append({
            "sec_code": sec_code,
            "entry_ratio": init["entry_ratio"],
            "entry_mc": init["entry_mc"],
            "entry_price": init["entry_price"],
            "exit_date": exit_date,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "days_held": days_held,
            "return_pct": period_return,
        })

    df = pd.DataFrame(results)
    print(f"  {len(df)} stocks tracked")
    print(f"  exit reasons: {df['exit_reason'].value_counts().to_dict()}")

    # Step 3: bucket and cross-table
    df["ratio_bucket"] = df["entry_ratio"].apply(lambda r: bucket(r, RATIO_BUCKETS))
    df["mc_bucket"] = df["entry_mc"].apply(lambda m: bucket(m, MC_BUCKETS))

    print("\n=== Count of stocks by (ratio × mc) bucket ===")
    pivot_count = df.pivot_table(
        index="ratio_bucket",
        columns="mc_bucket",
        values="return_pct",
        aggfunc="count",
        fill_value=0,
    ).reindex(
        index=[b[0] for b in RATIO_BUCKETS],
        columns=[b[0] for b in MC_BUCKETS],
        fill_value=0,
    )
    print(pivot_count.to_string())

    print("\n=== Mean 3-year return % by (ratio × mc) bucket ===")
    pivot_mean = df.pivot_table(
        index="ratio_bucket",
        columns="mc_bucket",
        values="return_pct",
        aggfunc="mean",
    ).reindex(
        index=[b[0] for b in RATIO_BUCKETS],
        columns=[b[0] for b in MC_BUCKETS],
    )
    print(pivot_mean.round(1).to_string())

    print("\n=== Median 3-year return % by (ratio × mc) bucket ===")
    pivot_med = df.pivot_table(
        index="ratio_bucket",
        columns="mc_bucket",
        values="return_pct",
        aggfunc="median",
    ).reindex(
        index=[b[0] for b in RATIO_BUCKETS],
        columns=[b[0] for b in MC_BUCKETS],
    )
    print(pivot_med.round(1).to_string())

    print("\n=== Marginal totals (across all market caps) ===")
    marg = df.groupby("ratio_bucket").agg(
        n=("return_pct", "count"),
        mean_ret=("return_pct", "mean"),
        median_ret=("return_pct", "median"),
    ).reindex([b[0] for b in RATIO_BUCKETS])
    print(marg.round(1).to_string())

    # Benchmark comparison
    with engine.connect() as conn:
        bench_rows = conn.execute(
            text(
                'SELECT "Date", adj_close FROM t_daily_stock_perf '
                'WHERE "ShokenCode" = \'13060\' '
                'AND "Date" IN (:entry, :exit_)'
            ),
            {"entry": ENTRY_DATE, "exit_": EXIT_DEADLINE},
        ).fetchall()
    bench = {r[0]: float(r[1]) for r in bench_rows}
    if ENTRY_DATE in bench and EXIT_DEADLINE in bench:
        bench_ret = (bench[EXIT_DEADLINE] / bench[ENTRY_DATE] - 1) * 100
        print(f"\nTOPIX (1306) 3-year return: {bench_ret:+.1f}%   "
              f"({ENTRY_DATE}: {bench[ENTRY_DATE]:.2f} → {EXIT_DEADLINE}: {bench[EXIT_DEADLINE]:.2f})")


if __name__ == "__main__":
    main()
