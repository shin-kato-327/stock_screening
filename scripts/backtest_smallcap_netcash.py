"""One-off backtest: net-cash-ratio screen filtered to mid-/small-cap
companies (¥30B–¥60B ≈ $200M–$400M), $100k initial capital, equal-
weight top-N daily rebalance with strict-improvement swap rule.

Differs from the production sim in two ways:

1. Adds a marketCap filter on top of the screen — only names in the
   target range are eligible.
2. Filters mart rows by `t_doc_list.submitDateTime <= run_date`, not
   the production screen's `period_end <= run_date`. This is the
   look-ahead fix the plan documented as a follow-up — necessary for
   any honest backtest. (The production screen still uses period_end,
   which is fine for live trading where filings are known once
   submitted; it just produces optimistic backtests.)

Bypasses the STRATEGIES registry and writes nothing to the sim
tables — output is to stdout only. Run it as exploratory analysis,
then promote to a registered strategy if it looks promising.

Usage: scripts/backtest_smallcap_netcash.py 2025-04-25 2026-04-24
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stock_screening import db
from stock_screening.simulation import portfolio
from stock_screening.simulation.rebalance import (
    compute_target_shares,
    generate_trades,
    select_target_names,
)

# --- backtest parameters -------------------------------------------------

INITIAL_CAPITAL = 15_000_000      # ¥15M ≈ $100k @ 150 JPY/USD
MIN_MARKET_CAP = 30_000_000_000   # ¥30B ≈ $200M
MAX_MARKET_CAP = 60_000_000_000   # ¥60B ≈ $400M
MIN_VOLUME = 1_000                # daily volume floor; skips dead tickers
MAX_POSITIONS = 10                # narrower than 20 since the cap band is tight
TRANSACTION_COST_BPS = 10
BENCHMARK_TICKER = "13060"   # 1306 + JPX check digit, as stored by JQuants v2
QUALIFY_THRESHOLD = 1.0
INVESTMENT_SECURITIES_HAIRCUT = 0.7

# --- screen with market-cap filter and look-ahead fix --------------------

_SCREEN_SQL = text(
    """
    WITH visible AS (
        SELECT fa."secCode" AS sec_code,
               fa.period_end,
               fa.current_assets,
               fa.total_liabilities,
               fa.investment_securities
        FROM t_financials_annual fa
        JOIN t_doc_list dl ON dl."docID" = fa.source_doc_id
        WHERE dl."submitDateTime"::date <= :run_date
    ),
    latest AS (
        SELECT DISTINCT ON (sec_code) sec_code, period_end,
               current_assets, total_liabilities, investment_securities
        FROM visible
        ORDER BY sec_code, period_end DESC
    )
    SELECT l.sec_code,
           l.current_assets, l.total_liabilities, l.investment_securities,
           d."marketCap" AS market_cap,
           d.adj_close,
           d.volume
    FROM latest l
    JOIN t_daily_stock_perf d
      ON d."ShokenCode" = l.sec_code
     AND d."Date" = :run_date
    WHERE d."marketCap" BETWEEN :min_mc AND :max_mc
      AND COALESCE(d.volume, 0) >= :min_vol
      AND d.adj_close IS NOT NULL
    """
)


def screen(engine, run_date: date) -> pd.DataFrame:
    with engine.connect() as conn:
        rows = conn.execute(
            _SCREEN_SQL,
            {
                "run_date": run_date,
                "min_mc": MIN_MARKET_CAP,
                "max_mc": MAX_MARKET_CAP,
                "min_vol": MIN_VOLUME,
            },
        ).fetchall()
    cols = [
        "sec_code", "current_assets", "total_liabilities",
        "investment_securities", "market_cap", "adj_close", "volume",
    ]
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df.assign(ratio=pd.Series(dtype=float), qualifies=pd.Series(dtype=bool))
    for c in ("current_assets", "total_liabilities", "investment_securities", "market_cap", "adj_close"):
        df[c] = df[c].astype(float)
    df["ratio"] = (
        df["current_assets"].fillna(0)
        - df["total_liabilities"].fillna(0)
        + INVESTMENT_SECURITIES_HAIRCUT * df["investment_securities"].fillna(0)
    ) / df["market_cap"]
    df["qualifies"] = df["ratio"] >= QUALIFY_THRESHOLD
    return df


# --- benchmark -----------------------------------------------------------

def fetch_benchmark_series(engine, ticker: str, start: date, end: date) -> dict[date, float]:
    """Use adj_close — handles splits / consolidations correctly."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                'SELECT "Date", adj_close FROM t_daily_stock_perf '
                'WHERE "ShokenCode" = :c AND "Date" BETWEEN :s AND :e AND adj_close IS NOT NULL '
                'ORDER BY "Date"'
            ),
            {"c": ticker, "s": start, "e": end},
        ).fetchall()
    return {r[0]: float(r[1]) for r in rows}


def fetch_adj_prices(engine, run_date: date, sec_codes: list[str]) -> dict[str, float]:
    """Adj-close lookup for any names — strategy MTM and trade execution."""
    if not sec_codes:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                'SELECT "ShokenCode", adj_close FROM t_daily_stock_perf '
                'WHERE "Date" = :d AND "ShokenCode" = ANY(:codes) AND adj_close IS NOT NULL'
            ),
            {"d": run_date, "codes": sec_codes},
        ).fetchall()
    return {r[0]: float(r[1]) for r in rows}


# --- backtest loop -------------------------------------------------------

def main():
    if len(sys.argv) != 3:
        print("usage: backtest_smallcap_netcash.py START END", file=sys.stderr)
        sys.exit(1)

    start = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    end = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
    engine = db.get_engine()

    bench_prices = fetch_benchmark_series(engine, BENCHMARK_TICKER, start, end)
    print(f"benchmark ({BENCHMARK_TICKER}) trading days: {len(bench_prices)}")

    # In-memory portfolio state.
    cash = float(INITIAL_CAPITAL)
    positions = pd.DataFrame(
        columns=["secCode", "shares", "avg_cost", "last_price", "market_value"]
    )
    sim_start_date: date | None = None
    nav_history: list[tuple[date, float, float | None, int, int]] = []
    all_trades = 0
    total_commission = 0.0

    d = start
    while d <= end:
        # Skip weekends — JQuants returns no quotes
        if d.weekday() >= 5 or d not in bench_prices:
            d += timedelta(days=1)
            continue

        df = screen(engine, d)
        qual = df[df["qualifies"]].sort_values("ratio", ascending=False).reset_index(drop=True)

        # Bootstrap on first trading day with any qualifying name.
        if sim_start_date is None:
            if qual.empty:
                d += timedelta(days=1)
                continue
            sim_start_date = d

        # Pull split-adjusted prices for held + qualifying names.
        names = sorted(set(qual["sec_code"]) | set(positions["secCode"]))
        prices = fetch_adj_prices(engine, d, names) if names else {}

        marked = portfolio.mark_to_market(positions, prices) if not positions.empty else positions
        nav_pre = cash + (float(marked["market_value"].sum()) if not marked.empty else 0.0)

        target_names = select_target_names(
            qual,
            list(marked["secCode"]) if not marked.empty else [],
            MAX_POSITIONS,
            swap_rule="strict_improvement",
        )
        target_shares = compute_target_shares(
            target_names, prices, nav_pre, MAX_POSITIONS, weighting="equal"
        )
        current_shares = (
            {row.secCode: int(row.shares) for row in marked.itertuples()}
            if not marked.empty
            else {}
        )
        trades = generate_trades(current_shares, target_shares, prices, TRANSACTION_COST_BPS)
        positions, cash = portfolio.apply_trades(marked, trades, cash)

        all_trades += len(trades)
        total_commission += sum(t.commission for t in trades)

        nav = cash + (float(positions["market_value"].sum()) if not positions.empty else 0.0)
        bench_nav = INITIAL_CAPITAL * (
            bench_prices[d] / bench_prices[sim_start_date]
        )
        nav_history.append((d, nav, bench_nav, len(positions), len(qual)))

        d += timedelta(days=1)

    # --- report ----------------------------------------------------------

    if not nav_history:
        print("no qualifying names in window — nothing simulated")
        return

    print(f"\nSim period: {sim_start_date} → {nav_history[-1][0]}  "
          f"({len(nav_history)} trading days)")
    print(f"Initial capital: ¥{INITIAL_CAPITAL:,}")

    df = pd.DataFrame(nav_history, columns=["date", "nav", "bench_nav", "n_pos", "n_qual"])
    final_nav = df["nav"].iloc[-1]
    final_bench = df["bench_nav"].iloc[-1]
    sim_ret = (final_nav / INITIAL_CAPITAL - 1) * 100
    bench_ret = (final_bench / INITIAL_CAPITAL - 1) * 100

    # Max drawdown
    running_max = df["nav"].cummax()
    dd = (df["nav"] / running_max - 1) * 100
    max_dd = dd.min()

    bench_max = df["bench_nav"].cummax()
    bench_dd = (df["bench_nav"] / bench_max - 1) * 100
    bench_max_dd = bench_dd.min()

    print(f"\nFinal NAV:        ¥{final_nav:>15,.0f}    ({sim_ret:+.2f}%)")
    print(f"Benchmark NAV:    ¥{final_bench:>15,.0f}    ({bench_ret:+.2f}%)")
    print(f"Excess return:                            {sim_ret - bench_ret:+.2f}%")
    print(f"Max drawdown:     sim {max_dd:.2f}%  vs  benchmark {bench_max_dd:.2f}%")
    print(f"Trades:           {all_trades}")
    print(f"Total commission: ¥{total_commission:,.0f}  ({total_commission/INITIAL_CAPITAL*100:.2f}% of initial)")
    print(f"Avg positions:    {df['n_pos'].mean():.1f}")
    print(f"Avg eligible:     {df['n_qual'].mean():.1f}  (passing screen + cap filter)")

    print("\nMonthly NAV (last day of month):")
    df["ym"] = df["date"].apply(lambda x: x.strftime("%Y-%m"))
    monthly = df.groupby("ym").last().reset_index()
    for _, r in monthly.iterrows():
        sim_pct = (r["nav"] / INITIAL_CAPITAL - 1) * 100
        bench_pct = (r["bench_nav"] / INITIAL_CAPITAL - 1) * 100
        print(f"  {r['ym']}  sim ¥{r['nav']:>15,.0f} ({sim_pct:+6.2f}%)   "
              f"bench ¥{r['bench_nav']:>15,.0f} ({bench_pct:+6.2f}%)   "
              f"n_pos={int(r['n_pos'])}, n_eligible={int(r['n_qual'])}")

    print(f"\nFinal positions on {nav_history[-1][0]}:")
    if positions.empty:
        print("  (none)")
    else:
        for _, p in positions.sort_values("market_value", ascending=False).iterrows():
            print(f"  {p['secCode']}: {int(p['shares']):>6} sh @ ¥{float(p['last_price']):>9,.0f}  "
                  f"= ¥{float(p['market_value']):>15,.0f}  (avg cost ¥{float(p['avg_cost']):>8,.0f})")


if __name__ == "__main__":
    main()
