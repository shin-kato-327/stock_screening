"""Multi-entry-date robustness test for moonshot strategies and the
buy-and-hold version of net-cash baseline. Same 4 entry dates as
strategy_lab_robustness.py, fixed 2-year hold per entry. Always
buy-and-hold (no ratio<1 exit) since most moonshot strategies don't
have a ratio.

Reports per-strategy: per-entry mean/max/triple-rate/negative-rate
plus aggregate across entries. Goal is to see whether the standout
single-period finding (low_float_proxy mean +180% in 2022 cohort)
survives across multiple windows.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stock_screening import db

ENTRIES = [
    date(2022, 12, 30),
    date(2023, 6, 30),
    date(2023, 12, 29),
    date(2024, 6, 28),
]
HOLD_DAYS = 730
DATA_END = date(2026, 4, 24)
HAIRCUT = 0.7
TOPIX_CODE = "13060"


@dataclass
class Strategy:
    name: str
    desc: str
    filter_fn: Callable[[pd.DataFrame], pd.DataFrame]


def build_universe(engine, entry: date) -> pd.DataFrame:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH visible AS (
                    SELECT fa."secCode" sec_code, fa.period_end,
                           fa.current_assets, fa.total_liabilities,
                           fa.investment_securities, fa.operating_income,
                           fa.net_sales, fa.net_income, fa.issued_shares,
                           fa.source_doc_id
                    FROM t_financials_annual fa
                    JOIN t_doc_list dl ON dl."docID" = fa.source_doc_id
                    WHERE dl."submitDateTime"::date <= :entry
                      AND fa.current_assets IS NOT NULL
                ),
                latest AS (
                    SELECT DISTINCT ON (sec_code) * FROM visible
                    ORDER BY sec_code, period_end DESC
                ),
                ranked AS (
                    SELECT fa2."secCode", fa2.period_end,
                           fa2.operating_income, fa2.net_sales,
                           LAG(fa2.operating_income) OVER w op_prev,
                           LAG(fa2.net_sales) OVER w sales_prev
                    FROM t_financials_annual fa2
                    JOIN t_doc_list dl2 ON dl2."docID" = fa2.source_doc_id
                    WHERE dl2."submitDateTime"::date <= :entry
                    WINDOW w AS (PARTITION BY fa2."secCode" ORDER BY fa2.period_end)
                ),
                yoy AS (
                    SELECT DISTINCT ON ("secCode") "secCode" sec_code, op_prev, sales_prev
                    FROM ranked WHERE op_prev IS NOT NULL OR sales_prev IS NOT NULL
                    ORDER BY "secCode", period_end DESC
                )
                SELECT l.sec_code, l.current_assets, l.total_liabilities,
                       l.investment_securities, l.operating_income,
                       l.net_sales, l.net_income, l.issued_shares,
                       y.op_prev, y.sales_prev,
                       d_now.adj_close price_now, d_now."marketCap" mc,
                       d_6mo.adj_close p_6mo, d_12mo.adj_close p_12mo
                FROM latest l
                LEFT JOIN yoy y ON y.sec_code = l.sec_code
                JOIN t_daily_stock_perf d_now
                  ON d_now."ShokenCode" = l.sec_code AND d_now."Date" = :entry
                  AND d_now.adj_close IS NOT NULL AND d_now."marketCap" IS NOT NULL
                LEFT JOIN LATERAL (
                    SELECT adj_close FROM t_daily_stock_perf p
                    WHERE p."ShokenCode" = l.sec_code
                      AND p."Date" BETWEEN :s6_start AND :s6_end
                      AND p.adj_close IS NOT NULL
                    ORDER BY p."Date" DESC LIMIT 1
                ) d_6mo ON TRUE
                LEFT JOIN LATERAL (
                    SELECT adj_close FROM t_daily_stock_perf p
                    WHERE p."ShokenCode" = l.sec_code
                      AND p."Date" BETWEEN :s12_start AND :s12_end
                      AND p.adj_close IS NOT NULL
                    ORDER BY p."Date" DESC LIMIT 1
                ) d_12mo ON TRUE
                """
            ),
            {
                "entry": entry,
                "s6_start": entry - timedelta(days=200),
                "s6_end": entry - timedelta(days=160),
                "s12_start": entry - timedelta(days=380),
                "s12_end": entry - timedelta(days=350),
            },
        ).fetchall()
    cols = [
        "sec_code", "current_assets", "total_liabilities", "investment_securities",
        "operating_income", "net_sales", "net_income", "issued_shares",
        "op_prev", "sales_prev", "price_now", "mc", "p_6mo", "p_12mo",
    ]
    df = pd.DataFrame(rows, columns=cols)
    for c in cols[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ratio"] = (
        df["current_assets"] - df["total_liabilities"]
        + HAIRCUT * df["investment_securities"].fillna(0)
    ) / df["mc"]
    df["per"] = df["mc"] / df["net_income"]
    df["op_yield"] = df["operating_income"] / df["mc"]
    df["op_yoy"] = (df["operating_income"] - df["op_prev"]) / df["op_prev"].abs()
    df["sales_yoy"] = (df["net_sales"] - df["sales_prev"]) / df["sales_prev"]
    df["mom_6m"] = df["price_now"] / df["p_6mo"] - 1
    df["mom_12m"] = df["price_now"] / df["p_12mo"] - 1
    return df


def compute_returns_bah(engine, sec_codes, entry, deadline):
    """Buy at entry, sell at deadline (or last available trading day before)."""
    with engine.connect() as conn:
        prices = pd.DataFrame(
            conn.execute(
                text(
                    'SELECT "ShokenCode" sec_code, "Date" d, adj_close '
                    'FROM t_daily_stock_perf '
                    'WHERE "ShokenCode" = ANY(:codes) '
                    'AND ("Date" = :s OR "Date" = ('
                    '    SELECT MAX("Date") FROM t_daily_stock_perf WHERE "Date" <= :e '
                    ')) AND adj_close IS NOT NULL'
                ),
                {"codes": sec_codes, "s": entry, "e": deadline},
            ).fetchall(),
            columns=["sec_code", "d", "adj_close"],
        )
    prices["adj_close"] = prices["adj_close"].astype(float)
    out = {}
    for sec_code, g in prices.groupby("sec_code"):
        g = g.sort_values("d")
        if len(g) < 2:
            continue
        ent_row = g[g["d"] == entry]
        if ent_row.empty:
            continue
        ent_p = float(ent_row.iloc[0]["adj_close"])
        ext_p = float(g.iloc[-1]["adj_close"])
        out[sec_code] = {"return_pct": (ext_p / ent_p - 1) * 100}
    return out


def topix_return(engine, entry, deadline):
    with engine.connect() as conn:
        s = conn.execute(
            text('SELECT adj_close FROM t_daily_stock_perf '
                 'WHERE "ShokenCode" = :c AND "Date" >= :d ORDER BY "Date" LIMIT 1'),
            {"c": TOPIX_CODE, "d": entry}).scalar()
        e = conn.execute(
            text('SELECT adj_close FROM t_daily_stock_perf '
                 'WHERE "ShokenCode" = :c AND "Date" <= :d ORDER BY "Date" DESC LIMIT 1'),
            {"c": TOPIX_CODE, "d": deadline}).scalar()
    return (float(e) / float(s) - 1) * 100


# ----- strategies -----

def strategy_low_float_proxy(u):
    """≤¥30B + bottom 10% by issued_shares + mom_6m > 0."""
    small = u[(u["mc"] >= 1e9) & (u["mc"] <= 30e9)].dropna(subset=["issued_shares"])
    if small.empty:
        return small
    n_low = max(20, int(len(small) * 0.10))
    low = small.nsmallest(n_low, "issued_shares")
    return low[low["mom_6m"] > 0]


def strategy_low_float_strict(u):
    """≤¥15B + bottom 5% by issued_shares + mom_6m > 0. More concentrated."""
    small = u[(u["mc"] >= 1e9) & (u["mc"] <= 15e9)].dropna(subset=["issued_shares"])
    if small.empty:
        return small
    n_low = max(15, int(len(small) * 0.05))
    low = small.nsmallest(n_low, "issued_shares")
    return low[low["mom_6m"] > 0]


def strategy_low_float_with_value(u):
    """Hybrid: net-cash sweet-spot (¥3-30B, ratio>1.5, PER<=10) +
    bottom 25% by issued_shares within that subset."""
    sweet = u[(u["mc"] >= 3e9) & (u["mc"] <= 30e9)
            & (u["ratio"] > 1.5) & (u["per"] > 0) & (u["per"] <= 10)
            ].dropna(subset=["issued_shares"])
    if sweet.empty:
        return sweet
    n_low = max(5, int(len(sweet) * 0.25))
    return sweet.nsmallest(n_low, "issued_shares")


def strategy_baseline_sweet_spot_bah(u):
    """Same baseline as strategy_lab.py but evaluated under buy-and-hold."""
    return u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
            & (u["ratio"] > 1.5) & (u["per"] > 0) & (u["per"] <= 10)]


def strategy_top10_by_ratio_bah(u):
    return strategy_baseline_sweet_spot_bah(u).nlargest(10, "ratio")


def strategy_earnings_rocket(u):
    """≤¥30B + op_yoy > +100% (no value filter)."""
    small = u[(u["mc"] >= 1e9) & (u["mc"] <= 30e9)]
    return small[small["op_yoy"] > 1.0]


STRATEGIES = [
    Strategy("low_float_proxy",
             "≤¥30B + bot10% issued_shares + mom_6m>0",
             strategy_low_float_proxy),
    Strategy("low_float_strict",
             "≤¥15B + bot5% issued_shares + mom_6m>0",
             strategy_low_float_strict),
    Strategy("low_float_with_value",
             "Sweet-spot + bot25% issued_shares (hybrid)",
             strategy_low_float_with_value),
    Strategy("baseline_sweet_spot_BAH",
             "Sweet-spot, buy-and-hold (no ratio<1 exit)",
             strategy_baseline_sweet_spot_bah),
    Strategy("top10_by_ratio_BAH",
             "Top 10 by ratio, buy-and-hold",
             strategy_top10_by_ratio_bah),
    Strategy("earnings_rocket",
             "≤¥30B + op_yoy > +100% (no value filter)",
             strategy_earnings_rocket),
]


def summarize(rets):
    s = pd.Series(rets)
    return {
        "n": len(s),
        "mean": s.mean(),
        "median": s.median(),
        "max": s.max(),
        "min": s.min(),
        "triple_pct": (s >= 200).mean() * 100,
        "double_pct": (s >= 100).mean() * 100,
        "neg_pct": (s < 0).mean() * 100,
    }


def main():
    engine = db.get_engine()
    results: dict[str, list[dict]] = {s.name: [] for s in STRATEGIES}
    topix_per_entry: dict[date, float] = {}

    for entry in ENTRIES:
        deadline = min(entry + timedelta(days=HOLD_DAYS), DATA_END)
        topix = topix_return(engine, entry, deadline)
        topix_per_entry[entry] = topix
        print(f"\n=== entry {entry} → {deadline} ({(deadline-entry).days}d) | TOPIX {topix:+.1f}% ===")
        u = build_universe(engine, entry)
        codes = u["sec_code"].tolist()
        rets = compute_returns_bah(engine, codes, entry, deadline)
        print(f"  universe {len(u)}, {len(rets)} priced")

        for strat in STRATEGIES:
            picks = strat.filter_fn(u)
            r = [rets[c]["return_pct"] for c in picks["sec_code"] if c in rets]
            if not r:
                results[strat.name].append({"entry": entry, **summarize([0])})
                results[strat.name][-1]["n"] = 0
                continue
            stats = summarize(r)
            stats["entry"] = entry
            stats["excess"] = stats["mean"] - topix
            results[strat.name].append(stats)

    # per-entry table
    print("\n\n" + "=" * 130)
    print("PER-ENTRY (mean / max / triple% / negative%)")
    print("=" * 130)
    header = f"{'strategy':<26}" + "".join(f" {e.isoformat():>26}" for e in ENTRIES)
    print(header)
    print("-" * len(header))
    for strat in STRATEGIES:
        line = f"{strat.name:<26}"
        for r in results[strat.name]:
            if r["n"] == 0:
                line += f" {'(n=0)':>26}"
            else:
                line += (
                    f" n={r['n']:>3} {r['mean']:>+5.0f}% {r['max']:>+5.0f}% "
                    f"3x{r['triple_pct']:>3.0f} ng{r['neg_pct']:>3.0f}"
                )
        print(line)

    # aggregate
    print("\n" + "=" * 130)
    print("AGGREGATE across 4 entries (each entry equal-weighted)")
    print("=" * 130)
    print(
        f"{'strategy':<26}  {'avg_mean':>9} {'avg_excess':>10} {'avg_max':>9} "
        f"{'avg_3x%':>8} {'avg_neg%':>9} {'avg_n':>6}  desc"
    )
    print("-" * 130)
    for strat in STRATEGIES:
        rs = [r for r in results[strat.name] if r["n"] > 0]
        if not rs:
            continue
        avg_mean = sum(r["mean"] for r in rs) / len(rs)
        avg_excess = sum(r["excess"] for r in rs) / len(rs)
        avg_max = sum(r["max"] for r in rs) / len(rs)
        avg_3x = sum(r["triple_pct"] for r in rs) / len(rs)
        avg_neg = sum(r["neg_pct"] for r in rs) / len(rs)
        avg_n = sum(r["n"] for r in rs) / len(rs)
        print(
            f"{strat.name:<26}  {avg_mean:>+8.1f}% {avg_excess:>+9.1f}% {avg_max:>+8.0f}% "
            f"{avg_3x:>7.0f}% {avg_neg:>8.0f}% {avg_n:>6.1f}  {strat.desc}"
        )
    print(f"\nAvg TOPIX: {sum(topix_per_entry.values())/len(topix_per_entry):+.1f}% "
          f"({HOLD_DAYS}d hold capped at {DATA_END})")


if __name__ == "__main__":
    main()
