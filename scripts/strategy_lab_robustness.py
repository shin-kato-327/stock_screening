"""Multi-entry-date robustness check on the 10 strategies from
strategy_lab.py. The original lab result (top10_by_ratio +87.7%) was
from a single entry date — this script asks: does it survive when we
move the entry date around?

Setup: 4 entry dates spaced ~6 months apart, fixed 2-year hold each
(min(entry+730d, today)). Each entry gets its own TOPIX benchmark
computed over the same window so excess-return is honest.

Reports per-strategy: per-entry returns, aggregate mean across entries,
hit rate vs TOPIX, and how often the strategy beats TOPIX in any
single window.
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
EXIT_RATIO = 1.0
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
                    SELECT fa."secCode" AS sec_code, fa.period_end,
                           fa.current_assets, fa.total_liabilities,
                           fa.investment_securities, fa.total_assets,
                           fa.operating_income, fa.net_sales, fa.net_income,
                           fa.source_doc_id
                    FROM t_financials_annual fa
                    JOIN t_doc_list dl ON dl."docID" = fa.source_doc_id
                    WHERE dl."submitDateTime"::date <= :entry
                      AND fa.current_assets IS NOT NULL
                ),
                latest AS (
                    SELECT DISTINCT ON (sec_code) *
                    FROM visible ORDER BY sec_code, period_end DESC
                ),
                ranked AS (
                    SELECT fa2."secCode", fa2.period_end, fa2.operating_income,
                           fa2.net_sales, fa2.current_assets,
                           LAG(fa2.operating_income) OVER w op_prev,
                           LAG(fa2.net_sales) OVER w sales_prev,
                           LAG(fa2.current_assets) OVER w ca_prev
                    FROM t_financials_annual fa2
                    JOIN t_doc_list dl2 ON dl2."docID" = fa2.source_doc_id
                    WHERE dl2."submitDateTime"::date <= :entry
                    WINDOW w AS (PARTITION BY fa2."secCode" ORDER BY fa2.period_end)
                ),
                yoy AS (
                    SELECT DISTINCT ON ("secCode") "secCode" AS sec_code,
                           op_prev, sales_prev, ca_prev
                    FROM ranked WHERE op_prev IS NOT NULL OR sales_prev IS NOT NULL
                    ORDER BY "secCode", period_end DESC
                )
                SELECT l.sec_code, l.current_assets, l.total_liabilities,
                       l.investment_securities, l.total_assets,
                       l.operating_income, l.net_sales, l.net_income,
                       y.op_prev, y.sales_prev, y.ca_prev,
                       d_now.adj_close AS price_now,
                       d_now."marketCap" AS mc,
                       d_6mo.adj_close AS price_6mo,
                       d_12mo.adj_close AS price_12mo
                FROM latest l
                LEFT JOIN yoy y ON y.sec_code = l.sec_code
                JOIN t_daily_stock_perf d_now
                  ON d_now."ShokenCode" = l.sec_code AND d_now."Date" = :entry
                  AND d_now.adj_close IS NOT NULL AND d_now."marketCap" IS NOT NULL
                LEFT JOIN LATERAL (
                    SELECT adj_close FROM t_daily_stock_perf p
                    WHERE p."ShokenCode" = l.sec_code
                      AND p."Date" BETWEEN :six_mo_start AND :six_mo_end
                      AND p.adj_close IS NOT NULL
                    ORDER BY p."Date" DESC LIMIT 1
                ) d_6mo ON TRUE
                LEFT JOIN LATERAL (
                    SELECT adj_close FROM t_daily_stock_perf p
                    WHERE p."ShokenCode" = l.sec_code
                      AND p."Date" BETWEEN :twelve_mo_start AND :twelve_mo_end
                      AND p.adj_close IS NOT NULL
                    ORDER BY p."Date" DESC LIMIT 1
                ) d_12mo ON TRUE
                """
            ),
            {
                "entry": entry,
                "six_mo_start": entry - timedelta(days=200),
                "six_mo_end": entry - timedelta(days=160),
                "twelve_mo_start": entry - timedelta(days=380),
                "twelve_mo_end": entry - timedelta(days=350),
            },
        ).fetchall()

    cols = [
        "sec_code", "current_assets", "total_liabilities", "investment_securities",
        "total_assets", "operating_income", "net_sales", "net_income",
        "op_prev", "sales_prev", "ca_prev",
        "price_now", "mc", "price_6mo", "price_12mo",
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
    df["op_margin"] = df["operating_income"] / df["net_sales"]
    df["op_yoy"] = (df["operating_income"] - df["op_prev"]) / df["op_prev"].abs()
    df["sales_yoy"] = (df["net_sales"] - df["sales_prev"]) / df["sales_prev"]
    df["assets_yoy"] = (df["current_assets"] - df["ca_prev"]) / df["ca_prev"]
    df["mom_6m"] = df["price_now"] / df["price_6mo"] - 1
    df["mom_12m"] = df["price_now"] / df["price_12mo"] - 1
    df["mc_bn"] = df["mc"] / 1e9
    return df


def compute_returns(
    engine, sec_codes: list[str], universe: pd.DataFrame, entry: date, deadline: date
) -> dict[str, dict]:
    with engine.connect() as conn:
        prices = pd.DataFrame(
            conn.execute(
                text(
                    'SELECT "ShokenCode" sec_code, "Date" d, adj_close, "marketCap" '
                    'FROM t_daily_stock_perf '
                    'WHERE "ShokenCode" = ANY(:codes) AND "Date" BETWEEN :s AND :e '
                    'AND adj_close IS NOT NULL AND "marketCap" IS NOT NULL '
                    'ORDER BY "ShokenCode", "Date"'
                ),
                {"codes": sec_codes, "s": entry, "e": deadline},
            ).fetchall(),
            columns=["sec_code", "d", "adj_close", "market_cap"],
        )
    prices["adj_close"] = prices["adj_close"].astype(float)
    prices["market_cap"] = prices["market_cap"].astype(float)

    init = universe.set_index("sec_code")
    init["numerator"] = (
        init["current_assets"] - init["total_liabilities"]
        + HAIRCUT * init["investment_securities"].fillna(0)
    )

    out: dict[str, dict] = {}
    for sec_code, g in prices.groupby("sec_code"):
        if sec_code not in init.index:
            continue
        ent_price = float(init.loc[sec_code, "price_now"])
        num = float(init.loc[sec_code, "numerator"])
        g = g.sort_values("d").reset_index(drop=True)
        exit_d, exit_p = None, None
        for _, r in g.iterrows():
            ratio = num / r["market_cap"] if r["market_cap"] > 0 else 0
            if ratio < EXIT_RATIO:
                exit_d, exit_p = r["d"], r["adj_close"]
                break
        if exit_d is None:
            last = g.iloc[-1]
            exit_d, exit_p = last["d"], last["adj_close"]
        out[sec_code] = {"return_pct": (exit_p / ent_price - 1) * 100}
    return out


def topix_return(engine, entry: date, deadline: date) -> float:
    with engine.connect() as conn:
        s = conn.execute(
            text(
                'SELECT adj_close FROM t_daily_stock_perf '
                'WHERE "ShokenCode" = :c AND "Date" >= :d '
                'ORDER BY "Date" LIMIT 1'
            ),
            {"c": TOPIX_CODE, "d": entry},
        ).scalar()
        e = conn.execute(
            text(
                'SELECT adj_close FROM t_daily_stock_perf '
                'WHERE "ShokenCode" = :c AND "Date" <= :d '
                'ORDER BY "Date" DESC LIMIT 1'
            ),
            {"c": TOPIX_CODE, "d": deadline},
        ).scalar()
    return (float(e) / float(s) - 1) * 100


# --- strategies (copied verbatim from strategy_lab.py) ---

def strategy_baseline_sweet_spot(u):
    return u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
            & (u["ratio"] > 1.5) & (u["per"] > 0) & (u["per"] <= 10)]

def strategy_top10_by_ratio(u):
    return strategy_baseline_sweet_spot(u).nlargest(10, "ratio")

def strategy_value_momentum(u):
    s = strategy_baseline_sweet_spot(u)
    return s[s["mom_6m"] > 0]

def strategy_value_quality(u):
    s = strategy_baseline_sweet_spot(u)
    return s[s["op_margin"] > 0.10]

def strategy_sales_growth(u):
    s = strategy_baseline_sweet_spot(u)
    return s[s["sales_yoy"] > 0.05]

def strategy_cash_compounders(u):
    return u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
           & (u["ratio"] > 1.0) & (u["assets_yoy"] > 0.05) & (u["sales_yoy"] > 0)
           & (u["per"] > 0) & (u["per"] <= 15)]

def strategy_qmom(u):
    return u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
           & (u["op_yield"] > 0.10) & (u["mom_6m"] > 0.10)
           & (u["per"] > 0) & (u["per"] <= 15)]

def strategy_reversal(u):
    return u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
           & (u["ratio"] > 1.0) & (u["mom_12m"] < -0.20)
           & (u["sales_yoy"] > -0.05) & (u["op_margin"] > 0)]

def strategy_z_composite(u):
    s = strategy_baseline_sweet_spot(u).copy()
    if s.empty:
        return s
    for col in ("ratio", "op_yield", "sales_yoy", "mom_6m"):
        s[f"z_{col}"] = (s[col] - s[col].mean()) / s[col].std()
    s["z_total"] = s[["z_ratio", "z_op_yield", "z_sales_yoy", "z_mom_6m"]].sum(axis=1)
    return s.nlargest(max(10, int(len(s) * 0.20)), "z_total")

def strategy_low_per_top10(u):
    s = u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
        & (u["ratio"] > 1.0) & (u["per"] > 0) & (u["per"] <= 5)]
    return s.nsmallest(10, "per")


STRATEGIES = [
    Strategy("baseline_sweet_spot", "¥3-50B + ratio>1.5 + PER≤10", strategy_baseline_sweet_spot),
    Strategy("top10_by_ratio", "Top 10 by ratio (concentrated)", strategy_top10_by_ratio),
    Strategy("value_momentum", "Sweet-spot + 6m_mom > 0", strategy_value_momentum),
    Strategy("value_quality", "Sweet-spot + op_margin > 10%", strategy_value_quality),
    Strategy("sales_growth", "Sweet-spot + sales_yoy > 5%", strategy_sales_growth),
    Strategy("cash_compounders", "ratio>1 + assets+sales growing", strategy_cash_compounders),
    Strategy("qmom", "Quality+Momentum (no value)", strategy_qmom),
    Strategy("reversal", "12m drawdown but biz OK", strategy_reversal),
    Strategy("z_composite", "Top 20% by z-score blend", strategy_z_composite),
    Strategy("low_per_top10", "Top 10 lowest PER ≤5", strategy_low_per_top10),
]


def main():
    engine = db.get_engine()
    # rows: strategy x entry → (n, mean_return, excess vs topix)
    results: dict[str, list[dict]] = {s.name: [] for s in STRATEGIES}
    topix_per_entry: dict[date, float] = {}

    for entry in ENTRIES:
        deadline = min(entry + timedelta(days=HOLD_DAYS), DATA_END)
        topix = topix_return(engine, entry, deadline)
        topix_per_entry[entry] = topix
        print(f"\n=== entry {entry} → {deadline} ({(deadline-entry).days}d hold) | TOPIX {topix:+.1f}% ===")
        u = build_universe(engine, entry)
        codes = u["sec_code"].tolist()
        rets = compute_returns(engine, codes, u, entry, deadline)
        print(f"  universe: {len(u)} stocks, {len(rets)} with full price history")

        for strat in STRATEGIES:
            picks = strat.filter_fn(u)
            r = [rets[c]["return_pct"] for c in picks["sec_code"] if c in rets]
            if not r:
                results[strat.name].append({
                    "entry": entry, "n": 0, "mean": None, "excess": None, "beat_topix": None
                })
                continue
            mean = float(pd.Series(r).mean())
            excess = mean - topix
            beat = (pd.Series(r) > topix).mean() * 100
            results[strat.name].append({
                "entry": entry, "n": len(r), "mean": mean,
                "excess": excess, "beat_topix": beat,
            })

    # per-entry x per-strategy table
    print("\n\n" + "=" * 100)
    print("PER-ENTRY MEAN RETURN (excess vs TOPIX in parens)")
    print("=" * 100)
    header = f"{'strategy':<22}" + "".join(f"  {e.isoformat():>22}" for e in ENTRIES)
    print(header)
    print("-" * len(header))
    for strat in STRATEGIES:
        line = f"{strat.name:<22}"
        for r in results[strat.name]:
            if r["mean"] is None:
                line += f"  {'(n=0)':>22}"
            else:
                line += f"  n={r['n']:>2} {r['mean']:>+6.1f}% ({r['excess']:>+5.1f})"
        print(line)
    print(f"{'TOPIX':<22}" + "".join(f"  {topix_per_entry[e]:>+22.1f}%" for e in ENTRIES))

    # aggregate (mean across entries, weighting all entries equally)
    print("\n" + "=" * 100)
    print("AGGREGATE: mean of per-entry mean returns (each entry weighted equally)")
    print("=" * 100)
    print(f"{'strategy':<22}  {'avg_ret':>8}  {'avg_excess':>10}  {'win_rate':>9}  {'avg_n':>6}  desc")
    print("-" * 110)
    for strat in STRATEGIES:
        rs = [r for r in results[strat.name] if r["mean"] is not None]
        if not rs:
            continue
        avg_ret = sum(r["mean"] for r in rs) / len(rs)
        avg_excess = sum(r["excess"] for r in rs) / len(rs)
        win_rate = sum(1 for r in rs if r["excess"] > 0) / len(rs) * 100
        avg_n = sum(r["n"] for r in rs) / len(rs)
        print(
            f"{strat.name:<22}  {avg_ret:>+7.1f}%  {avg_excess:>+9.1f}%  "
            f"{win_rate:>8.0f}%  {avg_n:>6.1f}  {strat.desc}"
        )
    avg_topix = sum(topix_per_entry.values()) / len(topix_per_entry)
    print(f"\nAvg TOPIX across {len(ENTRIES)} entries: {avg_topix:+.1f}%")
    print(f"Hold length: {HOLD_DAYS} days (~{HOLD_DAYS/365:.1f} yr) capped at {DATA_END}")


if __name__ == "__main__":
    main()
