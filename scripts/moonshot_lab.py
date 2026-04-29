"""Moonshot search: do non-value strategies have the fat right tails
that net-cash value lacks? net-cash caps out around +180% over 3 years
(no stock in our universe exceeded +200%). This lab tests whether
momentum / earnings-acceleration / small-cap-anomaly strategies
produce the >+300% rockets that net-cash investing doesn't.

Setup: same window as strategy_lab.py (entry 2022-12-30, exit on the
deadline 2025-12-30 — no ratio<1 exit because most of these don't
have a ratio). Universe: all stocks with adj_close + financials.

Strategies tested:
1. earnings_rocket — small-cap (≤¥30B) + op_yoy > +100% (acceleration)
2. price_momentum — top decile 12m momentum within small-cap
3. high_growth — small-cap + sales_yoy > +30% (top-line rocket)
4. micro_momentum — micro-cap (≤¥10B) + 6m_mom > +30%
5. earnings_x_momentum — op_yoy > +50% AND mom_6m > +10%
6. roa_x_momentum — op_yield > +20% AND mom_6m > +20%
7. low_float_proxy — bottom decile by issued_shares within small-cap +
   any momentum (proxy for small-float since we don't have free-float)
8. random_smallcap (n=30) — control: random 30 small caps for shape ref

For each, prints distribution stats + bucket histogram + top 10 picks.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stock_screening import db

ENTRY = date(2022, 12, 30)
DEADLINE = date(2025, 12, 30)
TOPIX_3Y = 81.2
RNG_SEED = 42


def build_universe(engine):
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
                "entry": ENTRY,
                "s6_start": ENTRY - timedelta(days=200),
                "s6_end": ENTRY - timedelta(days=160),
                "s12_start": ENTRY - timedelta(days=380),
                "s12_end": ENTRY - timedelta(days=350),
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
    df["op_yield"] = df["operating_income"] / df["mc"]
    df["op_yoy"] = (df["operating_income"] - df["op_prev"]) / df["op_prev"].abs()
    df["sales_yoy"] = (df["net_sales"] - df["sales_prev"]) / df["sales_prev"]
    df["mom_6m"] = df["price_now"] / df["p_6mo"] - 1
    df["mom_12m"] = df["price_now"] / df["p_12mo"] - 1
    df["per"] = df["mc"] / df["net_income"]
    return df


def compute_returns(engine, sec_codes):
    """Buy-and-hold to deadline (no exit rule)."""
    with engine.connect() as conn:
        prices = pd.DataFrame(
            conn.execute(
                text(
                    'SELECT "ShokenCode" sec_code, "Date" d, adj_close '
                    'FROM t_daily_stock_perf '
                    'WHERE "ShokenCode" = ANY(:codes) AND "Date" IN (:s, :e) '
                    'AND adj_close IS NOT NULL '
                    'UNION ALL '
                    'SELECT "ShokenCode" sec_code, "Date" d, adj_close '
                    'FROM t_daily_stock_perf '
                    'WHERE "ShokenCode" = ANY(:codes) '
                    'AND "Date" = (SELECT MAX("Date") FROM t_daily_stock_perf WHERE "Date" <= :e) '
                    'AND adj_close IS NOT NULL'
                ),
                {"codes": sec_codes, "s": ENTRY, "e": DEADLINE},
            ).fetchall(),
            columns=["sec_code", "d", "adj_close"],
        )
    if prices.empty:
        return {}
    prices["adj_close"] = prices["adj_close"].astype(float)
    out = {}
    for sec_code, g in prices.groupby("sec_code"):
        g = g.sort_values("d")
        if len(g) < 2:
            continue
        ent = g.iloc[0]
        ext = g.iloc[-1]
        if ent["d"] != ENTRY:
            continue
        out[sec_code] = {"return_pct": (float(ext["adj_close"]) / float(ent["adj_close"]) - 1) * 100}
    return out


def fetch_names(engine, sec_codes):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                'SELECT DISTINCT ON ("secCode") "secCode", "filerName" '
                'FROM t_doc_list WHERE "secCode" = ANY(:codes) '
                'ORDER BY "secCode", "submitDateTime" DESC'
            ),
            {"codes": list(sec_codes)},
        ).fetchall()
    return {c: n for c, n in rows}


def report(name, picks, returns, names):
    rs = []
    for code in picks["sec_code"]:
        if code in returns:
            rs.append({
                "code": code,
                "name": names.get(code, ""),
                "mc_bn": picks.set_index("sec_code").loc[code, "mc"] / 1e9,
                "ret_pct": returns[code]["return_pct"],
            })
    if not rs:
        print(f"\n=== {name} === (no stocks)")
        return
    df = pd.DataFrame(rs).sort_values("ret_pct", ascending=False).reset_index(drop=True)
    s = df["ret_pct"]

    print(f"\n{'='*100}")
    print(f"{name}  —  n={len(df)}")
    print("=" * 100)
    print(
        f"  mean {s.mean():+6.1f}%  median {s.median():+6.1f}%  stdev {s.std():>5.1f}%  "
        f"min {s.min():+6.1f}%  max {s.max():+6.1f}%  skew {s.skew():>5.2f}"
    )
    print(
        f"  P10 {s.quantile(0.10):+6.1f}%  P25 {s.quantile(0.25):+6.1f}%  "
        f"P75 {s.quantile(0.75):+6.1f}%  P90 {s.quantile(0.90):+6.1f}%  P95 {s.quantile(0.95):+6.1f}%"
    )
    buckets = [
        ("<-50%",     s < -50),
        ("-50 to 0",  (s >= -50) & (s < 0)),
        ("0 to 50",   (s >= 0) & (s < 50)),
        ("50 to 100", (s >= 50) & (s < 100)),
        ("100-200",   (s >= 100) & (s < 200)),
        ("200-300",   (s >= 200) & (s < 300)),
        (">=300",     s >= 300),
    ]
    print(f"  buckets:")
    for label, mask in buckets:
        n = mask.sum()
        pct = n / len(df) * 100
        bar = "█" * int(pct / 2)
        print(f"    {label:>10}: n={n:>3} ({pct:>4.0f}%) {bar}")
    n_topix = (s > TOPIX_3Y).sum()
    n_double = (s >= 100).sum()
    n_triple = (s >= 200).sum()
    n_neg = (s < 0).sum()
    print(
        f"  beat TOPIX ({n_topix}, {n_topix/len(df)*100:.0f}%) | "
        f"doubled ({n_double}, {n_double/len(df)*100:.0f}%) | "
        f"tripled ({n_triple}, {n_triple/len(df)*100:.0f}%) | "
        f"negative ({n_neg}, {n_neg/len(df)*100:.0f}%)"
    )
    print(f"  top 10 picks:")
    for i, row in df.head(10).iterrows():
        print(f"    {i+1:>2}. {row['code']:<6} {row['name'][:24]:<24}  MC ¥{row['mc_bn']:>5.1f}B  {row['ret_pct']:>+7.1f}%")


def main():
    engine = db.get_engine()
    print(f"Building universe at {ENTRY}, hold to {DEADLINE}...")
    u = build_universe(engine)
    print(f"  {len(u)} stocks")
    returns = compute_returns(engine, u["sec_code"].tolist())
    names = fetch_names(engine, u["sec_code"].tolist())
    print(f"  {len(returns)} with full price history\n")

    # filters
    small = u[(u["mc"] >= 1e9) & (u["mc"] <= 30e9)]
    micro = u[(u["mc"] >= 1e9) & (u["mc"] <= 10e9)]

    report("earnings_rocket  (≤¥30B + op_yoy > +100%)",
           small[small["op_yoy"] > 1.0], returns, names)

    report("price_momentum_top10pct  (≤¥30B, top 10% by 12m mom)",
           small.nlargest(max(10, int(len(small) * 0.10)), "mom_12m"), returns, names)

    report("high_growth  (≤¥30B + sales_yoy > +30%)",
           small[small["sales_yoy"] > 0.30], returns, names)

    report("micro_momentum  (≤¥10B + 6m_mom > +30%)",
           micro[micro["mom_6m"] > 0.30], returns, names)

    report("earnings_x_momentum  (≤¥30B + op_yoy>50% + mom_6m>10%)",
           small[(small["op_yoy"] > 0.5) & (small["mom_6m"] > 0.1)],
           returns, names)

    report("roa_x_momentum  (≤¥30B + op_yield>20% + mom_6m>20%)",
           small[(small["op_yield"] > 0.2) & (small["mom_6m"] > 0.2)],
           returns, names)

    # low-float proxy: bottom decile by issued shares within small cap
    small_with_shares = small.dropna(subset=["issued_shares"])
    n_low_float = max(20, int(len(small_with_shares) * 0.10))
    low_float = small_with_shares.nsmallest(n_low_float, "issued_shares")
    report("low_float_proxy  (≤¥30B, bottom 10% by issued_shares + mom_6m>0)",
           low_float[low_float["mom_6m"] > 0], returns, names)

    # control
    report("control_random_smallcap (n=30 random)",
           small.sample(n=min(30, len(small)), random_state=RNG_SEED), returns, names)

    # also baseline_sweet_spot for shape comparison
    sweet = u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
            & ((u["current_assets"] - u["total_liabilities"]
                + 0.7 * u["investment_securities"].fillna(0)) / u["mc"] > 1.5)
            & (u["per"] > 0) & (u["per"] <= 10)]
    report("REFERENCE: baseline_sweet_spot (net-cash value)", sweet, returns, names)

    print(f"\n{'='*100}")
    print(f"TOPIX 3-year: +{TOPIX_3Y}%")
    print(f"{'='*100}\n")


if __name__ == "__main__":
    main()
