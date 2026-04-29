"""Per-stock 3-year return distributions for the two robust winners
from the strategy lab: sales_growth and top10_by_ratio.

Same setup as strategy_lab.py (entry 2022-12-30, exit on ratio<1 OR
deadline 2025-12-30, $-equal-weight). Goal is to see whether returns
are fat-right-tailed (a few moonshots) or compressed.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stock_screening import db

ENTRY_DATE = date(2022, 12, 30)
EXIT_DEADLINE = date(2025, 12, 30)
HAIRCUT = 0.7
EXIT_RATIO = 1.0
TOPIX_3Y_RETURN = 81.2


def build_universe(engine):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH visible AS (
                    SELECT fa."secCode" sec_code, fa.period_end,
                           fa.current_assets, fa.total_liabilities,
                           fa.investment_securities, fa.operating_income,
                           fa.net_sales, fa.net_income, fa.source_doc_id
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
                    SELECT fa2."secCode", fa2.period_end, fa2.net_sales,
                           LAG(fa2.net_sales) OVER w sales_prev
                    FROM t_financials_annual fa2
                    JOIN t_doc_list dl2 ON dl2."docID" = fa2.source_doc_id
                    WHERE dl2."submitDateTime"::date <= :entry
                    WINDOW w AS (PARTITION BY fa2."secCode" ORDER BY fa2.period_end)
                ),
                yoy AS (
                    SELECT DISTINCT ON ("secCode") "secCode" sec_code, sales_prev
                    FROM ranked WHERE sales_prev IS NOT NULL
                    ORDER BY "secCode", period_end DESC
                )
                SELECT l.sec_code, l.current_assets, l.total_liabilities,
                       l.investment_securities, l.operating_income,
                       l.net_sales, l.net_income, y.sales_prev,
                       d_now.adj_close price_now, d_now."marketCap" mc
                FROM latest l
                LEFT JOIN yoy y ON y.sec_code = l.sec_code
                JOIN t_daily_stock_perf d_now
                  ON d_now."ShokenCode" = l.sec_code AND d_now."Date" = :entry
                  AND d_now.adj_close IS NOT NULL AND d_now."marketCap" IS NOT NULL
                """
            ),
            {"entry": ENTRY_DATE},
        ).fetchall()
    cols = [
        "sec_code", "current_assets", "total_liabilities", "investment_securities",
        "operating_income", "net_sales", "net_income", "sales_prev",
        "price_now", "mc",
    ]
    df = pd.DataFrame(rows, columns=cols)
    for c in cols[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ratio"] = (
        df["current_assets"] - df["total_liabilities"]
        + HAIRCUT * df["investment_securities"].fillna(0)
    ) / df["mc"]
    df["per"] = df["mc"] / df["net_income"]
    df["sales_yoy"] = (df["net_sales"] - df["sales_prev"]) / df["sales_prev"]
    return df


def compute_returns(engine, sec_codes, universe):
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
                {"codes": sec_codes, "s": ENTRY_DATE, "e": EXIT_DEADLINE},
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
    out = {}
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
        out[sec_code] = {
            "exit_d": exit_d,
            "return_pct": (exit_p / ent_price - 1) * 100,
            "exit_reason": "ratio<1" if (num / float(g.iloc[-1]["market_cap"]) < EXIT_RATIO and exit_d != g.iloc[-1]["d"]) else "deadline",
        }
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
            r = returns[code]
            rs.append({
                "code": code,
                "name": names.get(code, ""),
                "ratio": picks.set_index("sec_code").loc[code, "ratio"],
                "per": picks.set_index("sec_code").loc[code, "per"],
                "mc_bn": picks.set_index("sec_code").loc[code, "mc"] / 1e9,
                "ret_pct": r["return_pct"],
                "exit_d": r["exit_d"],
            })
    df = pd.DataFrame(rs).sort_values("ret_pct", ascending=False).reset_index(drop=True)

    print(f"\n{'='*100}")
    print(f"{name}  —  n={len(df)}")
    print("=" * 100)

    s = df["ret_pct"]
    print(f"\nDistribution stats:")
    print(f"  mean:   {s.mean():+7.1f}%")
    print(f"  median: {s.median():+7.1f}%")
    print(f"  stdev:  {s.std():>7.1f}%")
    print(f"  min:    {s.min():+7.1f}%")
    print(f"  max:    {s.max():+7.1f}%")
    print(f"  P10:    {s.quantile(0.10):+7.1f}%")
    print(f"  P25:    {s.quantile(0.25):+7.1f}%")
    print(f"  P75:    {s.quantile(0.75):+7.1f}%")
    print(f"  P90:    {s.quantile(0.90):+7.1f}%")
    print(f"  skew:   {s.skew():>7.2f}  (positive = fat right tail)")

    print(f"\nReturn buckets:")
    buckets = [
        ("losses",        s < 0),
        ("0-25%",         (s >= 0) & (s < 25)),
        ("25-50%",        (s >= 25) & (s < 50)),
        ("50-100%",       (s >= 50) & (s < 100)),
        ("100-200%",      (s >= 100) & (s < 200)),
        (">=200%",        s >= 200),
    ]
    for label, mask in buckets:
        n = mask.sum()
        pct = n / len(df) * 100 if len(df) else 0
        bar = "█" * int(pct / 2)
        print(f"  {label:>10}: n={n:>3} ({pct:>4.0f}%) {bar}")

    n_beat_topix = (s > TOPIX_3Y_RETURN).sum()
    n_doubled = (s >= 100).sum()
    n_negative = (s < 0).sum()
    print(f"\n  beat TOPIX (+{TOPIX_3Y_RETURN:.0f}%): {n_beat_topix}/{len(df)} ({n_beat_topix/len(df)*100:.0f}%)")
    print(f"  doubled (>=100%):    {n_doubled}/{len(df)} ({n_doubled/len(df)*100:.0f}%)")
    print(f"  negative:            {n_negative}/{len(df)} ({n_negative/len(df)*100:.0f}%)")

    print(f"\nFull list (sorted by return):")
    print(f"  {'rank':>4}  {'code':<6}  {'name':<24}  {'ratio':>5}  {'PER':>5}  {'MC ¥B':>6}  {'return':>8}")
    for i, row in df.iterrows():
        print(
            f"  {i+1:>4}  {row['code']:<6}  {row['name'][:24]:<24}  "
            f"{row['ratio']:>5.2f}  {row['per']:>5.1f}  {row['mc_bn']:>6.1f}  {row['ret_pct']:>+7.1f}%"
        )


def strategy_top10_by_ratio(u):
    s = u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
        & (u["ratio"] > 1.5) & (u["per"] > 0) & (u["per"] <= 10)]
    return s.nlargest(10, "ratio")


def strategy_sales_growth(u):
    s = u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
        & (u["ratio"] > 1.5) & (u["per"] > 0) & (u["per"] <= 10)]
    return s[s["sales_yoy"] > 0.05]


def main():
    engine = db.get_engine()
    print(f"Building universe at {ENTRY_DATE}, exit by {EXIT_DEADLINE}...")
    u = build_universe(engine)
    print(f"  {len(u)} stocks in raw universe")
    returns = compute_returns(engine, u["sec_code"].tolist(), u)
    names = fetch_names(engine, u["sec_code"].tolist())

    report("top10_by_ratio (concentrated, sweet-spot top 10 by ratio)",
           strategy_top10_by_ratio(u), returns, names)
    report("sales_growth (sweet-spot + sales_yoy > 5%)",
           strategy_sales_growth(u), returns, names)

    print(f"\n{'='*100}")
    print(f"Reference: TOPIX 3-yr return = +{TOPIX_3Y_RETURN}%")
    print(f"{'='*100}\n")


if __name__ == "__main__":
    main()
