"""Battery of strategy variants tested on the same 3-year window
(entry 2022-12-30, exit on ratio<1 or 2025-12-30 deadline).

Each strategy is a function that filters a pre-computed universe of
stocks into a sub-universe of "picks." The lab runs all of them on
the same data, applies the same exit rule, and reports mean/median
returns + hit rate vs TOPIX (+81% benchmark).

The point: which combinations of value / quality / momentum signals
genuinely beat the market when applied uniformly over a 3-year hold?
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

ENTRY_DATE = date(2022, 12, 30)
EXIT_DEADLINE = date(2025, 12, 30)
HAIRCUT = 0.7
EXIT_RATIO = 1.0
TOPIX_3Y_RETURN = 81.2  # measured from 13060 over the window


@dataclass
class Strategy:
    name: str
    desc: str
    filter_fn: Callable[[pd.DataFrame], pd.DataFrame]


# ------------- universe build (entry-time snapshot of all stocks) -------------

def build_universe(engine) -> pd.DataFrame:
    """One row per stock with all entry-time fundamentals + market cap +
    derived signals. Doesn't apply any filters except 'has data'."""
    print(f"Building universe at {ENTRY_DATE}...")

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
                "entry": ENTRY_DATE,
                "six_mo_start": ENTRY_DATE - timedelta(days=200),
                "six_mo_end": ENTRY_DATE - timedelta(days=160),
                "twelve_mo_start": ENTRY_DATE - timedelta(days=380),
                "twelve_mo_end": ENTRY_DATE - timedelta(days=350),
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

    # Derived signals
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

    print(f"  {len(df)} stocks in raw universe")
    return df


# ------------- compute returns once for the universe -------------

def compute_returns(engine, sec_codes: list[str], universe: pd.DataFrame) -> dict[str, dict]:
    """Walk forward day-by-day for each stock; exit at ratio<1 (using
    static entry-numerator vs daily market cap) or at deadline."""
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
        out[sec_code] = {
            "exit_d": exit_d,
            "exit_p": exit_p,
            "return_pct": (exit_p / ent_price - 1) * 100,
            "days_held": (exit_d - ENTRY_DATE).days,
        }
    return out


# ------------- strategies -------------

def strategy_baseline_sweet_spot(u: pd.DataFrame) -> pd.DataFrame:
    return u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
            & (u["ratio"] > 1.5) & (u["per"] > 0) & (u["per"] <= 10)]


def strategy_top10_by_ratio(u: pd.DataFrame) -> pd.DataFrame:
    """Concentrated: top 10 by ratio within sweet-spot."""
    return strategy_baseline_sweet_spot(u).nlargest(10, "ratio")


def strategy_value_momentum(u: pd.DataFrame) -> pd.DataFrame:
    """Sweet-spot + positive 6m price momentum."""
    s = strategy_baseline_sweet_spot(u)
    return s[s["mom_6m"] > 0]


def strategy_value_quality(u: pd.DataFrame) -> pd.DataFrame:
    """Sweet-spot + op margin > 10% (real profitability)."""
    s = strategy_baseline_sweet_spot(u)
    return s[s["op_margin"] > 0.10]


def strategy_sales_growth(u: pd.DataFrame) -> pd.DataFrame:
    """Sweet-spot + sales accelerating > 5% YoY."""
    s = strategy_baseline_sweet_spot(u)
    return s[s["sales_yoy"] > 0.05]


def strategy_cash_compounders(u: pd.DataFrame) -> pd.DataFrame:
    """Net-cash growing organically: ratio>1 + current_assets up YoY +
    sales up YoY (cash piling up while business expands)."""
    s = u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
         & (u["ratio"] > 1.0) & (u["assets_yoy"] > 0.05) & (u["sales_yoy"] > 0)
         & (u["per"] > 0) & (u["per"] <= 15)]
    return s


def strategy_qmom(u: pd.DataFrame) -> pd.DataFrame:
    """Quality + Momentum, no value filter. op_yield > 10%, mom_6m > 10%."""
    return u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
           & (u["op_yield"] > 0.10) & (u["mom_6m"] > 0.10)
           & (u["per"] > 0) & (u["per"] <= 15)]


def strategy_reversal(u: pd.DataFrame) -> pd.DataFrame:
    """Big 12m drawdown but business holding up: ratio>1, mom_12m<-20%,
    sales_yoy > -5%, op_margin > 0."""
    return u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
           & (u["ratio"] > 1.0) & (u["mom_12m"] < -0.20)
           & (u["sales_yoy"] > -0.05) & (u["op_margin"] > 0)]


def strategy_z_composite(u: pd.DataFrame) -> pd.DataFrame:
    """Top decile by z-score of (ratio + op_yield + sales_yoy + mom_6m).
    Multi-factor blend within sweet-spot universe."""
    s = strategy_baseline_sweet_spot(u).copy()
    if s.empty:
        return s
    for col in ("ratio", "op_yield", "sales_yoy", "mom_6m"):
        s[f"z_{col}"] = (s[col] - s[col].mean()) / s[col].std()
    s["z_total"] = s[["z_ratio", "z_op_yield", "z_sales_yoy", "z_mom_6m"]].sum(axis=1)
    return s.nlargest(max(10, int(len(s) * 0.20)), "z_total")


def strategy_low_per_top10(u: pd.DataFrame) -> pd.DataFrame:
    """Top 10 lowest PER within net-cash + cap range. Pure cheapness."""
    s = u[(u["mc"] >= 3e9) & (u["mc"] <= 50e9)
        & (u["ratio"] > 1.0) & (u["per"] > 0) & (u["per"] <= 5)]
    return s.nsmallest(10, "per")


STRATEGIES = [
    Strategy("baseline_sweet_spot",
             "¥3-50B + ratio>1.5 + PER≤10",
             strategy_baseline_sweet_spot),
    Strategy("top10_by_ratio",
             "Top 10 by ratio within sweet-spot (concentrated)",
             strategy_top10_by_ratio),
    Strategy("value_momentum",
             "Sweet-spot + 6m_mom > 0 (price catalyst)",
             strategy_value_momentum),
    Strategy("value_quality",
             "Sweet-spot + op_margin > 10% (real profitability)",
             strategy_value_quality),
    Strategy("sales_growth",
             "Sweet-spot + sales_yoy > 5% (top-line accelerating)",
             strategy_sales_growth),
    Strategy("cash_compounders",
             "ratio>1 + current_assets growing + sales growing",
             strategy_cash_compounders),
    Strategy("qmom",
             "Quality+Momentum (no value): op_yield>10%, mom_6m>10%",
             strategy_qmom),
    Strategy("reversal",
             "Down 20%+ in 12m but biz OK: mean reversion candidates",
             strategy_reversal),
    Strategy("z_composite",
             "Top 20% by composite z-score (ratio+op_yld+sales_yoy+mom)",
             strategy_z_composite),
    Strategy("low_per_top10",
             "Top 10 lowest PER (≤5) with ratio>1",
             strategy_low_per_top10),
]


def main():
    engine = db.get_engine()
    u = build_universe(engine)
    sec_codes = u["sec_code"].tolist()
    returns = compute_returns(engine, sec_codes, u)
    print(f"  {len(returns)} stocks have full price history\n")

    print(f"{'strategy':<26}  {'n':>4}  {'mean':>7} {'median':>7} {'>TOPIX':>7} {'>0%':>5}  description")
    print("-" * 130)
    for strat in STRATEGIES:
        picks = strat.filter_fn(u)
        rets = []
        for code in picks["sec_code"]:
            if code in returns:
                rets.append(returns[code]["return_pct"])
        if not rets:
            print(f"{strat.name:<26}  {0:>4}  {'-':>7} {'-':>7} {'-':>7} {'-':>5}  {strat.desc}")
            continue
        s = pd.Series(rets)
        beat_topix = (s > TOPIX_3Y_RETURN).mean() * 100
        positive = (s > 0).mean() * 100
        print(
            f"{strat.name:<26}  {len(rets):>4}  {s.mean():>+6.1f}% {s.median():>+6.1f}% "
            f"{beat_topix:>5.0f}% {positive:>4.0f}%  {strat.desc}"
        )

    print()
    print(f"TOPIX 3-year (2022-12-30 → 2025-12-30): +{TOPIX_3Y_RETURN}%")


if __name__ == "__main__":
    main()
