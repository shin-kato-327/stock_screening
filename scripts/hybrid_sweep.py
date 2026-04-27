"""Threshold sweep on the low_float_with_value hybrid strategy.
10 quarterly entry dates × 7 float thresholds × 12-month buy-and-hold.

Goal: find the diversification-vs-concentration sweet spot for the
hybrid (sweet-spot net-cash + bottom X% by issued shares within that
subset). Single-cohort showed +32pp excess at 25%; need to confirm
across many entries and pick a robust threshold.

Diagnostics beyond mean return:
- Per-entry mean stability (is edge consistent or driven by 1-2 windows?)
- Concentration check (does the strategy lean on outlier picks?)
- Bootstrapped confidence on average excess
- Cohort-by-cohort win rate vs TOPIX
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stock_screening import db

ENTRIES = [
    date(2022, 12, 30),
    date(2023, 3, 31),
    date(2023, 6, 30),
    date(2023, 9, 29),
    date(2023, 12, 29),
    date(2024, 3, 29),
    date(2024, 6, 28),
    date(2024, 9, 30),
    date(2024, 12, 30),
    date(2025, 3, 31),
]
HOLD_DAYS = 365
DATA_END = date(2026, 4, 24)
HAIRCUT = 0.7
TOPIX_CODE = "13060"

FLOAT_THRESHOLDS = [0.10, 0.25, 0.33, 0.50, 0.75, 1.00]  # 1.0 = no float filter
# Cap bands to test (min, max) in JPY
CAP_BANDS = {
    "3-30B": (3e9, 30e9),
    "3-50B": (3e9, 50e9),
    "3-15B": (3e9, 15e9),
}


def build_universe(engine, entry):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH visible AS (
                    SELECT fa."secCode" sec_code, fa.period_end,
                           fa.current_assets, fa.total_liabilities,
                           fa.investment_securities,
                           fa.net_income, fa.issued_shares,
                           fa.source_doc_id
                    FROM t_financials_annual fa
                    JOIN t_doc_list dl ON dl."docID" = fa.source_doc_id
                    WHERE dl."submitDateTime"::date <= :entry
                      AND fa.current_assets IS NOT NULL
                ),
                latest AS (
                    SELECT DISTINCT ON (sec_code) * FROM visible
                    ORDER BY sec_code, period_end DESC
                )
                SELECT l.sec_code, l.current_assets, l.total_liabilities,
                       l.investment_securities, l.net_income, l.issued_shares,
                       d_now.adj_close price_now, d_now."marketCap" mc
                FROM latest l
                JOIN t_daily_stock_perf d_now
                  ON d_now."ShokenCode" = l.sec_code AND d_now."Date" = :entry
                  AND d_now.adj_close IS NOT NULL AND d_now."marketCap" IS NOT NULL
                """
            ),
            {"entry": entry},
        ).fetchall()
    cols = [
        "sec_code", "current_assets", "total_liabilities", "investment_securities",
        "net_income", "issued_shares", "price_now", "mc",
    ]
    df = pd.DataFrame(rows, columns=cols)
    for c in cols[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ratio"] = (
        df["current_assets"] - df["total_liabilities"]
        + HAIRCUT * df["investment_securities"].fillna(0)
    ) / df["mc"]
    df["per"] = df["mc"] / df["net_income"]
    return df


def compute_returns(engine, codes, entry, deadline):
    with engine.connect() as conn:
        # Find first trading day at or after entry
        start_d = conn.execute(
            text('SELECT MIN("Date") FROM t_daily_stock_perf WHERE "Date" >= :d'),
            {"d": entry}).scalar()
        # Find last trading day at or before deadline
        end_d = conn.execute(
            text('SELECT MAX("Date") FROM t_daily_stock_perf WHERE "Date" <= :d'),
            {"d": deadline}).scalar()
        prices = pd.DataFrame(
            conn.execute(
                text(
                    'SELECT "ShokenCode" sec_code, "Date" d, adj_close '
                    'FROM t_daily_stock_perf WHERE "ShokenCode" = ANY(:codes) '
                    'AND "Date" IN (:s, :e) AND adj_close IS NOT NULL'
                ),
                {"codes": codes, "s": start_d, "e": end_d},
            ).fetchall(),
            columns=["sec_code", "d", "adj_close"],
        )
    prices["adj_close"] = prices["adj_close"].astype(float)
    out = {}
    for sec, g in prices.groupby("sec_code"):
        g = g.sort_values("d")
        if len(g) < 2:
            continue
        ent_p = float(g.iloc[0]["adj_close"])
        ext_p = float(g.iloc[-1]["adj_close"])
        out[sec] = (ext_p / ent_p - 1) * 100
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


def hybrid_picks(u, cap_band, float_pct):
    cap_min, cap_max = cap_band
    sweet = u[(u["mc"] >= cap_min) & (u["mc"] <= cap_max)
            & (u["ratio"] > 1.5) & (u["per"] > 0) & (u["per"] <= 10)
            ].dropna(subset=["issued_shares"])
    if sweet.empty:
        return sweet
    if float_pct >= 1.0:
        return sweet
    n = max(1, int(len(sweet) * float_pct))
    return sweet.nsmallest(n, "issued_shares")


def bootstrap_excess_ci(per_entry_excess, n_boot=10000, seed=42):
    """95% CI on the across-entries mean excess return via bootstrap."""
    arr = np.array(per_entry_excess)
    if len(arr) < 2:
        return (np.nan, np.nan)
    rng = np.random.default_rng(seed)
    means = []
    for _ in range(n_boot):
        sample = rng.choice(arr, size=len(arr), replace=True)
        means.append(sample.mean())
    return (float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5)))


def main():
    engine = db.get_engine()

    # Pre-compute per-entry universe + returns once (shared across thresholds)
    print("Pre-computing universes & returns for", len(ENTRIES), "entries...")
    per_entry: dict[date, dict] = {}
    for entry in ENTRIES:
        deadline = min(entry + timedelta(days=HOLD_DAYS), DATA_END)
        u = build_universe(engine, entry)
        rets = compute_returns(engine, u["sec_code"].tolist(), entry, deadline)
        topix = topix_return(engine, entry, deadline)
        per_entry[entry] = {"u": u, "rets": rets, "topix": topix, "deadline": deadline}
        n_visible = (u["sec_code"].isin(rets.keys())).sum()
        print(f"  {entry} → {deadline}  univ={len(u):>5}  with_ret={n_visible:>5}  topix={topix:+5.1f}%")

    # Sweep: cap_band × float_pct
    print("\n\n" + "=" * 130)
    print("HYBRID SWEEP: per-threshold aggregate across", len(ENTRIES), "quarterly entries (12-mo hold)")
    print("=" * 130)
    for band_name, band in CAP_BANDS.items():
        print(f"\n--- cap_band = {band_name} ---")
        print(
            f"{'float%':>8}  {'avg_n':>6}  {'avg_mean':>9} {'avg_excess':>11} "
            f"{'95%CI':>22}  {'win_rate':>9} {'avg_pos%':>9} {'avg_max':>9}  {'sharpe':>7}"
        )
        print("-" * 130)
        all_results = {}
        for fpct in FLOAT_THRESHOLDS:
            per_entry_stats = []
            all_picks = []
            for entry in ENTRIES:
                u = per_entry[entry]["u"]
                rets = per_entry[entry]["rets"]
                topix = per_entry[entry]["topix"]
                picks = hybrid_picks(u, band, fpct)
                pick_rets = [rets[c] for c in picks["sec_code"] if c in rets]
                if not pick_rets:
                    continue
                s = np.array(pick_rets)
                per_entry_stats.append({
                    "entry": entry,
                    "n": len(s),
                    "mean": s.mean(),
                    "excess": s.mean() - topix,
                    "max": s.max(),
                    "neg_pct": (s < 0).mean() * 100,
                })
                all_picks.extend(s.tolist())
            if not per_entry_stats:
                continue
            df = pd.DataFrame(per_entry_stats)
            avg_n = df["n"].mean()
            avg_mean = df["mean"].mean()
            avg_excess = df["excess"].mean()
            avg_max = df["max"].mean()
            avg_neg = df["neg_pct"].mean()
            avg_pos = 100 - avg_neg
            win_rate = (df["excess"] > 0).mean() * 100
            ci_lo, ci_hi = bootstrap_excess_ci(df["excess"].tolist())
            # Sharpe-like: avg_excess / stdev of per-entry means
            sharpe = avg_excess / df["mean"].std() if df["mean"].std() > 0 else float("nan")
            print(
                f"{fpct*100:>7.0f}%  {avg_n:>6.1f}  {avg_mean:>+8.1f}% {avg_excess:>+10.1f}% "
                f"  [{ci_lo:>+5.1f}, {ci_hi:>+5.1f}]   "
                f"{win_rate:>7.0f}% {avg_pos:>8.1f}% {avg_max:>+8.0f}%  {sharpe:>+7.2f}"
            )
            all_results[fpct] = {"per_entry": df, "all_picks": all_picks}

    # Drill: best threshold per cap band, show per-entry detail
    print("\n\n" + "=" * 130)
    print("DRILL: per-entry detail at the most concentrated threshold (25%) and broadest (100%) for cap=3-30B")
    print("=" * 130)
    for fpct in [0.25, 0.50, 1.00]:
        print(f"\n  --- float% = {fpct*100:.0f}%, cap=3-30B ---")
        print(f"  {'entry':<12} {'n':>3} {'mean':>7} {'topix':>7} {'excess':>7} {'max':>7} {'neg%':>5}  picks (top 3)")
        for entry in ENTRIES:
            u = per_entry[entry]["u"]
            rets = per_entry[entry]["rets"]
            topix = per_entry[entry]["topix"]
            picks = hybrid_picks(u, (3e9, 30e9), fpct)
            pr = [(c, rets[c]) for c in picks["sec_code"] if c in rets]
            if not pr:
                print(f"  {entry.isoformat():<12} {'(0)':>3} {'-':>7} {topix:>+6.1f}%")
                continue
            arr = np.array([r for _, r in pr])
            top3 = sorted(pr, key=lambda x: -x[1])[:3]
            top3s = " | ".join(f"{c}:{r:+.0f}%" for c, r in top3)
            print(
                f"  {entry.isoformat():<12} {len(arr):>3} {arr.mean():>+6.1f}% {topix:>+6.1f}% "
                f"{arr.mean()-topix:>+6.1f}% {arr.max():>+6.0f}% {(arr<0).mean()*100:>4.0f}%  {top3s}"
            )

    # Concentration check: is the edge driven by outliers?
    print("\n\n" + "=" * 130)
    print("CONCENTRATION CHECK (cap=3-30B, float%=25): excess vs TOPIX with/without max-return pick removed per entry")
    print("=" * 130)
    print(f"  {'entry':<12} {'n':>3} {'mean_all':>9} {'mean_excl_top':>13} {'topix':>7} {'edge w/top':>11} {'edge w/o top':>13}")
    for entry in ENTRIES:
        u = per_entry[entry]["u"]
        rets = per_entry[entry]["rets"]
        topix = per_entry[entry]["topix"]
        picks = hybrid_picks(u, (3e9, 30e9), 0.25)
        pr = [rets[c] for c in picks["sec_code"] if c in rets]
        if len(pr) < 2:
            continue
        arr = np.array(pr)
        without_max = arr[arr != arr.max()]
        print(
            f"  {entry.isoformat():<12} {len(arr):>3} {arr.mean():>+8.1f}% "
            f"{without_max.mean():>+12.1f}% {topix:>+6.1f}% "
            f"{arr.mean()-topix:>+10.1f}% {without_max.mean()-topix:>+12.1f}%"
        )


if __name__ == "__main__":
    main()
