"""Attribution check on the low_float_with_value hybrid edge.

The sweep showed +29pp excess at 25% float threshold within sweet-spot.
But is the edge actually FROM the float signal, or could it be:
  (a) selection bias from smaller n,
  (b) "smaller stocks" effect (low-float correlates with smaller cap),
  (c) one stock (ソマール) appearing in many windows by coincidence?

This script controls for each:
  1. random_pct_X — random subset of size matching float_pct cohort
  2. lowest_mc_X — same subset size but selected by smallest mc within sweet
  3. lowest_float_X — the actual hybrid
  4. top10_ratio   — concentrated value baseline
  5. baseline_sweet — full sweet-spot

Plus: per-pick distribution dump showing how often individual stocks
recur, to test single-stock dependence.
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
    date(2022, 12, 30), date(2023, 3, 31), date(2023, 6, 30), date(2023, 9, 29),
    date(2023, 12, 29), date(2024, 3, 29), date(2024, 6, 28), date(2024, 9, 30),
    date(2024, 12, 30), date(2025, 3, 31),
]
HOLD_DAYS = 365
DATA_END = date(2026, 4, 24)
HAIRCUT = 0.7
TOPIX_CODE = "13060"
SEED = 42


def build_universe(engine, entry):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH visible AS (
                    SELECT fa."secCode" sec_code, fa.period_end,
                           fa.current_assets, fa.total_liabilities,
                           fa.investment_securities, fa.net_income,
                           fa.issued_shares, fa.source_doc_id
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
    cols = ["sec_code", "current_assets", "total_liabilities", "investment_securities",
            "net_income", "issued_shares", "price_now", "mc"]
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
        start_d = conn.execute(
            text('SELECT MIN("Date") FROM t_daily_stock_perf WHERE "Date" >= :d'),
            {"d": entry}).scalar()
        end_d = conn.execute(
            text('SELECT MAX("Date") FROM t_daily_stock_perf WHERE "Date" <= :d'),
            {"d": deadline}).scalar()
        prices = pd.DataFrame(
            conn.execute(
                text('SELECT "ShokenCode" sec_code, "Date" d, adj_close '
                     'FROM t_daily_stock_perf WHERE "ShokenCode" = ANY(:codes) '
                     'AND "Date" IN (:s, :e) AND adj_close IS NOT NULL'),
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
        out[sec] = (float(g.iloc[-1]["adj_close"]) / float(g.iloc[0]["adj_close"]) - 1) * 100
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


def fetch_names(engine, codes):
    with engine.connect() as conn:
        rows = conn.execute(
            text('SELECT DISTINCT ON ("secCode") "secCode", "filerName" '
                 'FROM t_doc_list WHERE "secCode" = ANY(:codes) '
                 'ORDER BY "secCode", "submitDateTime" DESC'),
            {"codes": list(codes)}).fetchall()
    return {c: n for c, n in rows}


def sweet_spot(u, cap_band=(3e9, 30e9)):
    cap_min, cap_max = cap_band
    return u[(u["mc"] >= cap_min) & (u["mc"] <= cap_max)
            & (u["ratio"] > 1.5) & (u["per"] > 0) & (u["per"] <= 10)
            ].dropna(subset=["issued_shares"])


def bootstrap_ci(arr, n_boot=10000, seed=42):
    if len(arr) < 2:
        return (np.nan, np.nan)
    rng = np.random.default_rng(seed)
    means = [rng.choice(arr, size=len(arr), replace=True).mean() for _ in range(n_boot)]
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def run_strategy(per_entry, pick_fn, name):
    """Apply pick_fn at each entry; return per-entry stats + all picks."""
    per_entry_stats = []
    pick_log = []  # list of (entry, sec_code, return_pct)
    for entry, ctx in per_entry.items():
        u = ctx["u"]; rets = ctx["rets"]; topix = ctx["topix"]
        picks = pick_fn(u, entry)
        if picks is None or picks.empty:
            continue
        codes = picks["sec_code"].tolist()
        pr = [(c, rets[c]) for c in codes if c in rets]
        if not pr:
            continue
        for c, r in pr:
            pick_log.append({"entry": entry, "sec_code": c, "ret": r})
        arr = np.array([r for _, r in pr])
        per_entry_stats.append({
            "entry": entry, "n": len(arr),
            "mean": arr.mean(), "excess": arr.mean() - topix,
            "median": float(np.median(arr)), "max": arr.max(), "min": arr.min(),
            "neg_pct": (arr < 0).mean() * 100,
        })
    if not per_entry_stats:
        return None
    df = pd.DataFrame(per_entry_stats)
    log = pd.DataFrame(pick_log)
    avg_excess = df["excess"].mean()
    win = (df["excess"] > 0).mean() * 100
    ci = bootstrap_ci(df["excess"].tolist())
    sharpe = avg_excess / df["mean"].std() if df["mean"].std() > 0 else float("nan")
    return {
        "name": name,
        "df": df,
        "log": log,
        "avg_n": df["n"].mean(),
        "avg_mean": df["mean"].mean(),
        "avg_excess": avg_excess,
        "ci_lo": ci[0], "ci_hi": ci[1],
        "win_rate": win,
        "avg_neg_pct": df["neg_pct"].mean(),
        "sharpe": sharpe,
    }


def main():
    engine = db.get_engine()
    print("Pre-computing per-entry data...")
    per_entry = {}
    for entry in ENTRIES:
        deadline = min(entry + timedelta(days=HOLD_DAYS), DATA_END)
        u = build_universe(engine, entry)
        rets = compute_returns(engine, u["sec_code"].tolist(), entry, deadline)
        topix = topix_return(engine, entry, deadline)
        per_entry[entry] = {"u": u, "rets": rets, "topix": topix}
    print(f"  Avg TOPIX across {len(ENTRIES)} entries: "
          f"{np.mean([ctx['topix'] for ctx in per_entry.values()]):+.1f}%\n")

    rng = np.random.default_rng(SEED)

    # Strategy factories
    def f_lowest_float(pct):
        def go(u, _entry):
            s = sweet_spot(u)
            if s.empty:
                return s
            n = max(1, int(len(s) * pct))
            return s.nsmallest(n, "issued_shares")
        return go

    def f_lowest_mc(pct):
        def go(u, _entry):
            s = sweet_spot(u)
            if s.empty:
                return s
            n = max(1, int(len(s) * pct))
            return s.nsmallest(n, "mc")
        return go

    def f_random_subset(pct):
        def go(u, _entry):
            s = sweet_spot(u)
            if s.empty:
                return s
            n = max(1, int(len(s) * pct))
            return s.sample(n=min(n, len(s)), random_state=rng.integers(0, 2**31))
        return go

    def f_top10_ratio(_pct):
        def go(u, _entry):
            s = sweet_spot(u, cap_band=(3e9, 50e9))
            return s.nlargest(10, "ratio")
        return go

    def f_baseline(_pct):
        def go(u, _entry):
            return sweet_spot(u, cap_band=(3e9, 50e9))
        return go

    strategies = []
    for pct in (0.25, 0.33, 0.50):
        strategies += [
            (f"lowest_float_{int(pct*100)}", f_lowest_float(pct)),
            (f"lowest_mc_{int(pct*100)}", f_lowest_mc(pct)),
            (f"random_{int(pct*100)}", f_random_subset(pct)),
        ]
    strategies += [
        ("top10_by_ratio", f_top10_ratio(0)),
        ("baseline_sweet_3-50B", f_baseline(0)),
    ]

    print("=" * 130)
    print("ATTRIBUTION: float vs mc vs random vs ratio-based, all within sweet-spot 3-30B (or 3-50B for last two)")
    print("=" * 130)
    print(
        f"  {'strategy':<24}  {'avg_n':>5} {'avg_mean':>9} {'avg_excess':>11} {'95%CI':>22}"
        f"  {'win':>4}  {'avg_neg':>8} {'sharpe':>7}"
    )
    print("-" * 130)
    results = {}
    for name, fn in strategies:
        r = run_strategy(per_entry, fn, name)
        if r is None:
            continue
        results[name] = r
        print(
            f"  {r['name']:<24}  {r['avg_n']:>5.1f} {r['avg_mean']:>+8.1f}% {r['avg_excess']:>+10.1f}% "
            f"  [{r['ci_lo']:>+5.1f}, {r['ci_hi']:>+5.1f}]   "
            f"{r['win_rate']:>3.0f}%  {r['avg_neg_pct']:>7.1f}% {r['sharpe']:>+7.2f}"
        )

    # Recurrence analysis: at lowest_float_33, how concentrated is the strategy on a few stocks?
    print("\n\n" + "=" * 130)
    print("RECURRENCE: how often each pick appears across 10 entries (lowest_float_33)")
    print("=" * 130)
    log = results["lowest_float_33"]["log"]
    all_codes = log["sec_code"].unique()
    names = fetch_names(engine, list(all_codes))
    counts = (
        log.groupby("sec_code")
        .agg(times_picked=("ret", "size"), avg_ret=("ret", "mean"),
             min_ret=("ret", "min"), max_ret=("ret", "max"))
        .sort_values("times_picked", ascending=False)
    )
    print(f"  Total picks across 10 entries: {len(log)}")
    print(f"  Unique stocks: {len(counts)}")
    print(f"  {'code':<6} {'name':<28} {'times':>5} {'avg_ret':>8} {'min':>7} {'max':>7}")
    for code, row in counts.iterrows():
        print(
            f"  {code:<6} {names.get(code, '')[:28]:<28} {int(row['times_picked']):>5} "
            f"{row['avg_ret']:>+7.1f}% {row['min_ret']:>+6.1f}% {row['max_ret']:>+6.1f}%"
        )

    # Holdout: re-run with the most-recurring stock removed from universe
    print("\n\n" + "=" * 130)
    print("HOLDOUT: lowest_float_33 with the single most-recurring stock removed")
    print("=" * 130)
    top_recurring = counts.index[0]
    print(f"  Excluding: {top_recurring}  {names.get(top_recurring, '')}")
    def f_lowest_float_excl(pct, exclude_code):
        def go(u, _entry):
            s = sweet_spot(u)
            s = s[s["sec_code"] != exclude_code]
            if s.empty:
                return s
            n = max(1, int(len(s) * pct))
            return s.nsmallest(n, "issued_shares")
        return go
    r_holdout = run_strategy(per_entry, f_lowest_float_excl(0.33, top_recurring),
                             f"lowest_float_33_excl_{top_recurring}")
    print(
        f"  avg_n={r_holdout['avg_n']:.1f}  avg_mean={r_holdout['avg_mean']:+.1f}%  "
        f"avg_excess={r_holdout['avg_excess']:+.1f}%  "
        f"CI[{r_holdout['ci_lo']:+.1f}, {r_holdout['ci_hi']:+.1f}]  "
        f"win={r_holdout['win_rate']:.0f}%  sharpe={r_holdout['sharpe']:+.2f}"
    )

    # Per-pick distribution dump for the most concentrated robust threshold
    print("\n\n" + "=" * 130)
    print("PER-PICK DISTRIBUTION across all 10 entries (lowest_float_33, cap 3-30B)")
    print("=" * 130)
    rets_arr = log["ret"].values
    s = pd.Series(rets_arr)
    print(f"  total picks: {len(s)}")
    print(f"  mean {s.mean():+.1f}%  median {s.median():+.1f}%  stdev {s.std():.1f}%")
    print(f"  P10 {s.quantile(0.10):+.1f}  P25 {s.quantile(0.25):+.1f}  "
          f"P75 {s.quantile(0.75):+.1f}  P90 {s.quantile(0.90):+.1f}")
    print(f"  min {s.min():+.1f}  max {s.max():+.1f}  skew {s.skew():.2f}")
    buckets = [
        ("<-25%",     s < -25),
        ("-25 to 0",  (s >= -25) & (s < 0)),
        ("0 to 25",   (s >= 0) & (s < 25)),
        ("25 to 50",  (s >= 25) & (s < 50)),
        ("50 to 100", (s >= 50) & (s < 100)),
        (">=100",     s >= 100),
    ]
    for label, mask in buckets:
        n = mask.sum()
        pct = n / len(s) * 100
        bar = "█" * int(pct / 2)
        print(f"  {label:>10}: n={n:>3} ({pct:>4.0f}%) {bar}")


if __name__ == "__main__":
    main()
