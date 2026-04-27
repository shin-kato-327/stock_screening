"""Monte Carlo validation of the low_float edge + current picks.

The attribution script showed lowest_float_33 has +24.5pp excess vs
lowest_mc_33 (+6.3pp) and random_33 (+16.4pp from a single seed).
Single-seed random is noisy. This script runs 500 random subsets per
entry and reports the empirical distribution — i.e., what percentile
does lowest_float fall in vs random selection of the same size?

Also emits the current picks (today's universe) at the recommended
threshold so the user has actionable output.
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
N_RANDOM = 500


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


def build_universe_today(engine, entry):
    """Like build_universe but doesn't require entry-day price (for live
    screening)."""
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
                       l.period_end,
                       d_now.adj_close price_now, d_now."marketCap" mc, d_now."Date" pd
                FROM latest l
                JOIN LATERAL (
                    SELECT adj_close, "marketCap", "Date" FROM t_daily_stock_perf p
                    WHERE p."ShokenCode" = l.sec_code AND p."Date" <= :entry
                      AND p.adj_close IS NOT NULL AND p."marketCap" IS NOT NULL
                    ORDER BY p."Date" DESC LIMIT 1
                ) d_now ON TRUE
                """
            ),
            {"entry": entry},
        ).fetchall()
    cols = ["sec_code", "current_assets", "total_liabilities", "investment_securities",
            "net_income", "issued_shares", "period_end",
            "price_now", "mc", "price_date"]
    df = pd.DataFrame(rows, columns=cols)
    for c in ("current_assets", "total_liabilities", "investment_securities",
              "net_income", "issued_shares", "price_now", "mc"):
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


def main():
    engine = db.get_engine()
    print("Pre-computing per-entry data...")
    per_entry = {}
    for entry in ENTRIES:
        deadline = min(entry + timedelta(days=HOLD_DAYS), DATA_END)
        u = build_universe(engine, entry)
        rets = compute_returns(engine, u["sec_code"].tolist(), entry, deadline)
        topix = topix_return(engine, entry, deadline)
        per_entry[entry] = {"u": u, "rets": rets, "topix": topix, "deadline": deadline}

    # Monte Carlo: at each threshold, run lowest_float once + random N_RANDOM times
    print("\n" + "=" * 130)
    print(f"MONTE CARLO: lowest_float vs random subset of same size ({N_RANDOM} samples per entry)")
    print("=" * 130)
    rng = np.random.default_rng(42)
    for pct in (0.25, 0.33, 0.50):
        # actual lowest_float per-entry mean excess
        lf_per_entry = []
        rand_per_entry = []  # list of arrays (N_RANDOM samples per entry)
        for entry, ctx in per_entry.items():
            sweet = sweet_spot(ctx["u"])
            if sweet.empty:
                continue
            n = max(1, int(len(sweet) * pct))
            picks = sweet.nsmallest(n, "issued_shares")
            pr = [ctx["rets"][c] for c in picks["sec_code"] if c in ctx["rets"]]
            if not pr:
                continue
            lf_per_entry.append(np.mean(pr) - ctx["topix"])

            # random samples
            sweet_with_rets = sweet[sweet["sec_code"].isin(ctx["rets"].keys())]
            sweet_rets = sweet_with_rets["sec_code"].map(ctx["rets"]).values
            if len(sweet_rets) < n:
                continue
            samples = []
            for _ in range(N_RANDOM):
                idx = rng.choice(len(sweet_rets), size=n, replace=False)
                samples.append(sweet_rets[idx].mean() - ctx["topix"])
            rand_per_entry.append(np.array(samples))

        # actual: average across entries
        lf_mean = np.mean(lf_per_entry)

        # random null distribution: average across entries, per draw
        if len(rand_per_entry) == len(lf_per_entry):
            null_means = np.mean(np.stack(rand_per_entry), axis=0)
            pct_above = (null_means >= lf_mean).mean() * 100
            null_lo = np.percentile(null_means, 2.5)
            null_hi = np.percentile(null_means, 97.5)
            null_mean = null_means.mean()
            print(
                f"\n  threshold={pct*100:.0f}%  n_per_entry≈{int(len(lf_per_entry) and pct * 50):>2}"
            )
            print(f"    lowest_float avg excess: {lf_mean:+6.1f}pp")
            print(f"    random null mean:        {null_mean:+6.1f}pp  "
                  f"(95% range: [{null_lo:+5.1f}, {null_hi:+5.1f}])")
            print(f"    p-value (random ≥ float): {pct_above/100:.3f}  "
                  f"({pct_above:.1f}% of random draws beat lowest_float)")

    # Current picks at recommended threshold (33% within sweet-spot, cap 3-30B)
    print("\n\n" + "=" * 130)
    print("CURRENT PICKS (today's universe at recommended threshold = 33% within sweet-spot 3-30B)")
    print("=" * 130)
    today = date.today()
    u_now = build_universe_today(engine, today)
    sweet_now = sweet_spot(u_now, cap_band=(3e9, 30e9))
    if not sweet_now.empty:
        n_low = max(1, int(len(sweet_now) * 0.33))
        picks_now = sweet_now.nsmallest(n_low, "issued_shares").copy()
        names_now = fetch_names(engine, picks_now["sec_code"].tolist())
        picks_now["name"] = picks_now["sec_code"].map(names_now)
        picks_now["mc_bn"] = picks_now["mc"] / 1e9
        picks_now["shares_M"] = picks_now["issued_shares"] / 1e6
        picks_now = picks_now.sort_values("issued_shares")
        print(f"  Sweet-spot universe today: {len(sweet_now)}  →  picking lowest-float {n_low}")
        print(f"  {'code':<6} {'name':<32} {'ratio':>5} {'PER':>5} {'MC ¥B':>6} {'shares M':>9} {'price':>8} {'period':>11}")
        for _, r in picks_now.iterrows():
            print(
                f"  {r['sec_code']:<6} {(r['name'] or '')[:32]:<32} "
                f"{r['ratio']:>5.2f} {r['per']:>5.1f} {r['mc_bn']:>6.1f} "
                f"{r['shares_M']:>9.2f} ¥{r['price_now']:>7.0f} {str(r['period_end']):>11}"
            )

    # Also at 50% threshold for those wanting more diversification
    print("\n  Also at 50% threshold (n=5-7 typical, more diversified):")
    if not sweet_now.empty:
        n_med = max(1, int(len(sweet_now) * 0.50))
        picks_50 = sweet_now.nsmallest(n_med, "issued_shares").copy()
        names_50 = fetch_names(engine, picks_50["sec_code"].tolist())
        picks_50["name"] = picks_50["sec_code"].map(names_50)
        picks_50["mc_bn"] = picks_50["mc"] / 1e9
        picks_50["shares_M"] = picks_50["issued_shares"] / 1e6
        picks_50 = picks_50.sort_values("issued_shares")
        for _, r in picks_50.iterrows():
            print(
                f"  {r['sec_code']:<6} {(r['name'] or '')[:32]:<32} "
                f"{r['ratio']:>5.2f} {r['per']:>5.1f} {r['mc_bn']:>6.1f} "
                f"{r['shares_M']:>9.2f}M shares"
            )


if __name__ == "__main__":
    main()
