"""Per-stock 3-year returns for the 2022 cohort matching:
   - market_cap on 2022-12-30 between ¥15B and ¥50B
   - net-cash ratio > 1.5 at entry
   - exit when ratio drops below 1.0, or hold to 2025-12-30

Same exit semantics as cross_table_netcash_2022.py — just emits the
per-stock list instead of bucketing.
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
EXIT_DEADLINE = date(2025, 12, 30)

MIN_MC = 15_000_000_000
MAX_MC = 50_000_000_000
ENTRY_THRESHOLD = 1.5
EXIT_THRESHOLD = 1.0
HAIRCUT = 0.7


def main():
    engine = db.get_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH visible AS (
                    SELECT fa."secCode" AS sec_code, fa.period_end,
                           fa.current_assets, fa.total_liabilities, fa.investment_securities,
                           fa.source_doc_id
                    FROM t_financials_annual fa
                    JOIN t_doc_list dl ON dl."docID" = fa.source_doc_id
                    WHERE dl."submitDateTime"::date <= :entry
                      AND fa.current_assets IS NOT NULL
                      AND fa.total_liabilities IS NOT NULL
                ),
                latest AS (
                    SELECT DISTINCT ON (sec_code) *
                    FROM visible ORDER BY sec_code, period_end DESC
                )
                SELECT l.sec_code,
                       COALESCE(em."issuerNameJP", dl_e."filerName") AS company_name,
                       l.current_assets, l.total_liabilities, l.investment_securities,
                       l.period_end,
                       d.adj_close AS entry_price,
                       d."marketCap" AS entry_mc
                FROM latest l
                JOIN t_doc_list dl_e ON dl_e."docID" = l.source_doc_id
                LEFT JOIN t_edinet_code_mappings em ON em."edinetCode" = dl_e."edinetCode"
                JOIN t_daily_stock_perf d
                  ON d."ShokenCode" = l.sec_code AND d."Date" = :entry
                WHERE d.adj_close IS NOT NULL AND d."marketCap" BETWEEN :min_mc AND :max_mc
                """
            ),
            {"entry": ENTRY_DATE, "min_mc": MIN_MC, "max_mc": MAX_MC},
        ).fetchall()

    universe = pd.DataFrame(
        rows,
        columns=["sec_code", "company_name", "current_assets", "total_liabilities",
                 "investment_securities", "period_end", "entry_price", "entry_mc"],
    )
    for c in ("current_assets", "total_liabilities", "investment_securities", "entry_price", "entry_mc"):
        universe[c] = universe[c].astype(float)
    universe["numerator"] = (
        universe["current_assets"]
        - universe["total_liabilities"]
        + HAIRCUT * universe["investment_securities"].fillna(0)
    )
    universe["entry_ratio"] = universe["numerator"] / universe["entry_mc"]
    qualifying = universe[universe["entry_ratio"] > ENTRY_THRESHOLD].reset_index(drop=True)
    print(f"{len(qualifying)} stocks qualify at entry (mc ¥15-50B, ratio > {ENTRY_THRESHOLD})\n")

    sec_codes = qualifying["sec_code"].tolist()
    if not sec_codes:
        return

    with engine.connect() as conn:
        prices = pd.DataFrame(
            conn.execute(
                text(
                    'SELECT "ShokenCode" AS sec_code, "Date" AS d, adj_close, "marketCap" '
                    'FROM t_daily_stock_perf '
                    'WHERE "ShokenCode" = ANY(:codes) AND "Date" BETWEEN :start AND :end '
                    'AND adj_close IS NOT NULL AND "marketCap" IS NOT NULL '
                    'ORDER BY "ShokenCode", "Date"'
                ),
                {"codes": sec_codes, "start": ENTRY_DATE, "end": EXIT_DEADLINE},
            ).fetchall(),
            columns=["sec_code", "d", "adj_close", "market_cap"],
        )
    prices["adj_close"] = prices["adj_close"].astype(float)
    prices["market_cap"] = prices["market_cap"].astype(float)

    with engine.connect() as conn:
        new_filings = pd.DataFrame(
            conn.execute(
                text(
                    """
                    SELECT fa."secCode" AS sec_code,
                           dl."submitDateTime"::date AS submit_date,
                           fa.current_assets, fa.total_liabilities, fa.investment_securities
                    FROM t_financials_annual fa
                    JOIN t_doc_list dl ON dl."docID" = fa.source_doc_id
                    WHERE fa."secCode" = ANY(:codes)
                      AND dl."submitDateTime"::date BETWEEN :start AND :end
                      AND fa.current_assets IS NOT NULL AND fa.total_liabilities IS NOT NULL
                    ORDER BY fa."secCode", dl."submitDateTime"
                    """
                ),
                {"codes": sec_codes, "start": ENTRY_DATE, "end": EXIT_DEADLINE},
            ).fetchall(),
            columns=["sec_code", "submit_date", "current_assets", "total_liabilities",
                     "investment_securities"],
        )
    for c in ("current_assets", "total_liabilities", "investment_securities"):
        new_filings[c] = new_filings[c].astype(float)

    init = qualifying.set_index("sec_code")
    fbs = {c: g.sort_values("submit_date") for c, g in new_filings.groupby("sec_code")}
    pbs = {c: g.sort_values("d") for c, g in prices.groupby("sec_code")}

    results = []
    for sec_code, row in init.iterrows():
        sp = pbs.get(sec_code)
        if sp is None or sp.empty:
            continue
        numerator = row["numerator"]
        sf = fbs.get(sec_code, pd.DataFrame())
        exit_d, exit_p, reason = None, None, "deadline"
        for _, pr in sp.iterrows():
            if not sf.empty:
                applicable = sf[sf["submit_date"] <= pr["d"]]
                if not applicable.empty:
                    last = applicable.iloc[-1]
                    numerator = (
                        last["current_assets"]
                        - last["total_liabilities"]
                        + HAIRCUT * (last["investment_securities"] or 0)
                    )
            ratio = numerator / pr["market_cap"] if pr["market_cap"] > 0 else 0
            if pr["d"] >= ENTRY_DATE and ratio < EXIT_THRESHOLD:
                exit_d, exit_p, reason = pr["d"], pr["adj_close"], "ratio<1"
                break
        if exit_d is None:
            last = sp.iloc[-1]
            exit_d, exit_p = last["d"], last["adj_close"]
        results.append({
            "sec_code": sec_code,
            "company": row["company_name"],
            "entry_ratio": round(row["entry_ratio"], 2),
            "entry_mc_bn": round(row["entry_mc"] / 1e9, 1),
            "entry_price": round(row["entry_price"], 0),
            "exit_date": exit_d,
            "exit_price": round(exit_p, 0),
            "days_held": (exit_d - ENTRY_DATE).days,
            "return_pct": round((exit_p / row["entry_price"] - 1) * 100, 1),
            "reason": reason,
        })

    df = pd.DataFrame(results).sort_values("return_pct", ascending=False)
    print(df.to_string(index=False))

    print(f"\nN = {len(df)}, mean return = {df['return_pct'].mean():.1f}%, "
          f"median return = {df['return_pct'].median():.1f}%")
    print(f"Held to deadline: {(df['reason']=='deadline').sum()}, "
          f"exited via ratio<1: {(df['reason']=='ratio<1').sum()}")
    print(f"Avg days held: {df['days_held'].mean():.0f} (max possible: 1096)")


if __name__ == "__main__":
    main()
