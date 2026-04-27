"""3-year backtest matching the Shikiho-style criteria:
- market_cap on 2022-12-30 between ¥3B and ¥50B
- net-cash ratio >= 1.0 at entry
- PER (market_cap / net_income) <= 10 at entry
- exit when ratio drops below 1.0, or hold to 2025-12-30

Same exit semantics as cross_table_netcash_2022.py — just looser cap
floor + adds the PER filter to demand profitability.
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

MIN_MC = 3_000_000_000     # ¥3B
MAX_MC = 50_000_000_000    # ¥50B
ENTRY_RATIO_MIN = 1.0
EXIT_RATIO = 1.0
MAX_PER = 10.0
HAIRCUT = 0.7


def main():
    engine = db.get_engine()

    print(f"Building entry universe as of {ENTRY_DATE}...")
    print(f"  cap range: ¥{MIN_MC/1e9:.0f}B - ¥{MAX_MC/1e9:.0f}B")
    print(f"  ratio: >= {ENTRY_RATIO_MIN}, PER: <= {MAX_PER}")

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH visible AS (
                    SELECT fa."secCode" AS sec_code, fa.period_end,
                           fa.current_assets, fa.total_liabilities,
                           fa.investment_securities, fa.net_income,
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
                       COALESCE(em."issuerNameJP", dl_e."filerName") AS company,
                       l.current_assets, l.total_liabilities,
                       l.investment_securities, l.net_income, l.period_end,
                       d.adj_close AS entry_price, d."marketCap" AS entry_mc
                FROM latest l
                JOIN t_doc_list dl_e ON dl_e."docID" = l.source_doc_id
                LEFT JOIN t_edinet_code_mappings em ON em."edinetCode" = dl_e."edinetCode"
                JOIN t_daily_stock_perf d
                  ON d."ShokenCode" = l.sec_code AND d."Date" = :entry
                WHERE d.adj_close IS NOT NULL
                  AND d."marketCap" BETWEEN :min_mc AND :max_mc
                """
            ),
            {"entry": ENTRY_DATE, "min_mc": MIN_MC, "max_mc": MAX_MC},
        ).fetchall()

    universe = pd.DataFrame(
        rows,
        columns=["sec_code", "company", "current_assets", "total_liabilities",
                 "investment_securities", "net_income", "period_end", "entry_price", "entry_mc"],
    )
    for c in ("current_assets", "total_liabilities", "investment_securities",
              "net_income", "entry_price", "entry_mc"):
        universe[c] = pd.to_numeric(universe[c], errors="coerce")

    universe["numerator"] = (
        universe["current_assets"]
        - universe["total_liabilities"]
        + HAIRCUT * universe["investment_securities"].fillna(0)
    )
    universe["entry_ratio"] = universe["numerator"] / universe["entry_mc"]
    universe["per"] = universe["entry_mc"] / universe["net_income"]

    print(f"\n  {len(universe)} stocks ¥{MIN_MC/1e9:.0f}B-¥{MAX_MC/1e9:.0f}B with mart at entry")
    print(f"  {(universe['entry_ratio']>=ENTRY_RATIO_MIN).sum()} have ratio>={ENTRY_RATIO_MIN}")
    qualifying = universe[
        (universe["entry_ratio"] >= ENTRY_RATIO_MIN)
        & (universe["per"] > 0)
        & (universe["per"] <= MAX_PER)
    ].reset_index(drop=True)
    print(f"  {len(qualifying)} also have 0 < PER <= {MAX_PER}\n")

    if qualifying.empty:
        print("nothing qualifies — exiting")
        return

    # walk forward
    sec_codes = qualifying["sec_code"].tolist()
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

    with engine.connect() as conn:
        new_filings = pd.DataFrame(
            conn.execute(
                text(
                    """
                    SELECT fa."secCode" sec_code, dl."submitDateTime"::date submit_date,
                           fa.current_assets, fa.total_liabilities, fa.investment_securities
                    FROM t_financials_annual fa
                    JOIN t_doc_list dl ON dl."docID" = fa.source_doc_id
                    WHERE fa."secCode" = ANY(:codes)
                      AND dl."submitDateTime"::date BETWEEN :s AND :e
                      AND fa.current_assets IS NOT NULL AND fa.total_liabilities IS NOT NULL
                    ORDER BY fa."secCode", dl."submitDateTime"
                    """
                ),
                {"codes": sec_codes, "s": ENTRY_DATE, "e": EXIT_DEADLINE},
            ).fetchall(),
            columns=["sec_code","submit_date","current_assets","total_liabilities","investment_securities"],
        )
    for c in ("current_assets", "total_liabilities", "investment_securities"):
        new_filings[c] = pd.to_numeric(new_filings[c], errors="coerce")

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
                applic = sf[sf["submit_date"] <= pr["d"]]
                if not applic.empty:
                    last = applic.iloc[-1]
                    numerator = (
                        last["current_assets"] - last["total_liabilities"]
                        + HAIRCUT * (last["investment_securities"] or 0)
                    )
            ratio = numerator / pr["market_cap"] if pr["market_cap"] > 0 else 0
            if pr["d"] >= ENTRY_DATE and ratio < EXIT_RATIO:
                exit_d, exit_p, reason = pr["d"], pr["adj_close"], "ratio<1"
                break
        if exit_d is None:
            last = sp.iloc[-1]
            exit_d, exit_p = last["d"], last["adj_close"]
        results.append({
            "sec_code": sec_code,
            "company": row["company"][:24],
            "entry_ratio": round(row["entry_ratio"], 2),
            "per": round(row["per"], 1),
            "entry_mc_bn": round(row["entry_mc"] / 1e9, 1),
            "days_held": (exit_d - ENTRY_DATE).days,
            "return_pct": round((exit_p / row["entry_price"] - 1) * 100, 1),
            "reason": reason,
        })

    df = pd.DataFrame(results).sort_values("return_pct", ascending=False)
    print(df.to_string(index=False))
    print(f"\nN = {len(df)}")
    print(f"  mean return:  {df['return_pct'].mean():>+.1f}%")
    print(f"  median return: {df['return_pct'].median():>+.1f}%")
    print(f"  exited via ratio<1: {(df['reason']=='ratio<1').sum()}")
    print(f"  held to deadline:   {(df['reason']=='deadline').sum()}")
    print(f"  avg days held: {df['days_held'].mean():.0f} (max 1096)")

    # Benchmark
    with engine.connect() as conn:
        b = conn.execute(
            text(
                'SELECT "Date", adj_close FROM t_daily_stock_perf '
                'WHERE "ShokenCode"=\'13060\' AND "Date" IN (:s, :e)'
            ),
            {"s": ENTRY_DATE, "e": EXIT_DEADLINE},
        ).fetchall()
    bench = {r[0]: float(r[1]) for r in b}
    if ENTRY_DATE in bench and EXIT_DEADLINE in bench:
        print(f"\nTOPIX (1306) 3y return: {(bench[EXIT_DEADLINE]/bench[ENTRY_DATE]-1)*100:+.1f}%")


if __name__ == "__main__":
    main()
