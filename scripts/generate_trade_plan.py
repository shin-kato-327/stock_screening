"""Generate a manual-execution trade plan from current IBKR positions
+ today's screen + excess cash.

Output: classified status of each existing position (HOLD / WATCH /
EXIT / NO_DATA), a recommended deployment of excess cash into the
best-fit target pick, and the full target portfolio for reference.

You run this quarterly (or whenever you have new cash). It tells you
exactly which orders to place; you submit them manually in IBKR.

Usage:
    set -a && . ./.env && set +a && PYTHONPATH=src \\
      uv run python scripts/generate_trade_plan.py \\
      --excess-usd 889 [--fx 150] [--target-n 7]
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stock_screening import db
from stock_screening.ibkr.flex_client import FlexClient
from stock_screening.ibkr.positions import enrich_with_screen, parse_open_positions

LOT = 100  # Japanese stock minimum trading unit
HAIRCUT = 0.7
EXIT_RATIO = 1.0
ENTRY_RATIO = 1.5
LIMIT_BUFFER_PCT = 0.005  # buy 0.5% above last close


def build_target_picks(engine, today: date, n: int = 7) -> pd.DataFrame:
    """Today's top picks per the validated strategy: lowest-float 33%
    within sweet-spot 3-30B + ratio>1.5 + PER<=10. Returned in float-rank
    order; n controls cutoff (defaults to ~33%)."""
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
                    WHERE dl."submitDateTime"::date <= :today
                      AND fa.current_assets IS NOT NULL
                ),
                latest AS (SELECT DISTINCT ON (sec_code) * FROM visible
                           ORDER BY sec_code, period_end DESC),
                ranked AS (
                    SELECT fa2."secCode", fa2.period_end, fa2.operating_income,
                           fa2.net_sales,
                           LAG(fa2.operating_income) OVER w op_prev,
                           LAG(fa2.net_sales)        OVER w sales_prev
                    FROM t_financials_annual fa2
                    JOIN t_doc_list dl2 ON dl2."docID" = fa2.source_doc_id
                    WHERE dl2."submitDateTime"::date <= :today
                    WINDOW w AS (PARTITION BY fa2."secCode" ORDER BY fa2.period_end)
                ),
                yoy AS (
                    SELECT DISTINCT ON ("secCode") "secCode" sec_code, op_prev, sales_prev
                    FROM ranked WHERE op_prev IS NOT NULL OR sales_prev IS NOT NULL
                    ORDER BY "secCode", period_end DESC
                )
                SELECT l.sec_code, l.current_assets, l.total_liabilities,
                       l.investment_securities, l.operating_income, l.net_sales,
                       l.net_income, l.issued_shares,
                       y.op_prev, y.sales_prev,
                       d_now.adj_close price_now, d_now."marketCap" mc,
                       d_6mo.adj_close p_6mo,
                       (SELECT "filerName" FROM t_doc_list
                        WHERE "secCode"=l.sec_code ORDER BY "submitDateTime" DESC LIMIT 1) name
                FROM latest l
                JOIN LATERAL (SELECT adj_close, "marketCap" FROM t_daily_stock_perf p
                              WHERE p."ShokenCode"=l.sec_code AND p."Date" <= :today
                                AND adj_close IS NOT NULL AND "marketCap" IS NOT NULL
                              ORDER BY "Date" DESC LIMIT 1) d_now ON TRUE
                LEFT JOIN LATERAL (SELECT adj_close FROM t_daily_stock_perf p
                                   WHERE p."ShokenCode"=l.sec_code
                                     AND p."Date" BETWEEN :s6_s AND :s6_e
                                     AND adj_close IS NOT NULL
                                   ORDER BY "Date" DESC LIMIT 1) d_6mo ON TRUE
                LEFT JOIN yoy y ON y.sec_code = l.sec_code
                """
            ),
            {"today": today,
             "s6_s": today - timedelta(days=200),
             "s6_e": today - timedelta(days=160)},
        ).fetchall()

    cols = ["sec_code", "current_assets", "total_liabilities", "investment_securities",
            "operating_income", "net_sales", "net_income", "issued_shares",
            "op_prev", "sales_prev", "price_now", "mc", "p_6mo", "name"]
    df = pd.DataFrame(rows, columns=cols)
    for c in [c for c in cols if c not in ("sec_code", "name")]:
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
    df["q_flag"] = (df["op_yield"] > 0.05) & (df["mom_6m"] > 0)
    df["t_flag"] = (df["op_yoy"] > 0.20) & (df["sales_yoy"] > 0)

    sweet = df[
        (df["mc"] >= 3e9) & (df["mc"] <= 30e9)
        & (df["ratio"] > ENTRY_RATIO)
        & (df["per"] > 0) & (df["per"] <= 10)
    ].dropna(subset=["issued_shares"]).copy()
    sweet = sweet.sort_values("issued_shares").reset_index(drop=True)
    return sweet.head(n)


def limit_price(last_close: float) -> int:
    """Round limit price to a sensible tick. JPX tick rules vary by price;
    for most small caps in our screen, ¥1 ticks below ¥3000 are fine."""
    raw = last_close * (1 + LIMIT_BUFFER_PCT)
    return int(round(raw))


def best_deployment(excess_jpy: float, target_picks: pd.DataFrame,
                    current_codes: set[str]) -> dict | None:
    """Among target picks not already held, find the highest-conviction
    one whose 100-share lot fits the budget. Tiebreak: highest ratio."""
    candidates = target_picks[~target_picks["sec_code"].isin(current_codes)].copy()
    if candidates.empty:
        return None
    candidates["lot_cost"] = candidates["price_now"] * LOT
    affordable = candidates[candidates["lot_cost"] <= excess_jpy].copy()
    if affordable.empty:
        return None
    # Score: prefer higher ratio (margin of safety) then Q+T flag count
    affordable["flag_count"] = affordable["q_flag"].astype(int) + affordable["t_flag"].astype(int)
    affordable = affordable.sort_values(
        ["flag_count", "ratio"], ascending=[False, False]
    ).reset_index(drop=True)
    pick = affordable.iloc[0]
    return {
        "sec_code": pick["sec_code"],
        "name": pick["name"],
        "shares": LOT,
        "limit": limit_price(float(pick["price_now"])),
        "cost": LOT * limit_price(float(pick["price_now"])),
        "last_close": float(pick["price_now"]),
        "ratio": float(pick["ratio"]),
        "per": float(pick["per"]),
        "flags": (("Q" if pick["q_flag"] else "")
                  + ("T" if pick["t_flag"] else "")) or "—",
    }


def fetch_positions(token: str, query_id: str) -> pd.DataFrame:
    client = FlexClient(token)
    resp = client.fetch_query(query_id)
    positions = parse_open_positions(resp.root)
    if not positions:
        return pd.DataFrame()
    engine = db.get_engine()
    return enrich_with_screen(engine, positions)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--excess-usd", type=float, default=0,
                   help="Excess cash in USD; converted to JPY at --fx")
    p.add_argument("--excess-jpy", type=float, default=0,
                   help="Excess cash in JPY (alternative to --excess-usd)")
    p.add_argument("--fx", type=float, default=150.0,
                   help="USD/JPY rate for conversion (default 150)")
    p.add_argument("--target-n", type=int, default=7,
                   help="Number of target picks (default 7 = lowest-float 33%%)")
    args = p.parse_args()

    if args.excess_jpy:
        excess_jpy = args.excess_jpy
        excess_label = f"¥{excess_jpy:,.0f}"
    elif args.excess_usd:
        excess_jpy = args.excess_usd * args.fx
        excess_label = f"${args.excess_usd:.0f} × {args.fx} = ¥{excess_jpy:,.0f}"
    else:
        excess_jpy = 0
        excess_label = "(none)"

    token = os.environ.get("IBKR_FLEX_TOKEN")
    query_id = os.environ.get("IBKR_POSITIONS_QUERY_ID")
    if not token or not query_id:
        print("ERROR: IBKR_FLEX_TOKEN and IBKR_POSITIONS_QUERY_ID must be set",
              file=sys.stderr)
        return 2

    today = date.today()
    engine = db.get_engine()

    print(f"Generating trade plan for {today}...")
    print(f"  Excess cash: {excess_label}")
    print()
    print("Fetching positions from IBKR...")
    positions = fetch_positions(token, query_id)
    print(f"  {len(positions)} open positions")
    print(f"Building today's target portfolio (n={args.target_n})...")
    targets = build_target_picks(engine, today, n=args.target_n)
    print(f"  {len(targets)} target picks")

    current_codes = set(positions["sec_code"]) if not positions.empty else set()
    target_codes = set(targets["sec_code"])

    # === EXIT TRIGGERS ===
    print()
    print("=" * 100)
    print("EXIT TRIGGERS (ratio < 1.0 — strategy says sell)")
    print("=" * 100)
    if positions.empty:
        print("  (no positions)")
    else:
        exits = positions[positions["ratio"] < EXIT_RATIO]
        if exits.empty:
            print("  None. All current positions still above the exit line.")
        else:
            for _, r in exits.iterrows():
                print(f"  SELL {int(r['quantity']):>4} sh of {r['symbol']:<7} {r['description']}")
                print(f"       ratio={r['ratio']:.2f}  current value ¥{r['mark_price']*r['quantity']:,.0f}")

    # === EXISTING POSITION STATUS ===
    print()
    print("=" * 100)
    print("EXISTING POSITIONS — action per holding")
    print("=" * 100)
    if positions.empty:
        print("  (none)")
    else:
        for _, r in positions.iterrows():
            in_target = r["sec_code"] in target_codes
            ratio = r.get("ratio")
            if pd.isna(ratio):
                action = "MANUAL REVIEW (not in screen DB)"
            elif ratio < EXIT_RATIO:
                action = f"SELL ALL — ratio {ratio:.2f} below exit line"
            elif ratio < ENTRY_RATIO:
                action = f"HOLD — graduated to ratio {ratio:.2f} (1.0-1.5); do not add"
            elif in_target:
                action = f"HOLD — still in target portfolio (ratio {ratio:.2f})"
            else:
                action = f"HOLD — passes screen but not top-{args.target_n} (ratio {ratio:.2f})"
            print(f"  {r['symbol']:<7} {(r['description'] or '')[:30]:<30}  {action}")

    # === EXCESS CASH DEPLOYMENT ===
    print()
    print("=" * 100)
    print("EXCESS CASH DEPLOYMENT")
    print("=" * 100)
    if excess_jpy <= 0:
        print("  (no excess cash to deploy — pass --excess-usd or --excess-jpy)")
    else:
        plan = best_deployment(excess_jpy, targets, current_codes)
        if plan is None:
            print(f"  Budget ¥{excess_jpy:,.0f} insufficient for any target pick's lot, OR")
            print("  all target picks already held. Consider topping up underweight position.")
        else:
            leftover = excess_jpy - plan["cost"]
            print(f"  RECOMMENDED BUY:")
            print()
            print(f"    {plan['shares']} shares of {plan['sec_code']} {plan['name']}")
            print(f"    Limit price: ¥{plan['limit']:,}  (last close ¥{plan['last_close']:.0f} + {LIMIT_BUFFER_PCT*100:.1f}% buffer)")
            print(f"    Estimated cost: ¥{plan['cost']:,.0f}")
            print(f"    Leftover cash: ¥{leftover:,.0f}")
            print()
            print(f"    Screen state: ratio {plan['ratio']:.2f}, PER {plan['per']:.1f}, flags {plan['flags']}")
            print()
            print(f"  Order to place in IBKR:")
            print(f"    BUY {plan['shares']} {plan['sec_code']}  TYPE: LIMIT  PRICE: ¥{plan['limit']:,}  TIF: DAY")

    # === FULL TARGET PORTFOLIO ===
    print()
    print("=" * 100)
    print(f"FULL TARGET PORTFOLIO (top {args.target_n} hybrid picks today)")
    print("=" * 100)
    print(f"  {'code':<6} {'name':<28} {'ratio':>5} {'PER':>5} {'MC¥B':>5} "
          f"{'shares M':>9} {'price':>7} {'flags':>6}  status")
    for _, r in targets.iterrows():
        flags = (("Q" if r["q_flag"] else "") + ("T" if r["t_flag"] else "")) or "—"
        owned = "OWNED" if r["sec_code"] in current_codes else ""
        print(f"  {r['sec_code']:<6} {(r['name'] or '')[:28]:<28} "
              f"{r['ratio']:>5.2f} {r['per']:>5.1f} {r['mc']/1e9:>5.1f} "
              f"{r['issued_shares']/1e6:>9.2f} ¥{r['price_now']:>6.0f} {flags:>6}  {owned}")

    # === COVERAGE NOTES ===
    print()
    print("=" * 100)
    print("COVERAGE NOTES")
    print("=" * 100)
    held_in_target = current_codes & target_codes
    target_not_held = target_codes - current_codes
    held_not_in_target = current_codes - target_codes
    print(f"  Target picks held:        {len(held_in_target)}/{len(target_codes)}  "
          f"({sorted(held_in_target) if held_in_target else 'none'})")
    print(f"  Target picks NOT held:    {len(target_not_held)}  "
          f"({sorted(target_not_held) if target_not_held else 'none'})")
    print(f"  Held but not in target:   {len(held_not_in_target)}  "
          f"({sorted(held_not_in_target) if held_not_in_target else 'none'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
