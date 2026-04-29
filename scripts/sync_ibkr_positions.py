"""Fetch IBKR positions via Flex Web Service and show each holding's
current screen state (ratio, Q/T flags, exit-trigger).

Setup (one-time, in your IBKR account portal):
  1. Performance & Reports → Flex Queries → Flex Web Service → Configure
     Generate a token. Copy it to .env as IBKR_FLEX_TOKEN.
  2. Performance & Reports → Flex Queries → Custom Flex Queries → Create
     - Type: Activity
     - Sections: only "Open Positions" is required for this script
     - Default fields are fine; symbol, position, markPrice, costBasisPrice,
       fifoPnlUnrealized, currency, isin, description are all in defaults.
     - Save the query and copy its Query ID to .env as
       IBKR_POSITIONS_QUERY_ID.

Then run:
  set -a && . ./.env && set +a && PYTHONPATH=src uv run python scripts/sync_ibkr_positions.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stock_screening import db
from stock_screening.ibkr.flex_client import FlexClient
from stock_screening.ibkr.positions import enrich_with_screen, parse_open_positions


def main() -> int:
    token = os.environ.get("IBKR_FLEX_TOKEN")
    query_id = os.environ.get("IBKR_POSITIONS_QUERY_ID")
    if not token or not query_id:
        print("ERROR: IBKR_FLEX_TOKEN and IBKR_POSITIONS_QUERY_ID must be set in .env",
              file=sys.stderr)
        print("  See header docstring for setup steps.", file=sys.stderr)
        return 2

    client = FlexClient(token)
    print(f"Fetching Flex query {query_id}...")
    resp = client.fetch_query(query_id)
    positions = parse_open_positions(resp.root)
    print(f"  Got {len(positions)} open positions")
    if not positions:
        return 0

    engine = db.get_engine()
    df = enrich_with_screen(engine, positions)

    # Pretty print
    print()
    print("=" * 130)
    print("IBKR POSITIONS — joined with screen state")
    print("=" * 130)
    print(
        f"  {'code':<6} {'name':<24} {'qty':>6} {'cost':>8} {'mark':>8} "
        f"{'P&L':>9} {'ret%':>7} {'ratio':>6} {'PER':>5} {'flags':>6}  status"
    )
    print("-" * 138)

    for _, r in df.iterrows():
        flags = []
        if r.get("q_flag", False) is True: flags.append("Q")
        if r.get("t_flag", False) is True: flags.append("T")
        flag_s = "+".join(flags) or "—"

        ratio_s = f"{r['ratio']:.2f}" if pd.notna(r.get("ratio")) else "  ?"
        per_s = f"{r['per']:.1f}" if pd.notna(r.get("per")) else "  ?"

        if pd.isna(r.get("ratio")):
            status = "NO_DATA (not in screen DB — manual review)"
        elif r["ratio"] < 1.0:
            status = "EXIT TRIGGERED (ratio < 1.0)"
        elif r["ratio"] < 1.5:
            status = "WATCH (ratio in 1.0-1.5 weak cohort)"
        else:
            status = "HOLD (ratio > 1.5, screen still active)"

        cost_s = f"¥{r['cost_basis']:.0f}" if pd.notna(r.get("cost_basis")) else "?"
        mark_s = f"¥{r['mark_price']:.0f}" if pd.notna(r.get("mark_price")) else "?"
        pl_s = f"{r['unrealized_pl']:+,.0f}" if pd.notna(r.get("unrealized_pl")) else "?"

        if pd.notna(r.get("cost_basis")) and pd.notna(r.get("mark_price")) and r["cost_basis"]:
            ret_s = f"{(r['mark_price']/r['cost_basis']-1)*100:+.1f}%"
        else:
            ret_s = "?"

        print(
            f"  {r['symbol']:<6} {(r['description'] or '')[:24]:<24} "
            f"{r['quantity']:>6.0f} {cost_s:>8} {mark_s:>8} "
            f"{pl_s:>9} {ret_s:>7} {ratio_s:>6} {per_s:>5} {flag_s:>6}  {status}"
        )

    # Summary by status
    print()
    if "ratio" in df.columns:
        n_exit = int((df["ratio"] < 1.0).sum())
        n_watch = int(((df["ratio"] >= 1.0) & (df["ratio"] < 1.5)).sum())
        n_hold = int((df["ratio"] >= 1.5).sum())
        n_nodata = int(df["ratio"].isna().sum())
        print(f"  HOLD: {n_hold}  WATCH: {n_watch}  EXIT: {n_exit}  NO_DATA: {n_nodata}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
