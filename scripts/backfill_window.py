"""Run the full daily pipeline (doclist → xbrl → prices → screen → sim)
across a date window, in dependency order.

Same business logic as the Airflow DAGs (re-uses everything in src/),
just driven from a single process instead of via airflow scheduler.
Idempotent — every step upserts or deletes-then-rewrites for its date,
so a partial run can be re-launched safely.

Usage:
    python scripts/backfill_window.py 2026-04-15 2026-04-22
"""

from __future__ import annotations

import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stock_screening import config, db
from stock_screening.edinet.client import (
    DOC_LIST_COLUMNS,
    download_xbrl_bundle,
    list_documents,
)
from stock_screening.edinet.xbrl_parser import extract_facts_many
from stock_screening.financials.build_annual_mart import build_for_doc
from stock_screening.jquants.client import JQuantsClient
from stock_screening.screening.metrics import compute_screen, persist_screen_results
from stock_screening.simulation import portfolio
from stock_screening.simulation.rebalance import (
    compute_target_shares,
    generate_trades,
    select_target_names,
)
from stock_screening.simulation.strategies import STRATEGIES, Strategy


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def step_doclist(engine, edinet_key: str, d: date) -> int:
    entries = list_documents(d, edinet_key)
    if not entries:
        return 0
    rows = [e.to_row() for e in entries]
    cols = ", ".join(f'"{c}"' for c in DOC_LIST_COLUMNS)
    placeholders = ", ".join(f":{c}" for c in DOC_LIST_COLUMNS)
    update_set = ", ".join(
        f'"{c}" = EXCLUDED."{c}"' for c in DOC_LIST_COLUMNS if c != "docID"
    )
    sql = text(
        f"INSERT INTO t_doc_list ({cols}) VALUES ({placeholders}) "
        f'ON CONFLICT ("docID") DO UPDATE SET {update_set}'
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def step_xbrl_ingest(engine, edinet_key: str, d: date) -> tuple[int, int, int]:
    with engine.connect() as conn:
        pending = [
            r[0]
            for r in conn.execute(
                text(
                    """
                    SELECT dl."docID" FROM t_doc_list dl
                    WHERE dl."formCode"='030000'
                      AND dl."ordinanceCode"='010'
                      AND dl."submitDateTime"::date = :d
                      AND NOT EXISTS (SELECT 1 FROM t_financials f WHERE f."docID"=dl."docID")
                    """
                ),
                {"d": d},
            ).all()
        ]
    if not pending:
        return 0, 0, 0

    total_facts = 0
    succeeded: list[str] = []
    for doc_id in pending:
        with tempfile.TemporaryDirectory() as tmp:
            try:
                paths = download_xbrl_bundle(doc_id, edinet_key, Path(tmp))
                xbrl = [p for p in paths if p.suffix == ".xbrl"]
                facts = [f for f in extract_facts_many(doc_id, xbrl) if f.period_end is not None]
                if not facts:
                    succeeded.append(doc_id)
                    continue
                rows = [
                    {
                        "docID": f.doc_id, "itemName": f.item_name, "amount": f.amount,
                        "periodStart": f.period_start, "periodEnd": f.period_end,
                        "categoryID": f.category_id, "concept_id": f.concept_id,
                        "currency_code": f.currency_code,
                    }
                    for f in facts
                ]
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            INSERT INTO t_financials
                                ("docID","itemName",amount,"periodStart","periodEnd","categoryID",concept_id,currency_code)
                            VALUES (:docID,:itemName,:amount,:periodStart,:periodEnd,:categoryID,:concept_id,:currency_code)
                            ON CONFLICT ("docID","itemName","periodEnd","categoryID")
                            DO UPDATE SET amount=EXCLUDED.amount, concept_id=EXCLUDED.concept_id, currency_code=EXCLUDED.currency_code
                            """
                        ),
                        rows,
                    )
                total_facts += len(facts)
                succeeded.append(doc_id)
            except Exception as e:
                print(f"    [warn] {doc_id} failed: {type(e).__name__}: {str(e)[:120]}")

    # Mark latest filing per (secCode, periodEnd, formCode)
    if succeeded:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    WITH affected AS (
                        SELECT DISTINCT "secCode","periodEnd","formCode"
                        FROM t_doc_list WHERE "docID"=ANY(:ids)
                    ),
                    ranked AS (
                        SELECT dl."docID",
                               ROW_NUMBER() OVER (PARTITION BY dl."secCode",dl."periodEnd",dl."formCode"
                                                  ORDER BY dl."submitDateTime" DESC) AS rn
                        FROM t_doc_list dl JOIN affected a USING ("secCode","periodEnd","formCode")
                    )
                    UPDATE t_doc_list dl
                    SET is_latest_for_period = (r.rn=1)
                    FROM ranked r WHERE dl."docID"=r."docID"
                    """
                ),
                {"ids": succeeded},
            )

    mart_rows = sum(build_for_doc(engine, d_id) for d_id in succeeded)
    return len(succeeded), total_facts, mart_rows


def step_prices(engine, jq: JQuantsClient, d: date) -> int:
    quotes = jq.daily_quotes(target_date=d)
    if quotes.empty:
        return 0
    quotes = quotes.rename(columns={"Code": "ShokenCode", "C": "close", "Vo": "volume"})
    quotes["ShokenCode"] = quotes["ShokenCode"].astype(str)
    quotes = quotes.dropna(subset=["close"])

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                'SELECT DISTINCT ON ("secCode") "secCode" AS "ShokenCode", issued_shares '
                "FROM t_financials_annual WHERE issued_shares IS NOT NULL AND period_end <= :d "
                'ORDER BY "secCode", period_end DESC'
            ),
            {"d": d},
        ).fetchall()
    shares = pd.DataFrame(rows, columns=["ShokenCode", "issued_shares"])

    merged = quotes[["ShokenCode", "close", "volume"]].merge(shares, on="ShokenCode", how="left")
    merged["Date"] = d
    merged["marketCap"] = (
        merged["close"].astype("float64") * merged["issued_shares"].astype("float64")
    ).round()

    data = []
    for _, r in merged.iterrows():
        data.append(
            {
                "Date": r["Date"],
                "ShokenCode": r["ShokenCode"],
                "close": float(r["close"]) if pd.notna(r["close"]) else None,
                "volume": int(r["volume"]) if pd.notna(r["volume"]) else None,
                "marketCap": int(r["marketCap"]) if pd.notna(r["marketCap"]) else None,
            }
        )
    sql = text(
        """
        INSERT INTO t_daily_stock_perf ("Date","ShokenCode",close,volume,"marketCap")
        VALUES (:Date,:ShokenCode,:close,:volume,:marketCap)
        ON CONFLICT ("Date","ShokenCode") DO UPDATE SET
          close=EXCLUDED.close, volume=EXCLUDED.volume, "marketCap"=EXCLUDED."marketCap"
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, data)
    return len(data)


def step_screen(engine, d: date) -> tuple[int, int]:
    df = compute_screen(engine, d)
    n_qual = int(df["qualifies"].sum()) if not df.empty else 0
    persist_screen_results(engine, d, df)
    return len(df), n_qual


def step_sim_one(engine, d: date, strategy: Strategy, qual: pd.DataFrame) -> dict:
    portfolio.delete_outputs_for_date(engine, d, strategy.name)
    prev = portfolio.load_previous_snapshot(engine, d, strategy.name)

    if prev is None:
        cash = float(strategy.initial_capital)
        positions = pd.DataFrame(
            columns=["secCode", "shares", "avg_cost", "last_price", "market_value"]
        )
    else:
        cash = prev.cash
        positions = prev.positions

    names = sorted(set(qual["sec_code"]) | set(positions["secCode"]))
    prices = portfolio.fetch_prices(engine, d, names)
    marked = portfolio.mark_to_market(positions, prices)
    nav_pre = cash + (float(marked["market_value"].sum()) if not marked.empty else 0.0)

    target_names = select_target_names(
        qual,
        list(marked["secCode"]) if not marked.empty else [],
        strategy.max_positions,
        swap_rule=strategy.swap_rule,
    )
    ratios = dict(zip(qual["sec_code"], qual["ratio"], strict=True)) if not qual.empty else {}
    target_shares = compute_target_shares(
        target_names,
        prices,
        nav_pre,
        strategy.max_positions,
        weighting=strategy.weighting,
        ratios=ratios,
    )
    current_shares = (
        {row.secCode: int(row.shares) for row in marked.itertuples()}
        if not marked.empty
        else {}
    )
    trades = generate_trades(
        current_shares, target_shares, prices, strategy.transaction_cost_bps
    )
    new_positions, new_cash = portfolio.apply_trades(marked, trades, cash)
    portfolio.persist_positions(engine, d, new_positions, strategy.name)
    portfolio.persist_trades(engine, d, trades, strategy.name)
    portfolio.persist_nav(engine, d, new_cash, new_positions, None, strategy.name)

    pos_value = float(new_positions["market_value"].sum()) if not new_positions.empty else 0.0
    return {
        "n_positions": int(len(new_positions)),
        "n_trades": len(trades),
        "nav": new_cash + pos_value,
    }


def step_sim(engine, d: date) -> dict[str, dict]:
    """Run every registered strategy for date `d`. They all read the
    same screen output but maintain independent portfolios."""
    with engine.connect() as conn:
        qrows = conn.execute(
            text(
                'SELECT "secCode" AS sec_code, ratio FROM t_screen_results '
                "WHERE run_date=:d AND qualifies=TRUE ORDER BY ratio DESC"
            ),
            {"d": d},
        ).fetchall()
    qual = pd.DataFrame(qrows, columns=["sec_code", "ratio"])
    qual["ratio"] = qual["ratio"].astype(float)

    return {s.name: step_sim_one(engine, d, s, qual) for s in STRATEGIES}


def main():
    if len(sys.argv) != 3:
        print("usage: backfill_window.py START END", file=sys.stderr)
        sys.exit(1)
    start = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    end = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()

    engine = db.get_engine()
    edinet_key = config.edinet_key()
    jq = JQuantsClient(config.jquants_api_key())

    for d in daterange(start, end):
        print(f"\n=== {d} ({d.strftime('%a')}) ===")
        try:
            n_doc = step_doclist(engine, edinet_key, d)
            print(f"  doclist:  {n_doc} filings")
            n_filings, n_facts, n_mart = step_xbrl_ingest(engine, edinet_key, d)
            print(f"  xbrl:     {n_filings} parsed, {n_facts} facts, {n_mart} mart rows")
        except Exception as e:
            print(f"  [error] EDINET step failed: {type(e).__name__}: {e}")
            continue

        # Skip price/screen/sim on weekends — JQuants returns nothing.
        if d.weekday() >= 5:
            print("  (weekend — skipping prices/screen/sim)")
            continue

        try:
            n_q = step_prices(engine, jq, d)
            print(f"  prices:   {n_q} quotes")
            if n_q == 0:
                print("  (no prices — skipping screen/sim)")
                continue
            n_scored, n_qual = step_screen(engine, d)
            print(f"  screen:   {n_scored} scored, {n_qual} qualify")
            results = step_sim(engine, d)
            for name, r in results.items():
                print(
                    f"  sim[{name:<32}]: {r['n_positions']:>2} pos, "
                    f"{r['n_trades']:>2} tx, NAV={r['nav']:>15,.0f}"
                )
        except Exception as e:
            print(f"  [error] prices/screen/sim failed: {type(e).__name__}: {e}")

    print("\nbackfill complete.")


if __name__ == "__main__":
    main()
