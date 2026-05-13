"""Re-download and re-parse existing EDINET filings to capture
Prior1Year* facts that the original ingest did not keep.

The XBRL parser was extended (xbrl_parser.py) to keep
Prior1YearDuration / Prior1YearInstant contexts alongside the
CurrentYear* ones we already have. This script re-runs the
download → parse → upsert path for every securities report whose
t_financials row set lacks any Prior1Year* row.

Idempotent — once a doc has Prior1Year* rows in t_financials, it's
skipped on subsequent runs. After re-parsing, also rebuilds the mart
so the new prior-year mart rows are populated.

Usage:
    python scripts/reparse_for_prior_year.py 2022-04-25 2023-04-24
    python scripts/reparse_for_prior_year.py 2023-04-25 2024-04-24
    python scripts/reparse_for_prior_year.py 2024-04-25 2025-04-24
    python scripts/reparse_for_prior_year.py 2025-04-25 2026-04-24
"""

from __future__ import annotations

import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stock_screening import config, db
from stock_screening.edinet.client import download_xbrl_bundle
from stock_screening.edinet.xbrl_parser import extract_facts_many
from stock_screening.financials.build_annual_mart import build_for_doc
from stock_screening.financials.upsert import upsert_financial_facts


def select_pending_docs(engine, start: date, end: date) -> list[str]:
    """Securities reports submitted in [start, end] that already have
    CurrentYear* facts but no Prior1Year* facts."""
    sql = text(
        """
        SELECT dl."docID" FROM t_doc_list dl
        WHERE dl."formCode" = '030000'
          AND dl."ordinanceCode" = '010'
          AND dl."submitDateTime"::date BETWEEN :s AND :e
          AND EXISTS (
              SELECT 1 FROM t_financials f
              WHERE f."docID" = dl."docID"
                AND f."categoryID" IN ('CurrentYearInstant', 'CurrentYearDuration')
          )
          AND NOT EXISTS (
              SELECT 1 FROM t_financials f
              WHERE f."docID" = dl."docID"
                AND f."categoryID" IN ('Prior1YearInstant', 'Prior1YearDuration')
          )
        ORDER BY dl."submitDateTime"
        """
    )
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(sql, {"s": start, "e": end}).all()]


def reparse_one(engine, edinet_key: str, doc_id: str) -> tuple[int, int]:
    """Returns (facts_inserted, mart_rows_written)."""
    with tempfile.TemporaryDirectory() as tmp:
        paths = download_xbrl_bundle(doc_id, edinet_key, Path(tmp))
        xbrl_files = [p for p in paths if p.suffix == ".xbrl"]
        if not xbrl_files:
            return 0, 0
        facts = [
            f for f in extract_facts_many(doc_id, xbrl_files) if f.period_end is not None
        ]

    # Shared upsert — same SQL as the DAG and backfill_window.
    # See src/stock_screening/financials/upsert.py.
    n = upsert_financial_facts(engine, facts)
    mart_n = build_for_doc(engine, doc_id)
    return n, mart_n


def main():
    if len(sys.argv) != 3:
        print("usage: reparse_for_prior_year.py START END", file=sys.stderr)
        sys.exit(1)
    start = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    end = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()

    engine = db.get_engine()
    edinet_key = config.edinet_key()

    docs = select_pending_docs(engine, start, end)
    print(f"{len(docs)} docs to re-parse for {start} → {end}")

    total_facts = 0
    total_mart = 0
    for i, doc_id in enumerate(docs, 1):
        try:
            n_facts, n_mart = reparse_one(engine, edinet_key, doc_id)
            total_facts += n_facts
            total_mart += n_mart
            if i % 50 == 0 or i == len(docs):
                print(f"  [{i}/{len(docs)}] last: {doc_id} facts={n_facts}, mart_rows={n_mart}")
        except Exception as e:
            print(f"  [warn] {doc_id} failed: {type(e).__name__}: {str(e)[:120]}")

    print(f"\nfinished. total facts inserted: {total_facts}, mart rows written: {total_mart}")


if __name__ == "__main__":
    main()
