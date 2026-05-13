"""Shared upsert helpers for the financial fact tables.

This module exists to kill a recurring drift class: the same INSERT
... ON CONFLICT idiom was copy-pasted in three places (the airflow
DAG, scripts/backfill_window.py, scripts/reparse_for_prior_year.py),
and the DAG copy drifted on at least four occasions:

  - pd.read_sql + SA 1.4 incompat (PR #12)
  - NaN volume / Bigint reject (PR #13)
  - adj_close dropped from upsert columns (PR #17)
  - ON CONFLICT used the old PK (periodStart instead of periodEnd) (PR #22)

By routing every callsite through `upsert_financial_facts`, future
schema changes (column adds, PK changes, dtype tweaks) need to land
in exactly one function — not three.

The PK on t_financials is:
    ("docID", "itemName", "periodEnd", "categoryID")
Migration 0005 made periodStart nullable for Instant facts and
switched the PK off periodStart for that reason.
"""

from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

from stock_screening.edinet.xbrl_parser import FinancialFact

# Single canonical SQL. Any caller wanting different upsert semantics
# is a code smell — file an issue instead of branching this string.
_UPSERT_SQL = text(
    """
    INSERT INTO t_financials
        ("docID", "itemName", amount, "periodStart", "periodEnd",
         "categoryID", concept_id, currency_code)
    VALUES (:docID, :itemName, :amount, :periodStart, :periodEnd,
            :categoryID, :concept_id, :currency_code)
    ON CONFLICT ("docID", "itemName", "periodEnd", "categoryID")
    DO UPDATE SET
        amount = EXCLUDED.amount,
        "periodStart" = EXCLUDED."periodStart",
        concept_id = EXCLUDED.concept_id,
        currency_code = EXCLUDED.currency_code
    """
)


def _fact_to_row(f: FinancialFact) -> dict:
    return {
        "docID": f.doc_id,
        "itemName": f.item_name,
        "amount": f.amount,
        "periodStart": f.period_start,
        "periodEnd": f.period_end,
        "categoryID": f.category_id,
        "concept_id": f.concept_id,
        "currency_code": f.currency_code,
    }


def upsert_financial_facts(
    engine: Engine, facts: Iterable[FinancialFact]
) -> int:
    """Upsert XBRL facts into t_financials. Returns row count.

    Filters facts with `period_end is None` — the PK requires periodEnd
    NOT NULL, and Instant facts that fail to extract a period end are
    not useful for the mart anyway. Logged at the call site.
    """
    rows = [_fact_to_row(f) for f in facts if f.period_end is not None]
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(_UPSERT_SQL, rows)
    return len(rows)
