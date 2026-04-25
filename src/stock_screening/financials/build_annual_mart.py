"""Project raw EAV (t_financials) → typed t_financials_annual.

For each (secCode, period_end), pick the latest filing (via
t_doc_list.is_latest_for_period) and pivot its facts onto the typed
columns of t_financials_annual. The interest_bearing_debt column on
the mart is GENERATED STORED, so we don't compute it here — Postgres
sums the components automatically.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .concepts import CONCEPTS, resolve_field

logger = logging.getLogger(__name__)

MART_FIELDS = tuple(cm.field_name for cm in CONCEPTS)


def _fetch_latest_facts_for_doc(engine: Engine, doc_id: str) -> list[dict]:
    """Return every CurrentYearInstant fact for a doc — these are the
    balance-sheet line items the screen cares about."""
    sql = text(
        """
        SELECT "docID" AS doc_id, "itemName" AS item_name, amount,
               "periodEnd" AS period_end, concept_id
        FROM t_financials
        WHERE "docID" = :doc_id
          AND "categoryID" = 'CurrentYearInstant'
        """
    )
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(sql, {"doc_id": doc_id}).mappings()]


def build_for_doc(engine: Engine, doc_id: str) -> int:
    """Build / refresh the t_financials_annual row for the (secCode,
    period_end) of a single filing. Idempotent: upserts.

    Returns 1 if a row was written, 0 if the doc had no relevant facts
    or no secCode.
    """
    with engine.connect() as conn:
        meta = conn.execute(
            text(
                'SELECT "secCode", "periodEnd", "endOfFiscalYearDt" '
                "FROM t_doc_list dl "
                "LEFT JOIN t_edinet_code_mappings m "
                "  ON m.\"edinetCode\" = dl.\"edinetCode\" "
                'WHERE dl."docID" = :doc_id'
            ),
            {"doc_id": doc_id},
        ).mappings().first()
    if meta is None or not meta["secCode"] or not meta["periodEnd"]:
        return 0

    facts = _fetch_latest_facts_for_doc(engine, doc_id)
    if not facts:
        return 0

    # Pivot: pick the largest absolute amount when multiple facts map to
    # the same field (handles consolidated vs non-consolidated; consolidated
    # is typically the larger figure).
    pivoted: dict[str, float] = {}
    seen: dict[str, float] = defaultdict(lambda: -1.0)
    for f in facts:
        field = resolve_field(f.get("concept_id"), f.get("item_name"))
        if field is None or f.get("amount") is None:
            continue
        amt = float(f["amount"])
        if abs(amt) > seen[field]:
            seen[field] = abs(amt)
            pivoted[field] = amt

    if not pivoted:
        return 0

    cols = ["secCode", "period_end", "fiscal_year_end_dt", "source_doc_id", *MART_FIELDS]
    values = {
        "secCode": str(meta["secCode"]),
        "period_end": meta["periodEnd"],
        "fiscal_year_end_dt": meta["endOfFiscalYearDt"],
        "source_doc_id": doc_id,
        **{f: pivoted.get(f) for f in MART_FIELDS},
    }

    placeholders = ", ".join(f":{c}" for c in cols)
    quoted_cols = ", ".join(f'"{c}"' if c == "secCode" else c for c in cols)
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c not in ("secCode", "period_end")
    )

    sql = text(
        f"""
        INSERT INTO t_financials_annual ({quoted_cols})
        VALUES ({placeholders})
        ON CONFLICT ("secCode", period_end) DO UPDATE SET {update_set}
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, values)
    return 1


def build_for_docs(engine: Engine, doc_ids: Iterable[str]) -> int:
    n = 0
    for d in doc_ids:
        n += build_for_doc(engine, d)
    return n
