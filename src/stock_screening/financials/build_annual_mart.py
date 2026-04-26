"""Project raw EAV (t_financials) → typed t_financials_annual.

Each annual filing yields *two* mart rows:
  - one for the current fiscal year (period_end = doc's periodEnd),
    built from CurrentYear* facts.
  - one for the prior fiscal year (period_end = doc's periodEnd − 1y),
    built from Prior1Year* facts that the same filing carries in its
    Summary-of-Business-Results table.

This means a single FY2024 annual report populates BOTH FY2023 and
FY2024 mart rows, even if we never ingested the actual FY2023 filing.
The interest_bearing_debt column is a GENERATED STORED sum of the
debt components; Postgres maintains it on insert/update.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable
from datetime import date

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .concepts import CONCEPTS, resolve_field

logger = logging.getLogger(__name__)

MART_FIELDS = tuple(cm.field_name for cm in CONCEPTS)


def _fetch_facts_for_doc(
    engine: Engine, doc_id: str, categories: tuple[str, ...]
) -> list[dict]:
    """Return facts for a doc filtered to the given XBRL category set
    (e.g. CurrentYear* for the current period, Prior1Year* for the
    comparative)."""
    sql = text(
        """
        SELECT "docID" AS doc_id, "itemName" AS item_name, amount,
               "periodEnd" AS period_end, concept_id
        FROM t_financials
        WHERE "docID" = :doc_id AND "categoryID" = ANY(:cats)
        """
    )
    with engine.connect() as conn:
        return [
            dict(r)
            for r in conn.execute(
                sql, {"doc_id": doc_id, "cats": list(categories)}
            ).mappings()
        ]


def _pivot(facts: list[dict]) -> dict[str, float]:
    """Resolve concept ID / label to mart field, picking the larger-
    magnitude fact when multiple map to the same field (handles
    consolidated vs non-consolidated)."""
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
    return pivoted


def _upsert_mart_row(
    engine: Engine,
    sec_code: str,
    period_end: date,
    fiscal_year_end_dt: date | None,
    source_doc_id: str,
    pivoted: dict[str, float],
) -> int:
    if not pivoted:
        return 0
    cols = ["secCode", "period_end", "fiscal_year_end_dt", "source_doc_id", *MART_FIELDS]
    values = {
        "secCode": sec_code,
        "period_end": period_end,
        "fiscal_year_end_dt": fiscal_year_end_dt,
        "source_doc_id": source_doc_id,
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


def _prior_period_end(period_end: date) -> date:
    """One year before period_end. Handles Feb 29 by stepping back to
    Feb 28 in non-leap years."""
    try:
        return period_end.replace(year=period_end.year - 1)
    except ValueError:  # Feb 29
        return period_end.replace(month=2, day=28, year=period_end.year - 1)


def build_for_doc(engine: Engine, doc_id: str) -> int:
    """Build / refresh up to TWO t_financials_annual rows for a filing:
    one for the current fiscal year, one for the prior. Idempotent.

    Returns count of rows written (0, 1, or 2).
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

    sec_code = str(meta["secCode"])
    period_end = meta["periodEnd"]
    fy_end = meta["endOfFiscalYearDt"]

    n_written = 0

    current_facts = _fetch_facts_for_doc(
        engine, doc_id, ("CurrentYearInstant", "CurrentYearDuration")
    )
    n_written += _upsert_mart_row(
        engine, sec_code, period_end, fy_end, doc_id, _pivot(current_facts)
    )

    prior_facts = _fetch_facts_for_doc(
        engine, doc_id, ("Prior1YearInstant", "Prior1YearDuration")
    )
    n_written += _upsert_mart_row(
        engine, sec_code, _prior_period_end(period_end), fy_end, doc_id,
        _pivot(prior_facts),
    )

    return n_written


def build_for_docs(engine: Engine, doc_ids: Iterable[str]) -> int:
    n = 0
    for d in doc_ids:
        n += build_for_doc(engine, d)
    return n
