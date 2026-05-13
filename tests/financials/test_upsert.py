"""Tests for the shared XBRL upsert helper.

Mock the SQLAlchemy engine so we can verify the SQL text and rows
without needing a live postgres. The point of the centralization is
that there is exactly ONE SQL string in the codebase upserting
t_financials — so we lock that down here.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from stock_screening.edinet.xbrl_parser import FinancialFact
from stock_screening.financials.upsert import (
    _UPSERT_SQL,
    upsert_financial_facts,
)


def _fact(period_end: date | None, period_start: date | None = None, **kw) -> FinancialFact:
    defaults = {
        "doc_id": "S100Y2FB",
        "item_name": "営業収益",
        "amount": 100.0,
        "category_id": "CurrentYearDuration",
        "concept_id": "jpcrp_cor:NetSalesSummaryOfBusinessResults",
        "currency_code": "JPY",
    }
    defaults.update(kw)
    return FinancialFact(
        period_start=period_start, period_end=period_end, **defaults
    )


def test_upsert_uses_period_end_as_conflict_key():
    """Regression guard against the periodStart→periodEnd PK drift
    that bit us in PR #22. If this assertion fails, someone reverted
    the SQL to the wrong key shape."""
    sql_str = str(_UPSERT_SQL)
    assert 'ON CONFLICT ("docID", "itemName", "periodEnd", "categoryID")' in sql_str
    # Negative: must NOT use periodStart in the conflict key
    assert '"periodStart", "categoryID")' not in sql_str
    # The DO UPDATE SET should refresh periodStart, not periodEnd
    assert '"periodStart" = EXCLUDED."periodStart"' in sql_str
    assert '"periodEnd" = EXCLUDED."periodEnd"' not in sql_str


def test_skips_facts_with_null_period_end():
    """Facts without period_end can't be upserted (PK NOT NULL). Skip
    them silently — they're typically extraction edge cases (corrupted
    XBRL or unknown context), not callers' fault."""
    engine = MagicMock()
    facts = [
        _fact(period_end=None, period_start=date(2025, 1, 1)),
        _fact(period_end=None),
    ]
    n = upsert_financial_facts(engine, facts)
    assert n == 0
    # Engine should not even be opened if there's nothing to upsert.
    engine.begin.assert_not_called()


def test_keeps_facts_with_null_period_start():
    """Instant facts have period_end set but period_start NULL. These
    must be upserted (they're the most common balance-sheet rows)."""
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn

    facts = [
        _fact(period_end=date(2026, 2, 21), period_start=None,
              category_id="CurrentYearInstant"),
    ]
    n = upsert_financial_facts(engine, facts)
    assert n == 1
    # Verify the row passed to conn.execute has period_start=None
    args, _ = conn.execute.call_args
    rows = args[1]
    assert len(rows) == 1
    assert rows[0]["periodStart"] is None
    assert rows[0]["periodEnd"] == date(2026, 2, 21)


def test_mixed_batch_filters_correctly():
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn

    facts = [
        _fact(period_end=date(2026, 2, 21)),                      # keep
        _fact(period_end=None),                                    # drop
        _fact(period_end=date(2025, 2, 21),
              category_id="Prior1YearDuration"),                   # keep
        _fact(period_end=None, period_start=date(2024, 1, 1)),     # drop
    ]
    n = upsert_financial_facts(engine, facts)
    assert n == 2


def test_returns_zero_on_empty_input():
    engine = MagicMock()
    n = upsert_financial_facts(engine, [])
    assert n == 0
    engine.begin.assert_not_called()


def test_row_shape_matches_sql_bind_params():
    """Lock the row-dict shape against the SQL VALUES clause. If
    someone adds a column or renames a key, this test should scream."""
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn

    facts = [_fact(period_end=date(2026, 2, 21))]
    upsert_financial_facts(engine, facts)
    args, _ = conn.execute.call_args
    rows = args[1]
    assert set(rows[0].keys()) == {
        "docID", "itemName", "amount", "periodStart", "periodEnd",
        "categoryID", "concept_id", "currency_code",
    }
