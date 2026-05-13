"""Tests for the shared XBRL upsert helper.

Mock the SQLAlchemy engine so we can verify the SQL text and rows
without needing a live postgres. The point of the centralization is
that there is exactly ONE SQL string in the codebase upserting
t_financials — so we lock that down here.
"""

from __future__ import annotations

import re
import subprocess
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

from stock_screening.edinet.xbrl_parser import FinancialFact
from stock_screening.financials.upsert import (
    _UPSERT_SQL,
    _fact_to_row,
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


def test_fact_to_row_field_mapping():
    """Verify that snake_case FinancialFact fields map to the correct
    camelCase SQL column names with the correct values — not just that
    the right key names exist, but that the value under each key is the
    value from the corresponding fact field.

    This catches a class of rename bugs where the key name is right but
    the wrong field was used as the value (e.g., doc_id and item_name
    swapped)."""
    f = FinancialFact(
        doc_id="S100ZZZZ",
        item_name="純資産合計",
        amount=9_876_543.0,
        period_start=date(2025, 4, 1),
        period_end=date(2026, 3, 31),
        category_id="CurrentYearInstant",
        concept_id="jpcrp_cor:NetAssets",
        currency_code="JPY",
    )
    row = _fact_to_row(f)
    assert row["docID"] == "S100ZZZZ"
    assert row["itemName"] == "純資産合計"
    assert row["amount"] == 9_876_543.0
    assert row["periodStart"] == date(2025, 4, 1)
    assert row["periodEnd"] == date(2026, 3, 31)
    assert row["categoryID"] == "CurrentYearInstant"
    assert row["concept_id"] == "jpcrp_cor:NetAssets"
    assert row["currency_code"] == "JPY"


def test_single_insert_into_t_financials_in_codebase():
    """Structural anti-drift test: there must be exactly ONE
    'INSERT INTO t_financials' clause (not t_financials_annual or any
    other variant) in the entire Python source tree, and it must live
    in src/stock_screening/financials/upsert.py.

    This test fails immediately if anyone copy-pastes the upsert SQL
    back into the DAG, a script, or a new module — which is the exact
    drift class that caused the PR #22 production failure.

    Uses `grep -r` via subprocess so it scans every .py file without
    importing them (avoids transitive import errors from airflow/etc.)."""
    repo_root = Path(__file__).resolve().parents[2]
    this_file = str(Path(__file__).resolve())
    result = subprocess.run(
        ["grep", "-rn", "--include=*.py", "INSERT INTO t_financials", str(repo_root)],
        capture_output=True,
        text=True,
    )
    # grep exits 0 if matches found, 1 if none — both are fine here.
    # Non-zero exit for other reasons (e.g. permission errors) would be
    # a test-infrastructure problem; we ignore that and focus on matches.
    matches = [
        line for line in result.stdout.splitlines()
        # Exclude the annual-mart table (different table, intentional separate SQL)
        if "t_financials_annual" not in line
        # Exclude this test file itself (it contains the search term as a string literal)
        and this_file not in line
    ]

    canonical = str(repo_root / "src" / "stock_screening" / "financials" / "upsert.py")
    non_canonical = [m for m in matches if canonical not in m]

    assert not non_canonical, (
        f"Found INSERT INTO t_financials outside the canonical module "
        f"(src/stock_screening/financials/upsert.py).\n"
        f"Offending locations (DAG drift risk):\n"
        + "\n".join(f"  {m}" for m in non_canonical)
    )
    assert len(matches) == 1, (
        f"Expected exactly 1 INSERT INTO t_financials in the codebase "
        f"(in upsert.py), found {len(matches)}:\n"
        + "\n".join(f"  {m}" for m in matches)
    )
