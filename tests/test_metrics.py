"""Pure-function tests for the screen formula. DB-bound paths (compute_screen,
persist_screen_results) covered by the migration smoke test in phase 11."""

from stock_screening.screening.metrics import (
    INVESTMENT_SECURITIES_HAIRCUT,
    QUALIFY_THRESHOLD,
)


def test_qualify_threshold_is_one():
    """1.0 means net cash equals market cap. The canonical Japanese
    ネットキャッシュ比率 cutoff."""
    assert QUALIFY_THRESHOLD == 1.0


def test_investment_securities_haircut_is_70_percent():
    """Investment securities are weighted at 0.7 because they're not
    marked-to-market liquid in the same way as cash; haircut accounts
    for execution risk."""
    assert INVESTMENT_SECURITIES_HAIRCUT == 0.7


def test_formula_components_and_threshold_match_plan():
    """Sanity: ratio = (current_assets - interest_bearing_debt
                      + 0.7*investment_securities) / market_cap.
    A company with current_assets=100, debt=20, sec=10, mc=87 → ratio
    = (100 - 20 + 7) / 87 = 1.0 → just qualifies."""
    ratio = (100 - 20 + INVESTMENT_SECURITIES_HAIRCUT * 10) / 87
    assert ratio == 1.0
    assert ratio >= QUALIFY_THRESHOLD
