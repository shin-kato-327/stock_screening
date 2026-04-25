import pandas as pd
import pytest

from stock_screening.simulation.rebalance import (
    LOT_SIZE,
    compute_target_shares,
    generate_trades,
    select_target_names,
)


def _qual(items: list[tuple[str, float]]) -> pd.DataFrame:
    return pd.DataFrame(items, columns=["sec_code", "ratio"])


# select_target_names ----------------------------------------------------


def test_select_bootstrap_takes_top_n():
    qual = _qual([("A", 3.0), ("B", 2.5), ("C", 2.0), ("D", 1.5)])
    out = select_target_names(qual, current=[], max_positions=2)
    assert out == ["A", "B"]


def test_select_keeps_held_names_that_still_qualify():
    """Strict-improvement rule: held name H stays unless a non-held name
    has a strictly better ratio."""
    qual = _qual([("A", 3.0), ("B", 2.5), ("C", 2.4)])  # all three qualify
    # Holding B (ratio 2.5). C has 2.4 < 2.5, so no swap. A is non-held but
    # we already have two slots filled (B + ?).
    out = select_target_names(qual, current=["A", "B"], max_positions=2)
    assert sorted(out) == ["A", "B"]


def test_select_no_swap_on_equal_ratio():
    """Equality is not strict — same ratios don't trigger a swap."""
    qual = _qual([("A", 2.0), ("B", 2.0), ("C", 2.0)])
    out = select_target_names(qual, current=["A", "B"], max_positions=2)
    assert sorted(out) == ["A", "B"]


def test_select_swap_on_strict_improvement():
    """Candidate strictly better than weakest held → swap."""
    qual = _qual([("A", 2.0), ("B", 2.0), ("C", 2.5)])
    out = select_target_names(qual, current=["A", "B"], max_positions=2)
    assert "C" in out
    assert len(out) == 2


def test_select_drops_held_that_no_longer_qualifies():
    qual = _qual([("A", 3.0), ("C", 2.5)])  # B is gone
    out = select_target_names(qual, current=["A", "B"], max_positions=2)
    assert sorted(out) == ["A", "C"]


def test_select_promotes_to_fill_open_slots():
    qual = _qual([("A", 3.0), ("B", 2.5), ("C", 2.4)])
    # holding [A] → 1 slot open → promote highest non-held = B
    out = select_target_names(qual, current=["A"], max_positions=2)
    assert sorted(out) == ["A", "B"]


# compute_target_shares --------------------------------------------------


def test_equal_weight_lot_rounded():
    """5M slot at 1234 yen/share → raw 4051 → rounded down to 4000 (lot 100)."""
    shares = compute_target_shares(
        target_names=["A"],
        prices={"A": 1234.0},
        total_nav=100_000_000,
        max_positions=20,  # slot = 5M
    )
    assert shares["A"] == 4_000


def test_share_zero_when_price_too_high():
    shares = compute_target_shares(
        target_names=["X"],
        prices={"X": 10_000_000},
        total_nav=10_000_000,
        max_positions=20,
    )
    assert shares["X"] == 0


def test_missing_price_yields_zero_shares():
    shares = compute_target_shares(
        target_names=["A", "B"],
        prices={"A": 100.0},
        total_nav=10_000_000,
        max_positions=2,
    )
    assert shares["A"] > 0
    assert shares["B"] == 0


# generate_trades --------------------------------------------------------


def test_trades_emit_buys_for_new_names():
    trades = generate_trades(
        current_shares={},
        target_shares={"A": 100, "B": 200},
        prices={"A": 1000.0, "B": 500.0},
        cost_bps=10,
    )
    assert all(t.side == "B" for t in trades)
    assert {t.sec_code for t in trades} == {"A", "B"}


def test_trades_emit_sell_then_buy_ordering():
    """Sells must come before buys so the cash from sells can fund the
    buys (the apply step depends on this order)."""
    trades = generate_trades(
        current_shares={"A": 100},
        target_shares={"B": 100},
        prices={"A": 1000.0, "B": 1000.0},
        cost_bps=10,
    )
    assert [t.side for t in trades] == ["S", "B"]


def test_trades_skip_zero_delta():
    trades = generate_trades(
        current_shares={"A": 100, "B": 200},
        target_shares={"A": 100, "B": 300},
        prices={"A": 100.0, "B": 200.0},
        cost_bps=10,
    )
    assert len(trades) == 1
    assert trades[0].sec_code == "B"


def test_commission_at_10bps():
    trades = generate_trades(
        current_shares={},
        target_shares={"A": 100},
        prices={"A": 1000.0},
        cost_bps=10,
    )
    assert len(trades) == 1
    # 100 * 1000 = 100_000 notional; 10 bps = 100
    assert trades[0].commission == pytest.approx(100.0)


def test_no_trade_when_price_missing():
    trades = generate_trades(
        current_shares={},
        target_shares={"A": 100},
        prices={},  # no price for A
        cost_bps=10,
    )
    assert trades == []


# lot size ---------------------------------------------------------------


def test_lot_size_is_100():
    """JP standard board lot. Hardcoded constant; flag if it ever changes."""
    assert LOT_SIZE == 100


# Multi-strategy: full_rebalance + ratio weighting --------------------


def test_select_full_rebalance_ignores_holdings():
    """full_rebalance: target = top N regardless of current holdings."""
    qual = _qual([("A", 3.0), ("B", 2.5), ("C", 2.0)])
    out = select_target_names(qual, current=["X", "Y"], max_positions=2, swap_rule="full_rebalance")
    assert out == ["A", "B"]


def test_full_rebalance_drops_held_when_better_exists():
    qual = _qual([("X", 1.5), ("Y", 1.4), ("Z", 1.3)])
    out = select_target_names(qual, current=["Z"], max_positions=2, swap_rule="full_rebalance")
    assert out == ["X", "Y"]


def test_select_unknown_swap_rule_raises():
    with pytest.raises(ValueError, match="unknown swap_rule"):
        select_target_names(_qual([("A", 2.0)]), current=[], max_positions=1, swap_rule="other")


def test_ratio_weighting_concentrates_on_higher_ratio():
    """Two names, ratios 3:1 → first should get ~3× the second's shares
    (modulo lot rounding)."""
    shares = compute_target_shares(
        target_names=["HI", "LO"],
        prices={"HI": 100.0, "LO": 100.0},
        total_nav=1_000_000,
        max_positions=2,
        weighting="ratio",
        ratios={"HI": 3.0, "LO": 1.0},
    )
    # HI gets 3/4 of NAV (750K @ 100 = 7500 shares), LO gets 1/4 (2500)
    assert shares["HI"] == 7500
    assert shares["LO"] == 2500


def test_ratio_weighting_handles_zero_total():
    """If all ratios are zero/negative, every name gets 0 shares."""
    shares = compute_target_shares(
        target_names=["A", "B"],
        prices={"A": 100.0, "B": 100.0},
        total_nav=1_000_000,
        max_positions=2,
        weighting="ratio",
        ratios={"A": 0, "B": 0},
    )
    assert shares == {"A": 0, "B": 0}


def test_ratio_weighting_requires_ratios():
    with pytest.raises(ValueError, match="requires ratios"):
        compute_target_shares(
            target_names=["A"],
            prices={"A": 100.0},
            total_nav=1_000_000,
            max_positions=1,
            weighting="ratio",
        )


def test_unknown_weighting_raises():
    with pytest.raises(ValueError, match="unknown weighting"):
        compute_target_shares(
            target_names=["A"],
            prices={"A": 100.0},
            total_nav=1_000_000,
            max_positions=1,
            weighting="other",
        )
