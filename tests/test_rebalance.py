import pandas as pd
import pytest

from stock_screening.simulation.rebalance import (
    LOT_SIZE,
    Trade,
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
