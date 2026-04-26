from stock_screening.simulation.strategies import STRATEGIES, by_name


def test_strategy_names_are_unique():
    names = [s.name for s in STRATEGIES]
    assert len(names) == len(set(names))


def test_baseline_is_registered():
    s = by_name("netcash_top20_equal")
    assert s.max_positions == 20
    assert s.weighting == "equal"
    assert s.swap_rule == "strict_improvement"


def test_ratio_weighted_strategy_is_registered():
    s = by_name("netcash_top20_ratio_weighted")
    assert s.weighting == "ratio"


def test_full_rebalance_strategy_is_registered():
    s = by_name("netcash_top20_full_rebalance")
    assert s.swap_rule == "full_rebalance"


def test_concentrated_has_smaller_n():
    s = by_name("netcash_top10_concentrated")
    assert s.max_positions == 10


def test_all_strategies_have_valid_swap_and_weighting():
    """Every registered strategy must use values the rebalance code knows."""
    valid_swap = {"strict_improvement", "full_rebalance"}
    valid_weighting = {"equal", "ratio"}
    for s in STRATEGIES:
        assert s.swap_rule in valid_swap, f"{s.name}: bad swap_rule {s.swap_rule}"
        assert s.weighting in valid_weighting, f"{s.name}: bad weighting {s.weighting}"
