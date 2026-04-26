"""Strategy registry for the multi-strategy simulator.

A Strategy bundles the daily portfolio-construction parameters that
turn `t_screen_results` (qualifying names + ratios) into a target
portfolio. All strategies share the same screen output — they only
differ in selection / sizing / turnover rules — so equity curves are
directly comparable across the same dates.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Strategy:
    name: str
    max_positions: int
    weighting: str  # 'equal' | 'ratio'
    swap_rule: str  # 'strict_improvement' | 'full_rebalance'
    initial_capital: int = 10_000_000
    transaction_cost_bps: int = 10


STRATEGIES: tuple[Strategy, ...] = (
    # Baseline: top-20, equal-weight, anti-thrashing swap. Matches the
    # original plan and the 1-week smoke result in the refactor PR.
    Strategy(
        name="netcash_top20_equal",
        max_positions=20,
        weighting="equal",
        swap_rule="strict_improvement",
    ),
    # Concentration test: half the names, same rules. Higher idiosyncratic
    # risk but better signal capture if the top of the screen is real.
    Strategy(
        name="netcash_top10_concentrated",
        max_positions=10,
        weighting="equal",
        swap_rule="strict_improvement",
    ),
    # Turnover test: same N, same sizing, but no anti-thrashing rule —
    # rebalance to whatever's top-20 today. Costs accumulate on noise.
    Strategy(
        name="netcash_top20_full_rebalance",
        max_positions=20,
        weighting="equal",
        swap_rule="full_rebalance",
    ),
    # Conviction test: weight proportional to ratio. Names with higher
    # net-cash get more capital. Same N and swap rule as baseline.
    Strategy(
        name="netcash_top20_ratio_weighted",
        max_positions=20,
        weighting="ratio",
        swap_rule="strict_improvement",
    ),
)


def by_name(name: str) -> Strategy:
    for s in STRATEGIES:
        if s.name == name:
            return s
    raise KeyError(f"unknown strategy: {name}")
