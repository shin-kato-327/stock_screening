"""Top-N equal-weight rebalance with strict-improvement swap rule.

Daily rule:
1. Universe = qualifying names (ratio >= 1.0) from today's screen.
2. Target = top MAX_POSITIONS by ratio desc.
3. Strict-improvement swap: replace held name H with candidate C only
   if C.ratio > H.ratio strictly. Prevents thrashing on near-ties.
4. Equal-weight on today's NAV; round shares down to lot size (100 for
   JP equities); residual sits in cash.
5. Costs: TRANSACTION_COST_BPS each side on traded notional.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd

LOT_SIZE = 100  # JP equity standard board lot


@dataclass
class Trade:
    sec_code: str
    side: str  # 'B' or 'S'
    shares: int
    price: float
    commission: float
    reason: str

    def notional(self) -> float:
        return self.shares * self.price


def select_target_names(
    qualifying: pd.DataFrame, current: Sequence[str], max_positions: int
) -> list[str]:
    """Pick which names to hold tomorrow.

    qualifying: columns sec_code, ratio (sorted or not, both fine).
    current: sec_codes held today.
    Rule: keep current names that still qualify; fill remaining slots
    with non-held names that have a strictly better ratio than the
    weakest held name. Preserves stability.
    """
    if qualifying.empty:
        return []

    df = qualifying[["sec_code", "ratio"]].sort_values("ratio", ascending=False).reset_index(
        drop=True
    )
    qualifying_set = set(df["sec_code"])
    held = [c for c in current if c in qualifying_set]
    held_ratios = dict(zip(df["sec_code"], df["ratio"], strict=True))

    if len(held) >= max_positions:
        held_sorted = sorted(held, key=lambda c: held_ratios[c], reverse=True)
        return held_sorted[:max_positions]

    held_set = set(held)
    candidates = df[~df["sec_code"].isin(held_set)].reset_index(drop=True)

    if not held:
        # Bootstrap: just take top-N.
        return df["sec_code"].head(max_positions).tolist()

    weakest_held_ratio = min(held_ratios[c] for c in held) if held else float("-inf")
    slots = max_positions - len(held)
    promoted = [
        row.sec_code for row in candidates.itertuples()
        if row.ratio > weakest_held_ratio
    ][:slots]

    return held + promoted


def compute_target_shares(
    target_names: Sequence[str],
    prices: dict[str, float],
    total_nav: float,
    max_positions: int,
    lot_size: int = LOT_SIZE,
) -> dict[str, int]:
    """Equal-weight sizing. NAV split evenly across max_positions slots
    (so adding a name later doesn't dilute existing ones); shares per
    name rounded down to lot_size.
    """
    if not target_names:
        return {}
    slot_value = total_nav / max_positions
    shares: dict[str, int] = {}
    for name in target_names:
        p = prices.get(name)
        if not p or p <= 0:
            shares[name] = 0
            continue
        raw = int(slot_value // p)
        rounded = (raw // lot_size) * lot_size
        shares[name] = rounded
    return shares


def generate_trades(
    current_shares: dict[str, int],
    target_shares: dict[str, int],
    prices: dict[str, float],
    cost_bps: int,
    reason_per_name: dict[str, str] | None = None,
) -> list[Trade]:
    """Diff current vs target, emit one Trade per delta. Sells first
    (frees cash), then buys."""
    bps = cost_bps / 10_000
    reason_per_name = reason_per_name or {}
    sells: list[Trade] = []
    buys: list[Trade] = []
    all_names = set(current_shares) | set(target_shares)
    for name in all_names:
        cur = current_shares.get(name, 0)
        tgt = target_shares.get(name, 0)
        delta = tgt - cur
        if delta == 0:
            continue
        price = prices.get(name)
        if not price or price <= 0:
            continue
        side = "B" if delta > 0 else "S"
        shares = abs(delta)
        notional = shares * price
        trade = Trade(
            sec_code=name,
            side=side,
            shares=shares,
            price=price,
            commission=notional * bps,
            reason=reason_per_name.get(name, ""),
        )
        (buys if side == "B" else sells).append(trade)
    return sells + buys
