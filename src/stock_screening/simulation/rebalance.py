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
    qualifying: pd.DataFrame,
    current: Sequence[str],
    max_positions: int,
    swap_rule: str = "strict_improvement",
) -> list[str]:
    """Pick which names to hold tomorrow.

    swap_rule:
      'strict_improvement' — keep held names that qualify; fill open
        slots with top non-held; only displace a held name when a
        non-held has a STRICTLY better ratio (anti-thrashing).
      'full_rebalance' — ignore current holdings; target = top N by
        ratio. Higher turnover; surfaces noise vs signal in costs.
    """
    if qualifying.empty:
        return []

    df = qualifying[["sec_code", "ratio"]].sort_values(
        "ratio", ascending=False
    ).reset_index(drop=True)

    if swap_rule == "full_rebalance":
        return df["sec_code"].head(max_positions).tolist()

    if swap_rule != "strict_improvement":
        raise ValueError(f"unknown swap_rule: {swap_rule}")

    ratios = dict(zip(df["sec_code"], df["ratio"], strict=True))
    held_qualifying = [c for c in current if c in ratios]
    chosen = list(held_qualifying)
    candidates = [c for c in df["sec_code"] if c not in set(chosen)]

    while len(chosen) < max_positions and candidates:
        chosen.append(candidates.pop(0))

    if len(chosen) > max_positions:
        chosen.sort(key=lambda c: ratios[c], reverse=True)
        chosen = chosen[:max_positions]

    while candidates:
        cand = candidates[0]
        weakest = min(chosen, key=lambda c: ratios[c])
        if ratios[cand] > ratios[weakest]:
            chosen.remove(weakest)
            chosen.append(cand)
            candidates.pop(0)
        else:
            break

    return chosen


def compute_target_shares(
    target_names: Sequence[str],
    prices: dict[str, float],
    total_nav: float,
    max_positions: int,
    weighting: str = "equal",
    ratios: dict[str, float] | None = None,
    lot_size: int = LOT_SIZE,
) -> dict[str, int]:
    """Compute target share counts per target name.

    weighting:
      'equal' — NAV split evenly across max_positions slots, so adding
        a name later doesn't dilute existing ones (the un-allocated
        slots' worth sits in cash).
      'ratio' — weights proportional to (ratio_i / Σ ratios).
        Requires `ratios` keyed by sec_code; concentrates capital in
        the highest-conviction names. Total deployed capital ≈ NAV
        regardless of how many names actually qualify.

    All shares rounded down to lot_size.
    """
    if not target_names:
        return {}

    if weighting == "equal":
        slot_value = total_nav / max_positions
        slot_for: dict[str, float] = {n: slot_value for n in target_names}
    elif weighting == "ratio":
        if ratios is None:
            raise ValueError("weighting='ratio' requires ratios dict")
        weights = {n: max(ratios.get(n, 0), 0) for n in target_names}
        total_w = sum(weights.values())
        if total_w <= 0:
            return {n: 0 for n in target_names}
        slot_for = {n: total_nav * (w / total_w) for n, w in weights.items()}
    else:
        raise ValueError(f"unknown weighting: {weighting}")

    shares: dict[str, int] = {}
    for name in target_names:
        p = prices.get(name)
        if not p or p <= 0:
            shares[name] = 0
            continue
        raw = int(slot_for[name] // p)
        shares[name] = (raw // lot_size) * lot_size
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
