# Strategy — canonical reference

This is the **single source of truth** for the screening strategy as it
stands today. For current candidates, recent backtests, and ongoing
research, see [`docs/screen_summary.md`](docs/screen_summary.md).

## One-line description

Buy small-cap (¥3-30B) Japanese stocks trading below conservative
liquidation value (net-cash ratio > 1.5) at low PE multiples (≤ 10x),
and within that universe, concentrate on the lowest-float third.
Hold until the net-cash ratio drops below 1.0.

## The screen

Stocks must satisfy **all** four entry conditions:

| Filter | Threshold |
|---|---|
| Market cap | ¥3B ≤ MC ≤ ¥30B |
| Net-cash ratio | > 1.5 |
| PER | 0 < PER ≤ 10 |
| Float overlay | bottom 33% by issued shares within the above |

### Net-cash ratio formula

```
ratio = (current_assets − total_liabilities + 0.7 × investment_securities) / market_cap
```

The 0.7 haircut on `investment_securities` is the conservative
adjustment from the original Shikiho article. The 0.7 figure is
empirical (cross-section of liquidation realizations); we kept it.

**Why `total_liabilities` instead of `interest_bearing_debt`:** the
empirical Q5-collapse pattern in interest-bearing-debt cuts vanishes
under total_liabilities, and the cross-sectional return-vs-ratio
relationship is monotonic over multiple horizons. See
`memory/project_netcash_formula_finding.md` for the citation and the
Zenn blog cross-check.

### PER

`PER = market_cap / net_income`. Positive net income required (loss-
makers excluded by `> 0`). Cap at 10x.

### Float overlay

Compute the sweet-spot universe (cap + ratio + PER), then take the
bottom 33% by `issued_shares`. Implementation:

```python
sweet = u[(u.mc.between(3e9, 30e9)) & (u.ratio > 1.5)
       & (u.per > 0) & (u.per <= 10)]
n = max(1, int(len(sweet) * 0.33))
picks = sweet.nsmallest(n, "issued_shares")
```

## Exit rule

**Sell when net-cash ratio drops below 1.0.** No other mechanical
exit signal.

The 1.5 in the entry rule is **not** the exit rule. Once a holding
runs up enough that its ratio compresses to the 1.0–1.5 band, you
hold — the position has "graduated" from screen-active to
managed-decay. Our quintile analysis showed the 1.0–1.5 cohort still
returns ~+22% over 3 years. Exiting at 1.5 specifically *hurt*
returns in our backtests.

## Position sizing & hold horizon

- **Equal-weight** the picks. This is what was backtested.
- **Hold ~12 months** between rebalances. Validated horizon.
- **Re-screen quarterly** (every 3 months) to catch new entries and
  exits without over-trading.
- **Buy-and-hold vs early-exit:** these are essentially equivalent
  across multiple windows. The "buy-and-hold +50pp" finding from a
  single 2022 cohort was a fluke. Use whichever fits your discipline.

## The validated edge

10 quarterly entry dates (2022-12 → 2025-03), 12-month buy-and-hold:

| Layer | Avg return | Excess vs TOPIX | Monte Carlo p-value |
|---|---|---|---|
| Sweet-spot baseline (¥3-30B, ratio>1.5, PER≤10) | +33.3% | +13.6pp | — (baseline) |
| **+ Lowest-float 33% overlay** | **+44.2%** | **+24.5pp** | **0.006** |
| (lowest-float 50% — for reference) | +35.8% | +16.1pp | 0.226 (NOT significant) |
| (lowest-float 25% — most concentrated) | +49.1% | +29.4pp | 0.014 |

Avg TOPIX across the 10 entries: +19.7%. The 33% threshold is the
chosen point: highest Sharpe with ~3-4 stocks per window. At 50%, the
"low-float" effect washes out (p=0.226) — the apparent excess at 50%
is the sweet-spot effect, not the float effect.

### What the float overlay is *not*

- **Not a "smaller stocks" effect.** Selecting bottom 33% by market
  cap instead of issued shares gives only +6.3pp excess (vs +24.5pp
  for float). Float captures something distinct from cap.
- **Not single-stock dependence.** 34 picks across 10 entries span 15
  unique stocks. Removing the most-recurring stock (南海プライウッド)
  still leaves +17.7pp excess.
- **Not outlier-driven.** The 34 individual picks have median +46%,
  P10 −1.6%, skew +0.09 (basically symmetric). The edge is broad-based.

### Hypothesized mechanism

Low share count proxies for tightly-held / illiquid ownership. Within
an already-cheap, net-cash-rich universe, this selects stocks where
the value is more likely to be realized via buyout, special dividend,
or accumulation by long-horizon holders rather than diluted away.
This is hypothesis-shaped, not proven mechanism — what we know is
the statistical edge survives every robustness check we've run.

## Q and T flags (descriptive labels, not gating)

These do **not** enter the screen. They're tier-ranking labels for
choosing among candidates when you want fewer positions than the
screen returns:

| Flag | Definition |
|---|---|
| **Q (Quality)** | op_yield > 5% AND mom_6m > 0 |
| **T (Turnaround)** | op_yoy > 20% AND sales_yoy > 0 |

Preference order when more candidates than positions: `Q+T > T > Q > —`.

`op_yield = operating_income / market_cap`. `mom_6m = price_now /
price_6mo - 1`. `op_yoy = (op - op_prev) / |op_prev|`.

## What we tried and abandoned

(Recording these so we don't redo the work. Each was tested across
multiple entry dates.)

| Variant | Result | Why abandoned |
|---|---|---|
| Pure momentum (top decile by 12m mom in small cap) | +5.3pp avg excess | Inconsistent; high negative pick rate |
| Pure earnings rocket (op_yoy > +100%, no value) | −13pp avg excess | 38% negative pick rate; picks late-cycle peakers |
| Pure low-float (no value filter) | +6.7pp; high variance | 25% neg rate; standalone signal not robust |
| `qmom` (high op_yield + high momentum, no value) | −33.5pp avg excess | Worst signal tested. Picks late-cycle peakers. |
| `cash_compounders` (ratio>1 + assets+sales growing) | −12.2pp | Too inclusive; n=43 dilutes |
| `reversal` (down 20%+ but biz OK) | −19pp | Falling-knife trap |
| `high_growth` (sales_yoy > +30% small cap) | 50% neg pick rate | Hot-money trap |
| Cap floor ¥15B (original) | locked out 6 of top 12 winners | ¥3B floor strictly better |
| Exit at ratio < 1.5 | clipped winners after first leg | Exit at 1.0 captures full upside |
| Buy-and-hold vs ratio<1 exit (multi-window) | within 1pp | Single-2022-cohort difference was a fluke |

## Caveats

1. **Single regime.** All validation entries are in 2022-2026 Japan
   small-cap bull. Not stress-tested through a small-cap drawdown.
2. **Concentration / tracking error.** n=3-4 stocks per window means
   single-year tracking error vs TOPIX is high; a single bad pick
   meaningfully moves the portfolio. The +24.5pp is a long-run
   expectation, not the year-by-year mean.
3. **IFRS reporters mis-screened.** `concepts.py` only covers JGAAP
   namespace. Companies on IFRS get NaN for current_assets etc.
   Affects a small but non-zero subset.
4. **Financial-sector concept gap.** Brokers/banks/insurance use
   different XBRL concept IDs. Net-cash + PER filters fail; turnaround
   signal undefined. Verify any financial-sector position manually.
5. **Look-ahead bias.** Production `compute_screen` uses `period_end <=
   run_date`, but historical scripts use `submitDateTime <= run_date`.
   For live screening this is fine; for backtests, scripts intentionally
   use the corrected version.
6. **Survivorship bias.** Stocks that delisted between 2022-12-30 and
   today aren't in our DB. Realized backtest returns reflect this.

## Reproducibility

```bash
# Live screen (today's universe)
set -a && . ./.env && set +a
PYTHONPATH=src uv run python -c "
from datetime import date
from stock_screening import db
from stock_screening.screening.metrics import compute_screen
df = compute_screen(db.get_engine(), date.today())
print(df[(df['market_cap'].between(3e9, 30e9)) & (df['ratio'] > 1.5)]
        .sort_values('issued_shares').head(7))
"

# Trade plan (positions + screen + cash → orders)
PYTHONPATH=src uv run python scripts/generate_trade_plan.py --excess-usd 1000

# Re-validate the edge (Monte Carlo p-value across 10 quarterly entries)
PYTHONPATH=src uv run python scripts/hybrid_montecarlo.py
```

| Script | What it does |
|---|---|
| `scripts/sync_ibkr_positions.py` | Fetch IBKR positions, classify HOLD/WATCH/EXIT/NO_DATA |
| `scripts/generate_trade_plan.py` | Take excess cash → recommended order |
| `scripts/hybrid_montecarlo.py` | Re-run the edge validation |
| `scripts/hybrid_sweep.py` | Threshold sweep (10/25/33/50/75/100% float) |
| `scripts/hybrid_attribution.py` | Verify edge isn't a confounder (mc / random / single-stock) |
| `scripts/strategy_lab_robustness.py` | Compare all 10 strategy variants vs TOPIX, 4 entries |

## Memory pointers (for future context)

- `memory/project_netcash_formula_finding.md` — why total_liabilities, not interest-bearing debt
- `memory/project_lowfloat_hybrid_finding.md` — why 33% float threshold, statistical evidence

## When to revisit

- After 1-2 quarters of live trading: compare realized returns to backtest expectation
- If we accumulate 12+ months of post-screen-design data: re-run the Monte Carlo on out-of-sample windows
- If small-cap Japan enters a drawdown: stress-test the strategy through that period
- If the screen returns < 5 stocks for 2 consecutive quarters: investigate (regime change or universe shrinking)
