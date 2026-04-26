# Empirical findings — net-cash screening on TSE

A running log of what we've validated against the database. Decisions
that landed in production code are summarized here so future readers
(and future-us) can see the rationale.

## 1. Formula: `total_liabilities` beats `interest_bearing_debt`

The canonical Japanese ネットキャッシュ比率 textbook uses
`current_assets − interest_bearing_debt + 0.7 × investment_securities`.
Our quintile analysis on 285 small-caps (¥30B–¥60B mc, Aug 2025
baseline → Apr 2026) plus a 1,800-stock 3-year study at
<https://zenn.dev/morim34/articles/ff991f32187d96> both found that
substituting `total_liabilities` for `interest_bearing_debt` gives a
cleaner monotonic ratio→return relationship.

| Formula | Q1 | Q2 | Q3 | Q4 | Q5 |
|---|---|---|---|---|---|
| `interest_bearing_debt` | +8% | +12% | +20% | +21% | **+9%** ← Q5 collapse |
| `total_liabilities` | +7% | +11% | +17% | +22% | +22% |

The interest-bearing form scores companies with no bank debt but huge
trade payables (operating distress) artificially high. Total
liabilities catches that. The mart still stores both;
`screening/metrics.py` uses `total_liabilities`.

## 2. Op-income yield filter cuts value traps

Net-cash alone produces value traps — companies with structural cash
but stagnant operations the market correctly prices low. A 2022-cohort
test (19 stocks ¥15-50B mc with ratio > 1.5) showed:

- 4 stocks held to the 3-year deadline; 3 of those underperformed
  (双葉電子工業 +10%, コロナ +9%, 大平洋金属 +20%)
- All 3 had op_yield < 5% at entry

Adding `op_income / market_cap > 5%` to the screen catches 3 of 5
worst traps from this cohort. Cost: drops 1 turnaround winner (67940
フォスター電機 entered with an operating loss but returned +89%).
Net mean improves from +63.4% (unfiltered) to +73.9% (kept group).

Live screen on 2026-04-24: ratio≥1 universe shrinks 300 → 163 → 95
after stacking ratio + op_yield + 6m_momentum filters.

## 3. Turnaround signal — additive only, not a hard filter

We extended the parser to keep `Prior1Year*` XBRL contexts and added
`compute_turnaround_score(engine, run_date)` that returns YoY changes
in op income, sales, and operating margin via a self-join on
`t_financials_annual` (LAG window function).

Tested on the 2022 cohort with the rule
`op_income_yoy > 20% AND sales_yoy > 0`:

- Catches 9 of 19 stocks; mean return **+63%** (same as baseline)
- Misses 5 deep-trough winners with negative or undefined op_yoy at
  entry: 87060 (broker, N/A sales concept), 67940 (loss-to-loss
  transition, op_yoy = −∞), 18110, 51610, 88810
- Includes 1 cyclical-peak trap: 55410 大平洋金属 with op_yoy +1,074%
  on +77% sales — looked like a strong turnaround but was a
  commodities cycle high

**Conclusion**: turnaround is best used as an additive signal to
surface candidates, not as a hard filter. The "quality (op_yield>5%
AND mom>0%) AND turnaround (op_yoy>20% AND sales_yoy>0)" intersection
on the 2022 cohort had n=4 stocks at +84.6% mean — promising but
sample is too small to draw conclusions. Re-test in a year with more
data.

## 4. Look-ahead bias in the production screen — known caveat

`screening/metrics.py::_SCREEN_SQL` filters `period_end <= run_date`,
not `submitDateTime <= run_date`. Fine for live trading (filings are
known once submitted), but produces optimistic backtests because the
screen "sees" filings before their actual submission date. The
backtest scripts (`backtest_smallcap_netcash.py`, `cross_table_…`)
use `submitDateTime` for honest results.

## 5. Known concept-coverage gaps

Banks, brokers, and insurance companies use different XBRL concepts
than industrials:
- Brokers use `営業収益` (operating revenue), not `売上高` (net sales)
- Insurance companies use `経常収益`
- Banks use 業務粗利益 / 業務純益

Our `concepts.py` covers industrials. Financial firms appear with
`net_sales = NULL` and `sales_yoy` undefined. Adding these concept
IDs is straightforward — deferred until we want to screen the
financial sector specifically.
