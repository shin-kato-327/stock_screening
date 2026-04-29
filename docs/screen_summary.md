# Screen summary — backtest results + current candidates

*As of 2026-04-24. Snapshot in time; re-run the screen DAG for fresh data.*

## Screening criteria (live)

| Filter | Threshold |
|---|---|
| Market cap | ¥3B – ¥50B |
| Net-cash ratio | > 1.5 |
| PER (market_cap / net_income) | > 0 and ≤ 10 |

Net-cash ratio is the conservative form:

```
ratio = (current_assets − total_liabilities + 0.7 × investment_securities) / market_cap
```

Choice rationale, formula history, and limitations are tracked in
[`analysis_notes.md`](analysis_notes.md).

## 3-year backtest results (entry 2022-12-30, exit on ratio<1 OR 2025-12-30)

| Configuration | n | Mean Return | Median | vs TOPIX (+82.4%) |
|---|---|---|---|---|
| Old: ¥15-50B + ratio>1.5 (no PER) | 17 | +69.5% | +65.1% | −16pp |
| Wider: ¥3-50B + ratio≥1.0 + PER≤10 | 105 | +41.6% | +31.6% | −50pp |
| **Sweet spot: ¥3-50B + ratio>1.5 + PER≤10** | **34** | **+76.6%** | **+77.2%** | **−4pp** |

The sweet spot configuration is what's used for "current candidates"
below — it materially closed the gap to TOPIX while keeping
diversification (n=34 vs the original n=17).

### Why the wider universe helps

Lowering the cap floor from ¥15B to ¥3B unlocked **6 of the top 12
winners** (each returned ≥100% over 3 years) that the old criteria
excluded:

| Code | Company | Entry MC | Entry Ratio | 3y Return |
|---|---|---|---|---|
| 52800 | ヨシコン | ¥8.4B | 1.78 | **+177%** |
| 81520 | ソマール | ¥3.5B | 2.58 | **+176%** |
| 69730 | 協栄産業 | ¥5.2B | 1.65 | +144% |
| 75230 | アールビバン | ¥5.4B | 2.07 | +138% |
| 53630 | 東京窯業 | ¥12.0B | 1.86 | +123% |
| 39540 | 昭和パックス | ¥6.7B | 1.73 | +115% |

Top 12 overall (entries on 2022-12-30, ¥3-50B + ratio>1.5 + PER≤10):

| Code | Company | Ratio | PER | MC ¥B | Return |
|---|---|---|---|---|---|
| 52800 | ヨシコン | 1.78 | 5.1 | 8.4 | +177% |
| 81520 | ソマール | 2.58 | 5.1 | 3.5 | +176% |
| 69730 | 協栄産業 | 1.65 | 2.5 | 5.2 | +144% |
| 87060 | 極東証券 | 2.02 | 8.9 | 18.8 | +142% |
| 75230 | アールビバン | 2.07 | 4.7 | 5.4 | +138% |
| 53630 | 東京窯業 | 1.86 | 6.9 | 12.0 | +123% |
| 39540 | 昭和パックス | 1.73 | 6.1 | 6.7 | +115% |
| 80840 | 菱電商事 | 1.66 | 7.3 | 36.6 | +113% |
| 42340 | サンエー化研 | 1.91 | 3.3 | 5.1 | +106% |
| 42310 | タイガースポリマー | 2.02 | 9.7 | 8.1 | +105% |
| 59830 | イワブチ | 2.04 | 6.5 | 4.9 | +101% |
| 18790 | 新日本建設 | 1.73 | 4.2 | 45.7 | +100% |

Why ratio > 1.5 (not 1.0) is the right threshold despite the Shikiho
article using 1.0: our quintile analysis (see [`analysis_notes.md`
section 1](analysis_notes.md)) shows the 1.0–1.5 cohort returned only
+22% mean over 3 years, vs +60-70% for ratio > 1.5. Including 1.0–1.5
dilutes the universe with low-conviction names.

## Current candidates (2026-04-24)

29 stocks meet all three filters. **Flags**: `Q` = quality
(op_yield > 5% AND 6m_mom > 0%); `T` = turnaround
(op_income_yoy > 20% AND sales_yoy > 0).

### 🏆 Highest conviction — passes both Q and T

| Code | Company | Ratio | PER | MC ¥B | Op Yield | 6m Mom | Op YoY | Sales YoY |
|---|---|---|---|---|---|---|---|---|
| **41160** | 大日精化工業 | 2.96 | 1.9 | 19.2 | 36.4% | +10.9% | +54% | +4.1% |
| **61370** | 小池酸素工業 | 2.42 | 2.3 | 8.4 | 65.2% | +25.6% | +26% | +7.4% |
| **56030** | 虹技 | 1.53 | 5.8 | 4.6 | 24.2% | +8.1% | +42% | +1.4% |

### 💎 Quality (Q) only — already-strong businesses with positive momentum

| Code | Company | Ratio | PER | MC ¥B | Op Yield | 6m Mom |
|---|---|---|---|---|---|---|
| 80460 | 丸藤シートパイル | 5.56 | 2.4 | 3.7 | 42.4% | +26.8% |
| 21460 | UTグループ | 3.32 | 0.8 | 7.3 | 110.7% | +2.8% |
| 54510 | ヨドコウ | 3.26 | 3.2 | 42.8 | 32.5% | +7.0% |
| 40080 | 住友精化 | 2.02 | 2.8 | 16.7 | 64.0% | +12.7% |
| 67970 | 名古屋電機工業 | 1.89 | 3.5 | 7.8 | 35.2% | +1.5% |
| 26120 | かどや製油 | 1.60 | 6.3 | 14.7 | 21.5% | +22.1% |
| 18470 | イチケン | 1.54 | 4.2 | 19.7 | 34.9% | +25.9% |
| 15180 | 三井松島HD | 1.52 | 2.0 | 17.5 | 43.5% | +3.0% |
| 82190 | 青山商事 | 1.52 | 4.2 | 39.7 | 31.7% | +0.1% |

### 🔄 Turnaround (T) only — operating recovery + revenue growth

| Code | Company | Ratio | PER | MC ¥B | Op YoY | Sales YoY | Note |
|---|---|---|---|---|---|---|---|
| 49140 | 高砂香料工業 | 2.41 | 1.7 | 23.0 | **+562%** | +17.0% | Massive recovery; mom −28% (price hasn't caught up yet) |
| 80570 | 内田洋行 | 2.00 | 2.1 | 20.5 | +30% | +21.3% | Both top and bottom recovering |
| 46350 | 東京インキ | 3.31 | 3.0 | 3.6 | +70% | +6.6% | |
| 37980 | ULSグループ | 2.85 | 2.0 | 3.3 | +49% | +27.2% | |
| 80060 | ユアサ・フナショク | 2.08 | 3.9 | 8.0 | +47% | +2.9% | |

### Other passing names (no Q/T flag)

These pass net-cash + cap + PER but don't pass our quality OR
turnaround signals — closer to "raw value plays" without an
identified catalyst. Some of these are chronic value traps from the
2022 study (双葉電子工業 was on the original list and returned only
+10% over 3 years).

| Code | Company | Ratio | PER | MC ¥B | Notes |
|---|---|---|---|---|---|
| 54640 | モリ工業 | 4.62 | 1.8 | 7.3 | op_yld 74% but mom −6%, sales declining |
| 18220 | 大豊建設 | 3.82 | 3.6 | 13.5 | Sales −12%; op spike likely cyclical |
| 80750 | 神鋼商事 | 3.29 | 2.4 | 20.2 | mom slightly negative |
| 18660 | 北野建設 | 2.81 | 2.2 | 7.4 | op declining |
| 56020 | 栗本鐵工所 | 2.28 | 2.8 | 19.4 | mom −9% |
| 76280 | オーハシテクニカ | 2.06 | 9.7 | 14.8 | op_yld only 12% |
| 80430 | スターゼン | 1.96 | 1.8 | 22.2 | flat fundamentals |
| 27880 | アップルインターナショナル | 1.96 | 6.0 | 4.7 | op_yoy −59% |
| 64630 | TPR | 1.92 | 4.8 | 42.3 | op slightly declining |
| 74270 | エコートレーディング | 1.84 | 5.4 | 5.4 | op_yoy −21% |
| 52730 | 三谷セキサン | 1.66 | 3.6 | 36.4 | mom −4% |
| 75950 | アルゴグラフィックス | 1.59 | 4.2 | 31.3 | mom flat |

## Multi-entry robustness (2-year hold across 4 windows)

The single-period sweet-spot result (+76.6%) raised the obvious "is
this a 2022 cohort fluke?" question. The robustness lab
(`scripts/strategy_lab_robustness.py`) re-runs all strategies on 4
entry dates spaced ~6mo apart with a fixed 2-year hold, comparing
each to TOPIX over the same window.

| Strategy | Avg return | Avg excess vs TOPIX | Win rate | Avg n |
|---|---|---|---|---|
| **value_quality** (op_margin>10%) | +64.8% | **+25.2pp** | 100% | 1.7 |
| **sales_growth** (sales_yoy>5%) | +57.8% | **+19.9pp** | 100% | 7.5 |
| **z_composite** (top 20% blend) | +49.4% | +11.5pp | 100% | 9.5 |
| **top10_by_ratio** (concentrated) | +48.7% | +10.9pp | 100% | 9.5 |
| **value_momentum** (mom_6m>0) | +47.0% | +9.2pp | 75% | 11.5 |
| baseline_sweet_spot | +43.9% | +6.1pp | 100% | 20.0 |
| low_per_top10 | +29.6% | −8.2pp | 25% | 7.2 |
| cash_compounders | +25.6% | −12.2pp | 25% | 43.2 |
| reversal | +15.0% | −19.1pp | 0% | 2.3 |
| qmom (high op_yld + momentum) | +4.3% | **−33.5pp** | 0% | 158.0 |

Avg TOPIX across the 4 entries (2022-12-30, 2023-06-30, 2023-12-29,
2024-06-28): +37.9%.

**Key takeaways**:

- **The value-net-cash core works across all 4 windows.** Baseline
  sweet-spot beat TOPIX 4-for-4 by an average +6pp. Not a 2022 fluke.
- **`sales_growth` is the best usable signal.** +19.9pp excess with
  n=7.5 average — enough diversification, real edge. Adding "top-line
  accelerating" to net-cash value reliably finds compounders.
- **`top10_by_ratio` is robust as a concentrated bet.** +10.9pp excess
  with 100% win rate. The original +87.7% (3-yr hold) wasn't a fluke,
  just amplified by the longer horizon.
- **`value_quality` has the highest excess but n=1.7** — not a usable
  strategy, just a tiny set of stocks that happen to win.
- **`qmom` is consistently disastrous (−33.5pp).** High op_yield + high
  6m momentum picks late-cycle peak earners. Strong negative confirmation
  that "buy what's working" without value discipline destroys returns.
- **`cash_compounders` is too inclusive.** Big universe (n=43), mediocre
  picks. The composite filter dilutes too aggressively.

This is also the empirical answer to the surprising single-period
finding "value_momentum hurt returns." Across 4 entry dates,
value_momentum (+9.2pp) beats baseline (+6.1pp) on average. The 2022
finding was the noisy one, not the signal.

## Low-float hybrid (validated edge, recommended overlay)

**Setup**: within the sweet-spot screen (¥3-30B + ratio>1.5 + PER≤10),
take the bottom 33% by issued shares. Over 10 quarterly entry dates
(2022-12 → 2025-03) with 12-month buy-and-hold, this beats TOPIX by
**+24.5pp on average** (n=3.4 stocks per window, 80% win rate).

| Threshold | n/win | Avg return | Avg excess | MC p-value | Note |
|---|---|---|---|---|---|
| 25% | 2.4 | +49.1% | +29.4pp | 0.014 | Highest excess but single-stock risk |
| **33%** | **3.4** | **+44.2%** | **+24.5pp** | **0.006** | Recommended |
| 50% | 5.8 | +35.8% | +16.1pp | 0.226 | Edge is sweet-spot effect, not float |
| 100% (no float) | 13.4 | +33.3% | +13.6pp | — | Sweet-spot baseline |

The Monte Carlo column (p-value vs 500 random subsets per entry of
the same size within sweet-spot) is the key validation: **at the 50%
threshold the float signal is indistinguishable from random selection
within the sweet-spot.** Only at 25-33% does float add measurable
value beyond the sweet-spot effect itself.

**Why low float?** Empirically, low share count (proxy for tightly-held
ownership / illiquidity premium) within an already-cheap, net-cash-rich
universe selects for stocks where the value is more likely to be
realized via buyout, special dividend, or accumulation by long-horizon
holders rather than diluted away.

Sanity checks that ruled out alternative explanations:
- **Not "smaller stocks":** Selecting bottom 33% by market cap instead
  of issued shares gave only +6.3pp excess (vs +24.5pp for float).
- **Not single-stock dependence:** 34 total picks across 10 entries
  span 15 unique stocks. Removing the most-recurring stock still
  leaves +17.7pp excess.
- **Not just survival bias:** Per-pick distribution across all 34
  picks is nearly symmetric (skew 0.09), median +46%, P10 −1.6% —
  the edge is broad-based, not driven by outliers.

### Current picks at 33% threshold (n=7, 2026-04-26)

7 stocks chosen from today's 24-stock sweet-spot universe by lowest
issued shares:

| Code | Company | Ratio | PER | MC ¥B | Shares (M) | Existing flag |
|---|---|---|---|---|---|---|
| 46350 | 東京インキ | 3.31 | 3.0 | 3.6 | 2.73 | T |
| 56030 | 虹技 | 1.53 | 5.8 | 4.6 | 3.36 | **Q+T** |
| 80460 | 丸藤シートパイル | 5.56 | 2.4 | 3.7 | 4.00 | Q |
| 61370 | 小池酸素工業 | 2.42 | 2.3 | 8.4 | 4.52 | **Q+T** |
| 80060 | ユアサ・フナショク | 2.08 | 3.9 | 8.0 | 4.90 | T |
| 74270 | エコートレーディング | 1.84 | 5.4 | 5.4 | 6.08 | — |
| 37980 | ULSグループ | 2.85 | 2.0 | 3.3 | 6.23 | T |

5 of 7 already carry quality (Q) or turnaround (T) flags from the
fundamental screen — the float signal is correlated with but not
redundant to those signals.

## Caveats

1. **Look-ahead bias**: production `compute_screen` filters by
   `period_end <= run_date`, not `submitDateTime <= run_date`. For
   live screening this is fine (filings are knowable when submitted).
   Backtest scripts use the corrected version.

2. **IFRS reporters mis-screened**: companies on IFRS accounting
   (some industrials, including 5202 日本板硝子) get garbage values
   because our `concepts.py` only covers JGAAP namespace. Affects a
   small but non-zero subset of names. Fix would be to add IFRS
   concept IDs (`jpigp_cor:*`).

3. **Financial-sector concepts**: brokers/banks/insurance use
   `営業収益` instead of `売上高`, so `sales_yoy` is undefined for
   them. They still pass net-cash + PER filters but the turnaround
   signal can't be computed.

4. **Single-period backtest**: results are entry-2022-12-30 only.
   Different entry dates would give different numbers. The 2023-2026
   window was a strong Japan bull market; cohort returns reflect that.

5. **Survivorship bias**: stocks that delisted between 2022-12-30 and
   2025-12-30 are not in our DB. Their bad outcomes aren't reflected;
   actual realized returns of someone trying to reproduce this would
   be slightly lower.

## Reproducing

```bash
# Live screen (today's universe)
PYTHONPATH=src uv run python -c "
from datetime import date
from stock_screening import db
from stock_screening.screening.metrics import compute_screen
df = compute_screen(db.get_engine(), date.today())
qualifying = df[(df['market_cap'].between(3e9, 50e9)) & (df['ratio'] > 1.5)]
print(qualifying.sort_values('ratio', ascending=False))
"

# Re-run backtest
PYTHONUNBUFFERED=1 uv run python scripts/backtest_shikiho_style_2022.py
# Edit ENTRY_RATIO_MIN constant in the script to test different cutoffs
```

The `t_screen_results` table accumulates daily snapshots; rank changes
over time can be tracked via SQL:

```sql
SELECT run_date, "secCode", ratio, qualifies
FROM t_screen_results
WHERE "secCode" = '41160'  -- 大日精化工業
ORDER BY run_date DESC LIMIT 30;
```
