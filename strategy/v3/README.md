# Strategy v3: Indian index waterfall

v3 keeps v2's index waterfall and adds two risk rules. Both were designed on Tune, pre-registered, and checked
once on Validate and Hidden.
- **Overnight-shock guard.** If NIFTY opens at least 0.75 ATR below its previous close, that day's A trades are
  closed at 14:55 instead of being held overnight, and B carries are off. The volatility that follows an overnight
  shock tends to cluster, so the strategy avoids adding new overnight risk on those days.
- **Two-candle carry window.** Setup B may now trigger on the 14:45 candle or the 14:50 candle, whichever qualifies
  first. v2 only checked 14:50.

v3 also includes a live runner (`live_v3.py`) and an edge-health alert (§2). The runner tells you what to do after
every 5-minute candle using the same engine as the backtest.

The strategy holds one position at a time, trades one fixed lot, and only buys.
- **Entries:** strictly before 15:00.
- **Intraday exits:** by 14:55.
- **Overnight exits:** at the next session's 09:15 open.
- **P&L:** index points × lot.
- **Costs:** 2 × Zerodha charges plus slippage.

**Bottom line.**

| Whole period, 2015 – Sep 2026 | v3 | v1 (own costs) |
|---|---:|---:|
| Net | ₹26.19 L | ₹24.08 L |
| Max drawdown | ₹55 K | ₹89 K |
| Worst month | −₹32 K | −₹51 K |
| Months in profit | 80.5% | 77.5% |

Re-costed with the 2 × Zerodha charges the brief requires, v1 makes ₹22.52 L. v3 is profitable in every year. It
beats v1 on net, win rate, drawdown, participation and worst month in every split, measured with v1 re-costed
under those same charges.

It does **not** beat v1 on every number. §1 lists where it still trails. Validate is its weakest split, and the
forward weeks are negative.

---

## 1. Results

**How to read the tables:**
- **v1 own:** the unchanged v1 with its own point costs. These leave out the Zerodha charges the brief requires.
- **v1 same:** v1's trades re-costed exactly like v3.
- **Splits:** v1's 60/20/20 calendar. Validate and Hidden were scored as summaries only.
- **Months:** assigned by exit date.
- **Lowest equity:** the low point of a ₹1.5 L account started at the beginning of the split.

### Research splits

| Split | Version | Net ₹ | Trades | Win % | Avg/trade ₹ | Max DD ₹ | Days traded | Months > 0 | Worst month ₹ | Lowest equity ₹ |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Tune 2015-01-09 → 2022-01-07 | **v3** | **13,26,864** | 734 | 67.3 | 1,808 | **33,083** | 43.1% | 86.5% | −17,468 | 1,48,945 |
| | v2 | 13,07,098 | 715 | 67.1 | 1,828 | 54,837 | 42.0% | 83.8% | −33,173 | 1,49,100 |
| | v1 own | 12,56,635 | 637 | 66.4 | 1,973 | 54,016 | 37.4% | 84.8% | −32,009 | 1,39,764 |
| | v1 same | 11,82,137 | 637 | 65.6 | 1,856 | 54,838 | 37.4% | 84.8% | −33,174 | 1,37,980 |
| Validate → 2024-05-09 | **v3** | **5,11,044** | 298 | 64.1 | 1,715 | **42,122** | 52.1% | 70.4% | −17,889 | 1,37,043 |
| | v2 | 5,46,638 | 292 | 63.0 | 1,872 | 46,257 | 51.1% | 77.8% | −19,021 | 1,40,056 |
| | v1 own | 5,36,072 | 291 | 62.9 | 1,842 | 53,404 | 50.9% | 74.1% | −17,780 | 1,41,073 |
| | v1 same | 4,97,833 | 291 | 62.5 | 1,711 | 55,050 | 50.9% | 74.1% | −19,690 | 1,40,056 |
| Hidden → 2026-09-04 | **v3** | **7,80,667** | 342 | 61.1 | 2,283 | **54,835** | 59.4% | 72.4% | −31,818 | 1,38,857 |
| | v2 | 7,16,943 | 339 | 60.8 | 2,115 | 90,225 | 58.9% | 69.0% | −51,500 | 1,38,857 |
| | v1 own | 6,15,619 | 309 | 59.2 | 1,992 | 89,365 | 53.7% | 62.1% | −51,210 | 1,38,990 |
| | v1 same | 5,71,635 | 309 | 58.6 | 1,850 | 90,225 | 53.7% | 62.1% | −51,500 | 1,38,857 |

**Stress test.** With 5 extra adverse index points on every fill:

| | Tune | Validate | Hidden |
|---|---:|---:|---:|
| v3 | 11,02,736 | 4,25,467 | 6,33,186 |
| v2 | 10,87,970 | 4,61,562 | 5,70,712 |
| v1 own | 10,58,260 | 4,51,547 | 4,68,544 |

**Where v3 still trails v1.** This is the full list.
- **Tune average per trade:** ₹1,808 vs ₹1,856 (same costs) or ₹1,973 (own costs).
  - v3 also trades 2015, which v1 cannot because its indicators are not ready until April 2016.
  - Those 72 trades in 2015 add ₹74 K but average ₹1,034 each, which pulls the overall average down.
  - From 2016 on, v3 averages ₹1,892 per trade (win rate 67.7%), above v1-same's ₹1,856.
- **Validate:**
  - Months in profit: 70.4% vs 74.1%.
  - Lowest equity: ₹1.37 L vs ₹1.40 L.
  - Against v1's lighter own-cost figures, net is ₹5.11 L vs ₹5.36 L, average per trade ₹1,715 vs ₹1,842, and
    stressed net ₹4.25 L vs ₹4.52 L. Under the same costs v3 is ahead on net (₹5.11 L vs ₹4.98 L) and average.
- **Forward weeks:** −₹26.7 K vs v1's zero trades (below).

**Why not every number.** With about 15 noisy criteria, a genuinely better system rarely wins them all. Searching
until one does would just fit Validate and Hidden; see §2.

### Anchored walk-forward inside Tune (`research_v3.py --wfa`)

Each fold re-ranks the waterfall on its own training window, then applies v3's rules.

| Test year | v3 net ₹ | v3 max DD ₹ | v2 net ₹ | v1 net ₹ (same data and costs) | v1 max DD ₹ |
|---|---:|---:|---:|---:|---:|
| 2017 | 1,65,103 | 14,581 | 1,66,552 | 1,65,665 | 14,278 |
| 2018 | 1,40,435 | 33,083 | 1,23,142 | 1,26,391 | 54,837 |
| 2019 | 2,11,275 | 22,415 | 2,06,527 | 1,98,353 | 25,219 |
| 2020 | 2,34,294 | 20,732 | 2,37,332 | 2,37,332 | 21,119 |
| 2021 – Jan 2022 | 4,53,207 | 32,197 | 4,32,990 | 4,34,296 | 46,330 |
| **Total** | **12,04,314** | worst 33,083 | 11,66,543 | 11,62,038 | worst 54,837 |

- All folds are positive.
- v3 has the higher net in 3 of 5 folds and the lower drawdown in 4 of 5.

### Whole period

This is one continuous run from 2015-01-09 to 2026-09-04, produced after v3 was frozen.

| | v3 | v2 | v1 own | v1 same |
|---|---:|---:|---:|---:|
| Net ₹ | **26,18,575** | 25,70,679 | 24,08,326 | 22,51,605 |
| Per year | 2.25 L | 2.21 L | 2.07 L | 1.94 L |
| Trades / win % | 1,374 / 65.1 | 1,346 / 64.6 | 1,237 / 63.8 | 1,237 / 63.1 |
| Max drawdown ₹ | **54,835** | 90,225 | 89,365 | 90,225 |
| Months > 0 | 80.5% | 78.9% | 77.5% | 77.5% |
| Worst month ₹ | −31,818 | −51,500 | −51,210 | −51,500 |

Net ₹ by exit year:

| Year | v3 | v1 own | v1 same |
|---|---:|---:|---:|
| 2015 | 74,431 | 0 | 0 |
| 2016 | 57,216 | 76,557 | 68,942 |
| 2017 | 1,65,287 | 1,81,640 | 1,68,774 |
| 2018 | 1,40,924 | 92,608 | 80,826 |
| 2019 | 2,01,237 | 2,08,053 | 1,94,538 |
| 2020 | 2,34,294 | 2,45,679 | 2,35,352 |
| 2021 | 4,40,350 | 4,39,353 | 4,21,593 |
| 2022 | 1,75,880 | 2,11,491 | 1,98,248 |
| 2023 | 2,20,860 | 2,27,523 | 2,09,542 |
| 2024 | 4,06,820 | 3,31,286 | 3,10,160 |
| 2025 | 2,44,440 | 2,02,832 | 1,83,204 |
| 2026 (to 4 Sep) | 2,56,835 | 1,91,302 | 1,80,426 |

v3 is positive in every year. It beats v1-same in 8 of 12 years and v1-own in 6. Against v2, v3 gives up profit
in 2016, 2017 and 2022 and gains in 2018 and 2024–26.

### Forward test on unseen sessions (`results/forward_test.json`)

Entries were allowed on 7–29 Sep 2026; 30 Sep was settlement only.

| | Trades | Net ₹ | Wins | Max DD ₹ |
|---|---:|---:|---:|---:|
| v1 | 0 | 0 | – | 0 |
| v2 | 10 | −23,282 | 3 | 31,347 |
| v3 | 9 | −26,689 | 2 | 30,918 |

All the trades were Nifty Next 50. It stayed just above its own 200-day average while the other indices were below
theirs and the market fell. Nine trades is about one standard deviation of noise, and no rule was changed because of
it. Nifty Next 50 is still the least-proven index; §6 explains how to switch it off.

---

## 2. What changed from v2, and what was tested and rejected

Every rule change was designed on Tune only. Validate and Hidden were looked at, as summaries only, five times in
total, each with its pass rule written down beforehand where it was a new candidate:

| # | Candidate | Result |
|---|---|---|
| 1 | v2's 91-stock expansion | Rejected (earlier work) |
| 2 | v2 | Kept as the base |
| 3 | v3a (below) | Rejected |
| 4 | Edge monitor as a trading rule (`results/preregistration_v3.json`) | Passed its profit/drawdown check, but lowered Validate participation below v1's (50.4% vs 50.9%). It is now a **live alert only**. |
| 5 | Final v3 (`results/preregistration_v3_final.json`, check in `results/candidate_v3_final_check.json`) | **Passed every criterion in both splits and was adopted** |

The pass criteria for #5 were, in each of Validate and Hidden:
- net ≥ v2 − ₹47 K;
- max drawdown ≤ v2 and < v1;
- worst month ≥ v2;
- participation > v1.

**The two adopted rules (Tune evidence):**
- **Shock guard:**
  - Tune drawdown fell from ₹54.8 K to ₹37.0 K.
  - The cost is a little profit in calm years.
  - It applies on about 3% of sessions.
- **Two-candle carry window:** the B decision no longer depends on a single 5-minute print. It adds participation,
  and the shock guard removes some days, so the window keeps the total from falling.
- **Together:** Tune net +1.5% vs v2, drawdown ₹33.1 K, participation 43.1%, ahead of v2 in 5 of 8 years.

**Edge-health alert.** `live_v3.py` flags an index whose last 120 "carry nights" averaged ≤ 0 net. A carry night
is any session meeting setup B, held from the 14:55 open to the next 09:15 open, whether or not it was traded. The
alert is insurance against the overnight drift dying in one index. It never triggers on Tune.

**Rejected: v3a, bear-regime momentum and calendar carry** (`results/rejected_v3a_*.json`):
- **What it added:**
  - **D:** setup A's trigger below SMA200, with a 0.5 ATR stop and a ₹25 K cap on ATR × lot.
  - **C:** a 14:55 carry on pre-holiday and month-end sessions, with no up-day condition.
- **Tune:** +22% over v1, and it held up across a 48-point parameter grid.
- **Blind result:**
  - Validate ₹5.86 L.
  - Hidden ₹6.69 L, with a ₹1.90 L drawdown and a −₹1.51 L worst month.
- **Why it failed:** C lost money in both splits. D displaced better A/B trades and added bear-regime gap risk.

An independent review by a second model (Fable) found D's and C's Tune profits concentrated in a few events and in
the strong 2019–21 overnight-drift years.

**Rejected on Tune** (never sent to Validate/Hidden):

| Idea | Tune result |
|---|---|
| Volatility cap ATR14/ATR100 | Drawdown is lower only because one 2018 trade is skipped |
| Breadth or NIFTY-regime gates | −5 to −10% net |
| Dynamic index ranking | No gain without picking its window after the fact |
| Exit A at 14:55 when weak | Worse |
| Bear-regime intraday shorts | Profit came from 2020 alone |
| Different stops | No change |
| Wider 14:35–14:50 carry window | Positive in only 5 of 7 years |

---

## 3. Strategy logic (exact rules)

Times are IST candle starts. There are 75 five-minute candles a day, from 09:15 to 15:25. A signal is read on a
**completed** candle and filled at the **next** candle's open.

**Daily features** use only completed prior sessions:
- **ATR14:** Wilder ATR of the daily true range.
- **SMA200:** the 200-session average of daily closes.
- **Previous close:** the last completed session's close.

**Intraday features:**
- **Session open:** the day's first price.
- **Running mean:** the average of (H+L+C)/3 since 09:15. This is not volume-weighted.

**Gates** (every entry must pass all of them):
- **Regime:** close > SMA200.
- **Drop gate:** close − previous close > −1 × ATR14.
- **Cost gate:** ATR14 ≥ 20 × cost points, where cost points are 5 before 2021 and 10 from 2021.
- **Availability:** the index's options must be listed: FINNIFTY from 2021-01-11, MIDCPNIFTY from 2022-01-24,
  NIFTYNXT50 from 2024-04-24.

**Shock session:** NIFTY's 09:15 open ≤ NIFTY's previous close − 0.75 × NIFTY's ATR14. This is known at the open.

**Setup A – momentum-hold** (11:15–14:15 candles, so fills fall between 11:20 and 14:20):
- close − session open ≥ 0.40 × ATR14, and close > running mean. Only the first such candle per index per day
  counts.
- **Stop:** entry − 1 × ATR14, active on the entry day only. If a later candle opens through the stop, the exit is
  at that open; if a candle's low touches it, the exit is at the stop price.
- **Normal session:** exit at the next session's 09:15 open.
- **Shock session:** exit at the 14:55 open the same day.

**Setup B – strict carry** (14:45 or 14:50 candle, whichever qualifies first; filled at 14:50 or 14:55):
- close > session open and close > running mean.
- Exit at the next session's 09:15 open. There is no stop.
- B is off on shock sessions.

**Waterfall.** At every completed candle, if no position is open, the first slot with a live signal is filled.
This is one position at a time, and a blocked signal is not queued.

| Setup | Priority order |
|---|---|
| A (11:15–14:15) | MIDCPNIFTY → SENSEX → BANKNIFTY → FINNIFTY → NIFTY → NIFTYNXT50 |
| B (14:45/14:50) | MIDCPNIFTY → SENSEX → BANKNIFTY → NIFTY → FINNIFTY → NIFTYNXT50 |

How the order was set:
- Indices with Tune history are ordered by the Tune lower confidence bound of net ₹ expectancy (mean − 1 s.e.),
  using `research_v2.py --freeze`.
- MIDCPNIFTY has no Tune history, so it keeps v1's first place.
- NIFTYNXT50 was added and has no Tune history, so it goes last.

**Lots** (fixed, one lot): MIDCPNIFTY 120, SENSEX 20, BANKNIFTY 30, FINNIFTY 60, NIFTY 65, NIFTYNXT50 25.

**Data gaps.** If the held index has no data for a session, the book is blocked, and the position exits at the
next observed open. Days with missing or invalid bars are dropped, never repaired.

## 4. Costs

Each round trip is charged 2 × Zerodha option charges at the rates in force on the trade date. The premium is
estimated as 0.8% of spot.

| Charge | Rate |
|---|---|
| Brokerage | ₹20 per order |
| STT on the sell premium | 0.017% before Jun 2016, 0.05% to Mar 2023, 0.0625% to Sep 2024, 0.1% after |
| Exchange charges | NSE 0.053% → 0.03503% from Oct 2024; BSE 0.05% → 0.0325% |
| SEBI fee | ₹10 per crore |
| GST | 15% before Jul 2017, then 18% |
| Stamp duty | 0.003% on the buy |

Plus slippage:
- 5 points before 2021, 10 points from 2021, × lot.
- For NIFTYNXT50, max(10 points, 0.05% of notional).

## 5. Data

- **Supplied 5-minute files:** `data/<index folder>/YYYY_MM_DD_5m.csv`, unchanged (`data_hashes.json`).
- **`data/daily_ref/`:** daily bars from 2012.
- **`data/indices_5m/NIFTYNXT50.parquet`:** Nifty Next 50 5-minute bars.
- **`data/warmup_extra/MIDCPNIFTY.csv`:** Midcap Select closes. All of these are listed in `data_hashes_v2.json`
  and rebuilt by `fetch_data_v2.py`.
- **New sessions:** `forward_test.py` builds `data_forward/` and `live_v3.py` builds `data_live/`. Both link the
  supplied files and add new sessions validated by the same rules. Neither ever touches `data/`, which defines the
  frozen splits. Both folders can be deleted at any time.

## 6. Files and commands

| File | Purpose |
|---|---|
| `strategy_v3.py` | v3: shock guard, two-candle carry, the event engine with the 14:55 intraday exit, and the edge-health alert |
| `research_v3.py` | Fast engine twin and v3's anchored walk-forward |
| `scorecard_v3.py` | Summary-only scorecard: v3, v2 and v1 per split; `--stress N` adds adverse points per fill |
| `test_v3.py` | Engine and signal unit tests, slow/fast parity, prefix and lookahead tests (oracle-shift, shuffled-future, constant-future), and live-market parity |
| `live_v3.py` | Live signal runner (no broker integration) |
| `forward_test.py` | Fetches sessions after the supplied data, then runs v3, v2 and v1 on them |
| `strategy_v2.py`, `research_v2.py`, `scorecard_v2.py`, `test_v2.py`, `fetch_data_v2.py` | v2 (v3's base), plus its waterfall ranking (`research_v2.py --freeze`) |
| `strategy.py`, `scorecard.py` | Original v1, unchanged |
| `freeze_v3.json` | Checksums of the frozen v3 code and results (`freeze_v2.json` is v2's) |

```bash
python3 -m pip install -r requirements.txt                          # Python 3.12
OPENBLAS_NUM_THREADS=1 python3 test_v2.py && OPENBLAS_NUM_THREADS=1 python3 test_v3.py
OPENBLAS_NUM_THREADS=1 python3 scorecard_v3.py --split tune         # validate | hidden, optional --stress 5
OPENBLAS_NUM_THREADS=1 python3 research_v2.py --freeze               # waterfall ranking (Tune only)
OPENBLAS_NUM_THREADS=1 python3 research_v3.py --wfa                  # v3 walk-forward (Tune only)
OPENBLAS_NUM_THREADS=1 python3 forward_test.py                       # new sessions up to yesterday
python3 live_v3.py                                                   # live instruction (see §7)
```

## 7. How to trade it live

**Every trading day:**
1. **Before 09:15.** Run `python3 live_v3.py`. The first run of the day builds history (about a minute). If it
   says `SELL <index>`, sell that position at the 09:15 open.
2. **After every 5-minute candle closes, 09:20–15:00.** Run it again; each run takes a few seconds. It prints one
   of these:
   - `BUY 1 lot <index> (<setup>) at the HH:MM candle open; stop = …; SELL at …`. Buy right away. Use the index
     stop for A, and sell at the time it gives: 14:55 today on a shock session, otherwise the next 09:15 open.
   - `HOLD …`: keep the position until the exit it names.
   - `NOTHING`: do nothing.

   It also lists today's exits, flags a shock session, and prints the edge-health alert for any index whose
   overnight edge has faded. If an index is flagged for weeks, consider pausing it.
3. **After 15:00.** Never enter. Never hold two positions.

You can schedule it with cron, for example `*/5 9-15 * * 1-5 cd <folder> && python3 live_v3.py >> live.log`.

**Choosing the option:**
- Buy one lot of a slightly in-the-money call (delta about 0.7–0.8).
- Use the nearest expiry with at least two trading days left, and don't hold overnight into an expiry.
- Use limit orders near the mid-price. Every 5 points per fill costs about 19% of Hidden's net.

**Nifty Next 50:** check the bid-ask spread. If entry plus exit regularly costs more than about 35 index points,
remove it from `WATERFALL` in `strategy_v2.py`, or paper-trade it first.

**Expect:**
- **Drawdowns up to about ₹55 K,** a third of a ₹1.5 L account. v1/v2 had about ₹90 K. In every split equity stayed
  within ₹13 K of the start, but an account that starts just before a bad stretch takes the full drawdown.
- **Months with no trades** whenever the main indices are below their 200-day average. That is the case now
  (Oct 2026).
- **Live profit below the backtest:** options capture less than the full index move and lose time value overnight.

Start with 1–2 months of paper trading or single-lot trading, and compare real fills with the 10-point
assumption. Keep idle cash in a liquid fund.

## 8. Research record (v1 → v2 → v3)

**Protocol:**
- All selection, ranking and walk-forward work used Tune only.
- Validate and Hidden were used for summary checks only (the five looks in §2).
- The yearly, whole-period and forward reports were produced after freezing and changed nothing.

**Rejected in v2's research:**

| Idea | Result |
|---|---|
| 91 stock-option underlyings plus quiet-day index carry | Out of sample: Validate ₹3.60 L, Hidden ₹2.32 L. The quiet-day carry lost ₹1.41 L / ₹2.98 L |
| Overnight shorts | Every Tune variant loses |
| Intraday-only setups | No edge after costs |
| ETFs | About ₹4 per night |
| Crude oil | Cannot be tested blind |
| Stock momentum or carry fillers | Drawdown rose to ₹0.84–1.55 L |
| Breadth or last-hour confirmation | Inconsistent from year to year |
| Parameter tweaks | 6% or less |
| Trading every day, or two positions | More drawdown |

**Rejected in v3's research:** see §2.

**Lesson:**
- The edge is the overnight drift after strong, above-trend days.
- Carries on weak or quiet days fail out of sample.
- With one position, every added setup competes with the proven ones.
- What did transfer out of sample was risk control: not adding overnight risk right after an overnight shock.

## 9. Limitations

- **Options proxy:** index points stand in for option P&L. Strikes, greeks, premium decay and fills are not
  modelled, and costs are estimates.
- **Shock-session exits:** the 14:55 exit adds about 17 intraday round trips over Tune's seven years, and their
  costs are included.
- **SENSEX:** its options had little liquidity before May 2023. v1 trades them from 2018, and v2/v3 keep that.
- **MIDCPNIFTY:** the cost gate blocks it for most of Validate.
- **Nifty Next 50:** its evidence is thin (Hidden +₹1.28 L, forward −₹27 K).
- **Validate:** it is v3's weakest split (§1).
- **Data:** the live runner needs the public Upstox candle endpoints.
- Backtests do not guarantee future returns.
