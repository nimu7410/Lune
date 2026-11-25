# Lune tick backtester

Lune's HH/LL tick engine replayed on Lune's own stored ticks (all six indices), in Lune's UI.

Always running as systemd user service `lune-backtester` (`./run.sh` starts + opens it; `./run.sh stop|restart|log`).
Local: http://127.0.0.1:8060. Any device on your tailnet: http://free-a1.tail57c420.ts.net:8060
(`sudo tailscale serve --bg --tcp 8060 tcp://127.0.0.1:8060`, tailnet only; remove with `sudo tailscale serve --tcp=8060 off`).
Self-validation: `../venv/bin/python selfcheck.py` (~7 min; `selfcheck.py parity lookahead` for sections).

- `ticks.py` copies every finished session from Lune's database (`hhll.tick_archive`) into `data/ticks/`,
  byte for byte, SHA-256 and decode checked; SYNC in the header (or `python ticks.py`) pulls new ones.
  The session in progress is never used. Lune's settings in force are saved with each sync (LIVE button).
- `engine.py` is a copy of Lune's `hhll_tick.py` engine (pivots, costs, `Book`), so tuning here never
  touches the live trader. Added: `fill="next"` (default): a market order fills on the first tick stamped
  after decision + `latency_ms`, never on the tick that decided it. `fill="same"` is Lune's paper fill,
  and `selfcheck.py` proves it trades exactly like Lune's own engine.
- **Big legs** (`backtest.big`, `BIG_LEG`; PIVOTS → Big legs; the default until 15 Nov 2026), not Lune's own (LIVE button):
  one threshold per index (about a quarter of its median day range), no minimum leg. Every leg is at least the
  threshold; a pivot is the swing's true extreme, printed on the first tick that far back from it; nothing is
  forced, redrawn or held back; HH and LL alternate (`selfcheck.py biglegs` proves each on every session).
  That print is the earliest there is for legs that long: a pivot confirmed c from its extreme can be passed
  again from there, and then the next pivot is c away (a short leg), or the pivot is redrawn, or nothing prints
  (a stall). So confirmation distance = shortest leg, and the threshold in PIVOTS moves both; the line under the
  session net shows the session's legs and confirmation times. Lune's live pivots confirm sooner (smaller
  threshold + minimum leg) and pay for it in forced short legs. 3-13 Nov 2026, 8 sessions:

  | | pivots / day | shortest leg | median leg | legs under the leg setting | median confirmation |
  |---|---|---|---|---|---|
  | NIFTY big / live | 7.1 / 17.6 | 63 / 25 pts | 108 / 73 | 0 of 49 / 50 of 133 | 11.5 / 3.1 min |
  | BANKNIFTY | 5.0 / 1.9 | 218 / 281 | 369 / 368 | 0 of 32 / 5 of 7 | 13.9 / 2.9 |
  | SENSEX | 9.8 / 22.4 | 135 / 64 | 287 / 198 | 0 of 70 / 61 of 171 | 8.6 / 2.4 |
  | FINNIFTY | 4.4 / 3.5 | 115 / 99 | 193 / 233 | 0 of 27 / 8 of 20 | 15.1 / 9.9 |
  | MIDCPNIFTY | 5.0 / 14.1 | 51 / 19 | 76 / 49 | 0 of 32 / 40 of 105 | 12.8 / 3.6 |
  | NIFTYNXT50 | 5.9 / 4.4 | 224 / 147 | 322 / 293 | 0 of 39 / 15 of 27 | 11.7 / 6.2 |

  The engine is untouched (min leg 0 is its one-threshold zigzag), so the parity proof with Lune still holds.
- **Fast + min leg** (`backtest.fast`, `FAST_LEG`; PIVOTS → Fast + min leg): threshold a quarter of the big leg,
  the big leg as min leg. Pivots print about a minute after their extreme, never repaint, never stall, always
  alternate (`selfcheck.py fastlegs`), but about half the legs are short: a pivot that price breaks before the next
  big leg forces the next pivot on the spot. That cannot be avoided: HH printed, price runs past it and keeps going;
  the HH cannot move (repaint), the chain must follow price (no stall), so an LL prints between them, a short leg.
  3-13 Nov 2026, 8 sessions, legs at least the min leg / median confirmation: NIFTY 51% / 0.7 min, BANKNIFTY 42% /
  1.0, SENSEX 47% / 0.6, FINNIFTY 41% / 1.0, MIDCPNIFTY 46% / 1.0, NIFTYNXT50 46% / 1.1 (big legs: 100% / 9-15 min).
  Threshold half the big leg: 56-70% / 2-5 min. The line under the session net counts the session's.
- **Scaled legs** (`backtest.scaled`, `SCALED_LEG`; the default since 15 Nov 2026; PIVOTS → Scaled legs): one rule for
  every index in units of its own big leg U (volatility-scaled): threshold 0.4 U, min leg 4 U (about a day range), leg
  filter 2x R, Lune's default trade rules pinned (not read from the last sync). `selfcheck.py scaledlegs` proves
  Lune's literal pivot rules, alternation, no repaint and no stall on every session. Why:
  - HH confirmation - LL confirmation = leg - 2 x threshold, the gross of a trade from one confirmation to the next.
    On a memoryless price that averages exactly 0 for any rule that confirms in bounded time (optional stopping), and
    the share of ordered pairs is e^-1 = 37% at any threshold (50% at best, min leg = 2x threshold). No setting makes
    it always true: the only causal way (never force, wait) tested at -31k net, 2.4 lakh drawdown.
  - Real ticks against the same ticks shuffled (same moves, no memory): no edge at big legs (-0.10c vs -0.11c per
    pivot pair), +0.20c to +0.22c at thresholds of 1/2 to 1/4 of U (3-4 standard errors). The edge is at the short scale.
  - Picked as the best neighbourhood of a 5 x 6 x 4 grid (threshold x min leg x leg filter) on net - 1.5 x max DD.
  3-13 Nov 2026, 8 sessions, next-tick fills (Lune live settings in brackets, which were tuned on these same days):
  net 3.53 lakh (4.38), max DD 28k (31k), 82 trades (486), PF 3.6 (1.9), 6 of 8 days up (8), worst day -11k (+13k).
  Every index's threshold and min leg nudged x0.8-1.25 (25 runs): net 2.38-3.73 lakh (1.74-4.38), DD at most 37k (58k).
  +1 s latency and 3x spread + slippage: 3.44 lakh, DD 28k (3.86 lakh, 35k). Long 3.16 lakh, short 0.36 lakh: these
  sessions were a rally (indices +4.7 to +10.5%), so expect less in a falling or flat market. Only 8 sessions: re-check
  after every SYNC.
- **Needle** (NEEDLE at the top of the settings; backtester only): every pivot starts a needle on the tick that confirms
  it (the dash, where the entry is decided). Its move is how far price has gone the trade's way since (to its best
  price E). Once the move reaches ARM x R, the RULE ends the needle, and its trade with it (exit `needle`):
  - Give back part (`giveback`, arm 1, give back 0.5): price gives back that share of the best move (locks the rest).
  - Back to entry (`entry`, arm 2): price comes back to the dash (protects the entry, the rest runs).
  - Give back + stall (`stall`, arm 1, give back 0.7, wait 1800 s; Fable 5.2's idea): gives back that share AND no new
    extreme for WAIT seconds. Winners make new extremes; a needle that stalls and bleeds back is over.
  Every needle also ends at the next pivot (that reversal closes its trade, whatever EXIT ON says). Each one is shaded
  on the chart as the area between the price and the dash price, dash to end, greyed out translucently. Pivots never
  change (`selfcheck.py needle` re-derives every needle in exact paise and proves no lookahead).
  3-13 Nov 2026, 8 sessions, scaled legs, 82 trades (in sample): off 3.53 lakh, max DD 28k; give back 1.21 lakh, DD 9.3k;
  back to entry 2.58 lakh, DD 11k; give back + stall 3.18 lakh, DD 13.6k.
- **Live Lune** (PIVOTS → Live Lune): Lune's live HH/LL settings per index (R, threshold, min leg) as of the last
  SYNC, pivots only; every other setting (needle, filters, exits) stays. LIVE loads all of Lune's settings instead.
- No new entry from 15:00 (LAST ENTRY can only be earlier) and every trade is closed at 15:10.
- `backtest.py`: runs (each index-session from a fresh book, as Lune does), stats, sweeps of one or two
  settings, and walk-forward: each session traded on the combination that did best on earlier sessions only.
- Results with Lune's current settings are in sample (they were tuned on these days); trust the walk-forward number.
