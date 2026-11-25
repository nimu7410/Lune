# strategy_logic.py
"""Live wiring for the frozen strategy v3 (`strategy/v3`, used unchanged).

v3 makes every decision - when to buy, on which index (its waterfall), its
stop and its exit; this module runs it on time and hands the result to the
trade managers. Each v3 BUY is traded as one lot of a synthetic long future
(`getdata.GetSymbol`). Its data lives under `backups/` - the history it was
tested on (`data`) and the live cache (`data_live`) - and every new candle
comes from Shoonya. After each five-minute candle closes:

1. `build_live` brings `backups/data_live/` up to date from Shoonya - the
   sessions since the last complete one, then today's completed candles -
   validated and written exactly as v3's own live runner does it
   (`forward_test.good_days`, `live_v3.prefix_ok`);
2. `live_v3.market` splices today into the cached history (equal to a full
   live load - v3's own tests prove it);
3. the frozen engine is replayed over the last sessions exactly as
   `live_v3.instruction` does, and the entry it schedules is the trade.

Lune trades v3's carry (B) only, as the night trade. The momentum setup (A)
is retired: its slots are dropped before the replay (`live_slots`), so it
neither signals nor holds the slot. B is signalled exactly as frozen - on the
14:45 or 14:50 candle, its gates, the waterfall and the shock guard - but it is
filled at `NIGHT_ENTRY_TIME` (15:11), once the HH/LL desk has squared its day
off at 15:10, and only while the desk holds nothing (`set_entry_blocker`). The
managers close it at the next session's 09:15 open, and the desk takes no new
day trade until it is closed.
"""

from __future__ import annotations

import copy
import filecmp
import logging
import os
import pickle
import shutil
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date as dt_date, datetime, time as dt_time, timedelta
from functools import partial
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import numpy as np
import pandas as pd
import pyarrow

import ist
import log_throttle
from config import DATA_SETTINGS, TRADE_SETTINGS, parse_holiday
from data_handler import shoonya_candles
from log_throttle import LogThrottle

V3_DIR = Path(__file__).resolve().parent / "strategy" / "v3"
#: The strategy's data: the history v3 was tested on (byte for byte the same
#: files as `strategy/v3/data`), and the live cache - the history plus every
#: session since, from Shoonya.
DATA_DIR = Path(__file__).resolve().parent / "backups" / "data"
LIVE_DIR = Path(__file__).resolve().parent / "backups" / "data_live"
if str(V3_DIR) not in sys.path:
    # v3's modules import each other by bare name (`import strategy_v2`).
    # Appended, so none of them can shadow a module of Lune's.
    sys.path.append(str(V3_DIR))

# The frozen folder stays exactly as released: no bytecode caches written into it.
_write_bytecode, sys.dont_write_bytecode = sys.dont_write_bytecode, True
import forward_test as F  # noqa: E402
import live_v3 as L  # noqa: E402
import strategy_v2 as V2  # noqa: E402
import strategy_v3 as V3  # noqa: E402
sys.dont_write_bytecode = _write_bytecode

LOGGER = logging.getLogger(__name__)

# Refresh failures repeat every retry; see log_throttle.py.
_THROTTLE = LogThrottle()

P = V2.P
#: The portfolio, in v3's own instrument order (`strategy_v2.Market.names`).
NAMES: tuple[str, ...] = tuple(V2.GIVEN) + tuple(V2.EXTRA)
#: One fixed lot per index, as v3 trades it.
LOTS: dict[str, int] = {name: int(V2.LOTS[name]) for name in NAMES}

BAR_INTERVAL_MINUTES = 5
FULL_SESSION_BARS = 75
SESSION_OPEN_TIME = dt_time(9, 15)
SESSION_LAST_BAR_TIME = dt_time(15, 25)


def bar_time(bar: int) -> dt_time:
    """Start of candle `bar` (the 09:15 candle is 0)."""
    minutes = 555 + BAR_INTERVAL_MINUTES * int(bar)
    return dt_time(minutes // 60, minutes % 60)


#: The setups Lune trades. v3's momentum (A) is retired; only the carry (B),
#: the night trade, is replayed and traded.
LIVE_SETUPS: tuple[str, ...] = ("B",)

#: Every position is sold at the next session's 09:15 open.
CARRY_EXIT_TIME = SESSION_OPEN_TIME
#: A shock-session A trade was sold at the 14:55 open. A is retired; this is
#: kept only so a row opened before that still exits by its own rule.
INTRADAY_EXIT_TIME = bar_time(P["last_entry_bar"])
#: The night trade's fill: after the HH/LL desk's 15:10 square-off
#: (`manual_desk.ManualSettings.square_off_at`), set in `config.TradeSettings`.
NIGHT_ENTRY_TIME = TRADE_SETTINGS.night_entry_at
#: The last moment an entry can start; every position is opened before it.
LAST_ENTRY_TIME = NIGHT_ENTRY_TIME
#: The first candle that can signal B (14:45); option legs are kept ready from
#: its start so they are built well before the night fill.
LEGS_OPEN_TIME = bar_time(min(V3.B_CANDLES))
#: The last candle that can carry B (14:50), in minutes since midnight. The
#: console's carry window closes with it; `static/Lune-v6.js` mirrors it.
CARRY_TOD = 555 + BAR_INTERVAL_MINUTES * max(V3.B_CANDLES)

#: How long after `NIGHT_ENTRY_TIME` the order may still go in - wide enough
#: for the two-leg hedge, never so late it is no longer the night fill.
ENTRY_WINDOW_SECONDS = 180
#: When the night fill window closes (15:14). No entry can fill after it, so no
#: option legs are needed after it either.
LEGS_CLOSE_TIME = (datetime.combine(dt_date.min, NIGHT_ENTRY_TIME)
                   + timedelta(seconds=ENTRY_WINDOW_SECONDS)).time()
#: How long an entry waits for an index ranked above it whose signal candle has
#: not arrived yet. Past it that slot is not allocated: the index that might
#: have won it could not be read.
SETTLE_SECONDS = 45

#: How the trade row records which of v3's holds opened it.
#: The two A books are retired (no new row is opened on them); a row opened
#: before that is still managed by its own exit rules.
MOMENTUM_BOOK = "v3_momentum"            # A (retired): entry-day stop, sold at the next 09:15 open
INTRADAY_BOOK = "v3_momentum_intraday"   # A (retired) on a shock session: sold at 14:55
CARRY_BOOK = "v3_carry"                  # B, the night trade: no stop, sold at the next 09:15 open
BOOK_OF_HOLD = {V3.STOP: MOMENTUM_BOOK, V3.INTRADAY: INTRADAY_BOOK, V3.CARRY: CARRY_BOOK}


def is_momentum_book(book: Optional[str]) -> bool:
    """A trades carry the entry-day stop; B never does."""
    return book in (MOMENTUM_BOOK, INTRADAY_BOOK)


def live_slots(m) -> list:
    """v3's slots (`strategy_v3.build_slots`, in waterfall order) for the setups Lune trades."""
    return [slot for slot in V3.build_slots(m) if slot[0] in LIVE_SETUPS]


# =============================================================================
# Time helpers
# =============================================================================
#: The candle time format `DataHandler` publishes. Chart markers must use the
#: byte-identical format or they land on a different x-axis scale.
FEED_TIME_FORMAT = ist.FEED_TIME_FORMAT


def now_ist() -> datetime:
    """Current time as an IST-naive datetime, matching the candle times."""
    return ist.now_naive()


def parse_bar_timestamp(value: Any) -> Optional[datetime]:
    """Read a feed timestamp as IST-naive."""
    parsed = ist.parse_feed_time(value)
    return None if parsed is None else parsed.replace(tzinfo=None)


def to_feed_time(value: Any) -> str:
    """Render a timestamp in the feed's own candle-time format."""
    rendered = ist.format_feed_time(value)
    return str(value) if rendered is None else rendered


def candles_closed(now: datetime) -> int:
    """How many of `now`'s session candles have closed by `now` (0..75)."""
    elapsed = (now - datetime.combine(now.date(), SESSION_OPEN_TIME)).total_seconds()
    return max(0, min(FULL_SESSION_BARS, int(elapsed // (BAR_INTERVAL_MINUTES * 60))))


def bias_tag(direction: str) -> str:
    """Manager direction -> the directional label `GetSymbol` builds legs for.

    'HH' is bullish, 'LL' bearish. A *direction* label, not a pivot type.
    """
    return "HH" if direction == "LONG" else "LL"


# =============================================================================
# Console diagnostics
# =============================================================================
CARRY_GATES = (
    "Regime", "ATR vs cost", "Drop gate", "Positive session", "Running average",
    "No overnight shock",
)


def signal_reach(bar: int, close: float, line: float, f: dict[str, float], *, shock: bool
                 ) -> Optional[dict[str, Any]]:
    """How far `close` is from the price at which the carry's price gates pass.

    Every price gate is a floor under the close - SMA200, the drop limit, the
    session open and the running mean `line` - so one level answers "how far
    from a signal". Only the carry (B) is traded. `open` is False when no price
    can do it: past the carry window, an ATR below cost, or a shock session.
    Diagnostic, like the gates.
    """
    atr, sma = f["atr"], f["sma"]
    if not (atr == atr and atr > 0 and sma == sma):
        return None
    level = max(sma, f["pC"] - P["drop"] * atr, f["dO"], line)
    open_ = atr >= P["cov"] * f["cost"] and bar <= max(V3.B_CANDLES) and not shock
    return {"leg": "carry", "open": bool(open_),
            "price": round(level, 2), "points": round(level - close, 2),
            "atr": round((level - close) / atr, 2)}


def score_gates(
    bar: int, close: float, mean: float, f: dict[str, float], *, shock: bool, accepted: bool,
    line: Optional[float] = None,
) -> dict[str, Any]:
    """v3's carry (B) conditions for one candle of one index, by name.

    Diagnostic only: `strategy_v2.setup_signal` decides. This re-reads the same
    comparisons so the console can say which gate held a candle out, which is
    otherwise indistinguishable from a day that never met the trigger. Only the
    carry is traded, so its gates are the ones scored on every candle; `window`
    says whether the candle is one B can signal on.
    """
    atr, sma, prev_close, session_open = f["atr"], f["sma"], f["pC"], f["dO"]
    finite = atr == atr and atr > 0
    regime = finite and sma == sma and close > sma
    coverage = finite and atr >= P["cov"] * f["cost"]
    drop = finite and (close - prev_close) / atr > -P["drop"]
    carry = [{"name": name, "ok": bool(ok)}
             for name, ok in zip(CARRY_GATES, (regime, coverage, drop, close > session_open,
                                               close > mean, not shock))]
    window = bar in V3.B_CANDLES
    gates = carry
    tod = 555 + BAR_INTERVAL_MINUTES * bar
    return {
        "confirmed_time": to_feed_time(datetime.combine(f["day"], bar_time(bar))),
        "gates": gates,
        "passed": sum(gate["ok"] for gate in gates),
        "total": len(gates),
        "accepted": bool(accepted),
        "blocked_by": [gate["name"] for gate in gates if not gate["ok"]],
        "tod": tod,
        "carry": {
            "passed": sum(gate["ok"] for gate in carry),
            "total": len(carry),
            "blocked_by": [gate["name"] for gate in carry if not gate["ok"]],
            "window": window,
            "accepted": bool(accepted) and window,
        },
        "daily_atr": round(atr, 2) if finite else None,
        "bar": bar,
        # `line` is the close the mean gate needs; on a forming bar that is not
        # `mean`, which already counts the close itself.
        "reach": signal_reach(bar, close, mean if line is None else line, f, shock=shock),
    }


# =============================================================================
# One replay of the frozen engine
# =============================================================================
@dataclass(frozen=True)
class Snapshot:
    """What v3 knows and wants as of its newest completed candle. Never mutated."""

    as_of: datetime
    day: dt_date                     # the session the replay ended on
    live: bool                       # `day` is today and holds today's candles
    candles: int                     # completed candles of `day` (75 for a past one)
    entry: Optional[dict[str, Any]]  # the entry v3 has scheduled, or None
    shock: bool                      # `day` is a shock session
    edge_alerts: tuple[str, ...]     # indices whose overnight edge has faded
    behind: tuple[str, ...]          # indices trading today that lack the newest candle
    open_prices: dict[str, float]    # today's 09:15 open per index
    markers: dict[str, tuple]        # index -> today's signals, for the chart
    verdicts: dict[str, dict]        # index -> gates on its last completed candle
    forecast: dict[str, dict]        # index -> session constants for the live gates
    position: Optional[dict[str, Any]]  # v3's own book (diagnostic)
    exits: tuple[dict[str, Any], ...]   # v3's own exits on `day` (diagnostic)


def night_entry(m, slots, engine, d: int, counts) -> Optional[dict[str, Any]]:
    """Session `d`'s carry, as v3 allocated it, filled at `NIGHT_ENTRY_TIME`.

    v3 schedules B at the open after its signal candle (14:50 or 14:55) and
    then holds it in its own book; Lune fills that same allocation - the index
    that won the waterfall on the first qualifying B candle - at the night fill
    instead. So the carry is read from the engine whichever stage it is at:
    scheduled (`pending`), held (`position`), or, after a data gap, closed.
    """
    signal = None                                   # (slot, signal bar)
    if engine.pending is not None and engine.pending[0][0] == d:
        (_, b), j = engine.pending
        signal = (j, b - 1)
    else:
        held = engine.position
        if held is None or held["d0"] != d:
            held = next(({"k": m.idx[t["sym"]], "b0": t["b0"]} for t in engine.trades
                         if t["d0"] == d), None)
        if held is not None:
            j = next(i for i, s in enumerate(slots) if s[1] == held["k"])
            signal = (j, int(held["b0"]) - 1)
    if signal is None:
        return None
    j, b = signal
    setup, k, hold, _ = slots[j]
    day = m.cal[d].date()
    ahead = {slots[i][1] for i in range(j)}
    return {
        "index": m.names[k],
        "setup": setup,
        "book": BOOK_OF_HOLD[hold],
        "stop_loss_points": 0.0,                    # B has no stop
        "signal_time": datetime.combine(day, bar_time(b)),
        "execute_time": datetime.combine(day, NIGHT_ENTRY_TIME),
        # Every index that signalled on the bar, in waterfall order.
        "candidates": tuple(dict.fromkeys(
            m.names[s[1]] for s in slots if s[3][d, b] and m.valid[s[1], d])),
        # Indices ranked above the winner whose signal candle is missing:
        # until it arrives, the winner is not known.
        "unsettled": tuple(sorted(m.names[i] for i in ahead
                                  if m.valid[i, d] and counts[i] <= b)),
    }


def todays_features(m, root: Path) -> None:
    """Give the live session the daily features v3 defines for it, in place.

    v3 reads each session's ATR14, SMA200 and previous close from the completed
    sessions before it (`strategy_v2.Market._daily`, shifted one session). That
    needs a daily row for the session itself, and `live_v3.market` leaves the
    live one without: its candles are partial, and the supplied daily reference
    that used to stand in for it ends on 2026-09-04. Every live day after that
    would read NaN - no ATR, no SMA200, no previous close - and no entry could
    ever fire. A placeholder bar gives today its row; the one-session shift
    means its values are never read, so v3's own `_daily` returns exactly the
    features of the completed sessions.
    """
    d = len(m.cal) - 1
    probe = copy.copy(m)
    for name in "OHLC":
        arr = getattr(m, name).copy()
        arr[:, d] = 1.0
        setattr(probe, name, arr)
    probe.valid = m.valid.copy()
    probe.valid[:, d] = True
    probe._daily(root)
    for name in ("atr", "sma", "pC"):
        getattr(m, name)[:, d] = getattr(probe, name)[:, d]


def replay(m, now: datetime) -> Snapshot:
    """`live_v3.instruction`'s replay, on the setups Lune trades (`live_slots`),
    returning the engine's state instead of text."""
    moment = pd.Timestamp(now)
    slots = live_slots(m)
    engine = V3.Engine(m, slots)
    d = len(m.cal) - 1
    live = bool(m.cal[d] == moment.normalize())
    counts = np.isfinite(m.C[:, d]).sum(1)
    n = int(counts.max()) if live else FULL_SESSION_BARS
    for session in range(max(0, d - 3), d + 1):
        for b in range(FULL_SESSION_BARS if session < d else n):
            engine.on_open(session, b)
            engine.on_close(session, b)

    day = m.cal[d].date()
    shock = bool(V3.shock_sessions(m)[d])
    behind = tuple(name for k, name in enumerate(m.names)
                   if live and m.valid[k, d] and counts[k] < n)

    entry = night_entry(m, slots, engine, d, counts)

    position = None
    if engine.position is not None:
        p = engine.position
        position = {"index": m.names[p["k"]], "setup": p["setup"], "entry": float(p["entry"]),
                    "since": m.cal[p["d0"]].date(), "stop": p["stop"], "intraday": p["intraday"]}
    exits = tuple(
        {"index": t["sym"], "setup": t["setup"], "reason": t["reason"],
         "time": datetime.combine(day, bar_time(t["b1"])) if t["b1"] < FULL_SESSION_BARS
         else datetime.combine(day, SESSION_LAST_BAR_TIME), "price": float(t["exit"])}
        for t in engine.trades if t["d1"] == d
    )

    open_prices: dict[str, float] = {}
    markers: dict[str, tuple] = {}
    verdicts: dict[str, dict] = {}
    forecast: dict[str, dict] = {}
    if live:
        cost = 5.0 if day.year < 2021 else 10.0
        for k, name in enumerate(m.names):
            c = int(counts[k])
            if not m.valid[k, d] or c == 0:
                continue
            O, H, Lo, C = m.O[k, d, :c], m.H[k, d, :c], m.L[k, d, :c], m.C[k, d, :c]
            open_prices[name] = float(O[0])
            sums = np.cumsum((H + Lo + C) / 3.0)
            f = {"atr": float(m.atr[k, d]), "sma": float(m.sma[k, d]), "pC": float(m.pC[k, d]),
                 "dO": float(O[0]), "cost": cost, "day": day}
            signals = [(s[0], s[3][d, :c]) for s in slots if s[1] == k]
            markers[name] = tuple(
                {"time": to_feed_time(datetime.combine(day, bar_time(b))), "price": float(C[b]),
                 "low": float(C[b]), "type": "LL", "bias": "HH",
                 "leg": "carry" if setup == "B" else "momentum",
                 "confirmed_time": to_feed_time(datetime.combine(day, bar_time(b)))}
                for b in range(c) for setup, sig in signals if sig[b]
            )
            last = c - 1
            verdicts[name] = score_gates(
                last, float(C[last]), float(sums[last] / c), f, shock=shock,
                accepted=any(sig[last] for _setup, sig in signals))
            forecast[name] = {**f, "bar": last, "sum": float(sums[last]),
                              "closed_at": datetime.combine(day, bar_time(c))}

    return Snapshot(
        as_of=moment.to_pydatetime(), day=day, live=live, candles=n, entry=entry, shock=shock,
        edge_alerts=tuple(s for s, ok in zip(m.names, V3.edge_ok(m)[:, d]) if not ok),
        behind=behind, open_prices=open_prices, markers=markers, verdicts=verdicts,
        forecast=forecast, position=position, exits=exits,
    )


# =============================================================================
# The session calendar and the legs window
# =============================================================================
#: The exchange holidays configured for Lune (`config.TRADE_SETTINGS`): no
#: session is fetched for them.
_HOLIDAYS = pd.DatetimeIndex(sorted(
    {pd.Timestamp(day) for day in map(parse_holiday, TRADE_SETTINGS.expiry_holidays) if day}))


def in_legs_window(now: datetime) -> bool:
    """From the first B candle until the night fill window closes."""
    return LEGS_OPEN_TIME <= now.time() <= LEGS_CLOSE_TIME


# =============================================================================
# Market data, from Shoonya
# =============================================================================
#: The columns of v3's session files.
CSV_COLUMNS = ["Datetime", "Open", "High", "Low", "Close", "Volume"]


class MarketData:
    """The strategy's candles, from Shoonya: the six indices, 5-minute."""

    def __init__(self, api, tokens: dict[str, tuple[str, str]]) -> None:
        self.api = api
        # (exchange, token) of each index's spot feed, as main.py resolved them.
        self.tokens = dict(tokens)

    def candles(self, name: str, start: datetime, end: Optional[datetime] = None) -> pd.DataFrame:
        """`name`'s candles from `start` (to `end`), IST wall clock, in the
        columns of v3's session files."""
        exch, tok = self.tokens[name]
        bars = shoonya_candles(self.api, exch, tok, start, end)
        if bars.empty:
            return pd.DataFrame({column: pd.Series(dtype="datetime64[ns]" if column == "Datetime"
                                                   else float) for column in CSV_COLUMNS})
        return pd.DataFrame({"Datetime": bars.time.dt.tz_localize(None), "Open": bars.open,
                             "High": bars.high, "Low": bars.low, "Close": bars.close,
                             "Volume": bars.volume})


def settled(bars: pd.DataFrame, now: datetime) -> pd.DataFrame:
    """The candles of `bars` that closed at least `boundary_grace_seconds` ago:
    the chart feed's allowance for the broker to finish a candle."""
    done = bars.Datetime + pd.Timedelta(minutes=BAR_INTERVAL_MINUTES,
                                        seconds=DATA_SETTINGS.boundary_grace_seconds)
    return bars[done <= pd.Timestamp(now)]


def _replace(path: Path, write: Callable[[Path], None]) -> bool:
    """Write `path` through a temporary file, so a killed process never leaves
    it torn. False, and nothing written, when the content is what is there."""
    tmp = path.with_name(f".{path.name}.tmp")
    write(tmp)
    if path.exists() and filecmp.cmp(tmp, path, shallow=False):
        tmp.unlink()
        return False
    os.replace(tmp, path)
    return True


def save_session(live: Path, name: str, day: pd.Timestamp, bars: pd.DataFrame) -> bool:
    """`name`'s candles of session `day`, written as v3's own live runner
    writes them. True when that changed the file."""
    if name in V2.BASE.DATA_DIRS:
        path = live / V2.BASE.DATA_DIRS[name] / f"{day:%Y_%m_%d}_5m.csv"
        rows = bars.assign(Datetime=bars.Datetime.dt.strftime("%Y-%m-%d %H:%M:%S"))[CSV_COLUMNS]
        return _replace(path, lambda tmp: rows.to_csv(tmp, index=False))
    path = live / "indices_5m" / f"{name}.parquet"
    old = pd.read_parquet(path)
    rows = pd.concat([old[old.Datetime.dt.normalize() != day],
                      bars[["Datetime", "Open", "High", "Low", "Close"]]]).sort_values("Datetime")
    return _replace(path, lambda tmp: rows.to_parquet(tmp, index=False))


def link_history(source: Path, live: Path) -> None:
    """Link the history v3 was tested on into `live`, laid out as `forward_test.build` lays it."""
    def link(target: Path, at: Path) -> None:
        if not at.exists():
            if at.is_symlink():
                at.unlink()                  # left dangling by a move of the data
            os.symlink(os.path.relpath(target, at.parent), at)

    for folder in V2.BASE.DATA_DIRS.values():
        (live / folder).mkdir(parents=True, exist_ok=True)
        for f in (source / folder).glob("*_5m.csv"):
            link(f, live / folder / f.name)
    for sub in ("daily_ref", "warmup_extra"):
        link(source / sub, live / sub)
    (live / "indices_5m").mkdir(exist_ok=True)
    for name in V2.EXTRA:
        path, seed = (folder / "indices_5m" / f"{name}.parquet" for folder in (live, source))
        if not path.exists() and seed.exists():
            bars = pd.read_parquet(seed).drop_duplicates("Datetime").sort_values("Datetime")
            _replace(path, lambda tmp: bars.to_parquet(tmp, index=False))


def build_live(data: MarketData, now: datetime, live: Path = LIVE_DIR,
               source: Path = DATA_DIR, refetch_last: bool = False) -> bool:
    """`live_v3.build`, from Shoonya: bring `live` up to `now`.

    `live` holds the history v3 was tested on (linked from `source`), every
    complete session since, and today's completed candles. A session is kept
    only as v3 keeps its own (`forward_test.good_days`: an exact 09:15-15:25
    grid and sane prices, a bad day dropped and never repaired) and today's
    candles only as a gap-free prefix (`live_v3.prefix_ok`). `refetch_last`
    reads the last complete session again, once a day: its closing candle may
    have been saved while the broker was still finishing it. True when a past
    session's file changed.
    """
    moment = pd.Timestamp(now)
    today = moment.normalize()
    link_history(source, live)
    supplied_end = pd.Timestamp(V2.SPLITS["hidden"][1])
    first, stop = f"{supplied_end:%Y_%m_%d}", f"{today:%Y_%m_%d}"
    complete = [pd.Timestamp(p.stem[:10].replace("_", "-"))
                for p in (live / V2.BASE.DATA_DIRS["NIFTY"]).glob("*_5m.csv")
                if first < p.stem[:10] < stop and len(p.read_text().splitlines()) == 76]
    last = max([supplied_end] + complete)
    days = pd.bdate_range(last + pd.Timedelta(days=1), today - pd.Timedelta(days=1))
    days = list(days[~days.isin(_HOLIDAYS)])
    if refetch_last and last > supplied_end:
        days.insert(0, last)
    changed = False
    for day in days:
        end = day + pd.Timedelta(hours=23, minutes=59, seconds=59)
        nifty = data.candles("NIFTY", day, end)
        if nifty.empty:
            continue                         # no session that day
        for name in NAMES:
            bars = nifty if name == "NIFTY" else data.candles(name, day, end)
            session = F.good_days(bars).get(day)
            if session is not None:
                changed |= save_session(live, name, day, session)
    for name in NAMES:
        bars = settled(data.candles(name, today), moment)
        bars = bars[bars.Datetime.dt.normalize() == today].reset_index(drop=True)
        if not len(bars):
            continue
        if not L.prefix_ok(bars, today):
            raise RuntimeError(f"{name}: today's candles are not a gap-free prefix; "
                               "wait for the next candle")
        save_session(live, name, today, bars)
    return changed


# =============================================================================
# The live service
# =============================================================================
class StrategyEngine:
    """Keeps the v3 replay current and answers the managers and the console.

    A background thread refreshes after every candle close, so a Shoonya call
    that hangs can never hold up a stop or an exit in the trade loop. Readers
    only ever take the newest `snapshot`, replaced whole.
    Without a `source` nothing is fetched: the data on disk is replayed as it is.
    """

    #: Seconds between attempts while the newest candle is still missing.
    RETRY_SECONDS = (2.0, 3.0, 5.0, 10.0, 20.0, 30.0)
    #: How long a failed refresh (network, a broken candle prefix) waits.
    FAILURE_RETRY_SECONDS = 30.0

    def __init__(
        self,
        *,
        root: Path = LIVE_DIR,
        source: Optional[MarketData] = None,
        history: Path = DATA_DIR,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self.root = Path(root)
        self.source = source
        self.history = Path(history)
        self._now = now_fn or now_ist
        self.snapshot: Optional[Snapshot] = None
        self._settled_day: Optional[dt_date] = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._next_try = 0.0
        self._attempts = 0
        self._target = -1
        self._reported: set[str] = set()
        # Says why an entry may not be filled right now (the HH/LL desk still
        # holds a position), or None; see `set_entry_blocker`.
        self._entry_blocker: Optional[Callable[[], Optional[str]]] = None

    # -- refreshing --------------------------------------------------------
    def refresh(self, now: Optional[datetime] = None) -> Snapshot:
        """Bring the data up to `now` and replay v3's signals."""
        moment = pd.Timestamp(now or self._now())
        with self._lock:
            if self.source is not None:
                self._build(moment)
            m = self._market(moment.normalize())
            snapshot = replay(m, moment)
            self.snapshot = snapshot
        self._announce(snapshot)
        return snapshot

    def _build(self, moment: pd.Timestamp) -> None:
        refetch = self._settled_day != moment.date()
        try:
            changed = build_live(self.source, moment, self.root, self.history, refetch_last=refetch)
        except pyarrow.ArrowException as exc:
            # The live folder is the history plus Shoonya's sessions since, so
            # an unreadable file is rebuilt rather than patched.
            LOGGER.error("v3 live data unreadable (%s); rebuilding %s.", exc, self.root)
            shutil.rmtree(self.root, ignore_errors=True)
            changed = build_live(self.source, moment, self.root, self.history)
        self._settled_day = moment.date()
        if changed:
            # A past session changed under the day's history cache.
            for cache in self.root.glob("history_*.pkl"):
                cache.unlink(missing_ok=True)

    def _market(self, today: pd.Timestamp):
        try:
            m = L.market(today, self.root)
        except (pickle.UnpicklingError, EOFError) as exc:
            # The same for the day's history cache, which is rebuilt from data_live.
            LOGGER.error("v3 history cache unreadable (%s); rebuilding it.", exc)
            for cache in self.root.glob("history_*.pkl"):
                cache.unlink(missing_ok=True)
            m = L.market(today, self.root)
        if m.cal[-1] == today:
            todays_features(m, self.root)
        return m

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="strategy-v3", daemon=True)
            self._thread.start()

    def stop(self, timeout: float = 60.0) -> bool:
        """Stop refreshing, letting a refresh in progress finish its writes."""
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)
        return thread is None or not thread.is_alive()

    def _run(self) -> None:
        while not self._stop.is_set():
            now = self._now()
            if self._due(now):
                try:
                    snapshot = self.refresh(now)
                except Exception as exc:  # noqa: BLE001 - network and data faults are expected
                    emit, folded = _THROTTLE.should_log("refresh_failed")
                    if emit:
                        LOGGER.error(
                            "v3 refresh failed: %s. No new entries until it recovers; "
                            "open positions still exit on time.%s",
                            exc, log_throttle.suffix(folded),
                        )
                    self._next_try = time.monotonic() + self.FAILURE_RETRY_SECONDS
                else:
                    _THROTTLE.reset("refresh_failed")
                    if self._complete(snapshot, now):
                        self._attempts = 0
                    else:
                        delay = self.RETRY_SECONDS[min(self._attempts, len(self.RETRY_SECONDS) - 1)]
                        self._attempts += 1
                        self._next_try = time.monotonic() + delay
            self._stop.wait(1.0)

    def _due(self, now: datetime) -> bool:
        target = candles_closed(now)
        if target != self._target:
            # A candle has closed since the last look: try for it at once.
            self._target, self._attempts, self._next_try = target, 0, 0.0
        if time.monotonic() < self._next_try:
            return False
        snapshot = self.snapshot
        return snapshot is None or not self._complete(snapshot, now)

    def _complete(self, snapshot: Snapshot, now: datetime) -> bool:
        if snapshot.as_of.date() != now.date():
            return False                    # a new day: history first
        have = snapshot.candles if snapshot.live else 0
        return have >= candles_closed(now) and not snapshot.behind

    def _announce(self, snapshot: Snapshot) -> None:
        """Say each new thing once: what v3 wants, and what it is warning about."""
        day = snapshot.day.isoformat()
        if snapshot.live and snapshot.shock:
            self._once(f"shock:{day}", logging.WARNING,
                       "v3: %s is a shock session (NIFTY opened >= %.2f ATR below its previous "
                       "close); no night trade tonight.",
                       day, V3.SHOCK_ATR)
        if snapshot.edge_alerts:
            self._once(f"edge:{day}", logging.WARNING,
                       "v3 edge monitor: the last %d carry nights averaged <= 0 net for %s "
                       "(an alert, not a rule - consider pausing them).",
                       V3.MONITOR["nights"], ", ".join(snapshot.edge_alerts))
        entry = snapshot.entry
        if entry is not None:
            stop = (f"stop {entry['stop_loss_points']:.2f} pts below the fill, today only"
                    if entry["stop_loss_points"] else "no stop")
            self._once(f"entry:{entry['index']}:{entry['execute_time']}", logging.INFO,
                       "v3 %s signal on %s (%s, %s candle) -> night trade: BUY 1 lot (synthetic "
                       "long future) at %s once the HH/LL desk is flat; %s; sold at the next "
                       "09:15 open. Signalled: %s.",
                       entry["setup"], entry["index"], entry["book"],
                       entry["signal_time"].strftime("%H:%M"),
                       entry["execute_time"].strftime("%H:%M"), stop, ", ".join(entry["candidates"]))
        for out in snapshot.exits:
            self._once(f"exit:{out['index']}:{out['time']}", logging.INFO,
                       "v3's own book: %s (%s) exited at %s, index %.2f (%s).",
                       out["index"], out["setup"], out["time"].strftime("%H:%M"),
                       out["price"], out["reason"])

    def _once(self, key: str, level: int, message: str, *args: Any) -> None:
        if key not in self._reported:
            self._reported.add(key)
            LOGGER.log(level, message, *args)

    # -- what the managers ask ----------------------------------------------
    def set_entry_blocker(self, blocker: Optional[Callable[[], Optional[str]]]) -> None:
        """`blocker()` says why no entry may be filled now (None: it may).

        `main.py` points it at the HH/LL desk: the night trade is taken only
        once the desk's day is squared off. A blocker that raises blocks.
        """
        self._entry_blocker = blocker

    def _blocked(self) -> Optional[str]:
        blocker = self._entry_blocker
        if blocker is None:
            return None
        try:
            return blocker()
        except Exception as exc:  # noqa: BLE001 - an unknown desk state never lets an entry through
            LOGGER.error("v3 entry check failed (%s); entry held.", exc)
            return f"entry check failed: {exc}"

    def entry_intent(self, now: Optional[datetime] = None) -> Optional[dict[str, Any]]:
        """The night buy v3 wants filled right now, or None.

        The intent opens at `NIGHT_ENTRY_TIME` and expires `ENTRY_WINDOW_SECONDS`
        later, past which it is no longer the night fill. Inside that window it
        is held back while the entry blocker (the HH/LL desk) is not flat.
        """
        snapshot = self.snapshot
        entry = None if snapshot is None else snapshot.entry
        if entry is None:
            return None
        reference = now or self._now()
        fill = entry["execute_time"]
        if not fill <= reference < fill + timedelta(seconds=ENTRY_WINDOW_SECONDS):
            if (fill + timedelta(seconds=ENTRY_WINDOW_SECONDS) <= reference
                    and reference.date() == fill.date() and f"blocked:{fill}" in self._reported):
                self._once(f"blocked-skip:{fill}", logging.ERROR,
                           "v3 night trade on %s SKIPPED: the HH/LL desk was not flat by %s.",
                           entry["index"], (fill + timedelta(seconds=ENTRY_WINDOW_SECONDS)).strftime("%H:%M:%S"))
            return None
        if entry["unsettled"]:
            if reference >= fill + timedelta(seconds=SETTLE_SECONDS):
                self._once(f"unsettled:{fill}", logging.WARNING,
                           "%s had no %s candle within %ds of the open; %s's %s entry is "
                           "skipped because the waterfall could not be ranked. Check the Shoonya feed.",
                           ", ".join(entry["unsettled"]), entry["signal_time"].strftime("%H:%M"),
                           SETTLE_SECONDS, entry["index"], entry["setup"])
            return None
        reason = self._blocked()
        if reason is not None:
            self._once(f"blocked:{fill}", logging.WARNING,
                       "v3 night trade on %s held: %s. Entered once it is flat, until %s.",
                       entry["index"], reason,
                       (fill + timedelta(seconds=ENTRY_WINDOW_SECONDS)).strftime("%H:%M:%S"))
            return None
        return {
            "key": f"v3:{entry['index']}:{fill.isoformat()}",
            "position": "long",
            "index": entry["index"],
            "setup": entry["setup"],
            "source": entry["book"],
            "book": entry["book"],
            "stop_loss_points": entry["stop_loss_points"],
            "signal_time": entry["signal_time"].isoformat(),
            "execute_time": fill.isoformat(),
            "priority_candidates": list(entry["candidates"]),
        }

    def wants_long_legs(self, index: str, now: Optional[datetime] = None) -> bool:
        """True while `index` should have option legs standing by.

        Which index wins is not known until a signal candle closes, so every
        index gets them from the first B candle (14:45) until the night fill
        window closes (15:14). Past that no entry can fill, and asking for legs
        only produced false "no live price" alarms once the index stops
        printing at 15:15.
        """
        reference = now or self._now()
        snapshot = self.snapshot
        return (snapshot is not None and index in NAMES
                and snapshot.as_of.date() == reference.date()
                and in_legs_window(reference))

    def session_open_price(self, index: Optional[str], day: dt_date) -> Optional[float]:
        """`index`'s 09:15 open on `day`, once that candle has been read."""
        snapshot = self.snapshot
        if snapshot is None or not snapshot.live or snapshot.day != day:
            return None
        return snapshot.open_prices.get(index or "")

    def view(self, index: str) -> "IndexView":
        return IndexView(self, index)


class IndexView:
    """One index's slice of the replay, in the shape the chart routes serve."""

    #: How far behind the clock the newest candle may be before a live gate
    #: reading stops meaning anything: one candle forming is normal, two says
    #: the data has stalled.
    LIVE_BAR_TOLERANCE = 2

    def __init__(self, engine: StrategyEngine, index: str) -> None:
        self.engine = engine
        self.index = index

    def pivot_points(self) -> dict[str, list[dict[str, Any]]]:
        """Today's v3 signals for this index. v3 is long-only, so every marker
        is a buy, served in the list the console renders as a long; `hh_points`
        stays in the payload because the chart consumes the route's shape."""
        snapshot = self.engine.snapshot
        markers = () if snapshot is None else snapshot.markers.get(self.index, ())
        return {"hh_points": [], "ll_points": [dict(marker) for marker in markers]}

    def gate_verdicts(self) -> dict[str, dict[str, Any]]:
        snapshot = self.engine.snapshot
        verdict = None if snapshot is None else snapshot.verdicts.get(self.index)
        return {} if verdict is None else {verdict["confirmed_time"]: dict(verdict)}

    def live_verdict(self, price: Optional[float], now: Optional[datetime] = None
                     ) -> Optional[dict[str, Any]]:
        """The gates on the candle still forming, if it closed at `price`.

        A forecast that decides nothing - v3 acts on closed candles only. It can
        be exact because every input but the close is fixed for the session:
        the regime line, the ATR, the previous close and the session open. None
        whenever it would be fiction: no price, no data, a stalled feed, or a
        clock past the last candle.
        """
        snapshot = self.engine.snapshot
        f = None if snapshot is None else snapshot.forecast.get(self.index)
        if f is None or price is None or not price > 0:
            return None
        reference = now or now_ist()
        behind = (reference - f["closed_at"]).total_seconds()
        bar = f["bar"] + 1
        if (behind < 0 or behind > self.LIVE_BAR_TOLERANCE * BAR_INTERVAL_MINUTES * 60
                or bar >= FULL_SESSION_BARS):
            return None
        # The running mean is of typical prices; the forming candle has no high
        # or low yet, so the tick stands in for its typical price.
        verdict = score_gates(bar, float(price), (f["sum"] + float(price)) / (bar + 1), f,
                              shock=snapshot.shock, accepted=False, line=f["sum"] / bar)
        verdict["accepted"] = verdict["passed"] == verdict["total"]
        verdict["live"] = True
        verdict["price"] = float(price)
        return verdict


# =============================================================================
# Strategy decisions
# =============================================================================
EnterCallback = Callable[[str, float, dict[str, Any]], Awaitable[bool]]
ExitCallback = Callable[[str, float], Awaitable[None]]


class StrategyDecider:
    """Makes the broker agree with v3, once per poll.

    Entries are the engine's: the night trade, naming the index that won the
    waterfall. Exits are the held position's own: the next session's 09:15
    open. (A row opened before A was retired keeps its own: 14:55 for a
    shock-session A trade and A's entry-day stop.) Both trade managers drive
    one of these, which is why paper and live cannot drift apart.
    """

    def __init__(self, service, *, tag: str = "STRATEGY") -> None:
        self.service = service
        self.tag = tag
        # The entry attempt the broker has already acted on, so a retry inside
        # the fill window cannot open the same signal twice.
        self._acted_key: Optional[str] = None
        # The intent whose missing entry quote has already been reported, so a
        # feed reconnect costs one warning rather than one per poll.
        self._price_wait_key: Optional[str] = None
        # Latched when the stop level is breached. A resting stop does not
        # un-trigger because price came back, so once this is set the exit is
        # retried every poll until the broker is genuinely flat.
        self._stop_latched = False
        self._stop_trigger_price: Optional[float] = None

    # -- helpers -----------------------------------------------------------
    @staticmethod
    def protective_stop_level(
        direction: Optional[str], book: Optional[str], stop_price: Optional[float]
    ) -> Optional[float]:
        """Where an A trade's stop rests, or None if there is none.

        The manager owns the level: fixed at `stop` daily ATRs below the fill
        when the trade opens and persisted on the trade row, so it is the same
        number after a restart as before one. B has none, and A's lives only on
        its entry session, so this is only ever asked then by `step`.
        """
        if not is_momentum_book(book) or direction != "LONG":
            return None
        if not stop_price or stop_price <= 0:
            return None
        return float(stop_price)

    @staticmethod
    def stop_hit(price: Optional[float], level: Optional[float]) -> bool:
        return level is not None and price is not None and 0 < price <= level

    @staticmethod
    def exit_due(now: datetime, entry_day: Optional[dt_date]) -> bool:
        """True once a position has reached the 09:15 open of a *later* session."""
        if now.time() < CARRY_EXIT_TIME:
            return False
        if entry_day is not None:
            return entry_day < now.date()
        # Entry day unknown (a hand-edited or pre-upgrade row). Every position
        # is opened at the night fill, from `LAST_ENTRY_TIME`, so any moment
        # before that time of day is necessarily a later session than its own.
        return now.time() < LAST_ENTRY_TIME

    # -- the one call the managers make ------------------------------------
    async def step(
        self,
        *,
        in_trade: bool,
        direction: Optional[str],
        price_getter: Callable[[str], Awaitable[Optional[float]]],
        enter: EnterCallback,
        exit_: ExitCallback,
        live_price_getter: Optional[Callable[[], Optional[float]]] = None,
        book: Optional[str] = None,
        entry_day: Optional[dt_date] = None,
        stop_price: Optional[float] = None,
        index: Optional[str] = None,
        index_price_getter: Optional[
            Callable[[str, str], Awaitable[Optional[float]]]
        ] = None,
        index_live_price_getter: Optional[Callable[[str], Optional[float]]] = None,
    ) -> None:
        """Make the broker agree with v3.

        `index` is the index an open position belongs to; its exit reads that
        index's own 09:15 open, never another's. An entry has no index until the
        waterfall has allocated one, so the two `index_*` getters are asked for
        the *winner's* price once it is known.
        """
        now = now_ist()

        if in_trade:
            if await self._resolve_exit(now, entry_day, price_getter, exit_, index):
                return
            if await self._resolve_intraday_exit(now, book, entry_day, price_getter, exit_):
                return
            await self._watch_stop(
                now, direction, book, entry_day, stop_price, price_getter, exit_
            )
            return

        # Flat. Nothing is being held, so nothing can be stopped out.
        self._stop_latched = False
        self._stop_trigger_price = None

        intent = self.service.entry_intent(now)
        if intent is None or intent["key"] == self._acted_key:
            return
        # The slot has been allocated by now, so the price that matters is the
        # winning index's - the fill the stop is measured from.
        winner = intent.get("index")
        entry_price_getter = price_getter
        entry_live_getter = live_price_getter
        if winner and index_price_getter is not None:
            entry_price_getter = partial(index_price_getter, winner)
        if winner and index_live_price_getter is not None:
            entry_live_getter = partial(index_live_price_getter, winner)
        price = await self._entry_price(
            entry_price_getter, entry_live_getter,
            announce=intent["key"] != self._price_wait_key,
        )
        if price is None:
            # No current quote this poll - a feed reconnect, typically ~20s.
            # Retried every poll: `entry_intent` itself drops the signal once
            # its fill window has passed, the same window a late leg build is
            # given, and a cached quote is never substituted. Expiring here
            # instead lost the trade to any reconnect at the fill moment.
            self._price_wait_key = intent["key"]
            return
        self._price_wait_key = None
        if await enter("LONG", price, intent):
            self._acted_key = intent["key"]

    # -- exits -------------------------------------------------------------
    async def _resolve_exit(
        self,
        now: datetime,
        entry_day: Optional[dt_date],
        price_getter: Callable[[str], Awaitable[Optional[float]]],
        exit_: ExitCallback,
        index: Optional[str] = None,
    ) -> bool:
        """Exit unconditionally at the first available next-session open price."""
        if not self.exit_due(now, entry_day):
            return False

        # The 09:15 open itself once that candle has been read; the live tick
        # otherwise, which at 09:15 *is* the open. Reading the candle when it
        # exists keeps a process restarted mid-morning from booking a price
        # printed an hour after the open.
        getter = getattr(self.service, "session_open_price", None)
        open_price = getter(index, now.date()) if callable(getter) else None
        if open_price is None:
            open_price = await price_getter("carry exit")
        if open_price is None:
            LOGGER.error(
                "[%s] the next-open exit is due but no price is available; "
                "retrying on the next poll.",
                self.tag,
            )
            return True

        await exit_("carry_open", open_price)
        return True

    async def _resolve_intraday_exit(
        self,
        now: datetime,
        book: Optional[str],
        entry_day: Optional[dt_date],
        price_getter: Callable[[str], Awaitable[Optional[float]]],
        exit_: ExitCallback,
    ) -> bool:
        """A shock-session A trade is sold at the 14:55 open of its entry day."""
        if (book != INTRADAY_BOOK or entry_day != now.date()
                or now.time() < INTRADAY_EXIT_TIME):
            return False
        price = await price_getter("intraday exit")
        if price is None:
            LOGGER.error(
                "[%s] the %s intraday exit is due but no price is available; "
                "retrying on the next poll.",
                self.tag, INTRADAY_EXIT_TIME.strftime("%H:%M"),
            )
            return True
        await exit_("intraday_exit", price)
        return True

    async def _watch_stop(
        self,
        now: datetime,
        direction: Optional[str],
        book: Optional[str],
        entry_day: Optional[dt_date],
        stop_price: Optional[float],
        price_getter: Callable[[str], Awaitable[Optional[float]]],
        exit_: ExitCallback,
    ) -> None:
        """A's protective stop, live on its entry session only.

        `stop` daily ATRs below the fill. B never has one; A's does not survive
        the night.
        """
        if entry_day is not None and entry_day != now.date():
            return
        level = self.protective_stop_level(direction, book, stop_price)
        if level is None:
            return
        price = await price_getter("stop check")
        if not self._stop_latched and self.stop_hit(price, level):
            LOGGER.warning(
                "[%s] stop level %.2f breached at %.2f - flattening",
                self.tag, level, price,
            )
            self._stop_latched = True
            self._stop_trigger_price = price
        if not self._stop_latched:
            return
        # Retry until flat. A broker exit can fail and the manager then keeps
        # the trade open; a price that drifts back above the level must not
        # cancel a stop that already fired.
        if price is None:
            price = self._stop_trigger_price
            LOGGER.error("[%s] stop fired but no fresh price is available; retrying exit at trigger price.", self.tag)
            if price is None:
                return
        await exit_("stop_loss", price)

    # -- entry price -------------------------------------------------------
    async def _entry_price(
        self,
        price_getter: Callable[[str], Awaitable[Optional[float]]],
        live_price_getter: Optional[Callable[[], Optional[float]]],
        *,
        announce: bool = True,
    ) -> Optional[float]:
        """The price to enter at, or None if this poll cannot establish one.

        A current quote is required - the stop is measured from it, so a cached
        one is never substituted. `step` retries on the next poll until the
        fill window closes; `announce` is False once the gap has been reported.
        """
        if live_price_getter is None:
            price = await price_getter("entry")
            if price is not None and price > 0:
                return price
            if announce:
                LOGGER.warning(
                    "[%s] no price for entry from any source - entry held; retried "
                    "every poll until the fill window closes.",
                    self.tag,
                )
            return None

        raw: Any = None
        try:
            raw = live_price_getter()
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("[%s] live price lookup failed: %s", self.tag, exc)
        price = float(raw) if raw else None
        if price is not None and price > 0:
            return price
        if not announce:
            return None

        # No current tick. Find out what the slower sources know before
        # deciding, so the log says which source failed rather than only that
        # one did - the difference between "the feed is down" and "the feed
        # thinks it is down while ticks are arriving".
        fallback = await price_getter("entry")
        LOGGER.warning(
            "[%s] no live price for entry - entry held; retried every poll until "
            "the fill window closes. Fresh price getter returned %r; last known "
            "price %s. A last known price alongside no fresh tick means the feed "
            "is reporting itself closed or stale, not that ticks stopped.",
            self.tag,
            raw,
            "unavailable" if not fallback else f"{fallback:.2f}",
        )
        return None


__all__ = [
    "CARRY_BOOK",
    "DATA_DIR",
    "CARRY_EXIT_TIME",
    "CARRY_TOD",
    "ENTRY_WINDOW_SECONDS",
    "FEED_TIME_FORMAT",
    "INTRADAY_BOOK",
    "INTRADAY_EXIT_TIME",
    "IndexView",
    "LEGS_CLOSE_TIME",
    "LEGS_OPEN_TIME",
    "LIVE_DIR",
    "LIVE_SETUPS",
    "LOTS",
    "MOMENTUM_BOOK",
    "MarketData",
    "NAMES",
    "NIGHT_ENTRY_TIME",
    "SESSION_LAST_BAR_TIME",
    "SESSION_OPEN_TIME",
    "Snapshot",
    "StrategyDecider",
    "StrategyEngine",
    "bias_tag",
    "build_live",
    "candles_closed",
    "in_legs_window",
    "is_momentum_book",
    "link_history",
    "live_slots",
    "night_entry",
    "now_ist",
    "parse_bar_timestamp",
    "replay",
    "save_session",
    "settled",
    "to_feed_time",
    "todays_features",
]
