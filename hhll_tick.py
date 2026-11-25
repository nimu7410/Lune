"""HH/LL tick-only pivot trader for Lune: synthetic futures, paper, self-reviewing.

    python hhll_tick.py live       follow every tick, paper-trade, review every trade
    python hhll_tick.py selfcheck  prove the pivot rules, the cost model and the session rules
    python hhll_tick.py retest     re-test every setting (and re-derive the threshold) on all days
    python hhll_tick.py report     rebuild hhll_tick_data/report.md
    python hhll_tick.py health     does each index's threshold need retuning? (OK | ALARM | SWITCH | NOT ENOUGH DATA)
                                   (live applies a SWITCH by itself; this command only reports)
    python hhll_tick.py migrate    move the old hhll_tick_data files into the database
    python hhll_tick.py archive    compress every finished day's ticks now (live does it by itself)

Storage: PostgreSQL, schema `hhll` (`hhll_store`). The running Lune service's desk tape
(manual_desk.TickTape, on the service's own Shoonya websocket, so no second broker login can
knock its session out) writes every tick to `hhll.ticks` as (day, index, seq, epoch ms, price);
this process reads them from there. No candles. A finished day's ticks are compressed into
`hhll.tick_archive` and deleted from `hhll.ticks`, which so holds one session at most. Pivots,
trades, reviews and the settings live in `hhll.pivots`, `hhll.trades`, `hhll.reviews` and
`hhll.state`; only working state (each book, the replay cache) is kept in memory.

Pivots (`ZigZag`): HH and LL only, strictly alternating, each at the true extreme of its
swing. A pivot prints on the first tick fast_cost x cost back from the running extreme, once
the swing from the last pivot has reached the minimum leg, leg_cost x cost: a low threshold
for speed, the minimum leg to keep legs big. A move past the last pivot invalidates it, and
the opposite pivot prints at once, so the pivots stay with the trend: an HH broken upward
prints an LL. That LL sits on the last minor swing low, not the lowest tick since the HH, so it
is close to its confirmation, and what breaks it in turn is that lowest tick, not the mark, so
a mark near the price cannot start a run of flips. It is the only kind of pivot with a leg
below the minimum, and the only kind that is not the extreme since the pivot before it.
Confirmed pivots are append-only: nothing repaints, nothing waits.

Trading (`Book`, one per index, used identically by replay and live): HH -> short, LL -> long,
stop-and-reverse at market on the confirming tick. A synthetic future is two option orders
per side (ATM CE and PE), so a round trip is four orders, each charged on its own.

Improvement: after every closed trade its biggest weakness is named and ONE fix is tested on
the replay of every stored tick. A fix is adopted only if it beats the current settings in
total, is worse in neither half of the data (halves of the days; of the session's clock while there is
only one day), its nearby settings beat them too, the data spans MIN_HOURS and the current
settings made MIN_TRADES trades on it. An adopted fix applies at once: each index's book
switches to it the next time it is flat, so a position always closes under the rules that
opened it. After every session every setting is re-tested the same way.

The Lune desk's automatic trader runs this same `Book` on the same ticks with the same
settings (manual_desk.TickTrader), so every desk trade is a trade of this engine.
"""

from __future__ import annotations

import json
import logging
import math
import queue
import statistics
import sys
import threading
import time
from array import array
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, fields, replace
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Callable, Optional

import config
import hhll_store
import instruments

LOG = logging.getLogger("hhll_tick")
BASE = Path(__file__).resolve().parent
DATA = BASE / "hhll_tick_data"             # report.md only; the data itself is in the database
POLL_S = 1.0                               # live: longest wait for the recorder's NOTIFY (paces the settings check too)
READ_EVERY_S = 10.0                        # live: ticks are read without a NOTIFY at least this often (a lost one)
REPORT_EVERY_S = 300                       # live: report.md is rewritten at most this often after a trade or re-test
REPORT_REVIEWS = 1000                      # the newest reviews report.md lists (all of them stay in hhll.reviews)
ARCHIVE_EVERY_S = 600                      # live: how often finished days are looked for and archived

IST = timezone(timedelta(hours=5, minutes=30))
IST_OFFSET_S = 19800
OPEN_S = 9 * 3600 + 15 * 60          # 09:15 market open
CLOSE_S = 15 * 3600 + 30 * 60        # 15:30 market close
SQUARE_OFF_S = 15 * 3600 + 10 * 60   # flat by 15:10, ahead of Shoonya's ~15:20 MIS cut
SQUARE_OFF_DONE_S = 15 * 3600 + 35 * 60  # the daily re-test runs after this
CIRCUIT = 0.095      # NSE market-wide breaker halts at 10%; stop trading at 9.5%
CHOP_N = 4           # legs the chop filter looks back over
MIN_HOURS = 1.0      # never adopt a fix tested on less session than this
MIN_TRADES = 8       # ...nor one where neither side of the comparison made this many trades
FORWARD_MIN_H = 1.0  # an adopted change is judged on the ticks since adoption after this long (or MIN_TRADES)
#: Shadow tournament: alternative settings trade on paper beside the live ones, out of sample (from the
#: open of a session after the one they were picked on). The best that beats the live settings on those
#: ticks, by at least MIN_TRADES trades and FORWARD_MIN_H, is adopted (and then forward-tested as usual).
SHADOW_SEED = [  # picked 2026-11-03 on its ticks (signal exit, Delta-style trailing lock); judged from 11-04
    dict(thr_cost=8, chop_k=2.0, leg_k=2.5, exit_on="signal", exit_wide=3.0),
    dict(thr_cost=10, chop_k=0.0, leg_k=2.5, lock_at=2.0, lock_trail=0.5, exit_on="signal", exit_wide=3.0),
    dict(thr_cost=8, chop_k=0.0, leg_k=4.0, lock_at=1.5, lock_trail=1.0, exit_on="signal", exit_wide=3.0),
    dict(thr_cost=10, chop_k=2.0, leg_k=3.0, lock_at=1.5, lock_trail=0.5, exit_on="signal", exit_wide=3.0),
    dict(thr_cost=10, chop_k=2.0, leg_k=2.5, lock_at=2.0, lock_trail=0.5, exit_on="signal", exit_wide=3.0),
    dict(thr_cost=8, chop_k=2.0, leg_k=2.5, lock_at=2.0, lock_trail=0.5, exit_on="signal", exit_wide=3.0),
]
#: Settings are tuned by hand (hhll.state, written with this service stopped): reviews and re-tests still
#: run and log, but nothing is adopted. False hands the settings back to them.
MANUAL = True
#: The one thing that tunes itself: a SWITCH of the retune check (`health`) is applied without anyone,
#: each index at most once per finished session, from the next session. False: it is only reported.
AUTO_SWITCH = False   # frozen: settings held fixed while they are paper-tested (2026-11-13)
RETEST_EVERY_S = 1800  # during the session every setting is re-tested this often, too
CAPITAL = 250_000.0  # Rs per index book
MARGIN_PCT = 0.12    # SPAN + exposure of a synthetic future, as a share of notional

# ── charges (per executed option order), checked 2026-11 ─────────────────────────
BROKERAGE = Decimal("5")             # Shoonya: flat Rs 5 per executed F&O order
STT_SELL = Decimal("0.0015")         # options: 0.15% of sell premium (Finance Act 2026, from 1-Apr-2026)
TXN = {"NFO": Decimal("0.0003553"), "BFO": Decimal("0.000325")}  # NSE / BSE (SENSEX) on premium
SEBI = Decimal("0.000001")           # Rs 10 per crore
IPFT = {"NFO": Decimal("0.000000001"), "BFO": Decimal(0)}        # NSE Rs 0.01 per crore
STAMP_BUY = Decimal("0.00003")       # 0.003% on buy premium
GST = Decimal("0.18")                # on brokerage + transaction + SEBI (+ IPFT)
SLIP = 0.05                          # option points lost per leg by a market order beyond half-spread
PAISA = Decimal("0.01")

#  iv: ATM implied vol used to price the legs for STT/turnover; spread: full bid-ask of an ATM
#  leg in option points; freeze: max quantity per order (larger orders are sliced).
_EXTRA = {
    "NIFTY": (0.12, 0.10, 1800), "BANKNIFTY": (0.14, 0.30, 900), "FINNIFTY": (0.13, 0.30, 1800),
    "MIDCPNIFTY": (0.16, 0.20, 2800), "NIFTYNXT50": (0.16, 1.00, 600), "SENSEX": (0.12, 0.20, 1000),
}


@dataclass(frozen=True)
class Spec:
    name: str
    lot: int
    exch: str
    iv: float
    spread: float
    freeze: int
    tick: float = 0.05


SPECS = {n: Spec(n, int(i.quantity), i.fo_exchange, *_EXTRA[n]) for n, i in instruments.INSTRUMENTS.items()}


# ── settings ─────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Params:
    thr_cost: float = 10.0      # R = thr_cost x this index's own all-in round-trip cost, in points
    chop_k: float = 0.0         # 0 = off; else enter only if median of last CHOP_N legs >= chop_k * R
    lock_at: float = 0.0        # 0 = off; else trail once open profit reaches lock_at * R
    lock_trail: float = 0.75    # trailing distance, x R
    stop: float = 0.0           # 0 = off; else hard stop at stop * R against the entry
    order: str = "market"       # entry order: "market" | "limit" (exits are always market)
    limit_wait: float = 10.0    # seconds a limit entry may rest before it is sent at market
    last_entry: str = "15:00"   # no new entries from this time
    lots: int = 1
    target: float = 0.0         # 0 = off; else take the profit once it reaches target * R
    leg_k: float = 0.0          # 0 = off; else enter only if the leg into the pivot is >= leg_k * R
    exit_on: str = "pivot"      # "pivot": any opposite pivot closes | "signal": only one the entry filters
                                # (leg_k, chop_k) pass, i.e. one it reverses on; from last_entry, any again
    exit_wide: float = 0.0      # 0 = off; else an opposite pivot of a second zigzag at exit_wide * R closes
    max_hold_s: float = 0.0     # 0 = off; else close a trade this many seconds after entry
    fast_cost: float = 0.0      # the retrace that prints a pivot, x cost; 0 = R (thr_cost) does
    leg_cost: float = 0.0       # the minimum leg, x cost: no pivot prints short of it unless the last one breaks
    index_pivots: tuple = ()    # (name, thr_cost, fast_cost, leg_cost) per index, in place of the three above

    def __post_init__(self) -> None:    # lists when read back from hhll.state; settings must stay hashable
        object.__setattr__(self, "index_pivots", tuple(tuple(x) for x in self.index_pivots))

    def fixed_of(self, name: str) -> Optional[float]:
        """This index's own threshold, x cost (`index_pivots`), or None."""
        return next((x[2] for x in self.index_pivots if x[0] == name), None)


GRID = {
    "thr_cost": [3, 4, 5, 6, 8, 10, 12, 15, 18, 20, 25, 30, 40, 50],   # R as a multiple of cost
    "chop_k": [0, 1.5, 1.75, 2.0, 2.25, 2.5, 3.0, 3.5, 4.0],
    "lock_at": [0, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0],
    "lock_trail": [0.25, 0.5, 0.75, 1.0],
    "stop": [0, 0.5, 0.75, 1.0, 1.25],
    "order": ["market", "limit"],
    "limit_wait": [5.0, 10.0, 20.0, 30.0],
    "last_entry": ["13:30", "14:00", "14:30", "15:00"],
    "target": [0, 1.5, 2.0, 2.5, 3.0, 4.0],
    "leg_k": [0, 1.5, 2.0, 2.5, 3.0, 4.0],
    "exit_on": ["pivot", "signal"],
    "exit_wide": [0, 2.0, 3.0, 4.0, 6.0],
}
TOGGLE_ON = {"chop_k": 2.0, "lock_at": 2.0, "stop": 1.0, "target": 2.0, "leg_k": 2.0}   # first value tried when off


def hhmm(s: str) -> int:
    h, m = s.split(":")
    return int(h) * 3600 + int(m) * 60


def day_of(ms: int) -> date:
    return date.fromordinal(date(1970, 1, 1).toordinal() + (ms // 1000 + IST_OFFSET_S) // 86400)


def clock(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, IST).strftime("%H:%M:%S")


# ── pivots ───────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Pivot:
    kind: str            # "HH" | "LL"
    price: float         # the extreme
    ms: int
    idx: int
    conf_price: float    # the tick that confirmed it
    conf_ms: int
    conf_idx: int
    leg: Optional[float]  # distance from the previous pivot (None for the first)
    forced: bool = False  # printed because price broke the previous pivot, not by the threshold
    level: Optional[float] = None  # the swing's true extreme: past this the pivot is broken (= price unless forced)


class ZigZag:
    """HH/LL from ticks. Causal and append-only. A pivot prints on the first tick R back from the
    running extreme once the swing from the last pivot is >= `leg`: that is the swing's true extreme.
    The first tick past the last pivot (which that break invalidates) prints the opposite pivot
    whatever the swing, on the last minor swing, a pivot of a plain zigzag at R: an extreme a long
    pause back would put the confirmation far from its pivot. The first pivot of a day has no swing
    to measure, so R alone prints it. leg=0 is the one-threshold zigzag.

    Legs are measured from the pivot; a break from its `level`, the swing's true extreme. No tick
    between a pivot and the next is beyond that level, and between prints price is inside it and
    short of the rule above: nothing else can hold one back."""

    __slots__ = ("R", "leg", "_r", "_l", "dir", "hp", "hm", "hi", "lp", "lm", "li", "n", "last", "minor", "swing")

    def __init__(self, R: float, leg: float = 0.0) -> None:
        if not R > 0:
            raise ValueError("R must be positive")
        self.R, self.leg, self._r, self._l = R, leg, R - 1e-9, leg - 1e-9
        self.dir = 0            # 0 = no pivot yet, -1 = searching LL (last was HH), +1 = searching HH
        self.hp = self.lp = None
        self.n = 0
        self.last: Optional[Pivot] = None
        self.minor = ZigZag(R) if leg else None
        self.swing: dict = {}   # "HH" | "LL" -> the latest minor pivot

    def update(self, p: float, ms: int) -> Optional[Pivot]:
        i = self.n
        self.n = i + 1
        if self.minor is not None:
            m = self.minor.update(p, ms)
            if m is not None:
                self.swing[m.kind] = m
        if self.hp is None:
            self.hp = self.lp = p
            self.hm = self.lm = ms
            self.hi = self.li = i
            return None
        d, last = self.dir, self.last
        if d >= 0:
            if p > self.hp:
                self.hp, self.hm, self.hi = p, ms, i
            else:
                due = self.hp - p >= self._r and (last is None or self.hp - last.price >= self._l)
                if due or (last is not None and p < last.level):
                    pv = self._emit("HH", self.hp, self.hm, self.hi, p, ms, i, not due)
                    self.dir, self.lp, self.lm, self.li = -1, p, ms, i   # p is the lowest tick since the HH
                    return pv
        if d <= 0:
            if p < self.lp:
                self.lp, self.lm, self.li = p, ms, i
            else:
                due = p - self.lp >= self._r and (last is None or last.price - self.lp >= self._l)
                if due or (last is not None and p > last.level):
                    pv = self._emit("LL", self.lp, self.lm, self.li, p, ms, i, not due)
                    self.dir, self.hp, self.hm, self.hi = 1, p, ms, i
                    return pv
        return None

    def _emit(self, kind, level, ms, idx, cp, cms, cidx, forced) -> Pivot:
        price, prev = level, self.last
        if forced:                    # on the last minor swing, even one past the last mark: a climb's lows rise
            m = self.swing.get(kind)
            if m is not None and m.idx > prev.idx:
                price, ms, idx = m.price, m.ms, m.idx
        leg = abs(price - prev.price) if prev else None
        self.last = Pivot(kind, price, ms, idx, cp, cms, cidx, leg, forced, level)
        return self.last



# ── costs ────────────────────────────────────────────────────────────────────────
_WEEKDAYS = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")
_HOLIDAYS = {config.parse_holiday(h) for h in config.TRADE_SETTINGS.expiry_holidays} - {None}


def next_expiry(name: str, day: date) -> date:
    """The option expiry a synthetic opened on `day` uses; expiry day itself rolls to the next."""
    e = config.INDEX_EXPIRY[name]
    wd = _WEEKDAYS.index(e.option_weekday)
    d = day
    while True:
        d += timedelta(days=1)
        if d.weekday() != wd or (e.option_cadence == "monthly" and (d + timedelta(days=7)).month == d.month):
            continue
        x = d
        while x in _HOLIDAYS or x.weekday() >= 5:
            x -= timedelta(days=1)
        if x > day:
            return x


_PREM_DAYS: dict = {}


def atm_premium(spec: Spec, spot: float, ms: int) -> float:
    """ATM option price, 0.4 * S * iv * sqrt(T): the legs' premium for STT and turnover."""
    day = day_of(ms)
    key = (spec.name, day)
    if key not in _PREM_DAYS:
        _PREM_DAYS[key] = (next_expiry(spec.name, day) - day).days
    left = (CLOSE_S - (ms // 1000 + IST_OFFSET_S) % 86400) / 86400   # rest of today, to 15:30
    t_years = max(0.5, _PREM_DAYS[key] + left) / 365   # ponytail: calendar time, flat iv; quotes would be exact
    return round(max(spec.tick, 0.4 * spot * spec.iv * math.sqrt(t_years)), 2)


def _p(x: Decimal) -> Decimal:
    return x.quantize(PAISA, rounding=ROUND_HALF_UP)


def order_charges(exch: str, buy: bool, qty: int, premium: float, orders: int = 1) -> dict:
    """Every charge on one option order (sliced into `orders` at the freeze quantity)."""
    t = Decimal(qty) * Decimal(str(premium))
    c = {
        "brokerage": _p(BROKERAGE * orders),
        "stt": Decimal(0) if buy else _p(t * STT_SELL),
        "txn": _p(t * TXN[exch]),
        "sebi": _p(t * SEBI),
        "ipft": _p(t * IPFT[exch]),
        "stamp": _p(t * STAMP_BUY) if buy else Decimal(0),
    }
    c["gst"] = _p((c["brokerage"] + c["txn"] + c["sebi"] + c["ipft"]) * GST)
    return c


def round_trip_charges(spec: Spec, lots: int, side: int, e_spot: float, x_spot: float,
                       e_ms: int, x_ms: int) -> dict:
    """Charges in Rs for open + close of a synthetic: 4 option orders (ATM CE and PE at entry).

    Long = buy CE, sell PE; close = sell CE, buy PE. Short is the mirror. At exit each leg is
    repriced by half the spot move (delta 0.5)."""
    qty = lots * spec.lot
    orders = math.ceil(qty / spec.freeze)
    a = atm_premium(spec, e_spot, e_ms)
    ax, mv = atm_premium(spec, x_spot, x_ms), x_spot - e_spot
    ce_x, pe_x = max(spec.tick, ax + 0.5 * mv), max(spec.tick, ax - 0.5 * mv)
    if side > 0:
        legs = [(True, a), (False, a), (False, ce_x), (True, pe_x)]
    else:
        legs = [(True, a), (False, a), (False, pe_x), (True, ce_x)]
    tot: dict = {}
    for buy, prem in legs:
        for k, v in order_charges(spec.exch, buy, qty, prem, orders).items():
            tot[k] = tot.get(k, Decimal(0)) + v
    out = {k: float(v) for k, v in tot.items()}
    out["total"] = float(sum(tot.values()))
    out["orders"] = 4 * orders
    out["premium_in"], out["premium_out"] = a, ax
    return out


def cost_pts(spec: Spec, price: float, ms: int, lots: int = 1) -> float:
    """All-in cost of one flat round trip in index points: charges per unit + spread/slippage both sides."""
    return round_trip_charges(spec, lots, 1, price, price, ms, ms)["total"] / (lots * spec.lot) + 2 * friction_pts(spec)


def friction_pts(spec: Spec) -> float:
    """Synthetic points a market order loses per side: half-spread + slippage on each of 2 legs."""
    return round(2 * (spec.spread / 2 + SLIP), 4)


# ── the trader ───────────────────────────────────────────────────────────────────
class _Pos:
    __slots__ = ("side", "px", "ms", "sod", "pivot", "fric", "peak", "trough", "chop")

    def __init__(self, side, px, ms, sod, pivot, fric, chop):
        self.side, self.px, self.ms, self.sod, self.pivot, self.fric, self.chop = side, px, ms, sod, pivot, fric, chop
        self.peak = self.trough = 0.0


class Book:
    """One index, one session at a time. Feed ticks in order; closed trades land in `trades`.

    `keep=False` (a live book): every trade and pivot goes to `on_trade` / `on_pivot` (the
    database) and only the latest of each is held in `trades` / `pivots`."""

    def __init__(self, spec: Spec, params: Params, on_trade: Optional[Callable[[dict], None]] = None,
                 on_pivot: Optional[Callable[["Book", Pivot], None]] = None, keep: bool = True) -> None:
        self.spec, self.p, self.on_trade, self.on_pivot = spec, params, on_trade, on_pivot
        self.trades = [] if keep else deque(maxlen=1)
        self.pivots = [] if keep else deque(maxlen=1)
        self.day = None
        self.pos: Optional[_Pos] = None
        self.last_px = self.last_ms = None
        self.blocked = {"cutoff": 0, "chop": 0, "margin": 0, "circuit": 0}

    def _new_day(self, day: int, price: float, ms: int) -> None:
        self.day = day
        self.cost = cost_pts(self.spec, price, ms, self.p.lots)
        pts = lambda k: max(self.spec.tick, round(k * self.cost / self.spec.tick) * self.spec.tick) if k else 0.0
        thr, fast, leg = next((x[1:4] for x in self.p.index_pivots if x[0] == self.spec.name),
                              (self.p.thr_cost, self.p.fast_cost, self.p.leg_cost))
        self.R = pts(thr)
        self.zz = ZigZag(pts(fast) or self.R, pts(leg))
        self.zw = ZigZag(self.R * self.p.exit_wide) if self.p.exit_wide else None
        self.legs: deque = deque(maxlen=CHOP_N)
        self.ref, self.halted, self.realised = price, False, 0.0
        self.pend = None
        self.cut = hhmm(self.p.last_entry)
        self.fric = friction_pts(self.spec)

    def on_tick(self, price: float, ms: int) -> None:
        s = ms // 1000 + IST_OFFSET_S
        day, sod = divmod(s, 86400)
        if day != self.day:
            self.finish("data_end")
            self._new_day(day, price, ms)
        self.last_px, self.last_ms = price, ms
        if sod < OPEN_S or sod >= CLOSE_S:
            return
        pos = self.pos
        if pos is not None:
            prof = pos.side * (price - pos.px)
            if prof > pos.peak:
                pos.peak = prof
            elif prof < pos.trough:
                pos.trough = prof
        if not self.halted and abs(price / self.ref - 1) >= CIRCUIT:
            self.halted = True
            self.blocked["circuit"] += 1
            self.pend = None
            if pos is not None:
                self._close(price, ms, "circuit")
        if sod >= SQUARE_OFF_S:
            self.pend = None
            if self.pos is not None:
                self._close(price, ms, "square_off")
            self._next_pivot(price, ms)       # no more trades today, but the pivots run to the close
            return
        if self.pend is not None:
            self._work_limit(price, ms, sod)
        pos = self.pos
        if pos is not None:
            prof = pos.side * (price - pos.px)
            p, R = self.p, self.R
            if p.stop and prof <= -p.stop * R:
                self._close(price, ms, "stop")
            elif p.lock_at and pos.peak >= p.lock_at * R and prof <= pos.peak - p.lock_trail * R:
                self._close(price, ms, "lock")
            elif p.target and prof >= p.target * R:
                self._close(price, ms, "target")
            elif p.max_hold_s and ms - pos.ms >= p.max_hold_s * 1000:
                self._close(price, ms, "time")
        if self.zw is not None:
            pw = self.zw.update(price, ms)
            if pw is not None and self.pos is not None and self.pos.side == (1 if pw.kind == "HH" else -1):
                self._close(price, ms, "wide")
        pv = self._next_pivot(price, ms)
        if pv is None:
            return
        want = -1 if pv.kind == "HH" else 1
        self.pend = None                      # a resting entry from the previous pivot is stale now
        if self.pos is not None and self.pos.side != want and (
                self.p.exit_on != "signal" or sod >= self.cut or self._filtered(pv) is None):
            self._close(price, ms, "reverse")
        if self.pos is None and self._may_enter(sod, price, pv):
            if self.p.order == "limit":
                self.pend = (want, price, ms + int(self.p.limit_wait * 1000), pv, self._chop_ratio())
            else:
                self._open(want, price + want * self.fric, ms, sod, pv, self.fric, self._chop_ratio())

    @property
    def chain(self) -> float:
        """The key of this book's pivots in `hhll.pivots`: its zigzag's two settings in one number."""
        return self.zz.R + self.zz.leg / 1000

    def _next_pivot(self, price: float, ms: int) -> Optional[Pivot]:
        pv = self.zz.update(price, ms)
        if pv is not None:
            self.pivots.append(pv)
            if self.on_pivot:
                self.on_pivot(self, pv)
            if pv.leg is not None:
                self.legs.append(pv.leg)
        return pv

    def _chop_ratio(self) -> Optional[float]:
        return round(statistics.median(self.legs) / self.R, 3) if len(self.legs) == CHOP_N else None

    def _filtered(self, pv: Optional[Pivot]) -> Optional[str]:
        """The swing filter that turns this pivot down ("leg" | "chop"), or None when it passes."""
        if self.p.leg_k and (pv is None or pv.leg is None or pv.leg < self.p.leg_k * self.R - 1e-9):
            return "leg"
        if self.p.chop_k:
            r = self._chop_ratio()
            if r is None or r < self.p.chop_k:
                return "chop"
        return None

    def _may_enter(self, sod: int, price: float, pv: Optional[Pivot] = None) -> bool:
        if self.halted:
            return False
        if sod >= self.cut:
            self.blocked["cutoff"] += 1
            return False
        f = self._filtered(pv)
        if f is not None:
            self.blocked[f] = self.blocked.get(f, 0) + 1
            return False
        if price * self.p.lots * self.spec.lot * MARGIN_PCT > CAPITAL + self.realised:
            self.blocked["margin"] += 1
            return False
        return True

    def _work_limit(self, price: float, ms: int, sod: int) -> None:
        side, limit, until, pv, chop = self.pend
        if sod >= self.cut:                                   # no fills after the entry cutoff
            self.pend = None
            return
        if side * (limit - price) >= self.spec.spread:      # traded through by a full spread: filled
            self.pend = None
            self._open(side, limit, ms, sod, pv, 0.0, chop)
        elif ms >= until:                                     # not filled in time: go at market
            self.pend = None
            self._open(side, price + side * self.fric, ms, sod, pv, self.fric, chop)

    def _open(self, side, px, ms, sod, pv, fric, chop) -> None:
        self.pos = _Pos(side, round(px, 2), ms, sod, pv, fric, chop)

    def _close(self, price: float, ms: int, reason: str) -> None:
        pos, sp = self.pos, self.spec
        self.pos = None
        px = round(price - pos.side * self.fric, 2)
        units = self.p.lots * sp.lot
        gross = round(pos.side * (px - pos.px), 2)
        ch = round_trip_charges(sp, self.p.lots, pos.side, pos.px, px, pos.ms, ms)
        net_rs = round(gross * units - ch["total"], 2)
        self.realised += net_rs
        t = {
            "index": sp.name, "day": day_of(ms).isoformat(), "side": "LONG" if pos.side > 0 else "SHORT",
            "entry_ms": pos.ms, "exit_ms": ms, "entry": clock(pos.ms), "exit": clock(ms),
            "entry_px": pos.px, "exit_px": px, "pivot": pos.pivot.kind, "pivot_px": pos.pivot.price,
            "pivot_lag_s": round((pos.pivot.conf_ms - pos.pivot.ms) / 1000, 1),
            "R": round(self.R, 2), "reason": reason, "units": units,
            "gross_pts": gross, "friction_pts": round(pos.fric + self.fric, 2),
            "charges_rs": round(ch["total"], 2), "charges_pts": round(ch["total"] / units, 2),
            "net_rs": net_rs, "net_pts": round(net_rs / units, 2),
            "mfe_pts": round(pos.peak, 2), "mae_pts": round(-pos.trough, 2),
            "lag_pts": round(pos.side * (pos.px - pos.pivot.price), 2), "entry_sod": pos.sod,
            "chop": pos.chop, "charges": ch,
            "leg_R": round(pos.pivot.leg / self.R, 3) if pos.pivot.leg is not None else None,
        }
        self.trades.append(t)
        if self.on_trade:
            self.on_trade(t)

    def finish(self, reason: str) -> None:
        """Close what is still open at the last price (data stopped before square-off)."""
        self.pend = None
        if self.pos is not None and self.last_px is not None:
            self._close(self.last_px, self.last_ms, reason)


# ── tick store ───────────────────────────────────────────────────────────────────
STORE: Optional[hhll_store.Store] = None
_STORE_LOCK = threading.Lock()


def store() -> hhll_store.Store:
    """This process's HH/LL database (connected on first use)."""
    global STORE
    with _STORE_LOCK:
        if STORE is None:
            STORE = hhll_store.connect()
        return STORE


def use_store(s: Optional[hhll_store.Store]) -> None:
    """Share a store this process already connected (the Lune service's desk, tests)."""
    global STORE
    with _STORE_LOCK:
        STORE = s


def migrate_files() -> dict:
    """Move the files this engine and hhll_algo used to write into the database (once)."""
    return store().migrate_files(DATA, BASE / "logs" / "hhll_ledger.jsonl", BASE / "backups")


def read_ticks(day: date, name: str) -> tuple[list, list]:
    """(epoch ms, prices) of one index's day, live or archived."""
    _, ms, px = store().ticks(day, name)
    return ms, px


def stored_days() -> list[date]:
    return store().days()


def today_ist() -> date:
    return datetime.now(IST).date()


def session_over(day: date) -> bool:
    now = datetime.now(IST)
    return day < now.date() or (day == now.date() and now.hour * 3600 + now.minute * 60 >= SQUARE_OFF_DONE_S)


def in_session() -> bool:
    """Is today's session being traded right now? (Open, not over, and its ticks are stored: no holiday.)"""
    now = datetime.now(IST)
    return (now.hour * 3600 + now.minute * 60 >= OPEN_S and not session_over(now.date())
            and now.date() in stored_days())


# ── replay and the acceptance rule ───────────────────────────────────────────────
#: What `score` and `derive_threshold` read of a replayed trade; the cache holds only these.
REPLAY_KEYS = ("day", "entry_ms", "net_rs", "net_pts", "charges_pts", "friction_pts", "R", "entry_px", "exit_px")
#: The replay cache is bounded by the trades it holds, least recently used dropped first
#: (~0.5 KB a trade). It is only a cache: a dropped day is replayed again from the database.
CACHE_MAX_TRADES = 200_000
_CACHE: OrderedDict = OrderedDict()        # (params, finished day) -> its trades (REPLAY_KEYS)
_CACHE_TRADES = 0
_CACHE_LOCK = threading.Lock()


def replay_day(params: Params, day: date, data: dict) -> list[dict]:
    out = []
    for name, (ms, px) in data.items():
        b = Book(SPECS[name], params)
        for m, x in zip(ms, px):
            b.on_tick(x, m)
        if session_over(day):
            b.finish("data_end")
        out += b.trades
    return out


def _cached(key) -> Optional[list]:
    with _CACHE_LOCK:
        tr = _CACHE.get(key)
        if tr is not None:
            _CACHE.move_to_end(key)
        return tr


def _cache_put(key, trades: list) -> None:
    global _CACHE_TRADES
    with _CACHE_LOCK:
        if key in _CACHE:
            return
        _CACHE[key] = trades
        _CACHE_TRADES += len(trades)
        while _CACHE_TRADES > CACHE_MAX_TRADES and len(_CACHE) > 1:
            _, old = _CACHE.popitem(last=False)
            _CACHE_TRADES -= len(old)


# The session in progress, kept current instead of replayed from its first tick on every review
# and re-test (each of which used to re-read the whole day from the database and replay it for
# every setting it compared). Both are exact: the ticks of an open day only ever grow at the end
# (the recorder numbers them in order, and `Live.follow` relies on the same), and a `Book` fed the
# first n ticks and then the rest is the very `Book` fed them all at once.
_OPEN_LOCK = threading.RLock()
_OPEN_DAY: Optional[date] = None
_OPEN_TICKS: dict = {}                     # name -> [last seq, ms array, price array] of _OPEN_DAY
_OPEN_GAPPY: set = set()                   # names whose seqs ever skipped one: always read whole
_OPEN_BOOKS: OrderedDict = OrderedDict()   # (params, day) -> {name: (book, ticks fed, (ms, price) of the last)}
OPEN_BOOKS_MAX = 96                        # settings kept warm (a re-test pass compares ~40)


def open_day_ticks(day: date) -> dict[str, tuple]:
    """`day_ticks(day)` of an unfinished day, for the indices traded (SPECS), read incrementally:
    only the rows stored since the last call. (ms, prices) per index, as arrays."""
    global _OPEN_DAY
    with _OPEN_LOCK:
        if _OPEN_DAY != day:
            _OPEN_DAY = day
            _OPEN_TICKS.clear()
            _OPEN_GAPPY.clear()
        for name in SPECS:
            if name in _OPEN_GAPPY or name not in _OPEN_TICKS:
                seqs, ms, px = store().ticks(day, name)
                _OPEN_TICKS[name] = [seqs[-1] if seqs else -1, array("q", ms), array("d", px)]
                if any(b != a + 1 for a, b in zip(seqs, seqs[1:])) or (seqs and seqs[0] != 0):
                    _OPEN_GAPPY.add(name)
        while True:
            after = {n: v[0] for n, v in _OPEN_TICKS.items() if n not in _OPEN_GAPPY}
            rows = store().ticks_after(day, after) if after else []
            for name, seq, m, x in rows:
                cur = _OPEN_TICKS[name]
                if seq != cur[0] + 1:              # a seq skipped: read this index whole from now on
                    _OPEN_GAPPY.add(name)
                    continue
                cur[0] = seq
                cur[1].append(int(m))
                cur[2].append(float(x))
            if len(rows) < 50_000:
                break
        for name in list(_OPEN_GAPPY):           # whole, exactly as day_ticks reads it
            seqs, ms, px = store().ticks(day, name)
            _OPEN_TICKS[name] = [seqs[-1] if seqs else -1, array("q", ms), array("d", px)]
        return {n: (v[1], v[2]) for n, v in sorted(_OPEN_TICKS.items()) if len(v[1])}


def _replay_open(params: Params, day: date, data: dict) -> list[dict]:
    """`replay_day(params, day, data)` of an unfinished day, by feeding each index's book only
    the ticks it has not seen yet."""
    with _OPEN_LOCK:
        for key in [k for k in _OPEN_BOOKS if k[1] != day]:
            del _OPEN_BOOKS[key]
        books = _OPEN_BOOKS.get((params, day))
        if books is None:
            books = _OPEN_BOOKS[(params, day)] = {}
            while len(_OPEN_BOOKS) > OPEN_BOOKS_MAX:
                _OPEN_BOOKS.popitem(last=False)
        _OPEN_BOOKS.move_to_end((params, day))
        out = []
        for name, (ms, px) in data.items():
            b, n, last = books.get(name, (None, 0, None))
            if b is None or n > len(ms) or (n and (ms[n - 1], px[n - 1]) != last):
                b, n = Book(SPECS[name], params), 0   # not the day it has seen: replay it whole
                b.pivots = deque(maxlen=1)            # never read here; a whole day of them is memory
            k = len(b.trades)
            for i in range(n, len(ms)):
                b.on_tick(px[i], ms[i])
            b.trades[k:] = [{key: t[key] for key in REPLAY_KEYS} for t in b.trades[k:]]   # what is read
            if len(ms):
                books[name] = (b, len(ms), (ms[-1], px[-1]))
            out += b.trades
        return out


def evaluate(configs: list[Params], days: list[date]) -> dict:
    """Closed trades of each config over every day (REPLAY_KEYS only). Finished days are cached;
    the day in progress is replayed incrementally (`_replay_open`)."""
    uniq = list(dict.fromkeys(configs))
    res = {c: [] for c in uniq}
    for d in days:
        got = {c: _cached((c, d)) for c in uniq}
        need = [c for c in uniq if got[c] is None]
        if need:
            done = session_over(d)
            if not done:
                data = open_day_ticks(d)
                for c in need:
                    got[c] = [{k: t[k] for k in REPLAY_KEYS} for t in _replay_open(c, d, data)]
            else:
                data = {n: v for n, v in store().day_ticks(d).items() if n in SPECS}
                for c in need:
                    got[c] = [{k: t[k] for k in REPLAY_KEYS} for t in replay_day(c, d, data)]
                    _cache_put((c, d), got[c])
        for c in uniq:
            res[c] += got[c]
    return res


def tick_span(day: date) -> tuple[int, int]:
    """First and last stored tick of a day, over every index (epoch ms)."""
    return store().span(day)


def data_hours(days: list[date]) -> float:
    if not days:
        return 0.0
    if len(days) > 1:
        return float("inf")                      # whole sessions: the day split applies
    lo, hi = tick_span(days[0])
    return (hi - lo) / 3.6e6


def score(trades: list[dict], days: list[date], split_ms: Optional[int] = None) -> dict:
    """Net, and the two halves: by day, or (one day) trades entered before/after `split_ms`."""
    if split_ms is not None:
        h1 = sum(t["net_rs"] for t in trades if t["entry_ms"] < split_ms)
        h2 = sum(t["net_rs"] for t in trades if t["entry_ms"] >= split_ms)
        return {"net": round(h1 + h2, 2), "h1": round(h1, 2), "h2": round(h2, 2), "trades": len(trades),
                "wins": sum(t["net_rs"] > 0 for t in trades)}
    by = {d.isoformat(): 0.0 for d in days}
    for t in trades:
        by[t["day"]] = by.get(t["day"], 0.0) + t["net_rs"]
    keys = [d.isoformat() for d in days]
    h = len(keys) // 2
    return {"net": round(sum(by.values()), 2), "h1": round(sum(by[k] for k in keys[:h]), 2),
            "h2": round(sum(by[k] for k in keys[h:]), 2), "trades": len(trades),
            "wins": sum(t["net_rs"] > 0 for t in trades)}


def split_of(days: list[date]) -> Optional[int]:
    """Midpoint of the one day's ticks, or None when the halves are whole days."""
    if len(days) != 1:
        return None
    lo, hi = tick_span(days[0])
    return (lo + hi) // 2


def judge(base: Params, cand: Params, nbrs: list[Params], days: list[date]) -> dict:
    res = evaluate([base, cand, *nbrs], days)
    sp = split_of(days)
    sb, sc = score(res[base], days, sp), score(res[cand], days, sp)
    sn = [score(res[n], days, sp) for n in nbrs]
    why = []
    hrs = data_hours(days)
    if hrs < MIN_HOURS:
        why.append(f"only {hrs:.1f}h of ticks, need {MIN_HOURS}h")
    if max(sb["trades"], sc["trades"]) < MIN_TRADES:
        why.append(f"only {max(sb['trades'], sc['trades'])} trades on the replay either way, need {MIN_TRADES}")
    if sc["net"] <= sb["net"]:
        why.append("not better overall")
    # Better overall (checked above) and worse in neither half: an improvement that holds across
    # the data, not one half paying for the other.
    if sc["h1"] < sb["h1"] or sc["h2"] < sb["h2"]:
        why.append("worse in one half")
    if not nbrs or any(s["net"] <= sb["net"] for s in sn):
        why.append("nearby settings not better")
    return {"accepted": not why, "why": why, "base": sb, "cand": sc,
            "nbrs": [{"params": diff(base, n), **s} for n, s in zip(nbrs, sn)]}


def diff(a: Params, b: Params) -> dict:
    return {f.name: getattr(b, f.name) for f in fields(Params) if getattr(a, f.name) != getattr(b, f.name)}


def moved(p: Params, key: str, steps: int) -> Optional[Params]:
    g = GRID[key]
    i = g.index(getattr(p, key)) + steps
    return replace(p, **{key: g[i]}) if 0 <= i < len(g) else None


def neighbours(base: Params, cand: Params, key: str) -> list[Params]:
    """Settings next to the candidate on the grid: never the base value, never 'off'."""
    out = []
    for s in (-1, 1):
        n = moved(cand, key, s)
        if n is not None and getattr(n, key) not in (0, getattr(base, key), "market"):
            out.append(n)
    return out


def fix_for(p: Params, weakness: str) -> tuple[str, Optional[Params]]:
    """The one setting change that answers a weakness."""
    if weakness == "gave back open profit":
        return "lock_at", replace(p, lock_at=TOGGLE_ON["lock_at"]) if not p.lock_at else moved(p, "lock_at", -1)
    if weakness == "left profit at the peak":
        return "target", replace(p, target=TOGGLE_ON["target"]) if not p.target else moved(p, "target", -1)
    if weakness == "small leg into the pivot":
        return "leg_k", replace(p, leg_k=TOGGLE_ON["leg_k"]) if not p.leg_k else moved(p, "leg_k", 1)
    if weakness == "adverse move past the stop":
        return "stop", replace(p, stop=TOGGLE_ON["stop"]) if not p.stop else moved(p, "stop", -1)
    if weakness == "entered in chop":
        return "chop_k", replace(p, chop_k=TOGGLE_ON["chop_k"]) if not p.chop_k else moved(p, "chop_k", 1)
    if weakness == "costs bigger than the leg":
        return "thr_cost", moved(p, "thr_cost", 1)
    if weakness == "spread and slippage":
        return ("order", replace(p, order="limit")) if p.order == "market" else ("limit_wait", moved(p, "limit_wait", 1))
    if weakness == "late entry squared off":
        return "last_entry", moved(p, "last_entry", -1)
    return "thr_cost", moved(p, "thr_cost", -1)       # confirmation lag


def weakness(t: dict, p: Params) -> tuple[str, dict]:
    """What cost this trade the most, in points, each measured against the fix that answers it."""
    R, loss = t["R"], -t["net_pts"]
    trail = p.lock_trail * R
    trig = (p.lock_at or TOGGLE_ON["lock_at"]) * R
    stop = (p.stop or TOGGLE_ON["stop"]) * R
    k = p.chop_k + 0.25 if p.chop_k else TOGGLE_ON["chop_k"]
    cut = hhmm(GRID["last_entry"][max(0, GRID["last_entry"].index(p.last_entry) - 1)])
    tgt = (p.target or TOGGLE_ON["target"]) * R
    lk = p.leg_k + 0.5 if p.leg_k else TOGGLE_ON["leg_k"]
    leg = t.get("leg_R")
    s = {
        # a winner is reviewed too: what a profit target would have kept beyond what the exit kept
        "left profit at the peak": max(0.0, tgt - t["gross_pts"]) if t["mfe_pts"] >= tgt else 0.0,
        "small leg into the pivot": loss if loss > 0 and leg is not None and leg < lk else 0.0,
        "gave back open profit": max(0.0, t["mfe_pts"] - trail - t["gross_pts"]) if t["mfe_pts"] >= trig else 0.0,
        "adverse move past the stop": max(0.0, -t["gross_pts"] - stop),
        "entered in chop": loss if loss > 0 and t["chop"] is not None and t["chop"] < k else 0.0,
        "costs bigger than the leg": loss if loss > 0 and abs(t["gross_pts"]) < t["charges_pts"] + t["friction_pts"] else 0.0,
        "spread and slippage": t["friction_pts"] if p.order == "market" or t["friction_pts"] > 0 else 0.0,
        "late entry squared off": loss if loss > 0 and t["reason"] == "square_off" and t["entry_sod"] >= cut else 0.0,
    }
    s = {k_: round(v, 2) for k_, v in s.items()}
    worst = max(s, key=s.get)
    return (worst if s[worst] > 0 else "confirmation lag"), s


def analyse(t: dict, w: str, scores: dict) -> str:
    """The trade in words: where it entered, what it saw, what it kept, what cost it."""
    cost = round(t["charges_pts"] + t["friction_pts"], 2)
    chop = t["chop"]
    tone = "no swing history yet" if chop is None else (
        f"recent swings {chop}x R ({'trending' if chop >= 2 else 'choppy, below the 2x R a trend needs'})")
    kept = (f"kept {t['gross_pts']} of {t['mfe_pts']} pts open profit" if t["mfe_pts"] > 0
            else "never in profit after costs of entry")
    return (f"{t['side']} {t['index']} {t['entry']} at {t['entry_px']}: {t['lag_pts']} pts behind the {t['pivot']} "
            f"{t['pivot_px']} (R {t['R']}, confirmed {t['pivot_lag_s']}s after the extreme); {tone}. "
            f"Worst {-t['mae_pts']} pts; {kept}. Exit {t['reason']} {t['exit']} at {t['exit_px']}: gross "
            f"{t['gross_pts']} − costs {cost} ({t['charges_pts']} charges + {t['friction_pts']} spread/slip) = "
            f"net {t['net_pts']} pts, Rs {t['net_rs']:,.0f}. Biggest weakness: {w} (~{scores.get(w, 0)} pts).")


def review(t: dict, p: Params, days: list[date]) -> dict:
    w, scores = weakness(t, p)
    key, cand = fix_for(p, w)
    if cand is None:                                  # at the end of its grid: try the lag fix instead
        w, (key, cand) = w + " (fix at grid edge, testing lag fix)", fix_for(p, "confirmation lag")
    out = {"at": datetime.now(IST).isoformat(timespec="seconds"), "trade": _brief(t), "weakness": w,
           "analysis": analyse(t, w.split(" (")[0], scores),
           "scores_pts": scores, "fix": diff(p, cand) if cand else None, "days": len(days)}
    if cand is None:
        out.update(accepted=False, why=["no fix left on the grid"])
        return out
    out.update(judge(p, cand, neighbours(p, cand, key), days))
    out["candidate"] = asdict(cand)
    return out


def _brief(t: dict) -> dict:
    return {k: t[k] for k in ("index", "day", "side", "entry", "exit", "entry_px", "exit_px", "reason",
                              "gross_pts", "charges_pts", "friction_pts", "net_pts", "net_rs", "mfe_pts", "mae_pts", "R")}


def derive_threshold(p: Params, days: list[date]) -> dict:
    """Net result for every threshold on the grid; pick the smallest whose own result, both
    halves and both grid neighbours are net positive (least delay that still pays its costs)."""
    key = "fast_cost" if p.fast_cost else "thr_cost"      # the setting that prints the pivots
    if p.index_pivots:      # each index has its own threshold, which this global one never reaches: see `health`
        return {"rows": [], "pick": None, "key": key}
    cfgs = [replace(p, **{key: b}) for b in GRID["thr_cost"]]
    res = evaluate(cfgs, days)
    sp = split_of(days)
    rows = []
    for c in cfgs:
        tr = res[c]
        s = score(tr, days, sp)
        legs = [abs(t["exit_px"] - t["entry_px"]) for t in tr]
        s.update(thr_cost=getattr(c, key), avg_cost_pts=round(statistics.fmean([t["charges_pts"] + t["friction_pts"] for t in tr]), 2) if tr else None,
                 avg_net_pts=round(statistics.fmean([t["net_pts"] for t in tr]), 2) if tr else None,
                 avg_R=round(statistics.fmean([t["R"] for t in tr]), 2) if tr else None,
                 avg_move_pts=round(statistics.fmean(legs), 2) if legs else None)
        rows.append(s)
    pick = None
    for i, r in enumerate(rows):
        nb = [rows[j] for j in (i - 1, i + 1) if 0 <= j < len(rows)]
        if r["net"] > 0 and r["h1"] > 0 and r["h2"] > 0 and all(n["net"] > 0 for n in nb):
            pick = r["thr_cost"]
            break
    return {"rows": rows, "pick": pick, "key": key}


def retest_all(p: Params, days: list[date], log: Callable[[dict], None]) -> Params:
    """Re-test every setting against its alternatives with the same rule; adopt the best that passes."""
    d = derive_threshold(p, days)
    log({"kind": "derive", "at": datetime.now(IST).isoformat(timespec="seconds"), "days": len(days), **d})
    if d["pick"] is not None and d["pick"] != getattr(p, d["key"]):
        cand = replace(p, **{d["key"]: d["pick"]})
        r = judge(p, cand, [replace(p, **{d["key"]: g}) for g in GRID["thr_cost"]
                            if abs(GRID["thr_cost"].index(g) - GRID["thr_cost"].index(d["pick"])) == 1
                            and g != getattr(p, d["key"])], days)
        log({"kind": "retest", "setting": d["key"], "fix": diff(p, cand), **r})
        if r["accepted"]:
            p = cand
    for key in ("exit_on", "exit_wide", "chop_k", "leg_k", "target", "lock_at", "lock_trail", "stop", "order",
                "limit_wait", "last_entry", "thr_cost"):
        if key == "exit_on":
            if p.exit_on == "pivot":       # hold through filtered pivots, with a wide zigzag as the safety exit
                cand = replace(p, exit_on="signal", exit_wide=p.exit_wide or 3.0)
                r = judge(p, cand, [replace(cand, exit_wide=w) for w in (2.0, 4.0) if w != cand.exit_wide], days)
                log({"kind": "retest", "setting": key, "fix": diff(p, cand), **r})
                if r["accepted"]:
                    p = cand
            continue
        alts = [moved(p, key, -1), moved(p, key, 1)]
        if key in TOGGLE_ON:
            alts.append(replace(p, **{key: 0 if getattr(p, key) else TOGGLE_ON[key]}))
        best = None
        for cand in {a for a in alts if a is not None and a != p}:
            r = judge(p, cand, neighbours(p, cand, key), days)
            log({"kind": "retest", "setting": key, "fix": diff(p, cand), **r})
            if r["accepted"] and (best is None or r["cand"]["net"] > best[1]):
                best = (cand, r["cand"]["net"])
        if best:
            p = best[0]
    return p


# ── retune check, per index ──────────────────────────────────────────────────────
#: Does each index's own pivot threshold still hold? Two separate questions, on replays of the
#: current settings over the stored FINISHED sessions (today's open session is never used):
#:   ALARM   is the index doing worse than its OWN record? Each test compares the latest stretch
#:           with what this index's own earlier trades make likely, so noise alone rarely fires it
#:           (about 5% per test). It says "look", never "change".
#:   SWITCH  does a nearby threshold beat the current one on the last HEALTH_DAYS sessions by a
#:           clear margin in total AND in both halves, with enough trades, with its own grid
#:           neighbour beating the current one too (so not one lucky value), and ahead on the days
#:           themselves, not on one or two of them (HEALTH_CONF)? Only this says change.
#: Too little data says NOT ENOUGH DATA, never OK. A threshold best at the grid's end says so.
#: A SWITCH is applied by itself (AUTO_SWITCH, `Live._auto_switch`).
HEALTH_GRID = GRID["thr_cost"] + [60, 75, 100]   # past 50, up to the desk's own limit: 50 is no dead end
HEALTH_DAYS = 10            # the contest window, finished sessions
HEALTH_MIN_DAYS = 6         # fewer sessions in the window: no contest (each half needs 3 sessions)
HEALTH_STEPS = 2            # thresholds tried each side of the current one on GRID["thr_cost"]
HEALTH_MIN_TRADES = 20      # the current threshold and any challenger need this many trades in the window
HEALTH_MARGIN = 0.10        # a challenger must beat the current total by this share of its size
HEALTH_CONF = 0.90          # ...and stay ahead in this share of resamples of the window's sessions (paired by day)
HEALTH_RETRY_S = 300        # a retune check that failed (the database was away) is tried again after this long
HEALTH_COVER = 0.8          # a day counts as a session for an index only if its ticks cover this much of 09:15-15:30
HEALTH_PF_N = 20            # trades in the latest stretch the profit-factor alarm reads
HEALTH_LEG_N = 15           # swings in the latest stretch the swing alarm reads
HEALTH_PCTL = 0.05          # an alarm fires below this percentile of the index's own resampled record
HEALTH_BOOT = 2000          # resamples (fixed seed: the same data always gives the same verdict)
HEALTH_LOSING_DAYS = 3      # this many losing sessions in a row, the latest included: alarm
HEALTH_DD_MIN_DAYS = 5      # the drawdown alarm needs this much history (the first dip is always "deepest")
#: (index, settings, day) -> (trades as (day, net_rs), legs / threshold), and (index, day) -> whether
#: that day counts as a session. Finished days never change.
#: ponytail: grows by ~7 entries per index per new session or setting change; tiny, cleared on restart
_HEALTH_DAY: dict = {}


def pivot_value(p: Params, name: str) -> float:
    """The setting that prints this index's pivots, x cost (what the desk shows as its threshold)."""
    x = next((x for x in p.index_pivots if x[0] == name), None)
    return float((x[2] or x[1]) if x else (p.fast_cost or p.thr_cost))


def with_pivot_value(p: Params, name: str, v: float) -> Params:
    """`p` with only this index's pivot threshold set to `v`: R (stops, filters) and every other
    index stay as they are, exactly as a threshold saved on the desk does."""
    if not any(x[0] == name for x in p.index_pivots):
        return replace(p, index_pivots=tuple(sorted(p.index_pivots + ((name, p.thr_cost, float(v), 0.0),))))
    return replace(p, index_pivots=tuple((x[0], x[1], float(v), x[3]) if x[0] == name else x
                                         for x in p.index_pivots))


def _pf(xs) -> float:
    w = sum(x for x in xs if x > 0)
    lo = -sum(x for x in xs if x < 0)
    return w / lo if lo else math.inf


def _low_vs_own(recent: list, earlier: list, stat: Callable[[list], float]) -> Optional[tuple[float, float]]:
    """(stat of `recent`, the HEALTH_PCTL percentile of stat over resamples of `earlier` the same
    size) when `recent` is below it, else None. Needs `earlier` at least twice the size of `recent`."""
    n = len(recent)
    if not n or len(earlier) < 2 * n:
        return None
    import random
    rnd = random.Random(0)
    boot = sorted(stat([rnd.choice(earlier) for _ in range(n)]) for _ in range(HEALTH_BOOT))
    cut = boot[int(HEALTH_PCTL * HEALTH_BOOT)]
    now = stat(recent)
    return (now, cut) if now < cut else None


def health_alarms(trades: list[float], legs: list[float], daily: list[float]) -> list[str]:
    """What is off against this index's own record. `trades` net Rs in order, `legs` swings /
    pivot threshold in order, `daily` net per counted session in order."""
    out = []
    eq = peak = worst = 0.0
    for x in trades:
        eq += x
        peak = max(peak, eq)
        worst = max(worst, peak - eq)
    if len(daily) >= HEALTH_DD_MIN_DAYS and peak - eq > 0 and peak - eq >= worst:
        out.append(f"drawdown Rs {peak - eq:,.0f} is the deepest in {len(daily)} sessions")
    r = _low_vs_own(trades[-HEALTH_PF_N:], trades[:-HEALTH_PF_N], _pf) if len(trades) > HEALTH_PF_N else None
    if r:
        out.append(f"profit factor {r[0]:.2f} over the last {HEALTH_PF_N} trades, below {r[1]:.2f} "
                   f"(its own record's worst {HEALTH_PCTL:.0%})")
    r = _low_vs_own(legs[-HEALTH_LEG_N:], legs[:-HEALTH_LEG_N], statistics.median) if len(legs) > HEALTH_LEG_N else None
    if r:
        out.append(f"median swing {r[0]:.2f}x threshold over the last {HEALTH_LEG_N}, below {r[1]:.2f} "
                   f"(its own record's worst {HEALTH_PCTL:.0%}): choppier than usual")
    run = 0
    for x in reversed(daily):
        if x >= 0:
            break
        run += 1
    if run >= HEALTH_LOSING_DAYS:
        out.append(f"{run} losing sessions in a row")
    return out


def day_conf(cur: list, cand: list) -> float:
    """Share of resamples of the sessions (drawn with replacement, both thresholds on the same days)
    in which `cand` makes more than `cur`. One lucky day gives ~2/3, a lead on most days ~1."""
    import random
    d = [b - a for a, b in zip(cur, cand)]
    rnd = random.Random(0)
    return sum(sum(rnd.choice(d) for _ in d) > 0 for _ in range(HEALTH_BOOT)) / HEALTH_BOOT if d else 0.0


def health_contest(cur: float, scores: dict, nbrs: dict, cands: list) -> tuple[Optional[float], list[str]]:
    """The threshold in `cands` that should replace `cur`, or None, and why each did not.
    `scores[v]` is `score()` of threshold v on the window (plus "conf", `day_conf` against `cur`);
    `nbrs[v]` its grid neighbours."""
    b = scores[cur]
    need = b["net"] + HEALTH_MARGIN * abs(b["net"])
    best, why = None, []
    for v in cands:
        if v == cur:
            continue
        s = scores[v]
        miss = []
        if s["trades"] < HEALTH_MIN_TRADES:
            miss.append(f"only {s['trades']} trades")
        if not s["net"] > need:
            miss.append(f"not {HEALTH_MARGIN:.0%} better in total")
        elif not (s["h1"] > b["h1"] and s["h2"] > b["h2"]):
            miss.append("better in one half only")
        elif s.get("conf", 1.0) < HEALTH_CONF:
            miss.append(f"ahead in only {s['conf']:.0%} of resampled sessions, {HEALTH_CONF:.0%} needed (a day or two carry it)")
        far = [n for n in nbrs[v] if n != cur]
        if not far:
            miss.append("no neighbour to confirm it (grid end)")
        elif any(n not in scores or scores[n]["net"] <= b["net"] for n in far):
            miss.append("its neighbour is not better")
        if miss:
            why.append(f"{v:g}: " + ", ".join(miss))
        elif best is None or s["net"] > scores[best]["net"]:
            best = v
    return best, why


def covers(ms: list) -> bool:
    """Do these ticks cover HEALTH_COVER of the session? (A feed that died at 11:00 is no session.)"""
    if not ms:
        return False
    sod = lambda m: (m // 1000 + IST_OFFSET_S) % 86400
    return (min(CLOSE_S, sod(ms[-1])) - max(OPEN_S, sod(ms[0]))) >= HEALTH_COVER * (CLOSE_S - OPEN_S)


def _replay(name: str, p: Params, ms: list, px: list) -> tuple[list, list]:
    """One index, one finished day: its trades as (day, net Rs) and its swings / pivot threshold."""
    b = Book(SPECS[name], p)
    for m, x in zip(ms, px):
        b.on_tick(x, m)
    b.finish("data_end")
    return [(t["day"], t["net_rs"]) for t in b.trades], [pv.leg / b.zz.R for pv in b.pivots if pv.leg is not None]


def health(p: Params, days: list[date]) -> list[dict]:
    """Per index: OK | ALARM | SWITCH | NOT ENOUGH DATA | NO DATA, on the finished sessions among
    `days` (alarms on all, the contest on the last HEALTH_DAYS). Every number is a replay of `p`,
    with only that index's threshold changed, through the same `Book` that trades, all costs in."""
    days = sorted(d for d in set(days) if session_over(d))
    grid = HEALTH_GRID
    names = list(SPECS)
    vals, nbrs = {}, {}
    for n in names:
        cur = pivot_value(p, n)
        g = sorted(set(grid) | {cur})
        i = g.index(cur)
        vals[n] = g[max(0, i - HEALTH_STEPS - 1): i + HEALTH_STEPS + 2]   # one more each side: the neighbours' neighbours
        nbrs[n] = {v: [g[j] for j in (g.index(v) - 1, g.index(v) + 1) if 0 <= j < len(g)] for v in vals[n]}
    have = {n: [] for n in names}          # sessions with this index's ticks covering the session
    tr = {(n, v): [] for n in names for v in vals[n]}
    legs = {n: [] for n in names}
    win_all = set(days[-HEALTH_DAYS:])
    for d in days:
        data = None
        for n in names:
            if (n, d) not in _HEALTH_DAY:                       # does this day count as a session for n?
                data = store().day_ticks(d) if data is None else data
                _HEALTH_DAY[(n, d)] = covers((data.get(n) or ((), ()))[0])
            if not _HEALTH_DAY[(n, d)]:
                continue
            have[n].append(d)
            cur = pivot_value(p, n)
            for v in (vals[n] if d in win_all else [cur]):
                q = with_pivot_value(p, n, v)
                if (n, q, d) not in _HEALTH_DAY:
                    data = store().day_ticks(d) if data is None else data
                    _HEALTH_DAY[(n, q, d)] = _replay(n, q, *data[n])
                t, lg = _HEALTH_DAY[(n, q, d)]
                tr[(n, v)] += t
                if v == cur:
                    legs[n] += lg
    out = []
    for n in names:
        cur = pivot_value(p, n)
        win = [d for d in have[n] if d in win_all]
        row = {"index": n, "threshold": cur, "sessions": len(have[n]), "window": len(win),
               "first": have[n][0].isoformat() if have[n] else None,
               "last": have[n][-1].isoformat() if have[n] else None,
               "alarms": [], "why_not": [], "switch_to": None, "scores": {}, "edge": None}
        if not win:
            row.update(verdict="NO DATA", note=f"no full session of ticks in the last {HEALTH_DAYS} finished sessions")
            out.append(row)
            continue
        mine = [x for _, x in tr[(n, cur)]]
        daily = [sum(x for dd, x in tr[(n, cur)] if dd == d.isoformat()) for d in have[n]]
        row["alarms"] = health_alarms(mine, legs[n], daily)
        row["net"], row["trades"] = round(sum(mine), 2), len(mine)
        w0 = win[0].isoformat()
        scores = {v: score([{"day": dd, "net_rs": x} for dd, x in tr[(n, v)] if dd >= w0], win) for v in vals[n]}
        row["scores"] = {f"{v:g}": {k: scores[v][k] for k in ("net", "h1", "h2", "trades")} for v in vals[n]}
        by_day = {v: [sum(x for dd, x in tr[(n, v)] if dd == d.isoformat()) for d in win] for v in vals[n]}
        for v in vals[n]:
            scores[v]["conf"] = day_conf(by_day[cur], by_day[v])
        top = max(scores, key=lambda v: scores[v]["net"])
        if top == cur and cur in (min(grid), max(grid)) or cur < min(grid) or cur > max(grid):
            row["edge"] = (f"{cur:g} is at the end of the tested range ({min(grid)}-{max(grid)}): "
                           "values beyond it were not tested")
        short = []
        if len(win) < HEALTH_MIN_DAYS:
            short.append(f"{len(win)} of the {HEALTH_MIN_DAYS} sessions the contest needs")
        if scores[cur]["trades"] < HEALTH_MIN_TRADES:
            short.append(f"{scores[cur]['trades']} of the {HEALTH_MIN_TRADES} trades it needs")
        if short:
            to, row["why_not"] = None, short
        else:
            inner = [v for v in vals[n] if abs(vals[n].index(v) - vals[n].index(cur)) <= HEALTH_STEPS]
            to, row["why_not"] = health_contest(cur, scores, nbrs[n], inner)   # the outermost only vouch for neighbours
        row["switch_to"] = to
        if to is not None:
            row["verdict"] = "SWITCH"
            row["note"] = (f"{to:g} beat {cur:g} by {HEALTH_MARGIN:.0%}+ and in both halves of the last "
                           f"{len(win)} sessions ({w0} to {row['last']})")
        elif row["alarms"]:
            row["verdict"] = "ALARM"
            row["note"] = "; ".join(row["alarms"]) + ("; " + "; ".join(short) if short else "")
        elif short:
            row["verdict"] = "NOT ENOUGH DATA"
            row["note"] = "only " + " and ".join(short) + f"; nothing was judged ({row['sessions']} session(s) to {row['last']})"
        else:
            row["verdict"] = "OK"
            row["note"] = f"no nearby threshold beat {cur:g} over {len(win)} sessions to {row['last']}"
        if row["edge"]:
            row["note"] += "; " + row["edge"]
        out.append(row)
    return out


def switch_record(p: Params, made: list) -> list[dict]:
    """Out of sample: what each switch the retune check made has earned since, against the threshold it
    replaced, on the finished sessions after the one it was decided on (none of them it was picked on).
    Call after `health` (which marks the sessions that count). Replays of `p`, that threshold apart."""
    out = []
    for sw in made[-20:]:
        n, new, old = sw["index"], [], []
        days = [d for d in stored_days() if d.isoformat() > sw["after"] and _HEALTH_DAY.get((n, d))]
        for d in days:
            for v, acc in ((sw["to"], new), (sw["from"], old)):
                q = with_pivot_value(p, n, v)
                if (n, q, d) not in _HEALTH_DAY:
                    _HEALTH_DAY[(n, q, d)] = _replay(n, q, *store().day_ticks(d)[n])
                acc.append(sum(x for _, x in _HEALTH_DAY[(n, q, d)][0]))
        out.append({**sw, "sessions": len(days), "new": round(sum(new), 2), "old": round(sum(old), 2)})
    return out


def refresh_health(p: Optional[Params] = None) -> dict:
    """Run the retune check on the settings in force and store it (`hhll.state` row "health"),
    where the desk's Settings dialog and the report read it."""
    p = p or current_params(today_ist())[0]
    rows = health(p, stored_days())
    st = {"at": datetime.now(IST).isoformat(timespec="seconds"), "params": asdict(p), "rows": rows,
          "switches": switch_record(p, (store().load_state() or {}).get("switches") or [])}
    st = json.loads(json.dumps(st, default=str))
    store().save_state(st, "health")
    return st


def health_lines(st: dict) -> list[str]:
    rows = st.get("rows") or []
    if not rows:
        return []
    L = [f"## Retune check ({st['at']})", "",
         "OK = no nearby threshold beat it: leave it. ALARM = worse than its own record: look, change nothing "
         f"unless SWITCH follows. SWITCH = a nearby threshold beat it by {HEALTH_MARGIN:.0%}+ in total and in both "
         f"halves of the last {HEALTH_DAYS} finished sessions, with {HEALTH_MIN_TRADES}+ trades, its neighbour "
         f"beating it too, and ahead in {HEALTH_CONF:.0%}+ of resamples of those sessions" + (": applied by itself from the next session, each index once per finished session"
                             if AUTO_SWITCH else "") + ". NOT ENOUGH DATA = nothing was judged.", "",
         "| index | threshold | verdict | sessions | why | net Rs per threshold on the window (halves) |",
         "|---|---|---|---|---|---|"]
    for r in rows:
        cur = f"{r['threshold']:g}"
        cells = ", ".join(("**{}**" if v == cur else "{}").format(
            f"{v}: {s['net']:,.0f} ({s['h1']:,.0f}/{s['h2']:,.0f})") for v, s in r["scores"].items())
        verdict = f"SWITCH to {r['switch_to']:g}" if r["verdict"] == "SWITCH" else r["verdict"]
        L.append(f"| {r['index']} | {cur} | **{verdict}** | {r['sessions']} | {r.get('note', '')} | {cells or '-'} |")
    sw = st.get("switches") or []
    if sw:
        L += ["", "Switches the retune check made, judged on the sessions since (out of sample):", "",
              "| index | switch | decided after | sessions since | net Rs new | net Rs old | new - old |", "|---|---|---|---|---|---|---|"]
        L += [f"| {x['index']} | {x['from']:g} -> {x['to']:g} | {x['after']} | {x['sessions']} | {x['new']:,.0f} | "
              f"{x['old']:,.0f} | **{x['new'] - x['old']:+,.0f}** |" for x in sw]
    return L + [""]


# ── state, ledger, report ────────────────────────────────────────────────────────
REPORT = DATA / "report.md"


def _prior() -> Params:
    """Before any data: R = 10 x each index's own cost, so costs are 10% of the minimum leg."""
    return Params(thr_cost=10)


def load_state(day: date) -> dict:
    st = store().load_state()
    if not st:
        p = asdict(_prior())
        st = {"active": p, "active_day": day.isoformat(), "next": p, "retested": None,
              "ledger_start_ms": int(time.time() * 1000),     # a fresh ledger counts trades opened from now
              "history": [{"at": day.isoformat(), "params": p, "why": "prior: costs <= 10% of R"}]}
    if st["active_day"] < day.isoformat():         # a new session starts on the adopted settings
        st["active"], st["active_day"] = st["next"], day.isoformat()
    save_state(st)
    return st


def current_params(day: date) -> tuple[Params, int]:
    """The settings in force on `day`, and the epoch ms they took effect (0 = from the open).

    Read-only: the Lune desk calls this to trade on exactly what this trader runs."""
    st = store().load_state()
    if not st:
        return _prior(), 0
    if st["active_day"] < day.isoformat():
        return Params(**st["next"]), 0
    at = int(st.get("applied_ms") or 0)
    return Params(**st["active"]), (at if at and day_of(at) == day else 0)


def replayed_book(spec: Spec, params: Params, day: date, clear: bool = True, keep_ms: Optional[int] = None,
                  ticks: Optional[tuple[list, list]] = None,
                  on_pivot: Optional[Callable[[Book, Pivot], None]] = None, keep: bool = True) -> Book:
    """A book that has seen every stored tick of `day` (or `ticks`, (ms, prices)): pivots, legs
    and R as if it ran all day. `clear` drops the position the replay ends in, which the live
    account never held, unless it was opened on the tick at `keep_ms` (an entry taken now,
    under these settings). `on_pivot`/`keep` as for `Book`."""
    b = Book(spec, params, on_pivot=on_pivot, keep=keep)
    ms, px = ticks if ticks is not None else read_ticks(day, spec.name)
    for m, x in zip(ms, px):
        b.on_tick(x, m)
    if clear and not (b.pos is not None and keep_ms is not None and b.pos.ms == keep_ms):
        b.pos = None
    if clear:
        b.pend = None
    b.trades.clear()
    return b


def save_state(st: dict) -> None:
    store().save_state(st)


def log_review(row: dict) -> None:
    store().add_review(row)


def write_report() -> None:
    s = store()
    trades, st = s.trades(), s.load_state()
    revs, n_revs = s.recent_reviews(REPORT_REVIEWS)      # the report lists the newest; all stay in the table
    L = ["# HH/LL tick trader — paper results", "",
         f"Updated {datetime.now(IST):%Y-%m-%d %H:%M} IST. Every number is after all charges "
         "(brokerage, STT, exchange, SEBI, stamp, GST) and modelled spread + slippage. Synthetic futures, "
         "1 lot, 4 option orders per round trip.", ""]
    if st:
        L += [f"**Settings in force today:** `{json.dumps(st['active'])}`", "",
              f"**Settings for the next session:** `{json.dumps(st['next'])}`", ""]
    L += ["## Paper trades by day", "", "| day | index | trades | wins | gross Rs | charges Rs | net Rs |",
          "|---|---|---|---|---|---|---|"]
    agg: dict = {}
    for t in trades:
        a = agg.setdefault((t["day"], t["index"]), [0, 0, 0.0, 0.0, 0.0])
        a[0] += 1
        a[1] += t["net_rs"] > 0
        a[2] += t["gross_pts"] * t["units"]
        a[3] += t["charges_rs"]
        a[4] += t["net_rs"]
    tot = [0, 0, 0.0, 0.0, 0.0]
    for (d, n), a in sorted(agg.items()):
        L.append(f"| {d} | {n} | {a[0]} | {a[1]} | {a[2]:,.0f} | {a[3]:,.0f} | {a[4]:,.0f} |")
        tot = [x + y for x, y in zip(tot, a)]
    L += [f"| **all** | | {tot[0]} | {tot[1]} | {tot[2]:,.0f} | {tot[3]:,.0f} | **{tot[4]:,.0f}** |", ""]
    L += health_lines(s.load_state("health"))
    d = s.latest_review("derive")
    if d and d["rows"]:
        L += [f"## Threshold derivation ({d['at']}, {d['days']} day(s))", "",
              "| R / cost | avg R pts | trades | wins | avg cost pts | avg net pts | net Rs | 1st half | 2nd half |",
              "|---|---|---|---|---|---|---|---|---|"]
        for r in d["rows"]:
            L.append(f"| {r['thr_cost']} | {r['avg_R']} | {r['trades']} | {r['wins']} | {r['avg_cost_pts']} | "
                     f"{r['avg_net_pts']} | {r['net']:,.0f} | {r['h1']:,.0f} | {r['h2']:,.0f} |")
        L += ["", f"Smallest robust net-positive threshold: **{d['pick'] or 'none yet'}**", ""]
    L += ["## Every review and re-test (newest first)", ""]
    if n_revs > len(revs):
        L += [f"The newest {len(revs):,} of {n_revs:,}; every one is kept in `hhll.reviews`.", ""]
    for r in reversed([r for r in revs if r.get("kind") != "derive"]):
        verdict = "ACCEPTED" if r.get("accepted") else "rejected: " + "; ".join(r.get("why", []))
        head = (f"trade {r['trade']['index']} {r['trade']['side']} {r['trade']['entry']}→{r['trade']['exit']} "
                f"net {r['trade']['net_pts']} pts — weakness: {r['weakness']}" if r.get("trade")
                else f"daily re-test of `{r.get('setting')}`")
        b, c = r.get("base", {}), r.get("cand", {})
        L.append(f"- {r.get('at', '')} {head}; fix `{json.dumps(r.get('fix'))}` — base {b.get('net', '-')} "
                 f"(halves {b.get('h1', '-')}/{b.get('h2', '-')}) vs fix {c.get('net', '-')} "
                 f"(halves {c.get('h1', '-')}/{c.get('h2', '-')}), neighbours "
                 f"{[n['net'] for n in r.get('nbrs', [])]} → **{verdict}**")
        if r.get("analysis"):
            L.append(f"  - {r['analysis']}")
    if st.get("history"):
        L += ["", "## Settings history", ""] + [f"- {h['at']}: `{json.dumps(h['params'])}` — {h['why']}" for h in st["history"]]
    DATA.mkdir(parents=True, exist_ok=True)
    tmp = REPORT.with_suffix(".tmp")
    tmp.write_text("\n".join(L) + "\n")
    tmp.replace(REPORT)


# ── live ─────────────────────────────────────────────────────────────────────────
class Live:
    """Follow every index's ticks in the database, paper-trade, review each trade.

    One follower thread reads the rows the Lune desk tape has written (`hhll.ticks`, by seq)
    and feeds them to one book per index; one reviewer thread reviews, re-tests, reports and
    archives finished days. Trades, pivots and reviews go to the database as they happen."""

    def __init__(self) -> None:
        self.store = store()
        migrate_files()
        self.store.archive_closed_days(today_ist())
        self.today = today_ist()
        self._state_lock = threading.RLock()
        self.state = load_state(self.today)
        self.params, applied = current_params(self.today)
        self.seq: dict = {}             # name -> last seq of today fed to its book
        self.swap: dict = {}            # name -> settings the book switches to when next flat
        self._swap_lock = threading.Lock()
        self._unsaved: deque = deque()  # closed trades the database has not taken yet
        self.reviews: queue.Queue = queue.Queue()
        self._archived = time.monotonic()
        self.books = {}
        for n in SPECS:
            seqs, ms, px = self.store.ticks(self.today, n)
            self.seq[n] = seqs[-1] if seqs else -1
            # Settings changed during today: a replay would invent trades the old settings
            # never took, so warm up flat and trade on from here. Same settings all day:
            # replaying the stored ticks rebuilds the same position.
            self.books[n] = replayed_book(SPECS[n], self.params, self.today, clear=bool(applied),
                                          ticks=(ms, px), on_pivot=self._pivot, keep=False)
            self.books[n].on_trade = self._on_trade

    def _pivot(self, b: Book, pv: Pivot) -> None:
        self.store.writer.put_pivot(hhll_store.pivot_row(b.spec.name, day_of(pv.conf_ms), b.chain, pv))

    def _on_trade(self, t: dict) -> None:
        if t["entry_ms"] < self.state.get("ledger_start_ms", 0):
            return
        self._unsaved.append(t)
        self._save_trades()

    def _save_trades(self) -> None:
        """Store closed trades in order; one the database refuses now is retried next poll."""
        while self._unsaved:
            t = self._unsaved[0]
            try:
                new = self.store.add_trade(t)
            except Exception as exc:  # noqa: BLE001 - kept; retried on the next poll
                LOG.warning("trade %s %s not stored yet: %s", t["index"], t["entry"], exc)
                return
            self._unsaved.popleft()
            if new:
                LOG.info("trade %s %s %s→%s net %.2f pts (Rs %.0f) %s", t["index"], t["side"], t["entry"],
                         t["exit"], t["net_pts"], t["net_rs"], t["reason"])
                self.reviews.put(t)

    def _roll(self, day: date) -> None:
        """A new session: adopt the settings accepted since the last one."""
        with self._state_lock:
            if day <= self.today:
                return
            self.state = load_state(day)
            self.params = Params(**self.state["active"])
            with self._swap_lock:
                self.swap.clear()
            for b in self.books.values():
                b.p = self.params
            self.today = day

    def follow(self) -> None:
        """Feed the books every new tick of the session, in seq order, as it is stored.

        Ticks are read when the recorder's NOTIFY says some were committed (or the listener
        cannot say), and at least every READ_EVERY_S regardless: no query while nothing is
        written - nights, weekends, a quiet tape."""
        day, failing, requested_at = self.today, False, 0.0
        woke, read_at = True, 0.0
        while True:
            try:
                self._save_trades()
                if time.monotonic() >= requested_at:
                    requested_at = time.monotonic() + 1.0
                    self._requested()
                read = woke or failing or time.monotonic() - read_at >= READ_EVERY_S
                rows = self.store.ticks_after(day, self.seq) if read else []
                if read:
                    read_at = time.monotonic()
            except Exception as exc:  # noqa: BLE001 - the database is down or restarting; keep trying
                if not failing:
                    LOG.warning("reading ticks: %s", exc)
                failing = True
                time.sleep(2.0)
                continue
            if failing:
                LOG.info("reading ticks again")
                failing = False
            fed = set()
            for name, seq, ms, price in rows:
                self.seq[name] = seq
                self.books[name].on_tick(float(price), int(ms))
                fed.add(name)
            for name in fed:
                self._maybe_swap(name, day)
            if len(rows) >= 50_000:
                woke = True
                continue                            # catching up: no pause
            if read and not rows and today_ist() > day:   # the old session is read to its end
                day = today_ist()
                self.seq = {n: -1 for n in SPECS}
                self._roll(day)
                woke = True
                continue
            woke = self.store.wait_ticks(POLL_S) is not False   # woken by the recorder's commit

    def _maybe_swap(self, name: str, day: date) -> None:
        """Switch a book to newly adopted settings once its position is closed (ticks fed up to
        `self.seq[name]`): when flat, or on the tick a reversal closed it, where the new settings
        make their own entry decision on that same tick.
        ponytail: checked once per poll, not per tick; a poll is a tick or a few."""
        with self._swap_lock:
            p = self.swap.get(name)
        b = self.books[name]
        just_reversed = b.pos is not None and b.pos.ms == b.last_ms and b.trades and b.trades[-1]["exit_ms"] == b.last_ms
        if p is None or b.pend is not None or (b.pos is not None and not just_reversed):
            return
        nb = None
        if b.p != p:
            _, ms, px = self.store.ticks(day, name, stop=self.seq[name] + 1)
            nb = replayed_book(SPECS[name], p, day, keep_ms=b.last_ms, ticks=(ms, px), on_pivot=self._pivot, keep=False)
            nb.on_trade = self._on_trade
        with self._swap_lock:
            if self.swap.get(name) is not p:
                return                              # newer settings came meanwhile: next poll
            del self.swap[name]
        if nb is not None:
            self.books[name] = nb
            LOG.info("%s now trades on %s", name, json.dumps(diff(b.p, p)))

    def reviewer(self) -> None:
        while True:
            try:
                t = self.reviews.get(timeout=60)
            except queue.Empty:
                t = None
            try:
                if t is not None:
                    self._review(t)
                self._intraday()
                self._daily()
                self._archive()
            except Exception:  # noqa: BLE001 - a failed review must not stop the next one
                LOG.exception("review failed")
            try:
                self._health()
            except Exception:  # noqa: BLE001 - nor may a failed retune check stop the reviews
                LOG.exception("retune check failed")
            try:
                self._report()
            except Exception:  # noqa: BLE001 - nor may the report
                LOG.exception("report not written")

    def _report(self, force: bool = False) -> None:
        """Rewrite report.md: now when `force`, else when a trade or re-test is waiting for it,
        at most every REPORT_EVERY_S (it reads the whole ledger; a busy session closes a trade
        a minute)."""
        if not force and (not getattr(self, "_report_due", False)
                          or time.monotonic() - getattr(self, "_report_at", -math.inf) < REPORT_EVERY_S):
            return
        self._report_due, self._report_at = False, time.monotonic()
        write_report()

    def _health(self) -> None:
        """Re-run the retune check when the settings in force or the finished sessions change."""
        p = current_params(today_ist())[0]
        key = (p, tuple(d for d in stored_days() if session_over(d)))
        if key == getattr(self, "_health_key", None) or time.monotonic() < getattr(self, "_health_retry", 0.0):
            return
        self._health_retry = time.monotonic() + HEALTH_RETRY_S   # a failing check is tried again then, not every loop
        rows = refresh_health(p)["rows"]
        if not in_session():                     # mid-session (a restart, a desk save): no switch now; the close
            self._auto_switch(rows, key[1])      # changes the key, and the check runs again with today's ticks in
        self._health_key, self._health_retry = key, 0.0
        self._report(force=True)

    def _auto_switch(self, rows: list, done: tuple) -> None:
        """Apply the retune check's SWITCH verdicts, each index at most once per finished session, the
        way a threshold saved on the desk is (`hhll.state` row "request", which `_requested` takes within
        a second). A verdict on a threshold that has changed since (set after the close) is left alone."""
        if not AUTO_SWITCH or not done:
            return
        last = done[-1].isoformat()
        with self._state_lock:
            seen = self.state.setdefault("auto_switch", {})
            nxt = Params(**self.state["next"])
            to = {r["index"]: float(r["switch_to"]) for r in rows
                  if r["verdict"] == "SWITCH" and seen.get(r["index"], "") < last
                  and r["threshold"] == pivot_value(nxt, r["index"])}
            if not to:
                return
            why = "retune check: " + ", ".join(f"{n} {pivot_value(nxt, n):g} -> {v:g}" for n, v in to.items())
            self.store.save_state({"fixed": {**{n: pivot_value(nxt, n) for n in SPECS}, **to}, "why": why}, "request")
            seen.update(dict.fromkeys(to, last))
            self.state.setdefault("switches", []).extend(
                {"index": n, "from": pivot_value(nxt, n), "to": v, "after": last} for n, v in to.items())
            save_state(self.state)
        LOG.info("%s", why)

    def _archive(self) -> None:
        if time.monotonic() - self._archived < ARCHIVE_EVERY_S:
            return
        self._archived = time.monotonic()
        self.store.archive_closed_days(today_ist())

    def _review(self, t: dict) -> None:
        days = stored_days()
        nxt = Params(**self.state["next"])
        r = review(t, nxt, days)
        log_review({"kind": "review", **r})
        if r.get("accepted"):
            self._adopt(Params(**r["candidate"]), f"review of {t['index']} {t['entry']}: {r['weakness']}")
        LOG.info("review %s %s: %s -> %s", t["index"], t["entry"], r["weakness"],
                 "ACCEPTED" if r.get("accepted") else "rejected (" + "; ".join(r.get("why", [])) + ")")
        self._forward()
        self._report_due = True

    def _forward(self) -> None:
        """Forward test of the last adopted change: replay it and the settings it replaced on the
        ticks since it was adopted. Behind once there is evidence -> revert. Ahead for long enough -> kept."""
        tr = self.state.get("trial")
        if not tr or day_of(tr["at_ms"]) != self.today:
            return
        new, prev = Params(**tr["new"]), Params(**tr["prev"])
        res = evaluate([new, prev], [self.today])
        since = {c: [t for t in res[c] if t["entry_ms"] >= tr["at_ms"]] for c in (new, prev)}
        n_new, n_prev = (round(sum(t["net_rs"] for t in since[c]), 2) for c in (new, prev))
        trades = max(len(since[new]), len(since[prev]))
        hours = (time.time() * 1000 - tr["at_ms"]) / 3.6e6
        verdict = None
        if trades >= MIN_TRADES or hours >= FORWARD_MIN_H:
            if n_new < n_prev:
                verdict = "did not live up"
            elif trades >= 2 * MIN_TRADES or hours >= 2 * FORWARD_MIN_H:
                verdict = "lived up"
        row = {"kind": "forward", "at": datetime.now(IST).isoformat(timespec="seconds"),
               "setting": f"forward test of {json.dumps(diff(prev, new))}", "fix": diff(prev, new),
               "since": tr["at_ms"], "hours": round(hours, 2), "trades": trades,
               "base": {"net": n_prev}, "cand": {"net": n_new}, "accepted": verdict != "did not live up",
               "why": [verdict or "still testing"]}
        LOG.info("forward test %s: new %.0f vs old %.0f over %d trades, %.1fh -> %s",
                 json.dumps(diff(prev, new)), n_new, n_prev, trades, hours, verdict or "still testing")
        if verdict is None:
            return
        log_review(row)
        if verdict == "did not live up":
            with self._state_lock:
                failed = self.state.get("forward_failed") or {}
                if failed.get("day") != self.today.isoformat():
                    failed = {"day": self.today.isoformat(), "params": []}
                failed["params"].append(asdict(new))
                self.state["forward_failed"] = failed
            self._adopt(prev, f"forward test: {json.dumps(diff(prev, new))} made Rs {n_new:,.0f} vs Rs {n_prev:,.0f} "
                              f"for the settings it replaced since adoption", trial=False)
        with self._state_lock:
            self.state["trial"] = None
            save_state(self.state)

    def _requested(self) -> None:
        """Take the thresholds set on the desk (`hhll.state` row "request"): this process alone writes the settings."""
        row = self.store.load_state("request")
        req = row.get("fixed")
        nxt = Params(**self.state["next"])
        legs = {x[0]: x[3] for x in nxt.index_pivots} | {n: float(v) for n, v in (row.get("legs") or {}).items()}
        fixed = tuple(sorted((n, nxt.thr_cost, float(v), legs.get(n, 0.0)) for n, v in (req or {}).items()))
        if fixed and fixed != nxt.index_pivots:      # "why": the retune check's own switch (`_auto_switch`)
            self._adopt(replace(nxt, index_pivots=fixed), row.get("why") or "thresholds set on the desk",
                        trial=False, by_hand=True)

    def _adopt(self, p: Params, why: str, trial: bool = True, by_hand: bool = False) -> bool:
        """Adopt now: each book switches when it is next flat. After the session: from the next one.
        A live adoption is forward-tested against the settings it replaces (`_forward`).
        Settings that failed their forward test today are not adopted again today. True if adopted."""
        if MANUAL and not by_hand:
            LOG.info("manual tuning, not adopting %s: %s", json.dumps(diff(self.params, p)), why)
            return False
        failed = self.state.get("forward_failed") or {}
        if trial and failed.get("day") == self.today.isoformat() and asdict(p) in failed.get("params", []):
            LOG.info("not adopting %s: it did not live up in its forward test today (%s)",
                     json.dumps(diff(self.params, p)), why)
            return False
        with self._state_lock:
            self.state["next"] = asdict(p)
            live = self.today == today_ist() and not session_over(self.today)
            if live:
                now_ms = int(time.time() * 1000)
                self.state["trial"] = ({"at_ms": now_ms, "prev": asdict(self.params), "new": asdict(p), "why": why}
                                       if trial and p != self.params else None)
                self.state.update(active=asdict(p), active_day=self.today.isoformat(), applied_ms=now_ms)
            self.state["history"].append({"at": datetime.now(IST).isoformat(timespec="seconds"), "params": asdict(p),
                                          "why": why + (" (applied now, each index when flat)" if live else " (from next session)")})
            save_state(self.state)
            if live:
                self.params = p
                with self._swap_lock:
                    for n in SPECS:
                        self.swap[n] = p
        LOG.info("ADOPTED %s: %s", json.dumps(asdict(p)), why)
        return True

    def _tournament(self) -> None:
        """Adopt the shadow settings that beat the live ones on ticks none of them was picked on."""
        with self._state_lock:
            sh = self.state.get("shadows")
            if sh is None:
                sh = self.state["shadows"] = {"picked": "2026-11-03", "at_ms": 0,
                                              "params": [asdict(replace(Params(), **c)) for c in SHADOW_SEED]}
            if sh["picked"] >= self.today.isoformat():
                return                           # in sample today: they compete from the next open
            if not sh["at_ms"] or day_of(sh["at_ms"]) != self.today:
                sh["at_ms"] = int(datetime.combine(self.today, datetime.min.time(), IST).timestamp() * 1000) + OPEN_S * 1000
            save_state(self.state)
        start = sh["at_ms"]
        hours = (time.time() * 1000 - start) / 3.6e6
        if hours < FORWARD_MIN_H:
            return
        live = self.params
        cands = [Params(**c) for c in sh["params"]]
        cands = [c for c in dict.fromkeys(cands) if c != live]
        res = evaluate([live, *cands], [self.today])
        fw = {c: [t for t in res[c] if t["entry_ms"] >= start] for c in (live, *cands)}
        net = {c: round(sum(t["net_rs"] for t in fw[c]), 2) for c in fw}
        best = max(cands, key=lambda c: net[c], default=None)
        row = {"kind": "tournament", "at": datetime.now(IST).isoformat(timespec="seconds"),
               "setting": "shadow tournament", "since": start, "hours": round(hours, 2),
               "base": {"net": net[live], "trades": len(fw[live])},
               "shadows": [{"params": diff(live, c), "net": net[c], "trades": len(fw[c])} for c in cands]}
        win = best is not None and net[best] > net[live] and net[best] > 0 and len(fw[best]) >= MIN_TRADES
        row.update(fix=diff(live, best) if best else None, cand={"net": net[best] if best else None},
                   accepted=win, why=["best shadow beats the live settings" if win else "no shadow ahead yet"])
        log_review(row)
        LOG.info("shadow tournament (%.1fh): live %.0f, best %s %.0f -> %s", hours, net[live],
                 json.dumps(diff(live, best)) if best else "-", net[best] if best else 0, "ADOPT" if win else "keep")
        if win and self._adopt(best, f"shadow tournament: Rs {net[best]:,.0f} vs Rs {net[live]:,.0f} for the live "
                                     f"settings over {hours:.1f}h of ticks none was picked on"):
            with self._state_lock:               # only once adopted: a winner MANUAL turned down stays a shadow
                sh["params"] = [asdict(live)] + [asdict(c) for c in cands if c != best]
                sh["at_ms"] = int(time.time() * 1000)
                save_state(self.state)

    def _intraday(self) -> None:
        """Every RETEST_EVERY_S of a live session: re-test every setting on all the ticks so far."""
        now = time.time()
        if now - getattr(self, "_last_retest", 0.0) < RETEST_EVERY_S or self.today != today_ist() \
                or session_over(self.today):
            return
        self._last_retest = now
        self._forward()                          # judged on time, too, when trades are few
        self._tournament()
        old = Params(**self.state["next"])
        days = stored_days()
        new = retest_all(old, days, log_review)
        if new != old:
            self._adopt(new, f"intraday re-test of every setting ({data_hours([self.today]):.1f}h of today)")
        LOG.info("intraday re-test: %s", json.dumps(diff(old, new)) if new != old else "unchanged")
        self._report_due = True

    def _daily(self) -> None:
        days = [d for d in stored_days() if session_over(d)]
        if not days or (self.state.get("retested") or "") >= days[-1].isoformat():
            return
        old = Params(**self.state["next"])
        new = retest_all(old, days, log_review)
        if new != old:
            self._adopt(new, f"daily re-test over {len(days)} day(s)")
        with self._state_lock:
            self.state["retested"] = days[-1].isoformat()
            save_state(self.state)
        self._report(force=True)

    def run(self) -> None:
        write_report()
        threading.Thread(target=self.follow, name="hhll-follow", daemon=True).start()
        self.reviewer()


# ── self-check ───────────────────────────────────────────────────────────────────
def reference_pivots(prices: list, R: float, leg: float = 0.0) -> list[tuple]:
    """The pivot rules written out literally (O(n^2)), to check `ZigZag` against."""
    out, start, want, prev, level, last = [], 0, None, None, None, -1
    minor = reference_pivots(prices, R) if leg else []

    def spot(kind: str, j: int, i: int, x: float) -> tuple:
        got = [m for m in minor if m[0] == kind and m[3] <= j]      # known by tick j
        return (got[-1][2], got[-1][1]) if got and got[-1][2] > last else (i, x)

    for j in range(len(prices)):
        w = prices[start:j + 1]
        hi, lo = max(w), min(w)
        p = prices[j]
        high_due = hi - p >= R - 1e-9 and (prev is None or hi - prev >= leg - 1e-9)
        low_due = p - lo >= R - 1e-9 and (prev is None or prev - lo >= leg - 1e-9)
        if want in (None, "HH") and (high_due or (prev is not None and p < level)):
            i, x = (start + w.index(hi), hi) if high_due else spot("HH", j, start + w.index(hi), hi)
            out.append(("HH", x, i, j, not high_due))
            start, want, prev, level, last = j, "LL", x, hi, i
        elif want in (None, "LL") and (low_due or (prev is not None and p > level)):
            i, x = (start + w.index(lo), lo) if low_due else spot("LL", j, start + w.index(lo), lo)
            out.append(("LL", x, i, j, not low_due))
            start, want, prev, level, last = j, "HH", x, lo, i
    return out


def check_pivots(prices: list, R: float, pivots: list[Pivot], leg: float = 0.0) -> None:
    """Proof of every pivot rule on one tick series: extremes, minimum leg, no stall."""
    kinds = [p.kind for p in pivots]
    assert set(kinds) <= {"HH", "LL"} and all(a != b for a, b in zip(kinds, kinds[1:])), "alternation"
    prev = None
    for n, p in enumerate(pivots):
        sign = 1 if p.kind == "HH" else -1
        start = prev.idx if prev else 0
        nxt = pivots[n + 1].idx if n + 1 < len(pivots) else p.conf_idx
        seg = prices[start:max(nxt, p.conf_idx) + 1]               # pivot before to pivot after, both ends
        assert p.level == (max(seg) if sign > 0 else min(seg)), f"{p} level not the true extreme"
        assert p.price == prices[p.idx] and (p.forced or p.price == p.level), f"{p} not on its extreme"
        assert start <= p.idx < p.conf_idx, f"{p} out of order"
        broke = prev is not None and sign * (prev.level - p.conf_price) > 1e-9
        due = sign * (p.price - p.conf_price) >= R - 1e-9 and (prev is None or p.leg >= leg - 1e-9)
        assert p.forced == (not due) and (due or broke), f"{p} printed with no rule met"
        run = prices[start]
        for j in range(start, p.conf_idx):                         # no earlier tick met either rule
            run = max(run, prices[j]) if sign > 0 else min(run, prices[j])
            if prev is not None and j >= prev.conf_idx:
                assert sign * (prev.level - prices[j]) <= 1e-9, f"{p} late: tick {j} broke {prev}"
                assert not (sign * (run - prices[j]) >= R - 1e-9 and abs(run - prev.price) >= leg - 1e-9), \
                    f"{p} late: tick {j} met it"
        if prev is not None:
            assert abs(p.leg - abs(p.price - prev.price)) < 1e-9 and (p.forced or p.leg >= leg - 1e-9), "leg"
        prev = p
    if pivots:                                                     # nothing is due after the last pivot
        last = pivots[-1]
        sign = 1 if last.kind == "HH" else -1                      # searching the opposite now
        run = prices[last.conf_idx]
        for j in range(last.conf_idx, len(prices)):
            run = min(run, prices[j]) if sign > 0 else max(run, prices[j])
            assert sign * (prices[j] - last.level) <= 1e-9, "a broken pivot was not answered"
            assert not (sign * (prices[j] - run) >= R - 1e-9 and abs(run - last.price) >= leg - 1e-9), \
                "a due pivot was not confirmed"


def _zz(prices, R, leg=0.0):
    z, out = ZigZag(R, leg), []
    for i, x in enumerate(prices):
        p = z.update(x, 1_700_000_000_000 + i)
        if p:
            out.append(p)
    return out


def selfcheck(real: bool = True) -> None:
    import random
    rnd = random.Random(7)
    # 1. pivots: literal definition, invariants, no repaint, on random walks with gaps and ties
    for trial in range(60):
        R = rnd.choice([0.5, 1.0, 2.5, 3.0])
        px, x = [], 100.0
        for _ in range(rnd.randint(50, 1500)):
            x = round(x + rnd.choice([-1, -0.5, 0, 0, 0.5, 1]) * rnd.choice([0.5, 1, 1, 1, 4]), 2)
            px.append(x)
        piv = _zz(px, R)
        assert [(p.kind, p.price, p.idx, p.conf_idx, p.forced) for p in piv] == reference_pivots(px, R), f"trial {trial}"
        assert not any(p.forced for p in piv), "one threshold: nothing to invalidate"
        check_pivots(px, R, piv)
        for cut in (len(px) // 3, len(px) // 2, len(px) - 1):     # a prefix never sees a different past
            assert _zz(px[:cut], R) == [p for p in piv if p.conf_idx < cut], "repainted"
        for leg in (R * 2, R * 6):                                 # the same proofs with a minimum leg
            piv = _zz(px, R, leg)
            assert [(p.kind, p.price, p.idx, p.conf_idx, p.forced) for p in piv] == reference_pivots(px, R, leg), f"leg {trial}"
            check_pivots(px, R, piv, leg)
            assert _zz(px[:len(px) // 2], R, leg) == [p for p in piv if p.conf_idx < len(px) // 2], "repainted"
    # exact-tick confirmation and true extremes on a hand-made path
    piv = _zz([100, 101, 103, 102.1, 101, 100, 99, 100.5, 101.9, 102], 2.0)
    assert [(p.kind, p.price, p.conf_idx) for p in piv] == [("LL", 100, 2), ("HH", 103, 4), ("LL", 99, 8)], piv
    # 2. charges, by hand: NIFTY 65 x premium 100 = turnover 6,500
    b = order_charges("NFO", True, 65, 100.0)
    assert (b["brokerage"], b["txn"], b["sebi"], b["stamp"], b["stt"], b["gst"]) == (
        Decimal("5.00"), Decimal("2.31"), Decimal("0.01"), Decimal("0.20"), 0, Decimal("1.32")), b
    s = order_charges("NFO", False, 65, 100.0)
    assert s["stt"] == Decimal("9.75") and s["stamp"] == 0 and sum(s.values()) == Decimal("18.39"), s
    assert order_charges("BFO", True, 20, 200.0)["txn"] == Decimal("1.30")
    assert order_charges("NFO", True, 3600, 10.0, orders=2)["brokerage"] == Decimal("10.00")  # freeze slices
    rt = round_trip_charges(SPECS["NIFTY"], 1, 1, 25000, 25000, 1793680546432, 1793680546432)
    assert rt["orders"] == 4 and rt["brokerage"] == 20.0 and rt["stt"] > 0 and rt["stamp"] > 0
    assert next_expiry("NIFTY", date(2026, 11, 3)) == date(2026, 11, 9)       # expiry day rolls; 10-Nov holiday -> 9th
    assert next_expiry("NIFTY", date(2026, 11, 2)) == date(2026, 11, 3)       # the day before: tomorrow
    assert next_expiry("BANKNIFTY", date(2026, 11, 3)) == date(2026, 11, 23)  # 24-Nov holiday -> Mon 23rd
    # 3. session rules and trading on a synthetic day: entries only in hours, none after the
    #    cutoff, flat by square-off, alternating sides, nothing overnight
    t0 = int(datetime(2026, 11, 2, 9, 0, tzinfo=IST).timestamp() * 1000)
    ms, px, x = [], [], 25000.0
    for i in range(int(6.5 * 3600 * 2)):                           # 09:00-15:30 at 2 ticks/s
        x = round(x + rnd.choice([-1, 1]) * rnd.choice([0.05, 0.5, 1, 2, 5]), 2)
        ms.append(t0 + i * 500)
        px.append(x)
    fed = [y for m, y in zip(ms, px) if OPEN_S <= (m // 1000 + IST_OFFSET_S) % 86400 < CLOSE_S]
    ms += [t0 + 7 * 3600 * 1000 + i * 500 for i in range(1000)]   # 16:00: after the close
    px += [x + i for i in range(1000)]
    for prm in (Params(thr_cost=6), Params(thr_cost=6, order="limit"), Params(thr_cost=6, lock_at=1.0, stop=0.5, chop_k=2.0),
                Params(thr_cost=6, target=1.5, leg_k=2.0)):
        b = Book(SPECS["NIFTY"], prm)
        for m, y in zip(ms, px):
            b.on_tick(y, m)
        assert b.pos is None, "held after square-off"
        assert b.trades, "no trades on a busy synthetic day"
        for t in b.trades:
            assert OPEN_S <= t["entry_sod"] < hhmm(prm.last_entry), t
            assert (t["exit_ms"] // 1000 + IST_OFFSET_S) % 86400 <= SQUARE_OFF_S + 1, t
            assert abs(t["net_rs"] - (t["gross_pts"] * t["units"] - t["charges_rs"])) < 0.02
        check_pivots(fed, b.R, b.pivots)                        # the book's own pivots obey the rules
        rev = [t for t in b.trades if t["reason"] == "reverse"]
        assert all(t["side"] == ("SHORT" if t["pivot"] == "HH" else "LONG") for t in b.trades)
        assert prm.order == "limit" or all(t["friction_pts"] > 0 for t in b.trades), "market pays spread"
        assert rev, "no stop-and-reverse"
    # 4. acceptance rule: halves split by day; one day is never enough; neighbours exclude base/off
    p = Params()
    ds = [date(2026, 1, i) for i in (1, 2, 3, 4)]
    sc = score([{"day": "2026-01-01", "net_rs": 5.0}, {"day": "2026-01-04", "net_rs": -2.0}], ds)
    assert (sc["net"], sc["h1"], sc["h2"], sc["wins"]) == (3.0, 5.0, -2.0, 1), sc
    saved = STORE
    use_store(_NoTicks())                                           # a day without ticks, no database
    try:
        one = judge(p, replace(p, lock_at=2.0), [replace(p, lock_at=1.5)], [date(2000, 1, 3)])
    finally:
        use_store(saved)
    assert not one["accepted"] and any("h of ticks" in w for w in one["why"]), one
    sp = score([{"day": "2026-01-01", "net_rs": 5.0, "entry_ms": 10}, {"day": "2026-01-01", "net_rs": -2.0, "entry_ms": 30}],
               ds[:1], split_ms=20)
    assert (sp["net"], sp["h1"], sp["h2"]) == (3.0, 5.0, -2.0), sp      # one day: halves by the clock
    assert neighbours(p, replace(p, lock_at=2.0), "lock_at") == [replace(p, lock_at=1.5), replace(p, lock_at=2.5)]
    assert neighbours(p, replace(p, thr_cost=8), "thr_cost") == [replace(p, thr_cost=6)]
    # 5. retune check: alarms only against the index's own record; a switch only with margin, both
    #    halves, enough trades and a neighbour; too little data says so; the replay is the trading Book
    assert health_alarms([100, -50], [], [50, 0, 0, 0, 0]) == ["drawdown Rs 50 is the deepest in 5 sessions"]
    assert health_alarms([100, -50], [], [50, 0, 0, 0]) == []                # too little history to call it deepest
    assert health_alarms([100, -150, 200, -20], [], [130] * 5) == []        # shallower than the old one
    good = [30, -10, 25, -10] * 15                                            # pf 2.6, steady
    assert health_alarms(good + good[:20], [], [1]) == []                   # the same as its record: quiet
    a = health_alarms(good + [5, -20] * 10, [], [1])                          # pf 0.25 after pf 2.6
    assert len(a) == 1 and a[0].startswith("profit factor 0.25 over the last 20 trades, below "), a
    assert health_alarms([5, -20] * 10, [], [1]) == []                      # no earlier record: no verdict
    assert health_alarms([], [2.5, 2.0, 3.0] * 20 + [1.1] * 15, [1]) and \
        "choppier than usual" in health_alarms([], [2.5, 2.0, 3.0] * 20 + [1.1] * 15, [1])[0]
    assert health_alarms([], [1.1, 1.2, 1.3] * 20 + [1.2] * 15, [1]) == []   # always this choppy: its normal
    assert health_alarms([], [], [5, -1, -1, -1]) == ["3 losing sessions in a row"]
    assert health_alarms([], [], [5, -1, -1]) == []
    assert _low_vs_own([1.0] * 5, [2.0] * 10, statistics.median) == (1.0, 2.0)
    assert _low_vs_own([1.0] * 5, [2.0] * 9, statistics.median) is None    # earlier must be 2x as long
    sc = lambda net, h1, h2, n=30: {"net": net, "h1": h1, "h2": h2, "trades": n}
    nb = {15: [12, 18], 18: [15, 20], 20: [18, 25], 12: [10, 15], 25: [20, 30]}
    base = {18: sc(100, 50, 50), 12: sc(90, 45, 45), 25: sc(90, 45, 45)}
    assert health_contest(18, {**base, 15: sc(300, 250, 50), 20: sc(300, 150, 150)}, nb, [15, 18, 20])[0] is None
    assert health_contest(18, {**base, 15: sc(300, 150, 150), 20: sc(90, 45, 45), 12: sc(120, 60, 60)}, nb, [15, 20])[0] == 15
    assert health_contest(18, {**base, 15: sc(300, 150, 150, 5), 12: sc(120, 60, 60), 20: sc(1, 0, 0)}, nb, [15])[0] is None
    assert health_contest(18, {**base, 15: sc(100, 50, 50), 12: sc(120, 60, 60), 20: sc(1, 0, 0)}, nb, [15])[0] is None  # tie
    assert health_contest(18, {**base, 15: sc(109, 55, 54), 12: sc(120, 60, 60), 20: sc(1, 0, 0)}, nb, [15])[0] is None  # < 10%
    assert health_contest(18, {**base, 15: sc(111, 56, 55), 12: sc(120, 60, 60), 20: sc(1, 0, 0)}, nb, [15])[0] == 15
    assert health_contest(18, {**base, 15: sc(300, 150, 150), 12: sc(100, 60, 60), 20: sc(1, 0, 0)}, nb, [15])[0] is None  # nbr tie
    assert health_contest(40, {40: sc(100, 50, 50), 50: sc(300, 150, 150)}, {50: [40]}, [50])[1] == [
        "50: no neighbour to confirm it (grid end)"]
    assert health_contest(-5, {-5: sc(-100, -50, -50), 1: sc(-80, -40, -40), 2: sc(0, 0, 0)}, {1: [-5, 2]}, [1])[0] == 1
    lucky = {**base, 15: {**sc(300, 150, 150), "conf": 0.66}, 12: sc(120, 60, 60), 20: sc(1, 0, 0)}
    assert health_contest(18, lucky, nb, [15]) == (None, [
        "15: ahead in only 66% of resampled sessions, 90% needed (a day or two carry it)"])
    assert health_contest(18, {**lucky, 15: {**sc(300, 150, 150), "conf": 0.95}}, nb, [15])[0] == 15
    assert day_conf([0] * 6, [10] * 6) == 1.0 and day_conf([10] * 6, [0] * 6) == 0.0 and day_conf([], []) == 0.0
    assert 0.6 < day_conf([0] * 6, [600, -1, -1, -1, -1, -1]) < 0.7           # one lucky day: 1 - (5/6)^6
    assert day_conf([0] * 10, [10] * 8 + [-10] * 2) >= HEALTH_CONF > day_conf([0] * 10, [10] * 6 + [-10] * 4)
    q = Params(index_pivots=(("BANKNIFTY", 18.0, 50.0, 0.0), ("NIFTY", 18.0, 18.0, 0.0), ("SENSEX", 12.0, 0.0, 0.0)))
    assert pivot_value(q, "BANKNIFTY") == 50 and pivot_value(q, "SENSEX") == 12 and pivot_value(Params(thr_cost=8), "NIFTY") == 8
    assert with_pivot_value(q, "NIFTY", 15).index_pivots == (q.index_pivots[0], ("NIFTY", 18.0, 15.0, 0.0), q.index_pivots[2])
    assert with_pivot_value(q, "SENSEX", 15).index_pivots[2] == ("SENSEX", 12.0, 15.0, 0.0)   # R (x[1]) is kept
    assert pivot_value(with_pivot_value(Params(thr_cost=8), "NIFTY", 12), "NIFTY") == 12
    assert pivot_value(with_pivot_value(Params(thr_cost=8), "NIFTY", 12), "SENSEX") == 8
    for name in ("NIFTY", "SENSEX"):        # the desk's own path builds the same settings
        assert pivot_value(replace(q, index_pivots=tuple(sorted((n, q.thr_cost, float(v), 0.0) for n, v in
                                                                 {**{x[0]: pivot_value(q, x[0]) for x in q.index_pivots}, name: 15}.items()))), name) == 15
    # health() itself on a fake store: 12 sessions; NIFTY trends, SENSEX has only the first day,
    # BANKNIFTY's feed dies at 11:00 on the last day; no crash, honest verdicts, same as a full replay
    fake = _FakeTicks(rnd)
    saved = STORE
    use_store(fake)
    try:
        pq = Params(index_pivots=(("BANKNIFTY", 18.0, 50.0, 0.0), ("NIFTY", 18.0, 18.0, 0.0)))
        rows = {r["index"]: r for r in health(pq, fake.days())}
        assert rows["SENSEX"]["verdict"] == "NO DATA" and rows["SENSEX"]["sessions"] == 1, rows["SENSEX"]
        assert rows["FINNIFTY"]["verdict"] == "NO DATA" and rows["FINNIFTY"]["sessions"] == 0
        assert rows["BANKNIFTY"]["sessions"] == 11 and rows["BANKNIFTY"]["window"] == 9, rows["BANKNIFTY"]
        assert set(rows["BANKNIFTY"]["scores"]) == {"25", "30", "40", "50", "60", "75", "100"}   # 50 is no dead end
        assert all(not w.startswith(("25:", "50:")) for w in rows["BANKNIFTY"]["why_not"])   # 25 only vouches
        assert rows["BANKNIFTY"]["edge"] is None, rows["BANKNIFTY"]                 # only 3 and 100 are the ends
        r = rows["NIFTY"]
        assert set(r["scores"]) == {"10", "12", "15", "18", "20", "25", "30"} and r["window"] == 10, r
        assert r["verdict"] in ("OK", "ALARM", "SWITCH", "NOT ENOUGH DATA") and r["note"], r
        full = 0.0                                                                   # independent full replay
        for d in fake.days()[-10:]:
            for t in replay_day(pq, d, {"NIFTY": fake.day_ticks(d)["NIFTY"]}):
                full += t["net_rs"]
        assert abs(r["scores"]["18"]["net"] - full) < 0.01, (r["scores"]["18"], full)
        assert health(pq, fake.days()) == list(rows.values())                        # cached: identical
        few = {x["index"]: x for x in health(pq, fake.days()[:5])}["NIFTY"]
        assert few["verdict"] in ("NOT ENOUGH DATA", "ALARM") and few["switch_to"] is None, few
        assert health_lines({"at": "x", "rows": list(rows.values())})[0] == "## Retune check (x)"
        sw = switch_record(pq, [{"index": "NIFTY", "from": 18.0, "to": 20.0, "after": fake.days()[-3].isoformat()}])
        since = sum(t["net_rs"] for d in fake.days()[-2:] for t in replay_day(
            with_pivot_value(pq, "NIFTY", 20), d, {"NIFTY": fake.day_ticks(d)["NIFTY"]}))
        assert sw[0]["sessions"] == 2 and abs(sw[0]["new"] - since) < 0.01, (sw, since)   # only the sessions after it
        assert "| NIFTY | 18 -> 20 |" in "\n".join(health_lines({"at": "x", "rows": list(rows.values()), "switches": sw}))
        assert health_lines({}) == []
    finally:
        use_store(saved)
    # 6. the tick archive codec: lossless on paise prices, on any float, on an empty day
    for px in ([25000.05, 25000.1, 24999.95, 25000.1], [0.1 + 0.2, 1e-7, 123.456789, 5.0], []):
        sq = [3 + i + (i > 1) for i in range(len(px))]               # a gap in the seqs survives too
        ms = [1_793_680_546_432 + 7 * i - (i == 2) * 20 for i in range(len(px))]   # and a clock step back
        assert hhll_store.decode_ticks(hhll_store.encode_ticks(sq, ms, px)) == (sq, ms, px)
    # 7. the stored real ticks: every pivot rule, on every index, every day
    if real:
        for d in stored_days():
            for n, (ms, px) in store().day_ticks(d).items():
                if n not in SPECS or not px:
                    continue
                assert all(a <= b for a, b in zip(ms, ms[1:])), f"{d} {n} ticks out of order"
                for bps in (3, 10):
                    R = max(0.05, round(bps * 1e-4 * px[0] / 0.05) * 0.05)
                    piv = _zz(px, R)
                    check_pivots(px, R, piv)
                    k = min(len(px), 4000)
                    assert [(q.kind, q.price, q.idx, q.conf_idx, q.forced) for q in piv if q.conf_idx < k] == \
                        reference_pivots(px[:k], R)
                    print(f"  {d} {n}: {len(px)} ticks, {len(piv)} pivots at R={R:.2f}: all rules hold")
                c = cost_pts(SPECS[n], px[0], ms[0])                # and on the settings' own threshold and leg
                R, leg = 4 * c, 24 * c
                piv = _zz(px, R, leg)
                check_pivots(px, R, piv, leg)
                k = min(len(px), 4000)
                assert [(q.kind, q.price, q.idx, q.conf_idx, q.forced) for q in piv if q.conf_idx < k] == \
                    reference_pivots(px[:k], R, leg)
                print(f"  {d} {n}: {len(piv)} pivots ({sum(q.forced for q in piv)} on a break) at {R:.2f}/{leg:.2f}: all rules hold")
    print("hhll_tick self-check ok")


class _FakeTicks:
    """selfcheck's stand-in store for the retune check: 12 past sessions of synthetic ticks."""

    def __init__(self, rnd) -> None:
        self._d = [date(2001, 1, 1) + timedelta(days=i) for i in range(16) if (date(2001, 1, 1) + timedelta(days=i)).weekday() < 5][:12]
        self._t = {}
        for k, d in enumerate(self._d):
            t0 = int(datetime(d.year, d.month, d.day, 9, 15, tzinfo=IST).timestamp() * 1000)
            out = {}
            for n, x, step in (("NIFTY", 25000.0, 2.0), ("BANKNIFTY", 52000.0, 6.0), ("SENSEX", 80000.0, 6.0)):
                if n == "SENSEX" and k:
                    continue
                end = 6.25 * 3600 if not (n == "BANKNIFTY" and k == len(self._d) - 1) else 1.75 * 3600
                ms, px = [], []
                drift = rnd.choice([-1, 1]) * 0.15
                for i in range(int(end)):                                   # 1 tick/s
                    x = round(x + (rnd.gauss(drift, 1)) * step, 2)
                    ms.append(t0 + i * 1000)
                    px.append(x)
                out[n] = (ms, px)
            self._t[d] = out

    def days(self):
        return list(self._d)

    def day_ticks(self, day):
        return self._t.get(day, {})


class _NoTicks:
    """selfcheck's stand-in store: a day with no ticks."""

    @staticmethod
    def day_ticks(day):
        return {}

    @staticmethod
    def span(day):
        return (0, 0)


def main(argv: list[str]) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    cmd = argv[1] if len(argv) > 1 else "live"
    if cmd == "selfcheck":
        selfcheck()
    elif cmd == "report":
        write_report()
        print(REPORT)
    elif cmd == "retest":
        days = stored_days()
        st = load_state(today_ist())
        p = Params(**st["next"])
        new = retest_all(p, days, lambda r: (log_review(r), print(json.dumps(r, default=str)[:400])))
        print("settings:", diff(p, new) or "unchanged")
        write_report()
    elif cmd == "health":
        for r in refresh_health()["rows"]:
            print(f"{r['index']:11} {r['threshold']:>4g}  {r['verdict']:8} {r.get('note', '')}")
            print(f"{'':13}window: " + ", ".join(f"{v}: {s['net']:,.0f}" for v, s in r["scores"].items()))
        write_report()
    elif cmd == "migrate":
        print(json.dumps(migrate_files() or "nothing to move"))
    elif cmd == "archive":
        print(json.dumps(store().archive_closed_days(today_ist())))
    elif cmd == "live":
        Live().run()
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main(sys.argv)
