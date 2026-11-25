"""The HH/LL tick engine of Lune (`hhll_tick.py`), vendored for backtesting and tuning.

Copied from Lune/hhll_tick.py (pivots, costs, `Book`) so experiments here never touch the
live trader. `python selfcheck.py` proves this copy trades exactly as Lune's own `Book` with
`fill="same"`; a deliberate change here shows up there as a parity difference.

What this copy adds (nothing else differs):
  fill        "next" (default): a market order decided on a tick fills on the first later tick
              stamped more than `latency_ms` after it, at that tick's price ± friction (a tick
              with the deciding tick's own millisecond is not "later": nobody reacts in 0 ms).
              "same": fills at the deciding tick's own price, exactly as Lune's paper book.
  latency_ms  "next" only: see above.
  needle      "off" | "giveback" | "entry" | "stall". Else every pivot starts a needle at its confirming
              tick (the dash, price D): after an HH (short) it follows the lowest price since, after
              an LL (long) the highest (E). Its move is |E - D|. Once the move has reached
              `needle_arm` x R, the needle ends on the first tick that gives back
                giveback: `needle_give` of the move (it locks the rest)
                entry:    all of it, price back at D (the entry is protected, the rest runs)
                stall:    `needle_give` of the move AND no new extreme for `needle_wait_s`
                          (winners make new extremes; a needle that stalls and bleeds back is over)
              and at the latest at the next pivot. A trade opened on the pivot closes with the
              needle ("needle", or "reverse" at the pivot whatever `exit_on` says). Each finished
              needle lands in `Book.needles`.
Every decision reads only ticks up to and including the one it is made on (no lookahead).
"""

from __future__ import annotations

import math
import statistics
import sys
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Callable, Optional

LUNE = Path(__file__).resolve().parent.parent
if str(LUNE) not in sys.path:
    sys.path.insert(0, str(LUNE))

import config        # noqa: E402  Lune's expiry calendar and holidays
import instruments   # noqa: E402  Lune's lot sizes and exchanges

IST = timezone(timedelta(hours=5, minutes=30))
IST_OFFSET_S = 19800
OPEN_S = 9 * 3600 + 15 * 60          # 09:15 market open
CLOSE_S = 15 * 3600 + 30 * 60        # 15:30 market close
SQUARE_OFF_S = 15 * 3600 + 10 * 60   # flat by 15:10, ahead of Shoonya's ~15:20 MIS cut
LAST_ENTRY_S = 15 * 3600             # backtester: no new entry from 15:00, whatever last_entry says
CIRCUIT = 0.095      # NSE market-wide breaker halts at 10%; stop trading at 9.5%
CHOP_N = 4           # legs the chop filter looks back over
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
    fill: str = "next"          # backtester only: "next" = a market order fills on a later tick | "same" = Lune paper
    latency_ms: float = 0.0     # backtester only, fill="next": fill on a tick stamped more than this after the decision
    needle: str = "off"         # backtester only: "off" | "giveback" | "entry" | "stall", see the module docstring
    needle_arm: float = 1.0     # backtester only: the needle's move, x R, before it can end
    needle_give: float = 0.5    # backtester only, "giveback" | "stall": the share of the move given back that ends it
    needle_wait_s: float = 1800.0  # backtester only, "stall": seconds without a new extreme before it can end

    def __post_init__(self) -> None:    # lists when read back from hhll.state; settings must stay hashable
        object.__setattr__(self, "index_pivots", tuple(tuple(x) for x in self.index_pivots))

    def fixed_of(self, name: str) -> Optional[float]:
        """This index's own threshold, x cost (`index_pivots`), or None."""
        return next((x[2] for x in self.index_pivots if x[0] == name), None)


#: Lune's re-test grid; the tuner offers these as default sweep values.
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
    "needle": ["off", "giveback", "entry", "stall"],  # backtester only
    "needle_arm": [0.5, 1.0, 1.5, 2.0, 3.0],
    "needle_give": [0.3, 0.5, 0.7, 1.0],
    "needle_wait_s": [600, 1200, 1800, 2400],
}


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
    __slots__ = ("side", "px", "ms", "sod", "pivot", "fric", "peak", "trough", "chop", "sig")

    def __init__(self, side, px, ms, sod, pivot, fric, chop):
        self.side, self.px, self.ms, self.sod, self.pivot, self.fric, self.chop = side, px, ms, sod, pivot, fric, chop
        self.peak = self.trough = 0.0
        self.sig = ms           # the tick the entry was decided on (px is None until a "next" fill)


class Book:
    """One index, one session at a time. Feed ticks in order; closed trades land in `trades`.

    `keep=False` (a live book): every trade and pivot goes to `on_trade` / `on_pivot` (the
    database) and only the latest of each is held in `trades` / `pivots`."""

    def __init__(self, spec: Spec, params: Params, on_trade: Optional[Callable[[dict], None]] = None,
                 on_pivot: Optional[Callable[["Book", Pivot], None]] = None, keep: bool = True) -> None:
        self.spec, self.p, self.on_trade, self.on_pivot = spec, params, on_trade, on_pivot
        self.trades = [] if keep else deque(maxlen=1)
        self.pivots = [] if keep else deque(maxlen=1)
        self.needles = [] if keep else deque(maxlen=1)
        self.needle: Optional[dict] = None   # the live needle of the latest pivot (p.needle on)
        self.day = None
        self.pos: Optional[_Pos] = None
        self.last_px = self.last_ms = None
        self.blocked = {"cutoff": 0, "chop": 0, "margin": 0, "circuit": 0}
        self.q: deque = deque()  # fill="next": (open|close, position, reason, decision ms), oldest first

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
        self.cut = min(hhmm(self.p.last_entry), LAST_ENTRY_S)
        self.fric = friction_pts(self.spec)

    def on_tick(self, price: float, ms: int) -> None:
        s = ms // 1000 + IST_OFFSET_S
        day, sod = divmod(s, 86400)
        if day != self.day:
            self.finish("data_end")
            self._new_day(day, price, ms)
        self.last_px, self.last_ms = price, ms
        if self.q:                            # orders decided on earlier ticks fill at this one
            self._fill(price, ms)
        if sod < OPEN_S or sod >= CLOSE_S:
            return
        pos = self.pos
        if pos is not None and pos.px is not None:
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
        if pos is not None and pos.px is not None:   # an entry not filled yet has no profit to manage
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
        if self.pos is not None and self.pos.side != want and (self.p.needle != "off" or
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
        n = self.needle
        if n is not None:                     # the needle reads this tick before any new pivot does
            if n["side"] * (price - n["ext"]) > 0:
                n["ext"], n["ext_ms"] = price, ms
            elif self._needle_over(n, price, ms):
                self._needle_end(price, ms, "fade")
        pv = self.zz.update(price, ms)
        if pv is not None:
            self.pivots.append(pv)
            if self.on_pivot:
                self.on_pivot(self, pv)
            if pv.leg is not None:
                self.legs.append(pv.leg)
            if self.p.needle != "off":
                if self.needle is not None:
                    self._needle_end(price, ms, "pivot")
                self.needle = {"kind": pv.kind, "side": -1 if pv.kind == "HH" else 1, "ms": pv.ms, "price": pv.price,
                               "conf_ms": ms, "conf_px": price, "ext": price, "ext_ms": ms}
        return pv

    def _needle_over(self, n: dict, price: float, ms: int) -> bool:
        """This tick ends the needle `n` (its extreme already updated, and not new on this tick)."""
        p, side = self.p, n["side"]
        move = side * (n["ext"] - n["conf_px"])
        if move < p.needle_arm * self.R - 1e-9:
            return False
        if p.needle == "stall" and ms - n["ext_ms"] < p.needle_wait_s * 1000:
            return False
        give = 1.0 if p.needle == "entry" else p.needle_give
        return side * (n["ext"] - price) >= give * move - 1e-9

    def _needle_end(self, price: float, ms: int, why: str) -> None:
        """The live needle is over: on a fade its trade (opened on its pivot) closes, and a
        resting limit entry for it is dropped. At a pivot the reversal closes the trade."""
        n, self.needle = self.needle, None
        n.update(end_ms=ms, end_px=price, why=why, mode=self.p.needle)
        self.needles.append(n)
        if why == "fade":
            self.pend = None
            if self.pos is not None and self.pos.side == n["side"]:
                self._close(price, ms, "needle")

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
            self._open(side, limit, ms, sod, pv, 0.0, chop, market=False)
        elif ms >= until:                                     # not filled in time: go at market
            self.pend = None
            self._open(side, price + side * self.fric, ms, sod, pv, self.fric, chop)

    def _open(self, side, px, ms, sod, pv, fric, chop, market: bool = True) -> None:
        if market and self.p.fill == "next":   # price unknown until a later tick: see _fill
            self.pos = _Pos(side, None, ms, sod, pv, fric, chop)
            self.q.append(("open", self.pos, None, ms))
            return
        self.pos = _Pos(side, round(px, 2), ms, sod, pv, fric, chop)

    def _close(self, price: float, ms: int, reason: str) -> None:
        pos = self.pos
        self.pos = None
        if self.p.fill == "next":
            self.q.append(("close", pos, reason, ms))
        else:
            self._settle(pos, price, ms, reason, ms)

    def _fill(self, price: float, ms: int, force: bool = False) -> None:
        """fill="next": fill every queued order this tick may fill, oldest first, at its price.
        Only ever called before this tick's own decisions are made, so a fill is always on a
        tick after its decision. `force`: the data ended, the last price is the only one left."""
        while self.q and (force or ms > self.q[0][3] + self.p.latency_ms):
            kind, pos, reason, sig = self.q.popleft()
            if kind == "open":
                pos.px, pos.ms = round(price + pos.side * pos.fric, 2), ms
                pos.sod = (ms // 1000 + IST_OFFSET_S) % 86400
            else:
                self._settle(pos, price, ms, reason, sig)

    def _settle(self, pos: _Pos, price: float, ms: int, reason: str, sig: int) -> None:
        sp = self.spec
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
            "signal_ms": pos.sig, "exit_signal_ms": sig,
        }
        self.trades.append(t)
        if self.on_trade:
            self.on_trade(t)

    def finish(self, reason: str) -> None:
        """Close what is still open at the last price (data stopped before square-off)."""
        self.pend = None
        if self.needle is not None:           # still running when the data stopped
            self._needle_end(self.last_px, self.last_ms, "end")
        if self.pos is not None and self.last_px is not None:
            self._close(self.last_px, self.last_ms, reason)
        if self.q:
            self._fill(self.last_px, self.last_ms, force=True)
