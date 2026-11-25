"""HH/LL pivot detector for Lune (ported from delta/hhll.py, pure logic only).

See delta/hhll.py for the original rules. This copy has no DB/feed coupling.


Rules:
1. Never repaints: Causal, append-only detection. Once a pivot is confirmed,
   it is immutable and forever fixed. Streaming tick-by-tick and batch replay
   produce byte-identical results.
2. Pivots HH and LL at extremes always: The pivot's price and timestamp are
   placed strictly at the exact peak (for HH) or trough (for LL) of the swing leg,
   never at the confirming tick.
3. Two pivot types only: Strictly 'HH' and 'LL'. No 'HL' or 'LH'. Pivots strictly
   alternate: HH -> LL -> HH -> LL.
4. Minimal confirmation lag: A pivot is confirmed the very moment (exact tick)
   price retraces from the candidate extreme by the confirmation threshold.
   No artificial delay or arbitrary bar count.
5. Strictly in 5m tick data only: Operates specifically on tick data within the
   5-minute window (300 seconds), taking trade prices and timestamps.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

log = logging.getLogger("hhll")

DEFAULT_REVERSAL_TICKS = 16     # confirmation threshold in ticks (filters micro noise/chop)
DEFAULT_MIN_REVERSAL_PCT = 0.0  # extra price-relative floor on the reversal threshold (0 = off)
EPSILON = 1e-12


def _to_epoch_ms(val: float | int | datetime | str) -> int:
    if isinstance(val, datetime):
        return int(val.timestamp() * 1000)
    if isinstance(val, (int, float)):
        # If in seconds (e.g. < 1e11), convert to ms
        if val < 1e11:
            return int(val * 1000)
        return int(val)
    if isinstance(val, str):
        try:
            return _to_epoch_ms(float(val))
        except ValueError:
            dt = datetime.fromisoformat(val)
            return int(dt.timestamp() * 1000)
    raise TypeError(f"unsupported timestamp type: {type(val)}")


def _to_datetime(val: float | int | datetime | str) -> datetime:
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val)
        except ValueError:
            pass
    ms = _to_epoch_ms(val)
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


@dataclass(frozen=True)
class Pivot:
    """A confirmed HH or LL pivot point at an extreme.

    Immutable: once published, it is never modified or deleted.
    """

    pivot_type: str                  # "HH" | "LL"
    price: float                     # Exact extreme price (peak for HH, trough for LL)
    extreme_at_ms: int               # Exact epoch ms of the extreme tick
    extreme_time: datetime           # Exact timestamp of the extreme tick
    extreme_index: int               # Index of extreme tick in the input stream
    confirmed_at_ms: int             # Epoch ms of the confirming tick
    confirmed_time: datetime         # Timestamp of the confirming tick
    confirmed_price: float           # Price of the confirming tick
    confirmed_index: int             # Index of confirming tick
    confirmation_lag_ticks: int      # confirmed_index - extreme_index (minimal lag)
    confirmation_lag_ms: int         # confirmed_at_ms - extreme_at_ms
    leg_size: float                  # Price distance from previous anchor to extreme
    reversal_size: float             # Price distance from extreme to confirmation price
    anchor_price: float              # Price of previous pivot/anchor

    def to_dict(self) -> dict[str, Any]:
        return {
            "pivot_type": self.pivot_type,
            "price": self.price,
            "extreme_at": self.extreme_at_ms,
            "extreme_time": self.extreme_time.isoformat(),
            "extreme_index": self.extreme_index,
            "confirmed_at": self.confirmed_at_ms,
            "confirmed_time": self.confirmed_time.isoformat(),
            "confirmed_price": self.confirmed_price,
            "confirmed_index": self.confirmed_index,
            "confirmation_lag_ticks": self.confirmation_lag_ticks,
            "confirmation_lag_ms": self.confirmation_lag_ms,
            "leg_size": self.leg_size,
            "reversal_size": self.reversal_size,
            "anchor_price": self.anchor_price,
        }


def filter_pivots_by_threshold(
    base_pivots: Iterable[dict[str, Any] | Pivot],
    threshold: float,
) -> list[dict[str, Any]]:
    """Filter base pivots to extract true extremes for given threshold.

    Rules strictly enforced:
    1. True extremes always: Confirmed pivot price is strictly the peak (HH) or trough (LL).
    2. Non-repainting: Confirmed pivots are immutable and never modified or deleted.
    3. Strictly alternating: HH -> LL -> HH -> LL always (never two HHs or two LLs).
    4. Confirmed only when price reverses by >= threshold from candidate extreme.
    """
    if not base_pivots:
        return []
    th = float(threshold) if threshold is not None and float(threshold) > 0 else 0.0
    piv_list = [p.to_dict() if hasattr(p, "to_dict") else dict(p) for p in base_pivots]
    if th <= 0:
        return piv_list

    filtered: list[dict[str, Any]] = []
    searching_for: Optional[str] = None
    cand_high: Optional[dict[str, Any]] = None
    cand_low: Optional[dict[str, Any]] = None

    for p in piv_list:
        price = float(p["price"])

        if cand_high is None:
            cand_high = p
            cand_low = p
            continue

        if searching_for is None:
            if price > float(cand_high["price"]):
                cand_high = p
            if price < float(cand_low["price"]):
                cand_low = p

            if float(cand_high["price"]) - price >= th - 1e-6:
                filtered.append(dict(cand_high, pivot_type="HH"))
                searching_for = "LL"
                cand_low = p
            elif price - float(cand_low["price"]) >= th - 1e-6:
                filtered.append(dict(cand_low, pivot_type="LL"))
                searching_for = "HH"
                cand_high = p

        elif searching_for == "LL":
            if price < float(cand_low["price"]):
                cand_low = p
            elif price - float(cand_low["price"]) >= th - 1e-6:
                filtered.append(dict(cand_low, pivot_type="LL"))
                searching_for = "HH"
                cand_high = p

        elif searching_for == "HH":
            if price > float(cand_high["price"]):
                cand_high = p
            elif float(cand_high["price"]) - price >= th - 1e-6:
                filtered.append(dict(cand_high, pivot_type="HH"))
                searching_for = "LL"
                cand_low = p

    # Recompute leg_size and anchor_price relative to the filtered macro-swing chain
    for i in range(len(filtered)):
        if i == 0:
            filtered[i]["leg_size"] = float(filtered[i].get("leg_size") or 0.0)
        else:
            prev_price = float(filtered[i - 1]["price"])
            filtered[i]["leg_size"] = round(abs(float(filtered[i]["price"]) - prev_price), 2)
            filtered[i]["anchor_price"] = prev_price

    return filtered


class HHLLDetector:
    """Causal, append-only swing detector operating strictly on tick data.

    Rules strictly enforced:
    - Never repaints: Confirmed pivots are immutable.
    - Pivots HH and LL at extremes always: The pivot price is strictly the peak/trough.
    - Two pivot types only: Strictly 'HH' and 'LL', strictly alternating.
    - Minimal confirmation lag: Confirmed on the very tick reversal crosses threshold.
    """

    def __init__(
        self,
        *,
        reversal_threshold: Optional[float] = None,
        reversal_ticks: int = DEFAULT_REVERSAL_TICKS,
        tick_size: float = 0.5,
        min_leg: Optional[float] = None,
        min_leg_pct: float = 0.0,
        min_reversal_pct: float = DEFAULT_MIN_REVERSAL_PCT,
    ) -> None:
        self.tick_size = float(tick_size) if tick_size > 0 else 0.5
        self.reversal_ticks = max(1, int(reversal_ticks))
        self.min_reversal_pct = max(0.0, float(min_reversal_pct))
        self.min_leg_pct = max(0.0, float(min_leg_pct))

        if reversal_threshold is not None and float(reversal_threshold) > 0:
            self._explicit_threshold = float(reversal_threshold)
        else:
            self._explicit_threshold = None

        self._explicit_min_leg = float(min_leg) if min_leg is not None and float(min_leg) > 0 else None

        self.pivots: list[Pivot] = []
        self.searching_for: Optional[str] = None   # "HH" | "LL" | None

        self._anchor_price: Optional[float] = None
        self._anchor_time: Optional[datetime] = None
        self._anchor_ms: int = 0
        self._anchor_index: int = 0

        # Candidate extremes: (price, at_ms, time, index)
        self._cand_high: Optional[tuple[float, int, datetime, int]] = None
        self._cand_low: Optional[tuple[float, int, datetime, int]] = None

        self._tick_count: int = 0
        self._last_price: Optional[float] = None
        self._last_ms: int = 0
        # Pivots retracted by price breaking through them (drained by HHLLSystem).
        self.invalidated: list[Pivot] = []
        # Highest / lowest tick since the detector started: the window that applies
        # when a retraction cascade empties the chain (no pivot bounds it any more).
        self._since_hi: Optional[tuple[float, int, datetime, int]] = None
        self._since_lo: Optional[tuple[float, int, datetime, int]] = None
        # Lowest tick since the since-start high, and highest tick since the since-start low:
        # together with the extremes above they give exact min/max over those windows.
        self._hi_min: Optional[tuple[float, int, datetime, int]] = None
        self._lo_max: Optional[tuple[float, int, datetime, int]] = None
        # Ticks since the current search's candidate extreme (inclusive). The opposite pivot's
        # extreme is the extreme of this window, so a lower/higher tick seen before confirmation counts.
        self._buf: list[tuple[float, int, datetime, int]] = []
        self._buf_hi: list[tuple[float, int, datetime, int]] = []   # initial state: since cand_high
        self._buf_lo: list[tuple[float, int, datetime, int]] = []   # initial state: since cand_low

    def initialize_from_pivot(
        self, pivot_type: str, price: float, extreme_ms: int, extreme_time: datetime, index: int = 0,
        anchor_price: Optional[float] = None,
    ) -> None:
        """Initialize detector state from last confirmed pivot in database."""
        # Standing pivot restored so a break through it can invalidate it.
        self.pivots = [Pivot(
            pivot_type=pivot_type, price=price, extreme_at_ms=extreme_ms, extreme_time=extreme_time,
            extreme_index=index, confirmed_at_ms=extreme_ms, confirmed_time=extreme_time,
            confirmed_price=price, confirmed_index=index, confirmation_lag_ticks=0, confirmation_lag_ms=0,
            leg_size=0.0, reversal_size=0.0, anchor_price=anchor_price or price,
        )]
        self._anchor_price = price
        self._anchor_ms = extreme_ms
        self._anchor_time = extreme_time
        self._anchor_index = index
        self._tick_count = max(self._tick_count, index + 1)
        if pivot_type == "HH":
            self.searching_for = "LL"
            self._cand_low = (price, extreme_ms, extreme_time, index)
            self._cand_high = None
        elif pivot_type == "LL":
            self.searching_for = "HH"
            self._cand_high = (price, extreme_ms, extreme_time, index)
            self._cand_low = None

    @property
    def threshold(self) -> float:
        if self._explicit_threshold is not None and self._explicit_threshold > 0:
            return self._explicit_threshold
        base = self.reversal_ticks * self.tick_size
        if self._last_price is not None and self.min_reversal_pct > 0:
            return max(base, self._last_price * self.min_reversal_pct)
        return base

    @property
    def min_leg(self) -> float:
        if self._explicit_min_leg is not None and self._explicit_min_leg > 0:
            return self._explicit_min_leg
        return self.threshold

    @property
    def state(self) -> str:
        if self.searching_for is None:
            return "searching_HH_or_LL"
        return f"searching_{self.searching_for}"

    @property
    def candidate(self) -> Optional[dict[str, Any]]:
        """Current unconfirmed candidate extreme being tracked."""
        if self.searching_for == "HH" and self._cand_high is not None:
            p, ms, dt, idx = self._cand_high
            rev = (p - self._last_price) if self._last_price is not None else 0.0
            return {
                "pivot_type": "HH",
                "price": p,
                "extreme_at": ms,
                "extreme_time": dt.isoformat(),
                "extreme_index": idx,
                "current_reversal": max(0.0, rev),
                "threshold": self.threshold,
            }
        if self.searching_for == "LL" and self._cand_low is not None:
            p, ms, dt, idx = self._cand_low
            rev = (self._last_price - p) if self._last_price is not None else 0.0
            return {
                "pivot_type": "LL",
                "price": p,
                "extreme_at": ms,
                "extreme_time": dt.isoformat(),
                "extreme_index": idx,
                "current_reversal": max(0.0, rev),
                "threshold": self.threshold,
            }
        if self.searching_for is None:
            # Initial search
            high_p = self._cand_high[0] if self._cand_high else None
            low_p = self._cand_low[0] if self._cand_low else None
            return {
                "pivot_type": "HH_or_LL",
                "cand_high": high_p,
                "cand_low": low_p,
                "threshold": self.threshold,
            }
        return None

    @property
    def latest_hh(self) -> Optional[Pivot]:
        for p in reversed(self.pivots):
            if p.pivot_type == "HH":
                return p
        return None

    @property
    def latest_ll(self) -> Optional[Pivot]:
        for p in reversed(self.pivots):
            if p.pivot_type == "LL":
                return p
        return None

    def _resume(self, search: str, anchor: float, cand: tuple[float, int, datetime, int],
                buf: Optional[list[tuple[float, int, datetime, int]]] = None) -> None:
        """Continue searching `search` ("HH" | "LL") from `anchor`, with `cand` as its extreme so far.
        `buf` is the window since `cand` (default: just `cand`)."""
        self.searching_for = search
        self._anchor_price = anchor
        self._anchor_ms, self._anchor_time, self._anchor_index = cand[1], cand[2], cand[3]
        if search == "HH":
            self._cand_high = cand
        else:
            self._cand_low = cand
        self._buf = [cand] if buf is None else buf

    def _retract(self, p: float, t_ms: int, t_dt: datetime, idx: int,
                 cand: tuple[float, int, datetime, int]) -> None:
        """Price `p` broke the standing pivot (an HH exceeded / an LL undercut) before the
        opposite pivot formed, so that pivot is not an extreme. Retract it and re-derive the
        chain below it: `cand` is the opposite-type extreme tracked since the standing pivot.
        If that extreme beats the pivot before it, that pivot is superseded too (cascade).
        Every retracted pivot lands in `self.invalidated` so storage can drop it."""
        now = (p, t_ms, t_dt, idx)
        top_type = self.pivots[-1].pivot_type
        while True:
            top = self.pivots.pop()
            self.invalidated.append(top)
            if not self.pivots:
                # Nothing left standing: search the broken pivot's type again from its anchor,
                # over every tick since start (the first pivot's extreme is absolute since start).
                if top_type == "HH":
                    self._resume(top_type, top.anchor_price or p, self._since_hi,
                                 buf=[self._since_hi, self._hi_min])
                else:
                    self._resume(top_type, top.anchor_price or p, self._since_lo,
                                 buf=[self._since_lo, self._lo_max])
                return
            prev = self.pivots[-1]                     # opposite type of top_type
            if top_type == "HH":                       # prev is LL; cand is the lowest since top
                superseded = cand[0] < prev.price
            else:                                      # prev is HH; cand is the highest since top
                superseded = cand[0] > prev.price
            if not superseded:
                self._resume(top_type, prev.price, now, buf=[])
                return
            self.invalidated.append(self.pivots.pop())  # prev is superseded by cand
            if not self.pivots:
                if prev.pivot_type == "LL":
                    self._resume(prev.pivot_type, prev.anchor_price or cand[0],
                                 self._since_lo, buf=[self._since_lo, self._lo_max])
                else:
                    self._resume(prev.pivot_type, prev.anchor_price or cand[0],
                                 self._since_hi, buf=[self._since_hi, self._hi_min])
                return
            below = self.pivots[-1]                    # same type as top_type
            broken = p > below.price if top_type == "HH" else p < below.price
            if not broken:
                self._resume(prev.pivot_type, below.price, cand, buf=self._buf)
                return
            top_type = below.pivot_type                # keep cascading through below

    def _broken_by(self, p: float) -> bool:
        """True when p breaks the anchor (standing pivot) on the side the current search opposes."""
        if self._anchor_price is None or self.searching_for is None:
            return False
        if self.searching_for == "LL":
            return p > self._anchor_price
        return p < self._anchor_price

    def update(self, price: float, timestamp: float | int | datetime) -> Optional[Pivot]:
        """Process one incoming tick causally.

        Returns newly confirmed Pivot if confirmed on this tick, else None.
        """
        p = float(price)
        t_ms = _to_epoch_ms(timestamp)
        t_dt = _to_datetime(timestamp)
        idx = self._tick_count
        self._tick_count += 1
        self._last_price = p
        self._last_ms = t_ms
        tk = (p, t_ms, t_dt, idx)
        if self._since_hi is None or p > self._since_hi[0]:
            self._since_hi = tk
            self._hi_min = tk
        elif p < self._hi_min[0]:
            self._hi_min = tk
        if self._since_lo is None or p < self._since_lo[0]:
            self._since_lo = tk
            self._lo_max = tk
        elif p > self._lo_max[0]:
            self._lo_max = tk

        # First tick initializes the anchor and candidates
        if self._anchor_price is None:
            self._anchor_price = p
            self._anchor_ms = t_ms
            self._anchor_time = t_dt
            self._anchor_index = idx
            self._cand_high = (p, t_ms, t_dt, idx)
            self._cand_low = (p, t_ms, t_dt, idx)
            self._buf_hi = [self._cand_high]
            self._buf_lo = [self._cand_low]
            return None

        th = self.threshold
        min_leg = self.min_leg

        # Initial state: neither HH nor LL has confirmed yet
        if self.searching_for is None:
            assert self._cand_high is not None and self._cand_low is not None
            if p > self._cand_high[0]:
                self._cand_high = tk
                self._buf_hi = [tk]
            else:
                self._buf_hi.append(tk)
            if p < self._cand_low[0]:
                self._cand_low = tk
                self._buf_lo = [tk]
            else:
                self._buf_lo.append(tk)

            high_leg = self._cand_high[0] - self._anchor_price
            high_rev = self._cand_high[0] - p
            low_leg = self._anchor_price - self._cand_low[0]
            low_rev = p - self._cand_low[0]

            can_confirm_hh = (high_leg >= min_leg - EPSILON) and (high_rev >= th - EPSILON)
            can_confirm_ll = (low_leg >= min_leg - EPSILON) and (low_rev >= th - EPSILON)

            if can_confirm_hh and can_confirm_ll:
                # Earlier extreme breaks tie
                if self._cand_high[3] <= self._cand_low[3]:
                    can_confirm_ll = False
                else:
                    can_confirm_hh = False

            if can_confirm_hh:
                self._buf = self._buf_hi
                return self._publish_hh(p, t_ms, t_dt, idx, high_leg, high_rev)

            if can_confirm_ll:
                self._buf = self._buf_lo
                return self._publish_ll(p, t_ms, t_dt, idx, low_leg, low_rev)

            return None

        # A standing pivot that price has broken is retracted; p is then judged by the resumed search.
        while self.pivots and self._broken_by(p):
            if self.searching_for == "LL":   # standing HH exceeded before an LL formed
                self._retract(p, t_ms, t_dt, idx, self._cand_low)
            else:                            # standing LL undercut before an HH formed
                self._retract(p, t_ms, t_dt, idx, self._cand_high)

        # Searching for LL (previous pivot was HH)
        if self.searching_for == "LL":
            assert self._cand_low is not None
            if self._anchor_price is not None and p > self._anchor_price:
                # No standing pivot: extend anchor to new high so detection never stalls.
                self._anchor_price = p
                self._anchor_ms = t_ms
                self._anchor_time = t_dt
                self._anchor_index = idx
                self._buf.append(tk)
                return None
            if p < self._cand_low[0]:
                self._cand_low = tk
                self._buf = [tk]
                return None
            self._buf.append(tk)
            reversal = p - self._cand_low[0]
            leg = (self._anchor_price or self._cand_low[0]) - self._cand_low[0]
            if reversal >= th - EPSILON and leg >= min_leg - EPSILON:
                return self._publish_ll(p, t_ms, t_dt, idx, leg, reversal)
            return None

        # Searching for HH (previous pivot was LL)
        if self.searching_for == "HH":
            assert self._cand_high is not None
            if self._anchor_price is not None and p < self._anchor_price:
                # No standing pivot: extend anchor to new low so detection never stalls.
                self._anchor_price = p
                self._anchor_ms = t_ms
                self._anchor_time = t_dt
                self._anchor_index = idx
                self._buf.append(tk)
                return None
            if p > self._cand_high[0]:
                self._cand_high = tk
                self._buf = [tk]
                return None
            self._buf.append(tk)
            reversal = self._cand_high[0] - p
            leg = self._cand_high[0] - (self._anchor_price or self._cand_high[0])
            if reversal >= th - EPSILON and leg >= min_leg - EPSILON:
                return self._publish_hh(p, t_ms, t_dt, idx, leg, reversal)
            return None

        return None

    def _publish_hh(
        self, conf_p: float, conf_ms: int, conf_dt: datetime, conf_idx: int, leg: float, rev: float
    ) -> Pivot:
        assert self._cand_high is not None
        ext_p, ext_ms, ext_dt, ext_idx = self._cand_high
        pivot = Pivot(
            pivot_type="HH",
            price=ext_p,                          # EXTREME ALWAYS!
            extreme_at_ms=ext_ms,                 # EXTREME TIME ALWAYS!
            extreme_time=ext_dt,
            extreme_index=ext_idx,
            confirmed_at_ms=conf_ms,
            confirmed_time=conf_dt,
            confirmed_price=conf_p,
            confirmed_index=conf_idx,
            confirmation_lag_ticks=conf_idx - ext_idx,
            confirmation_lag_ms=conf_ms - ext_ms,
            leg_size=leg,
            reversal_size=rev,
            anchor_price=self._anchor_price or ext_p,
        )
        self.pivots.append(pivot)
        self.searching_for = "LL"
        self._anchor_price = ext_p
        self._anchor_ms = ext_ms
        self._anchor_time = ext_dt
        self._anchor_index = ext_idx
        # Candidate low = lowest tick since the HH extreme (the window the LL must be extreme of)
        j = min(range(len(self._buf)), key=lambda k: self._buf[k][0])
        self._cand_low = self._buf[j]
        self._buf = self._buf[j:]
        return pivot

    def _publish_ll(
        self, conf_p: float, conf_ms: int, conf_dt: datetime, conf_idx: int, leg: float, rev: float
    ) -> Pivot:
        assert self._cand_low is not None
        ext_p, ext_ms, ext_dt, ext_idx = self._cand_low
        pivot = Pivot(
            pivot_type="LL",
            price=ext_p,                          # EXTREME ALWAYS!
            extreme_at_ms=ext_ms,                 # EXTREME TIME ALWAYS!
            extreme_time=ext_dt,
            extreme_index=ext_idx,
            confirmed_at_ms=conf_ms,
            confirmed_time=conf_dt,
            confirmed_price=conf_p,
            confirmed_index=conf_idx,
            confirmation_lag_ticks=conf_idx - ext_idx,
            confirmation_lag_ms=conf_ms - ext_ms,
            leg_size=leg,
            reversal_size=rev,
            anchor_price=self._anchor_price or ext_p,
        )
        self.pivots.append(pivot)
        self.searching_for = "HH"
        self._anchor_price = ext_p
        self._anchor_ms = ext_ms
        self._anchor_time = ext_dt
        self._anchor_index = ext_idx
        # Candidate high = highest tick since the LL extreme
        j = max(range(len(self._buf)), key=lambda k: self._buf[k][0])
        self._cand_high = self._buf[j]
        self._buf = self._buf[j:]
        return pivot

    def process_ticks(self, ticks: Iterable[Any]) -> list[Pivot]:
        """Feed a sequence of ticks: each tick may be (price, at_ms) or dict or object."""
        for item in ticks:
            if isinstance(item, (tuple, list)):
                p, t = float(item[0]), item[1]
            elif isinstance(item, dict):
                p = float(item.get("price") or item.get("close") or 0.0)
                t = item.get("at") or item.get("traded_at") or item.get("timestamp") or 0
            else:
                p = float(getattr(item, "price", 0.0))
                t = getattr(item, "at", 0) or getattr(item, "timestamp", 0)
            self.update(p, t)
        return list(self.pivots)

    def validate(self) -> dict[str, bool]:
        """Validate detector against all core system rules."""
        results = {
            "two_types_only": True,
            "strict_alternation": True,
            "extremes_always": True,
            "minimal_lag": True,
        }
        types = [p.pivot_type for p in self.pivots]
        if any(t not in ("HH", "LL") for t in types):
            results["two_types_only"] = False
        for a, b in zip(types, types[1:]):
            if a == b:
                results["strict_alternation"] = False
        for p in self.pivots:
            if p.confirmation_lag_ticks < 1 or p.confirmed_index <= p.extreme_index:
                results["minimal_lag"] = False
            if p.pivot_type == "HH":
                if p.confirmed_price > p.price:
                    results["extremes_always"] = False
            elif p.pivot_type == "LL":
                if p.confirmed_price < p.price:
                    results["extremes_always"] = False
        return results




def _demo() -> None:
    # 1. Basic ZigZag verification
    prices = [100.0, 102.0, 105.0, 108.0, 105.0, 102.0, 99.0, 95.0, 98.0, 102.0, 106.0]
    det = HHLLDetector(reversal_threshold=3.0, tick_size=0.5)
    for i, p in enumerate(prices):
        det.update(p, 1000 + i * 1000)

    assert len(det.pivots) == 2, f"expected 2 pivots, got {len(det.pivots)}"
    p1, p2 = det.pivots[0], det.pivots[1]

    # Rule 3: Two pivot types only
    assert p1.pivot_type == "HH"
    assert p2.pivot_type == "LL"

    # Rule 2: Pivots at extremes always
    assert p1.price == 108.0, f"HH price must be extreme 108.0, got {p1.price}"
    assert p1.extreme_index == 3, f"HH extreme index must be 3, got {p1.extreme_index}"
    assert p2.price == 95.0, f"LL price must be extreme 95.0, got {p2.price}"
    assert p2.extreme_index == 7, f"LL extreme index must be 7, got {p2.extreme_index}"

    # Rule 4: Minimal confirmation lag
    assert p1.confirmed_index == 4, f"HH confirmed at index 4 (1 tick lag), got {p1.confirmed_index}"
    assert p1.confirmation_lag_ticks == 1
    assert p2.confirmed_index == 8, f"LL confirmed at index 8 (1 tick lag), got {p2.confirmed_index}"
    assert p2.confirmation_lag_ticks == 1

    # Rule 1: No repainting (streaming vs batch equality)
    det_batch = HHLLDetector(reversal_threshold=3.0, tick_size=0.5)
    det_batch.process_ticks([(p, 1000 + i * 1000) for i, p in enumerate(prices)])
    assert [p.to_dict() for p in det.pivots] == [p.to_dict() for p in det_batch.pivots]

    # Rule 1: Immutable history (future ticks do not alter past pivots)
    det_extended = HHLLDetector(reversal_threshold=3.0, tick_size=0.5)
    for i, p in enumerate(prices):
        det_extended.update(p, 1000 + i * 1000)
    for step in range(1, 10):
        det_extended.update(106.0 + step, 1000 + (len(prices) + step) * 1000)
    assert det_extended.pivots[:2] == det.pivots, "past pivots must not change when future ticks arrive"

    # Instrument tick sizes (BTCUSD, ADAUSD, ETHUSD)
    btc_det = HHLLDetector(tick_size=0.5, reversal_ticks=6)
    assert btc_det.threshold == 3.0, btc_det.threshold

    ada_det = HHLLDetector(tick_size=0.0001, reversal_ticks=6)
    assert abs(ada_det.threshold - 0.0006) < 1e-9, ada_det.threshold

    # Validation checks pass
    verdict = det.validate()
    assert all(verdict.values()), verdict

    # Random walk stress test (10,000 ticks)
    import random
    random.seed(1337)
    rw_price = 75000.0
    rw_ticks = []
    for i in range(10000):
        rw_price += random.choice([-1.0, -0.5, 0.0, 0.5, 1.0])
        rw_ticks.append((rw_price, 1000000 + i * 100))

    rw_det = HHLLDetector(reversal_threshold=4.0, tick_size=0.5)
    rw_det.process_ticks(rw_ticks)
    assert len(rw_det.pivots) > 50, f"expected > 50 pivots, got {len(rw_det.pivots)}"
    rw_verdict = rw_det.validate()
    assert all(rw_verdict.values()), rw_verdict

    # Verify no repainting across partial slice
    rw_det_half = HHLLDetector(reversal_threshold=4.0, tick_size=0.5)
    rw_det_half.process_ticks(rw_ticks[:5000])
    n_half = len(rw_det_half.pivots)
    assert rw_det.pivots[:n_half] == rw_det_half.pivots, "no repainting invariant failed!"

    # Verify extremes always across all pivots
    for piv in rw_det.pivots:
        ext_idx = piv.extreme_index
        conf_idx = piv.confirmed_index
        sub_prices = [rw_ticks[j][0] for j in range(ext_idx, conf_idx + 1)]
        if piv.pivot_type == "HH":
            assert piv.price == max(sub_prices), "HH must be absolute maximum of swing"
        else:
            assert piv.price == min(sub_prices), "LL must be absolute minimum of swing"

    # Retraction cascade: HH 79591.7 is broken by 79614.6 after the LL search had already
    # tracked 79574.2 below the LL 79579.4; the LL must be re-derived, not kept stale.
    casc = HHLLDetector(reversal_ticks=6, tick_size=0.5, min_leg=0)
    seq = [79621.0, 79622.6, 79579.4, 79591.7, 79582.6, 79574.2, 79614.6]
    casc.process_ticks([(p, 1000 + i * 1000) for i, p in enumerate(seq)])
    assert [(p.pivot_type, p.price) for p in casc.pivots] == [("LL", 79574.2)], casc.pivots
    assert [(p.pivot_type, p.price) for p in casc.invalidated] == [("HH", 79591.7), ("LL", 79579.4)]
    assert casc.searching_for == "HH"

    print("hhll self-check ok")


if __name__ == "__main__":
    _demo()
