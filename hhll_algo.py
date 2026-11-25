"""HH/LL algo for Lune: short on a confirmed HH, long on a confirmed LL.

- HH confirmed: close any long (sell), open a short (sell).
- LL confirmed: close any short (buy), open a long (buy).
- Pivots come from `hhll_detector.HHLLDetector` (causal, never repaints).
- Each execution is a separate order, charged with `zerodha_charges.fo_charges`.
- TEST: fills at the confirming tick's price. LIVE: sends through `router`
  (hhll_live.ShoonyaFutureRouter). Any failed or unknown order halts the algo.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Optional

from hhll_detector import HHLLDetector, Pivot
from hhll_live import LiveRoutingError
from zerodha_charges import Charges, fo_charges

log = logging.getLogger("hhll_algo")

LONG, SHORT = "LONG", "SHORT"
#: Recent pivots, executions and trades held in memory (status, tests). The full record is
#: the ledger: `sink` (the database, hhll_store.algo_ledger) and/or `ledger_path`.
KEEP = 1000


@dataclass
class Execution:
    side: str           # "BUY" | "SELL"
    units: int
    price: float
    at: float
    reason: str
    charges: Charges

    @property
    def total(self) -> Decimal:
        return self.charges.total


@dataclass
class Trade:
    direction: str
    units: int
    entry_price: float
    entry_at: float
    entry_charges: Decimal
    entry_reason: str
    anchor_price: float = 0.0
    exit_price: Optional[float] = None
    exit_at: Optional[float] = None
    exit_charges: Decimal = Decimal(0)
    exit_reason: Optional[str] = None
    gross_pnl: Decimal = Decimal(0)

    @property
    def net_pnl(self) -> Decimal:
        return self.gross_pnl - self.entry_charges - self.exit_charges

    @property
    def closed(self) -> bool:
        return self.exit_price is not None


class HHLLPaperAlgo:
    """HH/LL algo. TEST fills at the reference price; LIVE sends through `router`."""

    def __init__(
        self,
        *,
        lot_size: int = 65,            # NIFTY lot
        lots: int = 1,
        tick_size: float = 0.05,       # NIFTY tick
        reversal_ticks: int = 16,      # confirmation threshold = reversal_ticks x tick_size
        min_leg: float | str | None = 0.0, # Big leg threshold (0.0 = all, "auto" = auto-calc for charges)
        charges: float = 300.0,        # Round-trip statutory and broker charges in INR
        profit_buffer_pts: Optional[float] = None, # Target profit buffer above break-even
        instrument: str = "future",
        mode: str = "TEST",
        router: Any = None,
        ledger_path: Optional[Path] = None,
        sink: Optional[Callable[[dict], None]] = None,
    ) -> None:
        mode = mode.upper()
        if mode not in ("TEST", "LIVE"):
            raise ValueError(f"mode must be TEST or LIVE, not {mode!r}")
        if mode == "LIVE" and router is None:
            raise ValueError("LIVE needs a router")
        if mode == "TEST" and router is not None:
            raise ValueError("TEST must not have a router")
        if lot_size <= 0 or lots <= 0:
            raise ValueError("lot_size and lots must be positive")
        self.units = int(lot_size) * int(lots)
        self.instrument = instrument.lower()
        self.mode = mode
        self.router = router
        self.halted: Optional[str] = None
        self.ledger_path = Path(ledger_path) if ledger_path else None
        self.sink = sink

        self.charges = float(charges)
        self.tick_size = float(tick_size)
        self.reversal_ticks = int(reversal_ticks)
        self.reversal_pts = round(self.reversal_ticks * self.tick_size, 4)
        turnaround_pts = round(2.0 * self.reversal_pts, 2)
        charges_pts = round(self.charges / self.units, 2)
        self.break_even_leg = round(turnaround_pts + charges_pts, 2)

        min_leg_str = str(min_leg).strip().lower() if min_leg is not None else "0"
        if min_leg_str in ("auto", "be", "break_even"):
            default_buf = float(profit_buffer_pts) if profit_buffer_pts is not None else max(15.0, round(1000.0 / self.units, 2))
            self.min_leg = round(self.break_even_leg + default_buf, 2)
        elif min_leg is not None:
            self.min_leg = max(0.0, float(min_leg))
        else:
            self.min_leg = 0.0

        # Base detector tracks extremes with minimal confirmation lag (fast confirmation)
        self.detector = HHLLDetector(reversal_ticks=reversal_ticks, tick_size=tick_size, min_leg=0)
        self.position: Optional[Trade] = None
        self.trades: deque[Trade] = deque(maxlen=KEEP)
        self.executions: deque[Execution] = deque(maxlen=KEEP)
        self.pivots: deque[Pivot] = deque(maxlen=KEEP)
        self._n_trades = 0
        self._net = Decimal(0)
        self._charges_paid = Decimal(0)
        self._lock = threading.RLock()

    # ── inputs ──────────────────────────────────────────────────────────
    def on_price(self, price: float, at: Optional[float] = None) -> Optional[Pivot]:
        """Feed one trade price. `at` is epoch seconds (default: now)."""
        with self._lock:
            ts = time.time() if at is None else float(at)
            pivot = self.detector.update(float(price), ts)
            if pivot is not None:
                self.on_pivot(pivot)
            return pivot

    def on_pivot(self, pivot: Pivot) -> None:
        with self._lock:
            self.pivots.append(pivot)
            self._append_ledger({"kind": "pivot", "mode": self.mode, "pivot_type": pivot.pivot_type,
                                 "price": pivot.price, "extreme_at_ms": pivot.extreme_at_ms,
                                 "confirmed_at_ms": pivot.confirmed_at_ms, "confirmed_price": pivot.confirmed_price,
                                 "leg_size": pivot.leg_size, "reversal_size": pivot.reversal_size})
            if self.halted:
                log.error("HHLL halted (%s); ignoring %s@%s", self.halted, pivot.pivot_type, pivot.price)
                return

            # Simple logic: HH -> SHORT, LL -> LONG
            want = SHORT if pivot.pivot_type == "HH" else LONG

            # Redundant pivot in same direction as held position is ignored
            if self.position and self.position.direction == want:
                return

            # Measure leg length to ensure trade is profitable after 300 charges:
            # When flat, leg is the distance from previous anchor to this extreme.
            # When holding position, leg is the move from the entry anchor extreme to this opposite extreme.
            if self.position is None:
                leg = pivot.leg_size
            else:
                entry_anchor = self.position.anchor_price or self.position.entry_price
                leg = max(pivot.leg_size, abs(pivot.price - entry_anchor))

            # Filter: keep leg length big so we are profitable after 300 charges
            if self.min_leg > 0 and leg < self.min_leg - 1e-6:
                log.info(
                    "HHLL %s@%.2f skipped: leg %.2f < min_leg %.2f (need big leg for %s charges)",
                    pivot.pivot_type, pivot.price, leg, self.min_leg, self.charges,
                )
                return

            reason = f"{pivot.pivot_type}@{pivot.price}"
            px = float(pivot.confirmed_price)
            at = pivot.confirmed_at_ms / 1000.0

            # One exit condition only: exit on opposite pivot
            if self.position and not self._close(px, at, reason):
                return
            self._open(want, px, at, reason, anchor_price=pivot.price)

    # ── execution ───────────────────────────────────────────────────────
    def _execute(self, side: str, ref_price: float, at: float, reason: str) -> Optional[Execution]:
        """Paper: fills at `ref_price`. Live: the broker's fill. None = halted, no fill."""
        if self.router is None:
            price = ref_price
        else:
            try:
                fill = self.router.send(side, self.units, reason)
            except LiveRoutingError as exc:
                self.halted = f"{side} {reason}: {exc}"
                log.critical("HHLL live order failed, algo halted: %s", self.halted)
                return None
            price = fill.price
        ch = fo_charges(instrument=self.instrument, side=side, units=self.units, price=price)
        ex = Execution(side=side, units=self.units, price=price, at=at, reason=reason, charges=ch)
        self.executions.append(ex)
        self._charges_paid += ex.total
        log.info("HHLL %s %s %d @ %.2f (%s) charges=%s", self.mode, side, self.units, price, reason, ch.total)
        self._append_ledger({"kind": "execution", "mode": self.mode, "side": side, "units": self.units,
                             "price": price, "at": at, "reason": reason,
                             "brokerage": str(ch.brokerage), "stt": str(ch.stt),
                             "exchange": str(ch.exchange), "sebi": str(ch.sebi),
                             "stamp": str(ch.stamp), "ipft": str(ch.ipft),
                             "gst": str(ch.gst), "total": str(ch.total)})
        return ex

    def _open(self, direction: str, px: float, at: float, reason: str, anchor_price: float = 0.0) -> bool:
        side = "BUY" if direction == LONG else "SELL"
        ex = self._execute(side, px, at, reason)
        if ex is None:
            return False
        self.position = Trade(direction=direction, units=self.units, entry_price=ex.price,
                              entry_at=at, entry_charges=ex.total, entry_reason=reason,
                              anchor_price=anchor_price or ex.price)
        return True

    def _close(self, px: float, at: float, reason: str) -> bool:
        t = self.position
        side = "SELL" if t.direction == LONG else "BUY"
        ex = self._execute(side, px, at, reason)
        if ex is None:
            return False
        sign = 1 if t.direction == LONG else -1
        t.exit_price, t.exit_at, t.exit_charges, t.exit_reason = ex.price, at, ex.total, reason
        t.gross_pnl = Decimal(str(round(sign * t.units * (ex.price - t.entry_price), 2)))
        self.trades.append(t)
        self._n_trades += 1
        self._net += t.net_pnl
        self.position = None
        self._append_ledger({"kind": "trade", "mode": self.mode, "direction": t.direction,
                             "units": t.units, "entry_price": t.entry_price, "exit_price": ex.price,
                             "gross_pnl": str(t.gross_pnl), "entry_charges": str(t.entry_charges),
                             "exit_charges": str(t.exit_charges), "net_pnl": str(t.net_pnl)})
        return True

    def _append_ledger(self, row: dict[str, Any]) -> None:
        if self.sink is not None:
            try:
                self.sink(row)
            except Exception:  # noqa: BLE001 - the ledger must not stop the algo
                log.exception("HHLL ledger row not queued: %s", row.get("kind"))
        if self.ledger_path is None:
            return
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self.ledger_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, default=str) + "\n")

    # ── reporting ───────────────────────────────────────────────────────
    def realised_net(self) -> Decimal:
        return self._net

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "mode": self.mode,
                "halted": self.halted,
                "units": self.units,
                "min_leg": self.min_leg,
                "break_even_leg": self.break_even_leg,
                "reversal_pts": self.reversal_pts,
                "charges": self.charges,
                "position": None if not self.position else self.position.direction,
                "entry_price": None if not self.position else self.position.entry_price,
                "trades": self._n_trades,
                "realised_net": str(self.realised_net()),
                "charges_paid": str(self._charges_paid),
            }


def attach(ws: Any, algo: HHLLPaperAlgo, key: Optional[str] = None) -> Callable[[dict], None]:
    """Feed a LiveWebSocket's price ticks into `algo`.

    `key` is the subscribed `EXCH|TOKEN` to follow (None = every tick). Ticks
    without a valid `lp` are skipped, as the websocket itself does.
    """
    def handler(tick: dict) -> None:
        if key is not None and ws._tick_key(tick) != key:
            return
        raw = tick.get("lp")
        try:
            price = float(raw)
        except (TypeError, ValueError):
            return
        if price > 0:
            algo.on_price(price)

    ws.add_price_listener(handler)
    return handler
