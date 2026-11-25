"""Shoonya routing for the HH/LL algo: front-month NIFTY future, LMT IOC orders.

Used only in LIVE mode (the algo refuses a router in TEST). Pricing follows
trade_manager's rule: cross the top of book by a bounded 2%, round to the tick,
clamp to the circuit band. Every order is checked to a terminal state before
the algo treats it as done. Anything unknown halts the algo, because a
second order on top of an unseen fill is how a position gets doubled.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any, Optional

log = logging.getLogger("hhll_live")

FUT_TSYM = re.compile(r"^(?P<root>[A-Z]+)(?P<exp>\d{2}[A-Z]{3}\d{2})F$")
TERMINAL_OK = {"COMPLETE"}
TERMINAL_NO_FILL = {"REJECTED", "CANCELED", "CANCELLED"}


class LiveRoutingError(RuntimeError):
    """The order did not fill; the position is unchanged."""


class UncertainFillError(LiveRoutingError):
    """The order's outcome is unknown. Must halt the algo and be reconciled by hand."""


@dataclass(frozen=True)
class Contract:
    tsym: str
    token: str
    exchange: str
    expiry: date
    lot: int
    tick: Decimal
    freeze: int          # max quantity per order (freeze-qty multiple of lot)


@dataclass(frozen=True)
class Fill:
    orderno: str
    price: float
    qty: int


def resolve_front_future(api: Any, index: str = "NIFTY", exchange: str = "NFO",
                         today: Optional[date] = None) -> Contract:
    """The nearest monthly future whose expiry is still ahead of `today`.

    Expiry-day rolls forward (roll_days_before_expiry = 0 in config). Raises if
    the broker lists no such contract, or if its metadata is not usable.
    """
    today = today or date.today()
    found = api.searchscrip(exchange=exchange, searchtext=index)
    rows = found.get("values") if isinstance(found, dict) else found
    if not isinstance(rows, list):
        raise LiveRoutingError(f"no {index} future list from broker")
    candidates = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        tsym = str(row.get("tsym") or "").strip()
        m = FUT_TSYM.match(tsym)
        if not m or m.group("root") != index or not row.get("token"):
            continue
        # Expiry comes from the name itself, <ROOT><DDMONYY>F, the same form
        # getdata._contract_key parses. Options end CE/PE and never match.
        try:
            exp = datetime.strptime(m.group("exp"), "%d%b%y").date()
        except ValueError:
            continue
        if exp > today:
            candidates.append((exp, tsym, str(row["token"])))
    if not candidates:
        raise LiveRoutingError(f"no live {index} future contract on {exchange}")
    exp, tsym, token = min(candidates)
    meta = api.get_security_info(exchange=exchange, token=token)
    if not isinstance(meta, dict) or str(meta.get("stat") or "").lower() != "ok":
        raise LiveRoutingError(f"{tsym}: contract details unavailable")
    if meta.get("tsym") not in (None, tsym):
        raise LiveRoutingError(f"{tsym}: contract details mismatch")
    lot = int(float(meta["ls"]))
    tick = Decimal(str(meta["ti"]))
    if lot <= 0 or tick <= 0 or not tick.is_finite():
        raise LiveRoutingError(f"{tsym}: invalid lot or tick")
    try:
        freeze = int(float(str(meta.get("frzqty"))))
    except (TypeError, ValueError, OverflowError):
        freeze = 0
    freeze = max(freeze, lot)
    return Contract(tsym=tsym, token=token, exchange=exchange, expiry=exp, lot=lot,
                    tick=tick, freeze=freeze // lot * lot)


def assert_flat(api: Any, tsym: str) -> None:
    """Refuse to start the algo on top of an existing broker position."""
    rows = api.get_positions()
    if rows is None:
        rows = []
    if not isinstance(rows, list):
        raise LiveRoutingError("position book unavailable")
    for row in rows:
        if isinstance(row, dict) and row.get("tsym") == tsym and int(float(row.get("netqty") or 0)) != 0:
            raise LiveRoutingError(f"{tsym} already held ({row.get('netqty')}); algo starts flat")


class ShoonyaFutureRouter:
    """Sends one IOC limit order per call and waits for its terminal state."""

    def __init__(self, api: Any, contract: Contract, *, poll_s: float = 0.5,
                 timeout_s: float = 15.0, sleep=time.sleep) -> None:
        self.api = api
        self.contract = contract
        self.poll_s = poll_s
        self.timeout_s = timeout_s
        self._sleep = sleep

    @property
    def key(self) -> str:
        return f"{self.contract.exchange}|{self.contract.token}"

    def limit_price(self, side: str, quote: dict) -> Decimal:
        c = self.contract
        if not isinstance(quote, dict) or str(quote.get("stat") or "").lower() != "ok":
            raise LiveRoutingError(f"{c.tsym}: quote unavailable")
        if quote.get("tsym") not in (None, c.tsym):
            raise LiveRoutingError(f"{c.tsym}: quote contract mismatch")
        try:
            top = Decimal(str(quote["sp1" if side == "B" else "bp1"]))
        except (KeyError, ArithmeticError, ValueError) as exc:
            raise LiveRoutingError(f"{c.tsym}: invalid top of book") from exc
        if top <= 0 or not top.is_finite():
            raise LiveRoutingError(f"{c.tsym}: invalid top of book")
        buffered = top * (Decimal("1.02") if side == "B" else Decimal("0.98"))
        rounding = ROUND_CEILING if side == "B" else ROUND_FLOOR
        limit = (buffered / c.tick).to_integral_value(rounding=rounding) * c.tick
        band = quote.get("uc" if side == "B" else "lc")
        if band not in (None, ""):
            bound = Decimal(str(band))
            if bound <= 0:
                raise LiveRoutingError(f"{c.tsym}: invalid circuit band")
            limit = min(limit, bound) if side == "B" else max(limit, bound)
        if limit <= 0 or not limit.is_finite():
            raise LiveRoutingError(f"{c.tsym}: invalid limit price")
        return limit

    def send(self, side: str, units: int, reason: str = "") -> Fill:
        """side: "BUY" | "SELL". Returns the fill, or raises."""
        c = self.contract
        if side not in ("BUY", "SELL"):
            raise ValueError(f"bad side {side!r}")
        if units <= 0 or units % c.lot or units > c.freeze:
            raise LiveRoutingError(f"{c.tsym}: qty {units} must be a lot multiple up to {c.freeze}")
        code = "B" if side == "BUY" else "S"
        try:
            quote = self.api.get_quotes(exchange=c.exchange, token=c.token)
            limit = self.limit_price(code, quote)
            resp = self.api.place_order(
                buy_or_sell=code, product_type="M", exchange=c.exchange, tradingsymbol=c.tsym,
                quantity=units, discloseqty=0, price_type="LMT", price=float(limit),
                retention="IOC", remarks=(reason or "hhll")[:40],
            )
        except LiveRoutingError:
            raise
        except Exception as exc:  # network/API fault: the order may or may not be live
            raise UncertainFillError(f"{c.tsym}: {side} {units} request failed: {exc}") from exc
        orderno = str((resp or {}).get("norenordno") or "") if isinstance(resp, dict) else ""
        if not orderno:
            # The broker may have accepted it; we cannot tell from here.
            raise UncertainFillError(f"{c.tsym}: no order number returned for {side} {units}")
        try:
            return self._await(orderno, units)
        except LiveRoutingError:
            raise
        except Exception as exc:
            raise UncertainFillError(f"order {orderno}: status check failed: {exc}") from exc

    def _await(self, orderno: str, units: int) -> Fill:
        deadline = time.monotonic() + self.timeout_s
        while True:
            rows = self.api.single_order_history(orderno)
            last = rows[0] if isinstance(rows, list) and rows else None
            status = str((last or {}).get("status") or "").upper()
            if status in TERMINAL_OK:
                filled = int(float(last.get("fillshares") or last.get("flqty") or 0))
                price = float(last.get("avgprc") or 0.0)
                if filled != units or price <= 0:
                    raise UncertainFillError(f"order {orderno}: filled {filled}/{units} at {price}")
                return Fill(orderno=orderno, price=price, qty=filled)
            if status in TERMINAL_NO_FILL:
                raise LiveRoutingError(f"order {orderno} {status.lower()}; position unchanged")
            if time.monotonic() >= deadline:
                raise UncertainFillError(f"order {orderno} still '{status or 'unknown'}' after {self.timeout_s}s")
            self._sleep(self.poll_s)
