"""Zerodha F&O (NSE) charges per executed order, from zerodha.com/charges.

Schedule used (F&O, verified against https://zerodha.com/charges):

| Charge              | Futures                         | Options                         |
|---------------------|---------------------------------|---------------------------------|
| Brokerage           | min(Rs 20, 0.03% of turnover)   | flat Rs 20                      |
| STT                 | 0.05% on SELL turnover          | 0.15% on SELL premium           |
| Exchange txn (NSE)  | 0.00183% of turnover            | 0.03553% of premium             |
| SEBI charges        | Rs 10 / crore (0.0001%)         | Rs 10 / crore (0.0001%)         |
| Stamp duty          | 0.002% on BUY turnover          | 0.003% on BUY premium           |
| IPFT (NSE)          | Rs 0.01 / crore (1e-9)          | Rs 0.01 / crore (1e-9)          |
| GST                 | 18% of (brokerage + exch + SEBI + IPFT)  (same for options)     |

Each execution is charged on its own: a flip (close, then open) is two orders,
so two brokerage lines. Every line is rounded to the paisa, as the contract
note shows it. Turnover = units x price (units = lots x lot size).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

BROKERAGE_RATE = Decimal("0.0003")        # 0.03%
BROKERAGE_CAP = Decimal("20")             # Rs per executed order
FUT_STT_SELL = Decimal("0.0005")          # 0.05% on sell turnover
OPT_STT_SELL = Decimal("0.0015")          # 0.15% on sell premium
FUT_EXCH = Decimal("0.0000183")           # 0.00183%
OPT_EXCH = Decimal("0.0003553")           # 0.03553%
SEBI_RATE = Decimal("0.000001")           # Rs 10 / crore
IPFT_RATE = Decimal("0.000000001")        # Rs 0.01 / crore
FUT_STAMP_BUY = Decimal("0.00002")        # 0.002%
OPT_STAMP_BUY = Decimal("0.00003")        # 0.003%
GST_RATE = Decimal("0.18")
PAISA = Decimal("0.01")


@dataclass(frozen=True)
class Charges:
    brokerage: Decimal
    stt: Decimal
    exchange: Decimal
    sebi: Decimal
    stamp: Decimal
    ipft: Decimal
    gst: Decimal

    @property
    def total(self) -> Decimal:
        return (self.brokerage + self.stt + self.exchange + self.sebi
                + self.stamp + self.ipft + self.gst)


def _p(x: Decimal) -> Decimal:
    return x.quantize(PAISA, rounding=ROUND_HALF_UP)


def fo_charges(*, instrument: str, side: str, units: int, price: float) -> Charges:
    """Charges on one executed order. `instrument` is "future" or "option";
    `side` is "BUY" or "SELL"; `units` the contract quantity (lots x lot size);
    `price` the fill price (premium for options)."""
    instrument = instrument.lower()
    side = side.upper()
    if instrument not in ("future", "option"):
        raise ValueError(f"unknown instrument: {instrument}")
    if side not in ("BUY", "SELL"):
        raise ValueError(f"unknown side: {side}")
    if units <= 0 or price <= 0:
        raise ValueError("units and price must be positive")

    turnover = Decimal(int(units)) * Decimal(str(price))
    is_fut = instrument == "future"
    sell = side == "SELL"

    brokerage = BROKERAGE_CAP if not is_fut else min(BROKERAGE_CAP, turnover * BROKERAGE_RATE)
    stt = turnover * (FUT_STT_SELL if is_fut else OPT_STT_SELL) if sell else Decimal(0)
    exchange = turnover * (FUT_EXCH if is_fut else OPT_EXCH)
    sebi = turnover * SEBI_RATE
    stamp = Decimal(0) if sell else turnover * (FUT_STAMP_BUY if is_fut else OPT_STAMP_BUY)
    ipft = turnover * IPFT_RATE

    brokerage, exchange, sebi, ipft = _p(brokerage), _p(exchange), _p(sebi), _p(ipft)
    stt, stamp = _p(stt), _p(stamp)
    gst = _p((brokerage + exchange + sebi + ipft) * GST_RATE)
    return Charges(brokerage=brokerage, stt=stt, exchange=exchange, sebi=sebi,
                   stamp=stamp, ipft=ipft, gst=gst)


if __name__ == "__main__":
    # NIFTY future, 1 lot (65) at 24000: turnover 15,60,000.
    b = fo_charges(instrument="future", side="BUY", units=65, price=24000)
    assert b.brokerage == Decimal("20.00") and b.exchange == Decimal("28.55")
    assert b.sebi == Decimal("1.56") and b.stamp == Decimal("31.20")
    assert b.ipft == Decimal("0.00") and b.stt == 0 and b.gst == Decimal("9.02")
    assert b.total == Decimal("90.33"), b.total
    s = fo_charges(instrument="future", side="SELL", units=65, price=24000)
    assert s.stt == Decimal("780.00") and s.stamp == 0 and s.total == Decimal("839.13"), s.total
    # Small future order: brokerage is 0.03% when that is under Rs 20.
    small = fo_charges(instrument="future", side="BUY", units=10, price=100)
    assert small.brokerage == Decimal("0.30"), small.brokerage
    # Option, 1 lot premium 100: turnover 6,500.
    o = fo_charges(instrument="option", side="SELL", units=65, price=100)
    assert o.brokerage == Decimal("20.00") and o.stt == Decimal("9.75")
    assert o.exchange == Decimal("2.31") and o.stamp == 0
    print("zerodha charges self-check ok")
