# instruments.py
"""Broker contract details for the six indices strategy v3 trades."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from typing import Optional

from config import INDEX_EXPIRY, IndexExpiry
from strategy_logic import LOTS, NAMES as STRATEGY_NAMES


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    return raw.strip() if raw and raw.strip() else default


@dataclass(frozen=True)
class Instrument:
    """Everything the bot needs to know about one tradeable index."""

    #: The strategy's index name. Must appear in `strategy_logic.NAMES`.
    name: str
    #: Where the index level is quoted, and where its 5-minute candles come from.
    spot_exchange: str
    #: The name `get_quotes` resolves to a spot token (a Shoonya quirk: the
    #: `token` argument accepts the instrument *name* for an index).
    spot_symbol: str
    #: Where its derivatives trade. NSE indices are on NFO, SENSEX is on BSE's
    #: BFO - a different exchange, and the reason the order path can no longer
    #: hardcode one.
    fo_exchange: str
    #: The root of an option/future tradingsymbol, e.g. NIFTY26FEB24150PE.
    option_root: str
    #: Distance between listed strikes, in index points. Only the fallback:
    #: the strike is read from the exchange's listing (`getdata.GetSymbol`),
    #: and this grid is used when that listing cannot be read.
    strike_step: int
    #: Units in one order lot. Starts as the strategy's lot and follows the
    #: exchange's listed one (`set_lot`).
    quantity: int
    #: Where the legs sit is NOT a field here: `getdata.GetSymbol` builds them
    #: from the live price, this grid and the expiry calendar below.

    #: When this index's contracts expire, from `config.INDEX_EXPIRY`. Cadence
    #: and weekday are exchange facts that move, so they live in config rather
    #: than here; a wrong answer builds an unlisted symbol, which fails the
    #: scrip-master check and refuses the trade rather than mis-trading it.
    expiry: IndexExpiry

    @property
    def weekly_options(self) -> bool:
        """True while the exchange lists weekly options on this index."""
        return self.expiry.option_cadence == "weekly"

    @property
    def option_expiry_weekday(self) -> str:
        return self.expiry.option_weekday

    @property
    def future_expiry_weekday(self) -> str:
        return self.expiry.future_weekday


def subscription_key(instrument: "Instrument", token: str) -> str:
    """The websocket subscription key for an instrument's spot feed."""
    return f"{instrument.spot_exchange}|{token}"


#: Order quantity is taken directly from the frozen v3 strategy.
INSTRUMENTS: dict[str, Instrument] = {
    "NIFTY": Instrument(
        name="NIFTY",
        spot_exchange="NSE",
        spot_symbol=_env_str("LUNE_SPOT_NIFTY", "Nifty 50"),
        fo_exchange="NFO",
        option_root="NIFTY",
        strike_step=50,
        quantity=LOTS["NIFTY"],
        expiry=INDEX_EXPIRY["NIFTY"],
    ),
    "BANKNIFTY": Instrument(
        name="BANKNIFTY",
        spot_exchange="NSE",
        spot_symbol=_env_str("LUNE_SPOT_BANKNIFTY", "Nifty Bank"),
        fo_exchange="NFO",
        option_root="BANKNIFTY",
        strike_step=100,
        quantity=LOTS["BANKNIFTY"],
        expiry=INDEX_EXPIRY["BANKNIFTY"],
    ),
    "SENSEX": Instrument(
        name="SENSEX",
        spot_exchange="BSE",
        spot_symbol=_env_str("LUNE_SPOT_SENSEX", "SENSEX"),
        fo_exchange="BFO",
        option_root="SENSEX",
        strike_step=100,
        quantity=LOTS["SENSEX"],
        expiry=INDEX_EXPIRY["SENSEX"],
    ),
    "FINNIFTY": Instrument(
        name="FINNIFTY",
        spot_exchange="NSE",
        spot_symbol=_env_str("LUNE_SPOT_FINNIFTY", "Nifty Fin Service"),
        fo_exchange="NFO",
        option_root="FINNIFTY",
        strike_step=50,
        quantity=LOTS["FINNIFTY"],
        expiry=INDEX_EXPIRY["FINNIFTY"],
    ),
    "MIDCPNIFTY": Instrument(
        name="MIDCPNIFTY",
        spot_exchange="NSE",
        spot_symbol=_env_str("LUNE_SPOT_MIDCPNIFTY", "NIFTY MID SELECT"),
        fo_exchange="NFO",
        option_root="MIDCPNIFTY",
        strike_step=25,
        quantity=LOTS["MIDCPNIFTY"],
        expiry=INDEX_EXPIRY["MIDCPNIFTY"],
    ),
    "NIFTYNXT50": Instrument(
        name="NIFTYNXT50",
        spot_exchange="NSE",
        spot_symbol=_env_str("LUNE_SPOT_NIFTYNXT50", "Nifty Next 50"),
        fo_exchange="NFO",
        option_root="NIFTYNXT50",
        # NSE lists NIFTYNXT50 strikes 100 points apart.
        strike_step=100,
        quantity=LOTS["NIFTYNXT50"],
        expiry=INDEX_EXPIRY["NIFTYNXT50"],
    ),
}

#: Console order, taken from the strategy so the two can never disagree about
#: which indices exist. The waterfall order is v3's own (`strategy_v2.WATERFALL`).
NAMES: tuple[str, ...] = STRATEGY_NAMES

if set(NAMES) != set(INSTRUMENTS):
    raise RuntimeError(
        "instrument registry %s does not match the strategy's own %s; the "
        "strategy universe and tradeable universe must match"
        % (sorted(INSTRUMENTS), sorted(NAMES))
    )
if set(INDEX_EXPIRY) != set(INSTRUMENTS):
    raise RuntimeError(
        "config.INDEX_EXPIRY %s does not cover every tradeable index %s; an "
        "index with no expiry calendar has no contract to trade"
        % (sorted(INDEX_EXPIRY), sorted(INSTRUMENTS))
    )

#: The index the chart and the single-symbol console routes display. The book
#: holds one position across all six; this only decides what is drawn.
CHART_INDEX = _env_str("LUNE_CHART_INDEX", "NIFTY")
if CHART_INDEX not in INSTRUMENTS:
    raise ValueError(f"LUNE_CHART_INDEX must be one of {sorted(INSTRUMENTS)}")


def get(name: Optional[str]) -> Instrument:
    """The instrument record for a strategy index name.

    Raises rather than defaulting. A caller that cannot say which index it means
    is a caller about to send an order to a guessed exchange.
    """
    if name not in INSTRUMENTS:
        raise KeyError(f"unknown index {name!r}; expected one of {sorted(INSTRUMENTS)}")
    return INSTRUMENTS[name]


def quantity_of(name: Optional[str], default: int) -> int:
    """Order quantity for an index, falling back to `default` for a legacy row.

    Trade rows written before the book became multi-index carry no index name.
    Every one of those is NIFTY - it was the only index that could be traded -
    so the caller passes its configured NIFTY size as the default rather than
    refusing to size a position it is holding.
    """
    try:
        return get(name).quantity
    except KeyError:
        return int(default)


def set_lot(name: str, lot: int) -> None:
    """Size `name` on the exchange's listed lot (`getdata.GetSymbol._adopt_lot`).

    The strategy's own lot is only the starting value: the exchange revises
    lots, and an order that is not a multiple of the listed one is rejected.
    """
    if type(lot) is not int or lot <= 0:
        raise ValueError(f"{name}: lot must be a positive integer, not {lot!r}")
    INSTRUMENTS[name] = replace(get(name), quantity=lot)


def of_symbol(symbol: Optional[str]) -> Optional[Instrument]:
    """The instrument a derivatives tradingsymbol belongs to, by its root.

    Longest root first, so a NIFTYNXT50 contract is never read as NIFTY. Lets
    an order for a symbol route to its own exchange even when no position is
    held - a stray SENSEX leg is on BFO whatever the manager last traded.
    """
    text = str(symbol or "")
    for inst in sorted(INSTRUMENTS.values(), key=lambda i: -len(i.option_root)):
        if text.startswith(inst.option_root):
            return inst
    return None


def fo_exchange_of(name: Optional[str], default: str = "NFO") -> str:
    """Derivatives exchange for an index, falling back for a legacy row.

    Same reasoning as `quantity_of`: a row with no index is a NIFTY row, and
    NIFTY's derivatives are on NFO.
    """
    try:
        return get(name).fo_exchange
    except KeyError:
        return default


__all__ = [
    "CHART_INDEX",
    "INSTRUMENTS",
    "Instrument",
    "NAMES",
    "fo_exchange_of",
    "get",
    "of_symbol",
    "quantity_of",
    "set_lot",
    "subscription_key",
]
