# getdata.py
"""Option and future legs for each index: which contracts, built and verified."""

from __future__ import annotations

import calendar
import csv
import io
import logging
import math
import threading
import time
import zipfile
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Union

import requests

import instruments
import ist
import log_throttle
from config import parse_holiday
from log_throttle import LogThrottle

if TYPE_CHECKING:
    from instruments import Instrument
    from shoonya_api import ShoonyaApiPy

# Leg-building runs once a second; a persistent fault must be visible without
# writing a line per second. See log_throttle.py.
_THROTTLE = LogThrottle()

#: Why no legs are published while the strategy wants none - not a fault, so
#: `legs_status` reads it as idle rather than blocked.
_IDLE_REASON = "the strategy has no intended entry, so no legs were requested"

#: Trading sessions an option must have left after the entry day (v3's
#: README, "Choosing the option": the nearest expiry with at least two trading
#: days left, never held overnight into an expiry). v3 sells at the next
#: session's 09:15 open, so the position is always closed before expiry day.
MIN_SESSIONS_TO_EXPIRY = 2


#: Furthest expiry an entry may take, in calendar days. The next monthly is at
#: most five weeks out; past that the nearest listed series is a quarterly,
#: which is not what v3 trades, so the entry is refused instead.
MAX_DAYS_TO_EXPIRY = 45

#: Cap on how far the chosen strike may sit from the live price, in configured
#: strike steps. The listing decides the strike; this only refuses a series so
#: thinly listed near the money that the "nearest" strike is nowhere near it.
MAX_STRIKE_GAP_STEPS = 5


def _load_master_rows(exchange: str) -> list[dict]:
    """Shoonya's published contract master for one exchange, as rows."""
    response = requests.get(f"https://api.shoonya.com/{exchange}_symbols.txt.zip", timeout=15)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        with archive.open(f"{exchange}_symbols.txt") as source:
            return list(csv.DictReader(io.TextIOWrapper(source, encoding="utf-8-sig")))


def _index_contracts(rows, wanted: dict[str, str]) -> dict[str, dict[tuple[date, str, int], tuple[str, int]]]:
    """Index options and futures by index: (expiry, C|P|F, strike) -> (tradingsymbol, lot).

    `wanted` maps the master's `Symbol` to the index name. Expiry, strike and
    lot are all the exchange's own, so none of them has to be kept by hand.
    """
    books: dict[str, dict] = {}
    for row in rows:
        name = wanted.get(row["Symbol"])
        symbol = row["TradingSymbol"]
        if name is None or row["Instrument"] not in {"OPTIDX", "FUTIDX"} or not symbol.startswith(name):
            continue
        expiry = datetime.strptime(row["Expiry"], "%d-%b-%Y").date()
        kind = "F" if row["Instrument"] == "FUTIDX" else row["OptionType"][0]
        key = (expiry, kind, int(float(row["StrikePrice"])))
        contract = (symbol, int(row["LotSize"]))
        book = books.setdefault(name, {})
        if book.get(key, contract) != contract:
            raise ValueError(f"ambiguous {name} contract for {key}")
        book[key] = contract
    return books


def _load_bfo_contracts() -> dict[tuple[date, str, int], tuple[str, int]]:
    """Read Shoonya's published BFO contracts; its weekly and monthly names differ."""
    contracts = _index_contracts(
        _load_master_rows("BFO"), {"BSXOPT": "SENSEX", "BSXFUT": "SENSEX"}).get("SENSEX")
    if not contracts:
        raise ValueError("BFO symbol master contained no SENSEX contracts")
    return contracts


_NFO_LOCK = threading.Lock()
_NFO_BOOKS: dict[str, Any] = {"day": None, "books": {}}


def _load_nfo_contracts(today: date, fresh: bool = False) -> dict[str, dict]:
    """Every NFO index's contracts, read once a day and shared by all builders.

    `fresh` re-reads it: listings are added during the day.
    """
    with _NFO_LOCK:
        if fresh or _NFO_BOOKS["day"] != today:
            wanted = {i.option_root: n for n, i in instruments.INSTRUMENTS.items()
                      if i.fo_exchange == "NFO"}
            _NFO_BOOKS.update(day=today, books=_index_contracts(_load_master_rows("NFO"), wanted))
        return _NFO_BOOKS["books"]


class LegSelectionError(RuntimeError):
    """No legs can be built for the bias asked for.

    v3 is long-only, so a bearish bias has no legs.
    """


class GetSymbol:
    """
    Builds the option legs for the strategy: a synthetic long future.

    One instance per index v3 can allocate the slot to. Which index it serves,
    and everything that follows from that - the tradingsymbol root, the F&O
    exchange, the order quantity - comes from the `Instrument` record it is
    constructed with (see `instruments.py`).

    A bullish entry - the only kind v3 takes - buys the call and sells the
    put of one strike, the in-the-money strike (for the call) closest to the
    money: the highest listed strike below the live index price. Both legs are
    on the nearest expiry with `MIN_SESSIONS_TO_EXPIRY` trading sessions left
    (`_entry_expiry`). Any other bias is refused with `LegSelectionError`.

    Expiry weekday controls (Mon=0..Sun=6, or names such as "TUE"):
      - Options: option_expiry_weekday (defaults to the instrument's calendar)
      - Futures: future_expiry_weekday (defaults to the instrument's calendar)
    Roll controls (calendar days before expiry when we switch to the NEXT contract, inclusive of expiry day):
      - Options: option_roll_days_before_expiry (0 rolls on expiry day)
      - Futures: future_roll_days_before_expiry (0 rolls on expiry day)
    Holiday controls:
      - expiry_holidays: list/tuple of dates (DD-MMM-YYYY) that shift expiry to the previous day
    """

    #: Defaults for the fields `__init__` fills in, so a partially constructed
    #: instance (a test double, or one built with `__new__`) still reports which
    #: index it is rather than raising from a log statement.
    index_name: str = "NIFTY"
    fo_exch: str = "NFO"

    #: Today's exchange listing for this index: (expiry, C|P|F, strike) ->
    #: (tradingsymbol, lot), and per expiry the strikes that list both a call
    #: and a put. Empty until `_ensure_master` has read it; both are replaced
    #: whole, never changed in place.
    _contracts: dict = {}
    _strikes: Dict[date, list] = {}
    _master_day: Optional[date] = None
    _master_stale = False
    _master_retry_after = 0.0
    _master_error = "symbol master unavailable"
    _master_logged_day: Optional[date] = None

    # ------------------------------------------------------------------ #
    #  Construction & configuration                                      #
    # ------------------------------------------------------------------ #
    def __init__(
        self,
        api: ShoonyaApiPy,
        live_price_getter: Callable[[], Optional[float]],
        instrument: Instrument | None = None,
        option_expiry_weekday: Union[str, int, None] = None,
        future_expiry_weekday: Union[str, int, None] = None,
        option_roll_days_before_expiry: Union[int, str, None] = 0,
        future_roll_days_before_expiry: Union[int, str, None] = 0,
        expiry_holidays: Optional[Union[list, tuple]] = None,
    ):
        self.api = api
        self.get_live_price = live_price_getter  # real-time LTP from websocket
        self._pivot_type_getter: Optional[Callable[[], Optional[str]]] = None
        # Which index this builder trades. Everything that used to be a NIFTY
        # constant - the tradingsymbol root, the strike step, the exchange the
        # order goes to, how wide the hedge sits - is read off it, so one class
        # serves all three and none of them can pick up another's spelling.
        self.instrument = instrument if instrument is not None else instruments.get("NIFTY")
        self.index_name = self.instrument.name
        self.exch = self.instrument.spot_exchange
        self.symbol_name = self.instrument.spot_symbol
        self.fo_exch = self.instrument.fo_exchange
        self.option_root = self.instrument.option_root
        self.strike_step = int(self.instrument.strike_step)
        self.quantity = int(self.instrument.quantity)
        # Tradingsymbols the broker has confirmed it lists, and ones it has not.
        # Expiry calendars move and root spellings differ per exchange, so a
        # built symbol is a proposal until the scrip master agrees with it.
        self._symbol_checked: Dict[str, Optional[bool]] = {}
        self._symbol_retry_after: Dict[str, float] = {}
        # Expiry weekdays default to the instrument's own calendar
        # (`config.INDEX_EXPIRY`).
        self.option_expiry_weekday = self._parse_weekday(
            self.instrument.option_expiry_weekday
            if option_expiry_weekday is None else option_expiry_weekday
        )
        self.future_expiry_weekday = self._parse_weekday(
            self.instrument.future_expiry_weekday
            if future_expiry_weekday is None else future_expiry_weekday
        )
        self.option_roll_days_before_expiry = self._parse_roll_days(
            option_roll_days_before_expiry, "option_roll_days_before_expiry"
        )
        self.future_roll_days_before_expiry = self._parse_roll_days(
            future_roll_days_before_expiry, "future_roll_days_before_expiry"
        )
        self.expiry_holidays = self._parse_holidays(expiry_holidays or [])

        self.tok: Optional[str] = None
        self.final_symbol: Optional[str] = None              # legacy single-leg (kept = sell leg)
        self.final_symbols: Optional[Dict[str, str]] = None  # {"buy":..., "sell":...}
        # Why the last `update_symbol` did not publish legs, or None when it did.
        # Read by the trade managers so a refused entry names its cause.
        self.legs_blocked_reason: Optional[str] = None
        self._no_price_since: float | None = None

    # ------------------------------------------------------------------ #
    #  Helpers                                                           #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _parse_weekday(wd: Union[str, int]) -> int:
        """
        Convert weekday to int (Mon=0..Sun=6).
        Accepts:
          - int in 0..6
          - short/long names: 'MON', 'Monday', 'thu', etc.
        """
        if isinstance(wd, int):
            if 0 <= wd <= 6:
                return wd
            raise ValueError("expiry_weekday int must be in 0..6 (Mon=0)")

        s = str(wd).strip().lower()
        MAP = {
            "mon": 0, "monday": 0,
            "tue": 1, "tues": 1, "tuesday": 1,
            "wed": 2, "wednesday": 2,
            "thu": 3, "thurs": 3, "thursday": 3,
            "fri": 4, "friday": 4,
            "sat": 5, "saturday": 5,
            "sun": 6, "sunday": 6,
        }
        if s in MAP:
            return MAP[s]
        raise ValueError(f"Unrecognized expiry_weekday: {wd!r}")

    @staticmethod
    def _parse_roll_days(value: Union[int, str, None], label: str) -> int:
        if value is None:
            return 0
        try:
            days = int(value)
        except (TypeError, ValueError):
            raise ValueError(f"{label} must be a non-negative integer")
        if days < 0:
            raise ValueError(f"{label} must be a non-negative integer")
        return days

    @staticmethod
    def _parse_holidays(values: Union[list, tuple]) -> set[date]:
        holidays: set[date] = set()
        for v in values:
            if not str(v).strip():
                continue
            day = parse_holiday(v)
            if day is None:
                raise ValueError(f"Unrecognized holiday date format: {v!r}")
            holidays.add(day)
        return holidays

    def _apply_holiday_shift(self, expiry: date) -> date:
        """Shift expiry backward while it falls on a holiday."""
        adj = expiry
        while adj in self.expiry_holidays:
            adj = adj - timedelta(days=1)
        return adj

    def _get_expiry_date(self, current_date: date) -> date:
        """
        Upcoming option expiry for the configured weekday (same or next).
        Mon=0..Sun=6

        Weekly where the exchange lists weeklies, monthly where it does not -
        BANKNIFTY's weeklies were withdrawn, so asking for one builds a symbol
        that has never existed. `_resolve_tradingsymbol` is what catches it if
        this answer is wrong.
        """
        if not self.instrument.weekly_options:
            # Monthly options expire on the LAST occurrence of the option
            # weekday in the month. Read off the option calendar, not the
            # future's: the two need not share a weekday, and BANKNIFTY only
            # happens to.
            return self._monthly_expiry(
                current_date, self.option_expiry_weekday,
                self.option_roll_days_before_expiry,
            )
        target = self.option_expiry_weekday
        today_wd = current_date.weekday()
        days_ahead = target - today_wd
        if days_ahead < 0:
            days_ahead += 7
        base_expiry = current_date + timedelta(days=days_ahead)  # scheduled weekday
        expiry = self._apply_holiday_shift(base_expiry)
        if expiry < current_date:
            base_expiry = base_expiry + timedelta(days=7)
            expiry = self._apply_holiday_shift(base_expiry)
        roll_days = self.option_roll_days_before_expiry
        roll_start = expiry - timedelta(days=roll_days)
        if current_date >= roll_start:
            next_base = base_expiry + timedelta(days=7)
            expiry = self._apply_holiday_shift(next_base)
        return expiry

    def _format_symbol(self, yyMMdd_exp: str, opt_type: str, strike: int) -> str:
        # <ROOT><DDMONYY><C/P><strike>, e.g. NIFTY24DEC25C25000
        return f"{self.option_root}{yyMMdd_exp}{opt_type}{strike}"

    @staticmethod
    def _last_weekday_of_month(target_wd: int, year: int, month: int) -> date:
        """
        Last occurrence of target weekday (Mon=0..Sun=6) within the month.
        """
        last_day = date(year, month, calendar.monthrange(year, month)[1])
        offset = (last_day.weekday() - target_wd) % 7
        return last_day - timedelta(days=offset)

    def _monthly_expiry(self, current_date: date, target: int, roll_days: int) -> date:
        """
        Last occurrence of `target` weekday in the month; if already passed (or
        inside the roll window), the same weekday in the next month.
        """
        this_month_exp = self._last_weekday_of_month(target, current_date.year, current_date.month)
        this_month_exp = self._apply_holiday_shift(this_month_exp)
        if current_date.month == 12:
            next_year, next_month = current_date.year + 1, 1
        else:
            next_year, next_month = current_date.year, current_date.month + 1
        next_month_exp = self._apply_holiday_shift(self._last_weekday_of_month(target, next_year, next_month))

        if this_month_exp < current_date:
            return next_month_exp

        expiry = this_month_exp
        roll_start = expiry - timedelta(days=roll_days)
        if current_date >= roll_start:
            expiry = next_month_exp
        return expiry

    # ------------------------------------------------------------------ #
    #  Public API                                                        #
    # ------------------------------------------------------------------ #
    def set_pivot_type_getter(self, getter: Optional[Callable[[], Optional[str]]]) -> None:
        """Set the directional-bias getter.

        Returns 'HH' for bullish, 'LL' for bearish, or None to keep the current
        legs. See `_latest_forecast_pivot`: this is a direction, not a pivot
        type, and the two are opposites under this strategy.
        """
        self._pivot_type_getter = getter

    def fetch_exch_tok(self) -> tuple[str, str]:
        """
        Fetch exchange & token once for the configured spot index.
        (Shoonya quirk: this uses get_quotes with symbol name to resolve token.)

        Falls back to a scrip-master search when the name lookup fails. The
        six indices are named differently and on two exchanges - "Nifty 50"
        and "Nifty Bank" on NSE, "SENSEX" on BSE - and a name the broker spells
        differently would otherwise leave that index with no feed at all, which
        reads downstream as "this index never qualifies" rather than as a fault.
        """
        try:
            ret = self.api.get_quotes(exchange=self.exch, token=self.symbol_name)
            token = (ret or {}).get("token")
        except Exception as exc:  # noqa: BLE001 - broker/network errors are expected
            logging.warning(
                "[GetSymbol:%s] get_quotes(%s, %r) failed: %s; falling back to a "
                "scrip search.", self.index_name, self.exch, self.symbol_name, exc,
            )
            token = None
        if not token:
            token = self._search_spot_token()
        if not token:
            raise RuntimeError(
                f"could not resolve a spot token for {self.index_name} "
                f"({self.exch} {self.symbol_name!r}); set "
                f"LUNE_SPOT_{self.index_name} to the name your broker uses"
            )
        self.tok = str(token)
        logging.info(
            "[GetSymbol:%s] Resolved spot token %s for %s '%s'",
            self.index_name, self.tok, self.exch, self.symbol_name,
        )
        return self.exch, self.tok

    def _search_spot_token(self) -> Optional[str]:
        """Resolve the spot token from the broker's scrip master by name."""
        try:
            found = self.api.searchscrip(exchange=self.exch, searchtext=self.symbol_name)
        except Exception as exc:  # noqa: BLE001
            logging.error("[GetSymbol:%s] scrip search failed: %s", self.index_name, exc)
            return None
        for row in self._scrip_rows(found):
            tsym = str(row.get("tsym") or "").strip()
            if tsym.casefold() == str(self.symbol_name).casefold() and row.get("token"):
                return str(row["token"])
        # No exact name match: the first row of an index search is the index
        # itself often enough to be useful, but guessing is what this method
        # exists to avoid, so it is reported rather than used.
        names = [str(r.get("tsym")) for r in self._scrip_rows(found)][:5]
        logging.error(
            "[GetSymbol:%s] no exact scrip match for %r on %s; candidates were %s",
            self.index_name, self.symbol_name, self.exch, names or "none",
        )
        return None

    @staticmethod
    def _scrip_rows(payload) -> list:
        """The rows of a SearchScrip response, whatever shape it arrived in."""
        if isinstance(payload, dict):
            values = payload.get("values")
            return [r for r in values if isinstance(r, dict)] if isinstance(values, list) else []
        if isinstance(payload, list):
            return [r for r in payload if isinstance(r, dict)]
        return []

    def _ensure_master(self) -> Optional[str]:
        """Read today's exchange listing for this index; why not, when it cannot be read.

        The listing is what makes expiry, strike and lot follow the exchange
        on their own: a moved expiry, a re-spaced strike grid or a revised lot
        arrives in the master before it can be traded, and is used from there.
        """
        today = ist.today()
        if self._master_day == today:
            return None
        now = time.monotonic()
        if now < self._master_retry_after:
            return self._master_error
        self._master_retry_after = now + 60
        try:
            book = (_load_bfo_contracts() if self.fo_exch == "BFO"
                    else _load_nfo_contracts(today, self._master_stale).get(self.index_name))
            if not book:
                raise ValueError(f"it lists no {self.index_name} contracts")
        except Exception as exc:
            # Yesterday's listing is not kept: a stale one would hide exactly
            # the changes this exists to pick up.
            self._contracts, self._strikes = {}, {}
            self._master_error = f"{self.fo_exch} symbol master unavailable: {exc}"
            return self._master_error
        strikes: Dict[date, list] = {}
        for expiry, kind, strike in book:
            if kind == "C" and (expiry, "P", strike) in book:
                strikes.setdefault(expiry, []).append(strike)
        self._contracts = book
        self._strikes = {expiry: sorted(ks) for expiry, ks in strikes.items()}
        self._master_day, self._master_stale = today, False
        self._adopt_listing(today)
        return None

    def _adopt_listing(self, today: date) -> None:
        """Size on the lot of the contract an entry would take today, and say so
        once when the listing overrules the configured calendar."""
        try:
            expiry = self._entry_expiry(today)
            planned = self._calendar_expiry(today)
        except LegSelectionError:
            return
        lot = next((lot for (e, kind, _k), (_s, lot) in self._contracts.items()
                    if e == expiry and kind != "F"), None)
        if lot is None:
            return
        self._adopt_lot(lot)
        if planned != expiry and self._master_logged_day != today:
            logging.warning(
                "[GetSymbol:%s] the configured calendar expects a %s expiry but %s "
                "lists %s next; trading the listed one. Check config.INDEX_EXPIRY "
                "and expiry_holidays.", self.index_name, planned, self.fo_exch, expiry,
            )
        self._master_logged_day = today

    def _adopt_lot(self, lot: int) -> None:
        """Make the exchange's lot the size every new order on this index uses."""
        current = instruments.get(self.index_name).quantity
        if lot != current:
            logging.warning(
                "[GetSymbol:%s] %s lists a lot of %d, not %d; new orders on this "
                "index are now sized on %d.", self.index_name, self.fo_exch, lot, current, lot,
            )
            instruments.set_lot(self.index_name, lot)
        self.quantity = lot

    def _resolve_listed_legs(self, legs: dict) -> Optional[str]:
        """Replace proposed names with the exchange's own trading symbols and lot."""
        error = self._ensure_master()
        if error:
            return error
        resolved, lots = {}, set()
        for side in ("buy", "sell"):
            key = self._contract_key(legs[side])
            contract = self._contracts.get(key)
            if contract is None:
                # New listings can appear during the day: read it again.
                self._master_day, self._master_stale = None, True
                self._master_error = f"{self.fo_exch} master has no {self.index_name} contract for {key}"
                return self._master_error
            resolved[side], lot = contract
            lots.add(lot)
        if len(lots) != 1 or lot <= 0:
            return (f"{self.fo_exch} master lists unusable lots {sorted(lots)} for "
                    f"{resolved['buy']} and {resolved['sell']}")
        legs.update(resolved)
        legs["quantity"] = lot
        self._adopt_lot(lot)
        return None

    def _contract_key(self, proposed: str) -> tuple[date, str, int]:
        """The master's key for a symbol built as <ROOT><DDMONYY><P|C|F><strike>."""
        suffix = proposed[len(self.option_root):]
        expiry = datetime.strptime(suffix[:7], "%d%b%y").date()
        future = suffix[7:] == "F"
        return expiry, "F" if future else suffix[7:8], 0 if future else int(suffix[8:])

    def _resolve_tradingsymbol(self, symbol: str) -> Optional[bool]:
        """True if listed, False if absent, None if lookup is unavailable.

        This is the guard that turns an expiry-calendar guess into a refused
        trade instead of a wrong order. Every field the symbol is built from -
        the root spelling, the strike step, the expiry weekday, weekly versus
        monthly - is a fact about the exchange that can change without notice,
        and NSE and BSE have each moved their expiry day more than once. Rather
        than trust the arithmetic, the built symbol is looked up in the broker's
        own scrip master; if it is not there, no legs are published.

        Successful names are cached; missing names and failed lookups are
        checked again after a minute, since listings and availability can change.
        """
        cached = self._symbol_checked.get(symbol)
        if cached is True or time.monotonic() < self._symbol_retry_after.get(symbol, 0):
            return cached
        try:
            found = self.api.searchscrip(exchange=self.fo_exch, searchtext=symbol)
        except Exception as exc:  # noqa: BLE001
            self._symbol_checked[symbol] = None
            self._symbol_retry_after[symbol] = time.monotonic() + 60
            emit, folded = _THROTTLE.should_log(f"scrip_error:{self.index_name}")
            if emit:
                logging.error(
                    "[GetSymbol:%s] scrip lookup for %s failed: %s; retrying in 60s.%s",
                    self.index_name, symbol, exc, log_throttle.suffix(folded),
                )
            return None
        if found is None:
            self._symbol_checked[symbol] = None
            self._symbol_retry_after[symbol] = time.monotonic() + 60
            return None
        listed = any(str(row.get("tsym") or "").strip() == symbol
                     for row in self._scrip_rows(found))
        self._symbol_checked[symbol] = listed
        if not listed:
            self._symbol_retry_after[symbol] = time.monotonic() + 60
            logging.error(
                "[GetSymbol:%s] the broker does not list %r on %s. No legs will "
                "be published for %s until this resolves, so no order can be "
                "placed against a contract that does not exist. Check "
                "config.INDEX_EXPIRY['%s'] - currently %s options on %s, "
                "monthly future on %s - and the %d-point strike step.",
                self.index_name, symbol, self.fo_exch, self.index_name,
                self.index_name, self.instrument.expiry.option_cadence,
                self.instrument.expiry.option_weekday,
                self.instrument.expiry.future_weekday, self.strike_step,
            )
        return listed

    def _latest_forecast_pivot(self) -> Optional[str]:
        """Return the directional bias to build option legs for: "HH" or "LL".

        Despite the names, this is a *direction* label, not a pivot type:
        `update_symbol` reads "HH" as bullish (v3's synthetic long) and "LL" as
        bearish, which v3 never trades. Under the strategy these are the opposite of the
        pivot that produced them - a confirmed HH pivot is a SHORT - so the
        label must come from the execution engine's intended side, which is what
        the injected getter supplies.

        There is deliberately no fallback that reads the raw pivot stream. The
        old one returned the most recent pivot type directly, which under this
        strategy selects wrong-way strikes. Failing closed leaves the previously
        built legs in place and logs, which is recoverable; buying the wrong side
        is not.
        """
        if not self._pivot_type_getter:
            logging.error(
                "[GetSymbol:%s] no direction getter configured; refusing to guess "
                "option legs from the raw pivot stream.", self.index_name,
            )
            return None
        try:
            bias = self._pivot_type_getter()
        except Exception as exc:
            logging.warning(f"[GetSymbol:{self.index_name}] direction getter failed: {exc}")
            return None
        bias = (bias or "").strip().upper()
        return bias if bias in {"HH", "LL"} else None

    # ------------------------------------------------------------------ #
    #  Leg building                                                      #
    # ------------------------------------------------------------------ #
    def _atm_strike(self, spot: float, expiry: Optional[date] = None) -> int:
        """The at-the-money strike: the listed strike nearest the live price.

        `spot` is the WebSocket LTP that `update_symbol` just read, so this is
        the ATM the market is at right now, not the one it opened at.

        Rounds half up rather than through `round()`, whose banker's rounding
        sends a price exactly between two strikes to the even one - a coin
        flip decided by the strike grid rather than by the market.
        """
        listed = self._strikes.get(expiry)
        if listed:
            return min(listed, key=lambda k: (abs(k - spot), -k))
        step = self.strike_step
        return int(math.floor(spot / step + 0.5) * step)

    def _listed_itm_strike(self, spot: float, expiry: Optional[date], below: bool) -> Optional[int]:
        """The listed strike nearest `spot` on one side of it, for `expiry`.

        None when the listing for that expiry has not been read, so the caller
        falls back to the configured grid. The grid is not assumed: a series
        can be spaced wider than the front month (FINNIFTY's next month lists
        100s, not 50s), and the exchange re-spaces them as the index moves.
        """
        listed = self._strikes.get(expiry)
        if not listed:
            return None
        strike = (max if below else min)(
            (k for k in listed if (k < spot if below else k > spot)), default=None)
        reach = MAX_STRIKE_GAP_STEPS * self.strike_step
        if strike is None or abs(spot - strike) > reach:
            raise LegSelectionError(
                f"{self.index_name} {expiry} lists no call-and-put strike within {reach} "
                f"points {'below' if below else 'above'} {spot:.2f}")
        return strike

    def _itm_call_strike(self, spot: float, expiry: Optional[date] = None) -> int:
        """The in-the-money call strike closest to the money: the highest
        listed strike strictly below `spot`. A price exactly on a strike is at
        the money, so the strike one step below is taken."""
        listed = self._listed_itm_strike(spot, expiry, below=True)
        if listed is not None:
            return listed
        step = self.strike_step
        return int(math.ceil(spot / step) * step - step)

    def _is_session(self, day: date) -> bool:
        """True on a weekday that is not a configured exchange holiday."""
        return day.weekday() < 5 and day not in self.expiry_holidays

    def _sessions_after(self, day: date, expiry: date) -> int:
        """Trading sessions after `day`, up to and including `expiry`."""
        return sum(self._is_session(day + timedelta(days=n))
                   for n in range(1, (expiry - day).days + 1))

    def _entry_expiry(self, today: date) -> date:
        """The nearest option expiry with `MIN_SESSIONS_TO_EXPIRY` sessions left.

        Taken from the exchange's listing once it has been read, so a holiday
        shift or a moved expiry day needs no config change; the configured
        calendar (`_calendar_expiry`) answers until then.
        """
        for expiry in sorted(self._strikes):
            if (expiry - today).days > MAX_DAYS_TO_EXPIRY:
                break
            if self._sessions_after(today, expiry) >= MIN_SESSIONS_TO_EXPIRY:
                return expiry
        return self._calendar_expiry(today)

    def _calendar_expiry(self, today: date) -> date:
        """`_entry_expiry` from `config.INDEX_EXPIRY` and the holiday list alone.

        `_get_expiry_date` asked from an expiry returns the one after it, so
        this walks the calendar forward until one is far enough out.
        """
        expiry = self._get_expiry_date(today)
        for _ in range(12):
            if self._sessions_after(today, expiry) >= MIN_SESSIONS_TO_EXPIRY:
                return expiry
            expiry = self._get_expiry_date(expiry)
        raise LegSelectionError(
            f"no {self.index_name} expiry within reach has {MIN_SESSIONS_TO_EXPIRY} "
            f"sessions left after {today}")

    def _build_legs(self, current_price: float, pivot_type: str) -> Dict[str, Any]:
        """The legs for `pivot_type` at `current_price`. Changes no state.

        Only "HH" has legs - v3's synthetic long future
        (`_build_synthetic_legs`). Anything else raises `LegSelectionError`,
        which `update_symbol` turns into a named refusal rather than an order
        v3 never asked for.
        """
        if pivot_type != "HH":
            raise LegSelectionError(
                f"v3 is long-only; there are no {pivot_type!r} legs for {self.index_name}")
        return self._build_synthetic_legs(current_price)

    def _build_synthetic_legs(self, current_price: float) -> Dict[str, Any]:
        """A synthetic long future at the live price. Changes no state.

        `buy` is the call and `sell` the put of the same strike and expiry:
        the in-the-money call strike closest to the money
        (`_itm_call_strike`), on the nearest expiry with
        `MIN_SESSIONS_TO_EXPIRY` sessions left (`_entry_expiry`). One lot of
        each, the call bought first (`trade_manager._place_hedge_seq`).
        """
        expiry = self._entry_expiry(ist.today())
        strike = self._itm_call_strike(current_price, expiry)
        if strike <= 0:
            raise LegSelectionError(
                f"no strike below {current_price:.2f} on {self.index_name}'s "
                f"{self.strike_step}-point grid")
        code = expiry.strftime("%d%b%y").upper()
        legs = {
            "buy": self._format_symbol(code, "C", strike),
            "sell": self._format_symbol(code, "P", strike),
            "sell_side": "S",
            "bias": "HH",
            # The legs travel with the index and exchange they belong to, so a
            # manager holding several builders can never pair one index's legs
            # with another's order routing.
            "index": self.index_name,
            "exchange": self.fo_exch,
            "quantity": self.quantity,
            "atm_strike": self._atm_strike(current_price, expiry),
            "strike": strike,
            "expiry": expiry.isoformat(),
        }
        emit, folded = _THROTTLE.should_log(f"legs_v3:{self.index_name}")
        if emit:
            logging.info(
                "[GetSymbol:%s] synthetic long at live %.2f: buy %s, sell %s (strike %d, "
                "expiry %s).%s",
                self.index_name, current_price, legs["buy"], legs["sell"], strike,
                expiry.isoformat(), log_throttle.suffix(folded),
            )
        return legs

    def update_symbol(self) -> Optional[Dict[str, str]]:
        """
        Called every second (main.py).
        Uses WebSocket LTP for price, latest forecasted pivot, and builds BOTH legs.

        Returns:
            {"buy": "<sym>", "sell": "<sym>"}  or previously computed dict if not ready.
        """
        # Resolve token once
        if self.tok is None:
            self.fetch_exch_tok()

        # Read today's listing even while idle, so the lot the console and the
        # managers size on is the exchange's before any entry is wanted.
        listed = self.fo_exch in ("NFO", "BFO")
        if listed:
            self._ensure_master()

        # Directional bias first ("HH" = bullish, "LL" = bearish - a direction,
        # not a pivot type). With no entry able to fill there is nothing to
        # build and no price to demand: before the open, after the last fill
        # window, and after 15:15 when the index stops printing `lp`. Asking
        # for the price first reported every one of those as "every entry on
        # this index will be skipped".
        pivot_type = self._latest_forecast_pivot()
        if pivot_type not in {"HH", "LL"}:
            # Not a fault: the strategy is flat and wants no legs built. Most
            # polls land here, so it is deliberately silent - but the previous
            # legs stay published, which is why `bias` and `built_at` travel
            # with them and the managers refuse ones that are not current.
            self.legs_blocked_reason = _IDLE_REASON
            self._no_price_since = None
            _THROTTLE.reset(f"no_price:{self.index_name}")
            return self.final_symbols

        # Real-time price
        current_price = float(self.get_live_price() or 0.0)
        price_at = time.monotonic()
        if current_price <= 0.0:
            # A dead price feed blocks every entry downstream: without legs the
            # trade managers have no symbol to trade. Record why, so the manager
            # that refuses the entry can name the cause instead of reporting an
            # unexplained "symbol not ready".
            self.legs_blocked_reason = (
                "no live price from the WebSocket or its REST quote fallback"
            )
            self.final_symbols = None
            self.final_symbol = None
            if getattr(self, "_no_price_since", None) is None:
                self._no_price_since = time.monotonic()
            if time.monotonic() - self._no_price_since >= 15:
                emit, folded = _THROTTLE.should_log(f"no_price:{self.index_name}")
                if emit:
                    logging.warning(
                        "[GetSymbol:%s] %s; option legs cannot be built, so every "
                        "entry on this index will be skipped until ticks resume.%s",
                        self.index_name,
                        self.legs_blocked_reason,
                        log_throttle.suffix(folded),
                    )
            return self.final_symbols
        self._no_price_since = None
        _THROTTLE.reset(f"no_price:{self.index_name}")

        try:
            legs = self._build_legs(current_price, pivot_type)
        except LegSelectionError as exc:
            # No legs for this bias or price. Refuse and name the reason.
            return self._block_entries(str(exc), "no_legs")
        _THROTTLE.reset(f"no_legs:{self.index_name}")

        # Never publish a leg the broker does not list. Expiry calendars and
        # root spellings are facts about the exchange, not arithmetic, so the
        # built symbols are confirmed against the scrip master before anything
        # downstream can send an order against them.
        master_error = self._resolve_listed_legs(legs) if listed else None
        # NFO without its master falls back to the configured calendar and
        # grid, each leg confirmed with the broker's scrip search. BFO names
        # cannot be built.
        fallback = self.fo_exch == "NFO" and not self._contracts
        if fallback:
            emit, folded = _THROTTLE.should_log(f"no_master:{self.index_name}")
            if emit:
                logging.warning(
                    "[GetSymbol:%s] %s; building legs from the configured calendar "
                    "and strike grid instead.%s",
                    self.index_name, master_error, log_throttle.suffix(folded),
                )
            master_error = None
        checks = ([(sym, self._resolve_tradingsymbol(sym))
                   for sym in (legs["buy"], legs["sell"])] if fallback else [])
        unavailable = [sym for sym, listed_ok in checks if listed_ok is None]
        unlisted = [sym for sym, listed_ok in checks if listed_ok is False]
        if master_error or unavailable or unlisted:
            return self._block_entries(
                master_error or
                (f"{self.fo_exch} lookup unavailable for {', '.join(unavailable)}"
                 if unavailable else
                 f"{self.fo_exch} does not list {', '.join(unlisted)} "
                 f"(built for {self.index_name})"),
                "unlisted",
            )
        _THROTTLE.reset(f"unlisted:{self.index_name}")
        self.legs_blocked_reason = None
        legs["built_at"] = price_at

        # Publish the legs and the bias they were built for as ONE object, in a
        # single assignment. This runs on the symbol-updater thread while the
        # trade manager reads it from another; two separate assignments let the
        # manager pair freshly-flipped bias with the previous side's legs and
        # place a wrong-way order. Callers must take `bias` from the same dict
        # they took the symbols from.
        self.final_symbols = legs

        # Legacy mirror, for display only - never pair it with a bias.
        self.final_symbol = legs["sell"]

        return self.final_symbols

    def legs_status(self) -> Dict[str, Any]:
        """What this builder has published, for the dashboard. Changes no state.

        `final_symbols` is read once - it is published as one object - so a
        reading never pairs one build's sell leg with another's buy leg. Legs
        left standing from an earlier window are not current, so an idle
        builder reports none.
        """
        legs = self.final_symbols
        reason = self.legs_blocked_reason
        if reason is None and isinstance(legs, dict) and legs.get("sell") and legs.get("buy"):
            return {"state": "armed", "sell": legs["sell"], "buy": legs["buy"], "note": None}
        if reason is None or reason == _IDLE_REASON:
            return {"state": "idle", "sell": None, "buy": None, "note": _IDLE_REASON}
        return {"state": "blocked", "sell": None, "buy": None, "note": reason}

    def _block_entries(self, reason: str, kind: str) -> None:
        """Withdraw the published legs and say why, at ERROR, once per interval.

        Returns None - what `update_symbol` publishes while entries on this
        index are blocked.
        """
        self.legs_blocked_reason = reason
        self.final_symbols = None
        self.final_symbol = None
        emit, folded = _THROTTLE.should_log(f"{kind}:{self.index_name}")
        if emit:
            logging.error(
                "[GetSymbol:%s] %s; entries on this index are blocked until "
                "it resolves.%s",
                self.index_name, reason, log_throttle.suffix(folded),
            )
        return None

    def preview_legs(self, pivot_type: Optional[str]) -> Optional[Dict[str, str]]:
        """Best-effort legs for a bias, for display only — never for trading.

        Lets the dashboard show what the bot *would* trade off the current
        forecast pivot while it is flat (no pending order / open position).
        Deliberately does not touch `final_symbols`/`final_symbol`: those stay
        gated on the strategy's own intended entry (see `update_symbol`), so a
        preview here can never be picked up by order placement.
        """
        pivot_type = (pivot_type or "").strip().upper()
        if pivot_type != "HH":               # v3 has no bearish legs to show
            return None
        current_price = float(self.get_live_price() or 0.0)
        if current_price <= 0.0:
            return None
        try:
            if self.fo_exch in ("NFO", "BFO"):
                self._ensure_master()
            legs = self._build_legs(current_price, pivot_type)
            if (self.fo_exch in ("NFO", "BFO") and self._resolve_listed_legs(legs)
                    and (self.fo_exch == "BFO" or self._contracts)):
                return None
            return legs
        except Exception as exc:
            logging.warning(f"[GetSymbol:{self.index_name}] preview_legs failed: {exc}")
            return None
