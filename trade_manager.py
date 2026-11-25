# trade_manager.py
"""The two trade managers: paper (`TradeManagerTest`) and live (`TradeManagerLive`).

Merged from `trade_manager_test.py` and `trade_manager_live.py`. Both drive the
same `StrategyDecider`, so paper and live cannot drift apart on *what* to
trade. `_TradeManager` holds the wiring and plumbing they share; each subclass
only decides how an order is carried out - recorded on paper, or sent to the
broker.
"""

from __future__ import annotations

import asyncio
import logging
import re
import threading
import time
import uuid
from datetime import date, datetime, time as dt_time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Dict, List, Optional

import instruments
import ist
import log_throttle
from config import TRADE_SETTINGS, TradeSettings
from log_throttle import LogThrottle
from strategy_logic import (
    CARRY_BOOK,
    CARRY_EXIT_TIME,
    StrategyDecider,
    bias_tag,
    now_ist,
)

LOGGER = logging.getLogger(__name__)

# Entry conditions are re-evaluated once per poll; see log_throttle.py.
_THROTTLE = LogThrottle()

# An aware IST timestamp for every database write and order gate.
_now_ist = ist.now


class _OrderUpdateSignal:
    """Wakes the leg waiters the moment an order-update writer has stored a broker update,
    so a fill is acted on at once instead of on the next poll. The poll interval stays as
    the fallback for an update that never arrives over the websocket."""

    def __init__(self) -> None:
        self.seq = 0
        self._lock = threading.Lock()
        self._waiters: set = set()

    def notify(self) -> None:
        """Any thread, after the update is committed."""
        with self._lock:
            self.seq += 1
            waiters = list(self._waiters)
        for loop, event in waiters:
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:  # that waiter's loop has closed
                pass

    async def wait(self, seen: int, timeout: float) -> None:
        """Until an update lands after `seen` (the `seq` read before the row was fetched,
        so one landing in between is not missed), or `timeout`."""
        event = asyncio.Event()
        key = (asyncio.get_running_loop(), event)
        with self._lock:
            if self.seq != seen:
                return
            self._waiters.add(key)
        try:
            await asyncio.wait_for(event.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        finally:
            with self._lock:
                self._waiters.discard(key)


ORDER_UPDATES = _OrderUpdateSignal()


class UncertainOrderError(RuntimeError):
    """The broker may have accepted an order; retry or rollback is unsafe."""


class OrderPreflightError(RuntimeError):
    """No order was submitted because the contract or quote was unusable."""


class UncertainTradeCommitError(RuntimeError):
    """The OPEN trade transaction may have committed; compensation is unsafe."""

    def __init__(self, message: str, execution_id: Optional[str] = None) -> None:
        super().__init__(message)
        #: Resolves the outcome later: the row exists under it iff it committed.
        self.execution_id = execution_id


#: `botdata.order_logs` columns of one leg, keyed by (leg, is_exit). The entry
#: columns are numbered for historical reasons: state1/time1 is the buy leg,
#: state2/time2 the sell leg.
LEG_COLUMNS: Dict[tuple[str, bool], Dict[str, str]] = {
    ("buy", False): {
        "state": "state1", "time": "time1", "orderno": "buy_orderno",
        "exchordid": "buy_exchordid", "fill_qty": "buy_fill_qty", "avg_price": "buy_avg_price",
    },
    ("sell", False): {
        "state": "state2", "time": "time2", "orderno": "sell_orderno",
        "exchordid": "sell_exchordid", "fill_qty": "sell_fill_qty", "avg_price": "sell_avg_price",
    },
    ("buy", True): {
        "state": "buy_exit_state", "time": "time1_exit", "orderno": "buy_exit_orderno",
        "exchordid": "buy_exit_exchordid", "fill_qty": "buy_exit_fill_qty",
        "avg_price": "buy_exit_avg_price",
    },
    ("sell", True): {
        "state": "sell_exit_state", "time": "time2_exit", "orderno": "sell_exit_orderno",
        "exchordid": "sell_exit_exchordid", "fill_qty": "sell_exit_fill_qty",
        "avg_price": "sell_exit_avg_price",
    },
}


def _leg_columns(leg: str, is_exit: bool) -> Dict[str, str]:
    """The order_logs columns of one leg. Anything but "buy" is the sell leg."""
    return LEG_COLUMNS["buy" if leg == "buy" else "sell", is_exit]


class _TradeManager:
    """What paper and live trading share: the wiring, the database price
    fallback, the entry cutoff and the poll loop.

    Every decision - direction, entry timing, stop level and exit - comes
    from the frozen v3 strategy via `StrategyDecider`, and the legs (a
    synthetic long future) from the published legs; a manager only carries
    it out.
    """

    #: The decider's tag, and the loop's log prefix.
    TAG = ""
    #: Prefix on the database-fallback log lines.
    _LOG_PREFIX = ""
    #: Whether a failed poll logs its traceback as well as its message.
    _LOOP_ERROR_TRACEBACK = False

    def __init__(
        self,
        *,
        api,
        db_connector,
        symbol_fetcher,
        live_price_getter,
        strategy_service,
        fresh_price_getter=None,
        poll_interval: float | None = None,
        lot_size: int | None = None,
        trade_settings: TradeSettings | None = None,
        symbol_fetchers: Dict[str, Any] | None = None,
        index_live_price_getters: Dict[str, Any] | None = None,
    ) -> None:
        cfg = trade_settings or TRADE_SETTINGS
        self.api = api
        self.db = db_connector
        self.symbol_fetcher = symbol_fetcher
        # One leg builder per index v3 can allocate the slot to. Falls back to
        # the single fetcher keyed on the chart index, so a one-index caller
        # (every test that predates the book) still works unchanged.
        self.symbol_fetchers = dict(symbol_fetchers or {})
        if not self.symbol_fetchers and symbol_fetcher is not None:
            self.symbol_fetchers = {
                getattr(symbol_fetcher, "index_name", instruments.CHART_INDEX): symbol_fetcher
            }
        # Per-index spot price. The stop is measured from the fill, so an entry
        # on BANKNIFTY must be priced off BANKNIFTY's own ticks.
        self.index_live_price_getters = dict(index_live_price_getters or {})
        self.live_price_getter = live_price_getter
        # Entries and stop decisions require a genuinely current tick.
        self.fresh_price_getter = fresh_price_getter or live_price_getter
        self.strategy = strategy_service
        self.decider = StrategyDecider(strategy_service, tag=self.TAG)
        self.poll_interval = float(poll_interval if poll_interval is not None else cfg.poll_interval)
        self._stop_requested = threading.Event()
        self.lot_size = int(lot_size if lot_size is not None else cfg.lot_size)
        if isinstance(cfg.lots, bool) or not isinstance(cfg.lots, int) or cfg.lots <= 0:
            raise ValueError("lots must be a positive integer")
        # Lots per leg of every entry (`config.TradeSettings.lots`); v3 trades
        # one fixed lot.
        self.lots = cfg.lots
        # Manager-level entry cutoff for any book but the carry (only the
        # retired momentum books used it); the night trade is exempt.
        #
        # `close_all_positions_by` is deliberately not read. v3 holds into the
        # next session's 09:15 open, so a manager that flattened at 15:15 would
        # not be trimming the strategy, it would be deleting it - the night
        # trade is opened at 15:11 to be held overnight.
        self.NO_NEW_TRADES_AFTER = cfg.no_new_trades_after

        # The open position. A v3 position is carried across a process
        # shutdown - the bot is not even running for most of the time it is
        # held - so which leg opened it (`_book`), which index it is on
        # (`_index`) and the session it was opened in (`_entry_day`) cannot be
        # inferred at startup; all three are reloaded from the trade row.
        self.in_trade: bool = False
        self.trade_row_id: Optional[int] = None
        self.direction: Optional[str] = None        # "LONG" | "SHORT"
        self.entry_price: Optional[float] = None
        self.stop_price: Optional[float] = None
        self.symbol: Optional[str] = None
        self._book: Optional[str] = None
        self._index: Optional[str] = None
        self._entry_day: Optional[date] = None

    def _ensure_tables(self) -> None:
        self.db.ensure_schema()

    def _restore_position(self, rec: Dict[str, Any]) -> None:
        """Adopt the open `trade_logs` row `rec` as this manager's position.

        A row with no `book` predates the strategy wiring. It reads as the
        carry leg: that is the leg with no protective stop, so the position is
        managed to its 09:15 exit and never stopped out at a level this process
        did not set. A row with no index predates the multi-index book, and
        every one of those is the chart index - it was the only index that
        could be traded.
        """
        self.in_trade = True
        self.trade_row_id = int(rec["id"])
        self.symbol = rec["symbol"]
        self.direction = rec["direction"]
        self.entry_price = float(rec["fill_price"])
        self.stop_price = None if rec.get("stop_price") is None else float(rec["stop_price"])
        self._book = str(rec.get("book") or CARRY_BOOK)
        self._index = str(rec.get("index_name") or instruments.CHART_INDEX)
        logged = rec.get("log_time")
        self._entry_day = logged.date() if logged is not None else None

    def _clear_position(self) -> None:
        """Back to flat: forget the position an entry or `_restore_position` set."""
        self.in_trade = False
        self.trade_row_id = None
        self.direction = None
        self.entry_price = None
        self.stop_price = None
        self.symbol = None
        self._book = None
        self._index = None
        self._entry_day = None

    def _fetcher_for(self, index: Optional[str]):
        """The leg builder for the index that won the slot.

        Falls back to the manager's single fetcher so a one-index caller is
        unaffected. Returns None when the book named an index this process has
        no builder for: that is a configuration fault, and the entry is refused
        rather than filled with another index's contracts on another exchange.
        """
        if index and index in self.symbol_fetchers:
            return self.symbol_fetchers[index]
        if index and self.symbol_fetchers:
            return None
        return self.symbol_fetcher

    def busy_reason(self) -> Optional[str]:
        """Why this manager is not flat - a position held, or an entry or exit in
        flight - or None when it is. Any thread; reads only local flags.

        The HH/LL desk and the v3 night trade never overlap: each holds back
        its entries while the other's manager answers something here.
        """
        if self.in_trade:
            return f"holding {self._index or self.symbol or 'a position'}"
        if getattr(self, "entering", False):
            return "entering a position"
        if getattr(self, "exiting", False):
            return "exiting a position"
        return None

    def _win(self, exit_price: float) -> int:
        """1 when closing the open position at `exit_price` books a profit, else 0."""
        if self.entry_price is None or self.direction is None:
            return 0
        points = (
            exit_price - self.entry_price
            if self.direction == "LONG"
            else self.entry_price - exit_price
        )
        return 1 if points > 0 else 0

    def _past_entry_cutoff(self, pending: Dict[str, Any]) -> bool:
        """True, and said at WARNING, when `no_new_trades_after` refuses the entry.

        The cutoff is an operational trim on the retired momentum (A) books.
        The carry (B), the night trade, fills at `config.night_entry_at` by
        design, so applying the trim to it would delete the rule rather than
        tighten it; it is bounded instead by its own three-minute fill window
        in `strategy_logic.ENTRY_WINDOW_SECONDS`.
        """
        if pending.get("book") == CARRY_BOOK or now_ist().time() < self.NO_NEW_TRADES_AFTER:
            return False
        LOGGER.warning(
            "[%s] No new trades after %s; skipping entry signal.",
            self.TAG, self.NO_NEW_TRADES_AFTER.strftime("%H:%M"),
        )
        return True

    async def _cycle_once(self) -> None:
        raise NotImplementedError

    async def _main_loop(self) -> None:
        failures = 0
        while not self._stop_requested.is_set():
            try:
                await self._cycle_once()
            except Exception as exc:
                failures += 1
                LOGGER.error("[%s] Trade loop error: %s", self.TAG, exc,
                             exc_info=self._LOOP_ERROR_TRACEBACK)
                if failures >= 10:
                    LOGGER.critical("[%s] Trade loop failed 10 consecutive cycles; restarting.",
                                    self.TAG, exc_info=True)
                    raise
            else:
                failures = 0
            await self._nap()

    async def _nap(self) -> None:
        """The pause between cycles."""
        await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        self._stop_requested.set()

    def start(self) -> None:
        asyncio.run(self._main_loop())


class TradeManagerLive(_TradeManager):
    """
    Live-trading manager with:
      • Hedged entry (BUY first -> ensure exchordid -> then SELL)
      • Exit: SELL leg first, then BUY leg, using IOC limit orders
      • Persist order/exit ordnos + exchordids in botdata.order_logs (WS-written exchordid)
      • Robust waiters with after_ts guards
      • Fail-safe cancellations and emergency flatten during entry/exit on any failure

    Concurrency safeguards:
      • Cross-process advisory lock around the entire entry sequence
      • In-process flags to avoid re-entry/exit while pending
      • Pre-check: skip entry if latest order_logs row for the pair has non-terminal states

    This version adds:
      • HARD gate on BUY exchordid during entry (strict verify before SELL).
      • Same-direction re-entry allowed + cooldowned retries on same pivot.
      • STRICT exits: each leg considered closed only after exit exchordid AND state=COMPLETE.
      • Exit mutex + advisory lock to prevent duplicate exits.
      • Per-order row creation in order_logs (no reuse even if symbols repeat).
      • Exit status columns (buy_exit_state/sell_exit_state) supported by waiters.
    """

    TAG = "LIVE"

    #: How long the option legs may stay unavailable before a skipped entry is
    #: reported as an ERROR rather than a WARNING. Longer than the one-second
    #: race between the symbol updater and this loop, short enough that a dead
    #: price feed is named well inside a session.
    LEGS_MISSING_ALARM_SEC: float = 120.0
    LEGS_MAX_AGE_SEC: float = 15.0
    #: Monotonic stamp of the first consecutive leg-less entry attempt, or None
    #: while the legs are fine. Class-level default so instances need no
    #: constructor change.
    _legs_missing_since: Optional[float] = None

    BUY_WAIT_TIMEOUT_SEC: float = 60.0
    SELL_WAIT_TIMEOUT_SEC: float = 60.0
    EXCHORDID_WAIT_TIMEOUT_SEC: float = 30.0  # entry leg exchordid wait
    EXIT_EXCHORDID_WAIT_TIMEOUT_SEC: float = 30.0  # exit leg exchordid hard wait
    POLL_INTERVAL_SEC: float = 0.5
    # Cap on any single position-book lookup. The trade loop carries the stop
    # and the EOD flatten, so no broker call in it may block indefinitely.
    POSITION_QTY_TIMEOUT_SEC: float = 5.0
    # After a timeout, how long to stop issuing position lookups. Longer than
    # the book's own 10s HTTP timeout (`ShoonyaApiPy.get_positions`), so an
    # abandoned lookup has finished before the next one starts; short enough
    # that one slow answer does not blind an exit for a minute.
    POSITION_QTY_BACKOFF_SEC: float = 15.0
    # Position books can lag the final order update briefly.  Do not accept an
    # entry until the broker reports the exact two-leg exposure, but allow a
    # short convergence window before treating it as a failed entry.
    ENTRY_POSITION_VERIFY_TIMEOUT_SEC: float = 15.0
    ENTRY_POSITION_VERIFY_POLL_SEC: float = 0.5

    EXIT_MAX_RETRIES: int = 3
    EXIT_RETRY_BACKOFF_SEC: float = 2.0  # exponential backoff base
    #: Widest bid/ask spread, as a fraction of the mid, an entry leg is opened
    #: on (`config.TradeSettings.max_entry_spread_pct`). Never applied to exits.
    MAX_ENTRY_SPREAD_PCT: float = 0.03

    #: NSE/BSE F&O continuous session. Recovery orders wait for it.
    MARKET_OPEN = dt_time(9, 15)
    MARKET_CLOSE = dt_time(15, 30)

    NOT_OK_STATES: tuple[str, ...] = ("REJECTED", "CANCELED", "CANCELLED")

    # Emergency flatten parameters
    EMERGENCY_FLATTEN_RETRIES: int = 3
    EMERGENCY_FLATTEN_SLEEP_SEC: float = 1.5
    RECONCILE_INTERVAL_SEC: float = 5.0
    BROKER_RECONCILE_TIMEOUT_SEC: float = 5.0
    # While a waiter polls order_logs for a WebSocket update, it also asks the
    # broker's order history this often. An update lost to a reconnect, or
    # one that arrived before its order number was bound, then costs seconds
    # instead of the waiter's whole timeout - a hedge left one-legged for a
    # minute, or an exit a minute late.
    REST_RECONCILE_EVERY_SEC: float = 1.5
    # An order intent the broker's order and trade books still do not show
    # this long after it was written never reached the broker. Every order is
    # IOC, so anything accepted has long since filled or died, and any fill it
    # left is already in the position book that reconciliation flattens.
    ATTEMPT_NOT_FOUND_AFTER_SEC: float = 30.0

    # Advisory lock keys: (name, second key)
    _ENTRY_LOCK = ("tm_live_entry", 1)
    _EXIT_LOCK = ("tm_live_exit", 2)

    def __init__(self, *, trade_settings: TradeSettings | None = None, **kwargs) -> None:
        super().__init__(trade_settings=trade_settings, **kwargs)
        cfg = trade_settings or TRADE_SETTINGS
        self._holiday_years = {datetime.strptime(day, "%d-%b-%Y").year for day in cfg.expiry_holidays}
        self._held_quantity: Optional[int] = None
        self._contract_cache: Dict[tuple[str, str], Dict[str, Any]] = {}
        # Override per-instance timing/risk knobs from config
        self.BUY_WAIT_TIMEOUT_SEC = float(cfg.buy_wait_timeout_sec)
        self.SELL_WAIT_TIMEOUT_SEC = float(cfg.sell_wait_timeout_sec)
        self.EXCHORDID_WAIT_TIMEOUT_SEC = float(cfg.exchordid_wait_timeout_sec)
        self.EXIT_EXCHORDID_WAIT_TIMEOUT_SEC = float(cfg.exit_exchordid_wait_timeout_sec)
        self.POLL_INTERVAL_SEC = float(cfg.poll_interval_sec)
        self.EXIT_MAX_RETRIES = int(cfg.exit_max_retries)
        self.EXIT_RETRY_BACKOFF_SEC = float(cfg.exit_retry_backoff_sec)
        self.EMERGENCY_FLATTEN_RETRIES = int(cfg.emergency_flatten_retries)
        self.EMERGENCY_FLATTEN_SLEEP_SEC = float(cfg.emergency_flatten_sleep_sec)
        self.MAX_ENTRY_SPREAD_PCT = float(cfg.max_entry_spread_pct)

        # In-process guards against overlapping entries and exits.
        self.entering: bool = False
        self.exiting: bool = False

        self._buy_orderno: Optional[str] = None
        self._sell_orderno: Optional[str] = None
        self._active_orderlog_row_id: Optional[int] = None
        self._sell_entry_side: Optional[str] = None  # "S" (option short) or "B" (futures long)

        # Entry retry/cooldown state
        self._last_entry_fail_ts: float = 0.0
        self._entry_fail_count: int = 0
        self.ENTRY_FAIL_COOLDOWN_SEC: float = float(cfg.entry_fail_cooldown_sec)
        self.ENTRY_FAIL_BUDGET_PER_MIN: int = int(cfg.entry_fail_budget_per_min)

        # Initialize before any resume logic that may reference it.
        self._watch_symbols: set[str] = set()

        # Recovery/quarantine state must exist before persisted state is loaded.
        self._quarantine_active: bool = False
        self._quarantine_symbols: Optional[tuple[str, str]] = None
        self._last_reconcile_ts: float = 0.0
        self._flat_while_in_trade_count: int = 0
        self._position_lookup_blocked_until: float = 0.0
        self._entry_lock_conn = None
        self._exit_lock_conn = None
        self._resume_requires_verification = False
        self._resumed_trade_state: Optional[str] = None
        # Set when an OPEN commit's outcome could not be read back. Until it is
        # resolved nothing may flatten the legs: they may be a committed trade.
        self._uncertain_execution_id: Optional[str] = None
        # `_index` (set by the base) decides the derivatives exchange every
        # order for the open position routes to and how many units it is, so a
        # resumed position that could not name its index could not be flattened
        # correctly. It is read back from the trade row, never inferred.

        self._ensure_tables()
        self._load_open_trade_state()
        self._load_incomplete_execution_state()

    # ──────────────────────────────────────────────────────────────────────
    #  Persisted state
    # ──────────────────────────────────────────────────────────────────────
    def _load_open_trade_state(self) -> None:
        row = self.db.execute(
            """
            SELECT id, symbol, direction, fill_price, stop_price,
                   buy_orderno, sell_orderno, buy_symbol, sell_symbol, sell_entry_side,trade_state,
                   book, index_name, log_time, quantity
            FROM   botdata.trade_logs
            WHERE  is_in_trade = TRUE AND execution_mode='LIVE'
            ORDER  BY id DESC
            LIMIT  2;
            """,
            fetch=True,
        )
        if not row:
            return
        if len(row) > 1:
            raise RuntimeError("multiple open trade_logs rows found; refusing live startup")
        rec = row[0]
        self._restore_position(rec)
        self._resume_requires_verification = True
        self._resumed_trade_state = str(rec.get("trade_state") or "OPEN").upper()
        self._buy_orderno = rec.get("buy_orderno")
        self._sell_orderno = rec.get("sell_orderno")
        self._sell_entry_side = rec.get("sell_entry_side")
        # Older rows were always one lot. A later config change must never
        # resize an already-open position on restart.
        legacy_lot = instruments.quantity_of(self._index, self.lot_size)
        self._held_quantity = int(rec["quantity"]) if rec.get("quantity") is not None else legacy_lot
        # No lot-multiple check against today's lot: it follows the exchange
        # (`instruments.set_lot`), and a position opened before a lot revision
        # is still a whole number of its own contract's lots. Every order is
        # checked against that contract's lot in `_place_broker_order`.
        if self._held_quantity <= 0:
            raise RuntimeError("open trade has invalid persisted quantity")
        self._watch_symbols.update(
            str(sym) for sym in (rec.get("buy_symbol"), rec.get("sell_symbol")) if sym
        )

        LOGGER.warning(
            "⚠️  [LIVE] Resuming open trade id=%s %s %s on %s (%s x%d) @ %.2f | "
            "stop %s | leg=%s opened=%s",
            self.trade_row_id, self.direction, self.symbol, self._index,
            self.order_exchange, self.order_quantity, self.entry_price,
            "-" if self.stop_price is None else f"{self.stop_price:.2f}",
            self._book, self._entry_day,
        )
        if self._book == CARRY_BOOK:
            LOGGER.warning(
                "⚠️  [LIVE] That is a v3 carry opened on %s. It has no stop and is "
                "closed unconditionally at this session's %s open.",
                self._entry_day,
                CARRY_EXIT_TIME.strftime("%H:%M"),
            )

    # ──────────────────────────────────────────────────────────────────────
    #  Per-index order routing
    # ──────────────────────────────────────────────────────────────────────
    @property
    def order_exchange(self) -> str:
        """The derivatives exchange orders for the held position route to.

        NIFTY and BANKNIFTY options are on NFO; SENSEX options are on BSE's
        BFO. Sending one to the other is rejected at best and fills the wrong
        contract at worst, so it is read off the position's own index rather
        than being a constant.
        """
        # `getattr`: the property is read from order placement, which a test
        # (and a partially-constructed manager) can reach before `__init__` has
        # set the attribute. An unknown index is a legacy NIFTY row either way.
        return instruments.fo_exchange_of(getattr(self, "_index", None))

    @property
    def order_quantity(self) -> int:
        """Units for a new entry, or the immutable quantity of the open trade."""
        held = getattr(self, "_held_quantity", None)
        if held is not None:
            return held
        return instruments.quantity_of(
            getattr(self, "_index", None),
            getattr(self, "lot_size", TRADE_SETTINGS.lot_size),
        ) * self._order_lots

    @property
    def _order_lots(self) -> int:
        return getattr(self, "lots", 1)

    # ──────────────────────────────────────────────────────────────────────
    #  order_logs helpers
    # ──────────────────────────────────────────────────────────────────────
    async def _ensure_order_log_row(self, buy_sym: str, sell_sym: str, sell_side: Optional[str] = None) -> int:
        """
        Always open a brand-new row for each hedged ENTRY attempt, even if symbols repeat.
        """
        order_type1 = "BUY"
        order_type2 = "BUY" if (sell_side or "S").upper() == "B" else "SELL"
        newrow = await self.db.execute_async(
            """
            INSERT INTO botdata.order_logs
                (log_time, symbol1, symbol2, state1, state2,
                 order_type1, order_type2, time1, time2,
                 buy_orderno, sell_orderno, buy_exchordid, sell_exchordid,lifecycle_state)
            VALUES (%s,%s,%s,NULL,NULL,%s,%s,NULL,NULL,NULL,NULL,NULL,NULL,'ENTRY_PENDING')
            RETURNING id;
            """,
            (_now_ist(), buy_sym, sell_sym, order_type1, order_type2),
            fetch=True,
        )
        return int(newrow[0]["id"])

    async def _get_latest_order_logs_row_id(self, buy_sym: str, sell_sym: str) -> Optional[int]:
        row = await self.db.execute_async(
            """
            SELECT id FROM botdata.order_logs
            WHERE COALESCE(symbol1,'') = COALESCE(%s,'')
              AND COALESCE(symbol2,'') = COALESCE(%s,'')
            ORDER BY id DESC
            LIMIT  1;
            """,
            (buy_sym, sell_sym),
            fetch=True,
        )
        return int(row[0]["id"]) if row else None

    async def _find_order_log_row_by_ordno(self, ordno: Optional[str]) -> Optional[int]:
        if not ordno:
            return None
        row = await self.db.execute_async(
            """
            SELECT id
            FROM   botdata.order_logs
            WHERE  buy_orderno=%s OR sell_orderno=%s
               OR  buy_exit_orderno=%s OR sell_exit_orderno=%s
            ORDER  BY id DESC
            LIMIT  1;
            """,
            (ordno, ordno, ordno, ordno),
            fetch=True,
        )
        return int(row[0]["id"]) if row else None

    async def _resolve_order_log_row_id(self, buy_sym: str, sell_sym: str) -> Optional[int]:
        """
        Prefer the row bound to the active attempt; else locate by existing ordernos; else fall back to latest row for pair.
        """
        if self._active_orderlog_row_id is not None:
            row = await self.db.execute_async(
                "SELECT symbol1, symbol2 FROM botdata.order_logs WHERE id=%s;",
                (self._active_orderlog_row_id,),
                fetch=True,
            )
            if row and row[0].get("symbol1") == buy_sym and row[0].get("symbol2") == sell_sym:
                return self._active_orderlog_row_id
            self._active_orderlog_row_id = None

        for ordno in (self._buy_orderno, self._sell_orderno):
            row_id = await self._find_order_log_row_by_ordno(ordno)
            if row_id is not None:
                self._active_orderlog_row_id = row_id
                return row_id

        row_id = await self._get_latest_order_logs_row_id(buy_sym, sell_sym)
        if row_id is not None:
            self._active_orderlog_row_id = row_id
        return row_id

    async def _fetch_order_log_row(self, row_id: int) -> Optional[Dict[str, Any]]:
        rows = await self.db.execute_async(
            """
            SELECT id, symbol1, symbol2,
                   state1, state2, time1, time2,
                   buy_orderno, sell_orderno,
                   buy_exchordid, sell_exchordid,
                   buy_fill_qty, sell_fill_qty,
                   buy_avg_price, sell_avg_price,
                   buy_exit_state, sell_exit_state,
                   time1_exit, time2_exit,
                   buy_exit_orderno, sell_exit_orderno,
                   buy_exit_exchordid, sell_exit_exchordid,
                   buy_exit_fill_qty, sell_exit_fill_qty,
                   buy_exit_avg_price, sell_exit_avg_price
            FROM   botdata.order_logs
            WHERE  id=%s;
            """,
            (row_id,),
            fetch=True,
        )
        return rows[0] if rows else None

    async def _commit_open_trade_atomic(
        self,
        order_log_id: int,
        fill_price: float,
        direction: str,
        stop_price: Optional[float],
        buy_symbol: str,
        sell_symbol: str,
        buy_orderno: Optional[str],
        sell_orderno: Optional[str],
        sell_entry_side: Optional[str],
        # Defaulted for the same reason the column reads NULL as the carry
        # leg: it is the leg with no stop, so an unnamed book is never given
        # one it did not ask for.
        book: str = CARRY_BOOK,
        # Which index v3 allocated the slot to. Defaulted to the chart index
        # for the same reason the column reads NULL as that index: it is what
        # every row written before the book was multi-index is.
        index_name: str | None = None,
    ) -> int:
        if not buy_symbol or not sell_symbol:
            raise ValueError("buy_symbol and sell_symbol are required")
        if not direction:
            raise ValueError("direction is required")
        execution_id = f"TR-{uuid.uuid4().hex}"
        params = (
            _now_ist(), sell_symbol, direction, float(fill_price),
            None if stop_price is None else float(stop_price),
            str(buy_orderno) if buy_orderno is not None else None,
            str(sell_orderno) if sell_orderno is not None else None,
            buy_symbol, sell_symbol,
            (str(sell_entry_side) if sell_entry_side else None), execution_id,
            str(book), str(index_name or instruments.CHART_INDEX), self.order_quantity,
        )

        conn = await self.db.get_raw_connection_async()
        transaction_error: Optional[Exception] = None
        trade_id: Optional[int] = None
        try:
            def commit_transaction() -> int:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO botdata.trade_logs
                            (log_time,symbol,direction,fill_price,stop_price,exit_price,
                             is_in_trade,win,buy_orderno,sell_orderno,buy_symbol,sell_symbol,
                             sell_entry_side,trade_state,execution_id,execution_mode,book,
                             index_name,quantity)
                        VALUES (%s,%s,%s,%s,%s,NULL,TRUE,NULL,%s,%s,%s,%s,%s,'OPEN',%s,'LIVE',%s,%s,%s)
                        RETURNING id;
                        """,
                        params,
                    )
                    row = cur.fetchone()
                    if not row:
                        raise RuntimeError("trade insert returned no id")
                    new_id = int(row["id"] if isinstance(row, dict) else row[0])
                    cur.execute(
                        """UPDATE botdata.order_logs SET lifecycle_state='OPEN'
                           WHERE id=%s AND lifecycle_state='ENTRY_PENDING'
                           RETURNING id;""",
                        (order_log_id,),
                    )
                    if not cur.fetchone():
                        raise RuntimeError(
                            f"order_logs row {order_log_id} was not ENTRY_PENDING during OPEN commit"
                        )
                conn.commit()
                return new_id

            trade_id = await asyncio.to_thread(commit_transaction)
        except Exception as exc:
            transaction_error = exc
            try:
                await asyncio.to_thread(conn.rollback)
            except Exception:
                pass
        finally:
            try:
                await self.db.put_raw_connection_async(conn, close=transaction_error is not None)
            except Exception as pool_exc:
                # Pool bookkeeping failure after a confirmed commit must not be
                # misclassified as a failed trade commit and trigger flattening.
                LOGGER.error("Failed returning OPEN transaction connection to pool: %s", pool_exc)

        if trade_id is not None:
            return trade_id

        # Commit acknowledgement can be lost after PostgreSQL commits. Resolve
        # by the immutable execution id before deciding whether compensation is safe.
        try:
            recovered = await self.db.execute_async(
                "SELECT id FROM botdata.trade_logs WHERE execution_id=%s;",
                (execution_id,), fetch=True,
            )
        except Exception as recovery_exc:
            raise UncertainTradeCommitError(
                f"cannot determine OPEN transaction outcome for {execution_id}: {recovery_exc}",
                execution_id=execution_id,
            ) from transaction_error
        if recovered:
            return int(recovered[0]["id"])
        raise RuntimeError(f"OPEN transaction rolled back: {transaction_error}") from transaction_error

    async def _resolve_uncertain_open_commit(self) -> bool:
        """Settle an OPEN commit whose outcome was lost. True once settled.

        The legs are either a committed trade or an aborted entry, and only the
        database knows which. Flattening them before it answers could close a
        position the ledger still holds open - and the next entry would then
        leave two open rows, which refuses the next startup.
        """
        execution_id = getattr(self, "_uncertain_execution_id", None)
        if execution_id is None:
            return True
        try:
            rows = await self.db.execute_async(
                "SELECT id FROM botdata.trade_logs WHERE execution_id=%s;",
                (execution_id,), fetch=True,
            )
        except Exception as exc:
            emit, folded = _THROTTLE.should_log("uncertain_open_commit")
            if emit:
                LOGGER.critical("OPEN commit %s still unresolved (%s); all actions held.%s",
                                execution_id, exc, log_throttle.suffix(folded))
            return False
        if rows:
            LOGGER.critical("OPEN commit %s did land (trade id=%s); resuming it as an open trade.",
                            execution_id, rows[0]["id"])
            # Raises on a failed read, keeping the id: settled only once loaded.
            await asyncio.to_thread(self._load_open_trade_state)
        else:
            LOGGER.warning("OPEN commit %s did not land; the entry legs are reconciled flat.",
                           execution_id)
        self._uncertain_execution_id = None
        _THROTTLE.reset("uncertain_open_commit")
        return True

    async def _verify_resumed_trade(self) -> bool:
        """Do not let strategy actions run until persisted OPEN equals broker truth.

        Re-checked every poll, including for a row an earlier run quarantined:
        an exact match with the broker is the only thing that resumes it, and a
        quarantine that outlived the mismatch would otherwise block its exit
        forever after the next restart.
        """
        if not self._resume_requires_verification:
            return True
        if self._resumed_trade_state == "ENTRY_COMMITTING":
            emit, folded = _THROTTLE.should_log("resume_blocked")
            if emit:
                LOGGER.critical("Persisted trade state %s requires reconciliation; strategy "
                                "actions blocked.%s", self._resumed_trade_state,
                                log_throttle.suffix(folded))
            return False
        rows = await self.db.execute_async(
            "SELECT buy_symbol,sell_symbol,sell_entry_side FROM botdata.trade_logs WHERE id=%s AND is_in_trade=TRUE AND execution_mode='LIVE';",
            (self.trade_row_id,), fetch=True,
        )
        if not rows:
            emit, folded = _THROTTLE.should_log("resume_row_missing")
            if emit:
                LOGGER.critical("Resumed trade row %s disappeared; quarantining.%s",
                                self.trade_row_id, log_throttle.suffix(folded))
            self._quarantine_active = True
            return False
        row = rows[0]
        buy_sym, sell_sym = row.get("buy_symbol"), row.get("sell_symbol")
        if not buy_sym or not sell_sym:
            self._set_quarantine(buy_sym, sell_sym, reason="resume_missing_symbols")
            return False
        buy_qty = await self._position_qty(str(buy_sym))
        sell_qty = await self._position_qty(str(sell_sym))
        if buy_qty is None or sell_qty is None:
            emit, folded = _THROTTLE.should_log("resume_unknown")
            if emit:
                LOGGER.critical("Cannot verify resumed trade broker quantities; retrying without "
                                "strategy actions.%s", log_throttle.suffix(folded))
            return False
        expected_sell = self.order_quantity if str(row.get("sell_entry_side") or "S").upper() == "B" else -self.order_quantity
        if buy_qty == self.order_quantity and sell_qty == expected_sell:
            await self.db.execute_async(
                "UPDATE botdata.trade_logs SET trade_state='OPEN' WHERE id=%s AND trade_state='QUARANTINED';",
                (self.trade_row_id,),
            )
            self._resume_requires_verification = False
            self._resumed_trade_state = "OPEN"
            self._flat_while_in_trade_count = 0
            return True
        if buy_qty == 0 and sell_qty == 0:
            self._flat_while_in_trade_count += 1
            if self._flat_while_in_trade_count < 3:
                LOGGER.critical("Resumed trade appears flat; requiring repeated confirmation.")
                return False
            await self.db.execute_async(
                "UPDATE botdata.trade_logs SET is_in_trade=FALSE,trade_state='RECOVERY_FLAT' WHERE id=%s AND is_in_trade=TRUE;",
                (self.trade_row_id,),
            )
            await self.db.execute_async(
                "UPDATE botdata.order_logs SET lifecycle_state='CLOSED' WHERE buy_orderno=%s AND sell_orderno=%s;",
                (self._buy_orderno, self._sell_orderno),
            )
            # Fully flat, including the held size: a stale `_held_quantity`
            # would size the next entry - on any index - as this phantom.
            self._clear_position()
            self._buy_orderno = self._sell_orderno = self._sell_entry_side = None
            self._active_orderlog_row_id = None
            self._held_quantity = None
            self._resume_requires_verification = False
            self._resumed_trade_state = None
            self._flat_while_in_trade_count = 0
            return False
        # Re-checked every poll, so record the mismatch once, not once a second.
        if not self._quarantine_active:
            self._set_quarantine(str(buy_sym), str(sell_sym), reason="resume_position_mismatch")
        if self._resumed_trade_state != "QUARANTINED":
            await self.db.execute_async("UPDATE botdata.trade_logs SET trade_state='QUARANTINED' WHERE id=%s;", (self.trade_row_id,))
            self._resumed_trade_state = "QUARANTINED"
        emit, folded = _THROTTLE.should_log("resume_mismatch")
        if emit:
            LOGGER.critical("Resumed trade position mismatch expected=%s/%s actual=%s/%s; strategy "
                            "actions blocked.%s", self.order_quantity, expected_sell, buy_qty,
                            sell_qty, log_throttle.suffix(folded))
        return False

    async def _set_leg_orderno(self, row_id: int, leg: str, ordno: str) -> None:
        col = _leg_columns(leg, False)["orderno"]
        rows = await self.db.execute_async(
            f"UPDATE botdata.order_logs SET {col}=COALESCE({col}, %s) WHERE id=%s RETURNING {col} AS ordno;",
            (str(ordno), row_id),
            fetch=True,
        )
        if not rows or str(rows[0].get("ordno") or "") != str(ordno):
            raise UncertainOrderError(f"failed to bind {leg} order number {ordno} to row {row_id}")

    async def _set_leg_exit_orderno(self, row_id: int, leg: str, ordno: str) -> None:
        """
        Write the new exit order number and clear any previous exit exchordid
        iff the order number is changing. Prevents stale matches and is idempotent.
        """
        cols = _leg_columns(leg, True)
        col_ord, col_xoid, col_state = cols["orderno"], cols["exchordid"], cols["state"]
        col_time, col_fill, col_avg = cols["time"], cols["fill_qty"], cols["avg_price"]
        rows = await self.db.execute_async(
            f"""
            UPDATE botdata.order_logs
            SET {col_xoid} = CASE WHEN {col_ord} IS DISTINCT FROM %s THEN NULL ELSE {col_xoid} END,
                {col_state} = CASE WHEN {col_ord} IS DISTINCT FROM %s THEN NULL ELSE {col_state} END,
                {col_time} = CASE WHEN {col_ord} IS DISTINCT FROM %s THEN NULL ELSE {col_time} END,
                {col_fill} = CASE WHEN {col_ord} IS DISTINCT FROM %s THEN NULL ELSE {col_fill} END,
                {col_avg} = CASE WHEN {col_ord} IS DISTINCT FROM %s THEN NULL ELSE {col_avg} END,
                {col_ord}  = %s
            WHERE id=%s RETURNING {col_ord} AS ordno;
            """,
            (str(ordno),) * 6 + (row_id,),
            fetch=True,
        )
        if not rows or str(rows[0].get("ordno") or "") != str(ordno):
            raise UncertainOrderError(f"failed to bind EXIT {leg} order number {ordno} to row {row_id}")

    # ──────────────────────────────────────────────────────────────────────
    #  Concurrency helpers
    # ──────────────────────────────────────────────────────────────────────
    async def _try_acquire_entry_lock(self) -> bool:
        if self._entry_lock_conn is not None:
            LOGGER.debug("Entry advisory lock already held in-process.")
            return True
        self._entry_lock_conn = await self._take_advisory_lock(self._ENTRY_LOCK, "Entry", "entering")
        return self._entry_lock_conn is not None

    async def _release_entry_lock(self) -> None:
        conn, self._entry_lock_conn = self._entry_lock_conn, None
        await self._drop_advisory_lock(conn, self._ENTRY_LOCK, "Entry")

    async def _try_acquire_exit_lock(self) -> bool:
        if self._exit_lock_conn is not None:
            LOGGER.debug("Exit advisory lock already held in-process.")
            return True
        self._exit_lock_conn = await self._take_advisory_lock(self._EXIT_LOCK, "Exit", "exiting")
        return self._exit_lock_conn is not None

    async def _release_exit_lock(self) -> None:
        conn, self._exit_lock_conn = self._exit_lock_conn, None
        await self._drop_advisory_lock(conn, self._EXIT_LOCK, "Exit")

    async def _take_advisory_lock(self, key: tuple[str, int], label: str, holder: str):
        """The pooled connection now holding the cross-process lock, or None.

        None means another instance holds it or the attempt failed; either way
        the caller skips this cycle. The connection is kept checked out for as
        long as the lock is held, because a session-level advisory lock belongs
        to the connection that took it.
        """
        conn = await self.db.get_raw_connection_async()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(hashtext(%s), %s) AS ok;", key)
                row = cur.fetchone()
            conn.commit()
            if row and (row[0] if not isinstance(row, dict) else row.get("ok")):
                LOGGER.info("%s advisory lock acquired", label)
                return conn
            await self.db.put_raw_connection_async(conn)
            LOGGER.info("%s advisory lock busy (another instance %s) - skipping", label, holder)
            return None
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            await self.db.put_raw_connection_async(conn, close=True)
            LOGGER.warning("%s advisory lock failed (ignored): %s", label, exc)
            return None

    async def _drop_advisory_lock(self, conn, key: tuple[str, int], label: str) -> None:
        if conn is None:
            return
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(hashtext(%s), %s);", key)
            conn.commit()
            LOGGER.info("%s advisory lock released", label)
            await self.db.put_raw_connection_async(conn)
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            await self.db.put_raw_connection_async(conn, close=True)
            LOGGER.debug("%s advisory unlock failed (ignored): %s", label, exc)

    async def _has_nonterminal_orders(self, buy_sym: str, sell_sym: str) -> bool:
        rows = await self.db.execute_async(
            """
            SELECT state1, state2,
                   buy_exit_state, sell_exit_state,
                   buy_orderno, sell_orderno,
                   buy_exit_orderno, sell_exit_orderno
            FROM   botdata.order_logs
            WHERE  COALESCE(symbol1,'') = COALESCE(%s,'')
               AND COALESCE(symbol2,'') = COALESCE(%s,'')
            ORDER  BY id DESC
            LIMIT  1;
            """,
            (buy_sym, sell_sym),
            fetch=True,
        )
        if not rows:
            return False
        r = rows[0]
        terminal = {"COMPLETE", "REJECTED", "CANCELED", "CANCELLED"}

        def is_nonterminal(state: Optional[str], ordno: Optional[str]) -> bool:
            st = (state or "").upper()
            if not st:
                return bool(ordno)
            return st not in terminal

        return any(
            (
                is_nonterminal(r.get("state1"), r.get("buy_orderno")),
                is_nonterminal(r.get("state2"), r.get("sell_orderno")),
                is_nonterminal(r.get("buy_exit_state"), r.get("buy_exit_orderno")),
                is_nonterminal(r.get("sell_exit_state"), r.get("sell_exit_orderno")),
            )
        )

    # ──────────────────────────────────────────────────────────────────────
    #  Waiters
    # ──────────────────────────────────────────────────────────────────────
    async def _wait_leg_status(
        self,
        *,
        row_id: int,
        leg: str,                          # "buy" | "sell"
        expect: str = "COMPLETE",
        timeout_sec: float = 45.0,
        poll_sec: float = 0.5,
        not_ok: tuple[str, ...] = ("REJECTED", "CANCELED", "CANCELLED"),
        after_ts: Optional[datetime] = None,
        is_exit: bool = False,             # check the exit status/time columns
        ordno: Optional[str] = None,
    ) -> None:
        cols = _leg_columns(leg, is_exit)
        deadline = time.monotonic() + timeout_sec
        next_history = time.monotonic() + self.REST_RECONCILE_EVERY_SEC
        while time.monotonic() < deadline:
            seen = ORDER_UPDATES.seq
            r = await self._fetch_order_log_row(row_id)
            if not r:
                raise RuntimeError(f"order_logs row {row_id} not found while waiting for state")

            state, tmark, row_ordno = r[cols["state"]], r[cols["time"]], r[cols["orderno"]]

            if ordno and row_ordno and str(row_ordno) != str(ordno):
                raise RuntimeError(f"{leg.upper()} orderno mismatch for row {row_id} (expected {ordno}, got {row_ordno})")

            state_up = (state or "").upper()
            if state_up in not_ok:
                raise RuntimeError(f"{'EXIT ' if is_exit else ''}{leg.upper()} leg {state}")
            if state_up == expect and (after_ts is None or (tmark and tmark > after_ts)):
                return
            if ordno and time.monotonic() >= next_history:
                await self._poll_order_history(row_id=row_id, leg=leg, ordno=ordno, is_exit=is_exit)
                next_history = time.monotonic() + self.REST_RECONCILE_EVERY_SEC
                continue
            await ORDER_UPDATES.wait(seen, poll_sec)
        if ordno and await self._reconcile_exact_order(row_id=row_id, leg=leg, ordno=ordno, is_exit=is_exit):
            r = await self._fetch_order_log_row(row_id)
            state = r[cols["state"]] if r else None
            if (state or "").upper() == expect:
                return
        raise TimeoutError(f"Timed out waiting for {'EXIT ' if is_exit else ''}{leg.upper()} leg to {expect}")

    async def _wait_leg_exchordid(
        self,
        *,
        row_id: int,
        leg: str,
        timeout_sec: float,
        poll_sec: float,
        not_ok: tuple[str, ...],
        is_exit: bool = False,
        ordno: Optional[str] = None,
    ) -> None:
        cols = _leg_columns(leg, is_exit)
        deadline = time.monotonic() + timeout_sec
        next_history = time.monotonic() + self.REST_RECONCILE_EVERY_SEC
        while time.monotonic() < deadline:
            seen = ORDER_UPDATES.seq
            r = await self._fetch_order_log_row(row_id)
            if not r:
                raise RuntimeError(f"order_logs row {row_id} not found while waiting for exchordid")

            state, exchordid, row_ordno = r[cols["state"]], r[cols["exchordid"]], r[cols["orderno"]]

            if ordno and row_ordno and str(row_ordno) != str(ordno):
                raise RuntimeError(f"{leg.upper()} orderno mismatch for row {row_id} (expected {ordno}, got {row_ordno})")

            st = (state or "").upper()
            if st in not_ok:
                raise RuntimeError(f"{'EXIT ' if is_exit else ''}{leg.upper()} leg {st}")
            if exchordid:
                return
            if ordno and time.monotonic() >= next_history:
                await self._poll_order_history(row_id=row_id, leg=leg, ordno=ordno, is_exit=is_exit)
                next_history = time.monotonic() + self.REST_RECONCILE_EVERY_SEC
                continue
            await ORDER_UPDATES.wait(seen, poll_sec)
        if ordno and await self._reconcile_exact_order(row_id=row_id, leg=leg, ordno=ordno, is_exit=is_exit):
            r = await self._fetch_order_log_row(row_id)
            if r and r.get(cols["exchordid"]):
                return
        raise TimeoutError(f"Timed out waiting for {'EXIT ' if is_exit else ''}{leg.upper()} leg exchordid")

    async def _reconcile_exact_order(
        self, *, row_id: int, leg: str, ordno: str, is_exit: bool
    ) -> bool:
        """REST fallback for a lost WebSocket update, keyed only by order number."""
        getter = getattr(self.api, "single_order_history", None)
        if not getter:
            return False
        try:
            payload = await asyncio.wait_for(
                asyncio.to_thread(getter, ordno), timeout=self.BROKER_RECONCILE_TIMEOUT_SEC
            )
        except Exception as exc:
            LOGGER.warning("Order-history reconciliation failed for %s: %s", ordno, exc)
            return False
        rows = self._broker_rows(payload)
        if not rows:
            return False
        # Broker history is normally newest-first. Prefer a COMPLETE record,
        # otherwise use the first exact-order record without guessing by symbol.
        exact = [
            row for row in rows
            if self._valid_orderno(row.get("norenordno") or row.get("orderno")) == ordno
        ]
        if not exact:
            LOGGER.error("Order history for %s contained no matching order number", ordno)
            return False
        # A fill first, then any other outcome, then the newest record. The
        # terminal preference keeps a history listed oldest-first from writing
        # PENDING over an order that has already died.
        statuses = [str(row.get("status") or "").upper() for row in exact]
        record = next(
            (row for row, st in zip(exact, statuses) if st in {"COMPLETE", "FILLED"}),
            next((row for row, st in zip(exact, statuses)
                  if st in {"REJECTED", "CANCELED", "CANCELLED"}), exact[0]),
        )
        raw_state = str(record.get("status") or record.get("orderstatus") or "").upper()
        state = {"FILLED": "COMPLETE", "CANCELLED": "CANCELED"}.get(raw_state, raw_state)
        if state not in {"OPEN", "PENDING", "TRIGGER_PENDING", "COMPLETE", "REJECTED", "CANCELED"}:
            return False
        xoid = record.get("exchordid") or record.get("exch_order_id") or record.get("exchordno")
        fill = record.get("fillshares") or record.get("filled_qty") or record.get("fillqty")
        avg = record.get("avgprc") or record.get("avg_price")
        try:
            fill = int(float(str(fill))) if fill not in (None, "") else None
            avg = float(str(avg)) if avg not in (None, "") else None
        except (TypeError, ValueError):
            LOGGER.error("Order history for %s has unreadable fill %r @ %r", ordno, fill, avg)
            return False
        cols = _leg_columns(leg, is_exit)
        state_col, time_col, xoid_col = cols["state"], cols["time"], cols["exchordid"]
        fill_col, avg_col, ord_col = cols["fill_qty"], cols["avg_price"], cols["orderno"]
        await self.db.execute_async(
            f"""UPDATE botdata.order_logs SET
                {state_col}=CASE WHEN UPPER(COALESCE({state_col},'')) IN ('COMPLETE','REJECTED','CANCELED','CANCELLED') THEN {state_col} ELSE %s END,
                {time_col}=CASE WHEN UPPER(COALESCE({state_col},'')) IN ('COMPLETE','REJECTED','CANCELED','CANCELLED') THEN {time_col} ELSE %s END,
                {xoid_col}=COALESCE({xoid_col},%s),{fill_col}=COALESCE(%s,{fill_col}),
                {avg_col}=COALESCE(%s,{avg_col}) WHERE id=%s AND {ord_col}=%s;""",
            (state, _now_ist(), str(xoid) if xoid else None, fill, avg, row_id, ordno),
        )
        return True

    async def _poll_order_history(self, *, row_id: int, leg: str, ordno: str, is_exit: bool) -> None:
        """One in-wait order-history read. A failure only waits for the next one."""
        try:
            await self._reconcile_exact_order(row_id=row_id, leg=leg, ordno=ordno, is_exit=is_exit)
        except Exception as exc:  # noqa: BLE001 - the waiter's own deadline still applies
            LOGGER.warning("Order-history poll failed for %s: %s", ordno, exc)

    async def _wait_leg_terminal(
        self,
        *,
        row_id: int,
        leg: str,
        timeout_sec: float,
        poll_sec: float,
        is_exit: bool = False,
        ordno: Optional[str] = None,
    ) -> Optional[str]:
        terminal = {"COMPLETE", "REJECTED", "CANCELED", "CANCELLED"}
        cols = _leg_columns(leg, is_exit)
        deadline = time.monotonic() + timeout_sec
        next_history = time.monotonic() + self.REST_RECONCILE_EVERY_SEC
        while time.monotonic() < deadline:
            seen = ORDER_UPDATES.seq
            r = await self._fetch_order_log_row(row_id)
            if not r:
                raise RuntimeError(f"order_logs row {row_id} not found while waiting for terminal state")

            state, row_ordno = r[cols["state"]], r[cols["orderno"]]

            if ordno and row_ordno and str(row_ordno) != str(ordno):
                raise RuntimeError(
                    f"{leg.upper()} orderno mismatch for row {row_id} (expected {ordno}, got {row_ordno})"
                )

            st = (state or "").upper()
            if st in terminal:
                return st
            if ordno and time.monotonic() >= next_history:
                await self._poll_order_history(row_id=row_id, leg=leg, ordno=ordno, is_exit=is_exit)
                next_history = time.monotonic() + self.REST_RECONCILE_EVERY_SEC
                continue
            await ORDER_UPDATES.wait(seen, poll_sec)
        if ordno and await self._reconcile_exact_order(row_id=row_id, leg=leg, ordno=ordno, is_exit=is_exit):
            r = await self._fetch_order_log_row(row_id)
            state = r.get(cols["state"]) if r else None
            if str(state or "").upper() in terminal:
                return str(state).upper()
        return None

    # ──────────────────────────────────────────────────────────────────────
    #  Utilities
    # ──────────────────────────────────────────────────────────────────────
    async def _position_qty(self, tsym: str) -> Optional[int]:
        """Net NRML quantity of one symbol, or None when the book is unreadable."""
        book = await self._position_book()
        # A symbol with no row in a good book is the broker's zero; a row the
        # book could not parse reads None, never zero.
        return None if book is None else book.get(tsym, 0)

    async def _position_book(self) -> Optional[Dict[str, Optional[int]]]:
        """Net NRML quantity per symbol from one position-book read, or None.

        One read serves every symbol a pass needs, so watching more symbols
        does not multiply broker calls.
        """
        # Circuit breaker. `wait_for` abandons the await but cannot cancel the
        # worker thread, so a broker that hangs leaves one stuck thread per
        # attempt. Left unchecked those accumulate in the shared executor and
        # eventually starve the order and exit calls that also run on it. After
        # a timeout, stop asking for a while instead.
        if time.monotonic() < self._position_lookup_blocked_until:
            return None
        try:
            payload = await asyncio.wait_for(
                asyncio.to_thread(self.api.get_positions),
                timeout=self.POSITION_QTY_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            self._position_lookup_blocked_until = (
                time.monotonic() + self.POSITION_QTY_BACKOFF_SEC
            )
            LOGGER.warning(
                "get_positions() timed out; pausing position lookups for %.0fs so "
                "stuck broker threads cannot pile up.",
                self.POSITION_QTY_BACKOFF_SEC,
            )
            return None
        except Exception as exc:
            emit, folded = _THROTTLE.should_log("positions_failed")
            if emit:
                LOGGER.warning("get_positions() failed; broker quantity unknown: %s%s",
                               exc, log_throttle.suffix(folded))
            return None

        rows: List[Dict[str, Any]] = []
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            if str(payload.get("stat") or "").lower() == "not_ok":
                return None
            for k in ("day", "cf", "data", "positions", "pos"):
                v = payload.get(k)
                if isinstance(v, list):
                    rows.extend(v)
            if not any(isinstance(payload.get(k), list) for k in ("day", "cf", "data", "positions", "pos")):
                return None
        else:
            return None

        book: Dict[str, Optional[int]] = {}
        for r in rows:
            if not isinstance(r, dict) or str(r.get("stat") or "ok").lower() != "ok":
                return None
            if str(r.get("prd") or "M").upper() != "M":
                continue
            sym = str(r.get("tsym") or r.get("symbol") or "")
            try:
                qty = int(r["netqty"])
            except (KeyError, TypeError, ValueError, OverflowError):
                book[sym] = None    # unreadable stays unknown, whatever else is listed
                continue
            if sym not in book or book[sym] is not None:
                book[sym] = book.get(sym, 0) + qty
        return book

    async def _await_flat(self, *symbols: str) -> Dict[str, Optional[int]]:
        """Broker net per symbol, re-read until all are 0 or the settle window ends.

        The position book can trail an order's COMPLETE update. A residual
        sized off a reading taken before it caught up is a second exit order,
        and a reversed position.
        """
        deadline = time.monotonic() + max(0.0, self.ENTRY_POSITION_VERIFY_TIMEOUT_SEC)
        while True:
            book = await self._position_book()
            net = {sym: (None if book is None else book.get(sym, 0)) for sym in symbols}
            if all(qty == 0 for qty in net.values()) or time.monotonic() >= deadline:
                return net
            await asyncio.sleep(max(0.01, self.ENTRY_POSITION_VERIFY_POLL_SEC))

    def _live_price_for(self, index: str) -> Optional[float]:
        """A current tick for one index's spot, or None.

        Never falls back to another index's price. The stop is a fixed distance
        from the fill, so pricing a SENSEX entry off NIFTY's tick would set a
        stop at a level that never traded on the contract being bought.
        """
        getter = self.index_live_price_getters.get(index)
        if getter is None:
            if index == instruments.CHART_INDEX or not self.index_live_price_getters:
                return self._read_fresh_price()
            return None
        try:
            value = getter()
            value = float(value) if value is not None else None
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Live price for %s failed: %s", index, exc)
            return None
        return value if value is not None and 0 < value < float("inf") else None

    async def _index_price_with_fallback(self, index: str, context: str) -> Optional[float]:
        """`_get_price_with_fallback`, for a named index."""
        price = self._live_price_for(index)
        if price and price > 0:
            return float(price)
        if context == "stop check":
            # Every poll while the feed is out - and daily after 15:15, when the
            # index stops printing `lp` - so once a minute, not once a second.
            emit, folded = _THROTTLE.should_log(f"stop_deferred:{index}")
            if emit:
                LOGGER.warning("[%s] fresh price unavailable for %s; stop decision deferred.%s",
                               context, index, log_throttle.suffix(folded))
            return None
        if index == instruments.CHART_INDEX or not self.index_live_price_getters:
            return await self._get_price_with_fallback(context)
        LOGGER.error("[%s] no price available for %s.", context, index)
        return None

    async def _held_price_getter(self, context: str) -> Optional[float]:
        """Price for whichever index the open position is on.

        The carry exit and the protective stop are both judged against the held
        index's own level, so this resolves through `_index`.
        """
        price = (await self._index_price_with_fallback(self._index, context)
                 if self._index else await self._get_price_with_fallback(context))
        if price is None and context == "carry exit" and self.entry_price and self.entry_price > 0:
            LOGGER.error("Carry exit price unavailable for %s; using entry price only to trigger broker flatten.", self._index)
            return self.entry_price
        return price

    def _read_fresh_price(self) -> Optional[float]:
        try:
            value = self.fresh_price_getter() if callable(self.fresh_price_getter) else None
            value = float(value) if value is not None else None
        except Exception as exc:
            LOGGER.error("Fresh price getter failed: %s", exc)
            return None
        return value if value is not None and value > 0 and value < float("inf") else None

    def _update_watch_symbols_from_fetcher(self) -> None:
        fs = getattr(self.symbol_fetcher, "final_symbols", None)
        if isinstance(fs, dict):
            for key in ("buy", "sell"):
                sym = fs.get(key)
                if sym:
                    self._watch_symbols.add(str(sym))
            return
        sym = getattr(self.symbol_fetcher, "final_symbol", None)
        if sym:
            self._watch_symbols.add(str(sym))

    def _set_quarantine(self, buy_sym: Optional[str], sell_sym: Optional[str], *, reason: str) -> None:
        if not buy_sym and not sell_sym:
            return
        self._quarantine_active = True
        self._quarantine_symbols = (str(buy_sym or ""), str(sell_sym or ""))
        LOGGER.warning("Entry quarantine enabled (reason=%s) buy=%s sell=%s", reason, buy_sym, sell_sym)

    def _clear_quarantine(self) -> None:
        if self._quarantine_active:
            LOGGER.info("Entry quarantine cleared (positions flat).")
        self._quarantine_active = False
        self._quarantine_symbols = None

    async def _warn_if_broker_is_flat(self) -> None:
        """Report, loudly, when an open trade is not backed by broker exposure.

        Detection only - deliberately no auto-correction. A failed lookup reads
        None and resets the count; zero on *both* legs from a book that did
        answer is the alarm, and requiring several consecutive readings keeps a
        single odd response from raising a false one. Acting on it could still
        abandon a live hedge the book has momentarily lost.

        Clearing the state is left to a human because the correct recovery
        depends on why the legs vanished.
        """
        legs = [s for s in (self._watch_symbols or set()) if s]
        if len(legs) < 2:
            return
        book = await self._position_book()
        readings = [None if book is None else book.get(sym, 0) for sym in legs]
        if any(qty is None for qty in readings) or any(qty != 0 for qty in readings):
            self._flat_while_in_trade_count = 0
            return

        self._flat_while_in_trade_count += 1
        if self._flat_while_in_trade_count < 3:
            return
        LOGGER.critical(
            "Trade log says %s %s is open, but the broker reports zero quantity on "
            "every leg (%s) across %d checks. The bot will keep managing phantom "
            "state until this is resolved by hand.",
            self.direction, self.symbol, ", ".join(sorted(legs)),
            self._flat_while_in_trade_count,
        )

    async def _reconcile_positions(self) -> None:
        now = time.monotonic()
        if (now - self._last_reconcile_ts) < self.RECONCILE_INTERVAL_SEC:
            return
        self._last_reconcile_ts = now

        if await self._reconcile_unresolved_attempts():
            # A possibly-live order outranks position cleanup. Flattening while
            # it can still fill would race the broker and can reverse exposure.
            return

        if self.entering or self.exiting:
            return
        if self.in_trade:
            await self._warn_if_broker_is_flat()
            return

        self._update_watch_symbols_from_fetcher()
        symbols = set(self._watch_symbols)
        if self._quarantine_symbols:
            for sym in self._quarantine_symbols:
                if sym:
                    symbols.add(sym)
        if not symbols:
            return

        # One book read for every watched symbol; `_flatten_to_zero` re-reads
        # the broker itself before each order it sends.
        book = await self._position_book()
        if book is None:
            return
        flattened = False
        # Shorts (and unreadable rows, which may be shorts) first; while one of
        # them is still open, longs are kept as its hedge until the next pass.
        short_open = False
        for sym in sorted(symbols, key=lambda s: (book.get(s, 0) is not None
                                                   and book.get(s, 0) >= 0, s)):
            qty = book.get(sym, 0)
            if qty is None:
                short_open = True
                continue
            if qty != 0 and self._market_open():
                if qty > 0 and short_open:
                    LOGGER.critical("Reconcile: keeping long %s x%s as a hedge while a "
                                    "short leg is not flat.", sym, qty)
                    continue
                ok = await self._flatten_to_zero(sym, reason="reconcile")
                flattened = True
                if qty < 0 and not ok:
                    short_open = True

        if self._quarantine_symbols:
            if flattened:
                book = await self._position_book()
            if book is not None and all(book.get(sym, 0) == 0 for sym in self._quarantine_symbols if sym):
                self._clear_quarantine()

    async def _reconcile_unresolved_attempts(self) -> bool:
        attempts = await self.db.execute_async(
            """
            SELECT id,created_at,order_log_id,client_ref,phase,leg,symbol,orderno,status
            FROM botdata.order_attempts
            WHERE status IN ('INTENT','UNKNOWN','ACKNOWLEDGED','OPEN','PENDING','TRIGGER_PENDING')
            ORDER BY id ASC LIMIT 20;
            """,
            fetch=True,
        ) or []
        unresolved = False
        for attempt in attempts:
            if attempt.get("symbol"):
                self._watch_symbols.add(str(attempt["symbol"]))
            created = attempt.get("created_at")
            if isinstance(created, datetime) and ist.to_ist(created).date() < now_ist().date():
                # Every order is IOC, and no order outlives its session. The
                # broker's books and order history are per day, so one from an
                # earlier session can never be read back - left open it would
                # block entries, and a restart's exit, forever. What it filled
                # is in today's position book, which reconciliation acts on.
                LOGGER.warning(
                    "Order attempt %s (%s %s %s, order %s) is from an earlier session; "
                    "closing it as EXPIRED.", attempt.get("client_ref"), attempt.get("phase"),
                    attempt.get("leg"), attempt.get("symbol"), attempt.get("orderno") or "-",
                )
                await self.db.execute_async(
                    "UPDATE botdata.order_attempts SET status=%s,updated_at=%s WHERE id=%s;",
                    ("EXPIRED", _now_ist(), attempt["id"]),
                )
                continue
            ordno = self._valid_orderno(attempt.get("orderno"))
            if not ordno:
                client_ref = str(attempt["client_ref"])
                found = await self._client_ref_orders(client_ref)
                ordno = self._sole_order(client_ref, found)
                if ordno:
                    await self.db.execute_async(
                        "UPDATE botdata.order_attempts SET orderno=%s,status='ACKNOWLEDGED',updated_at=%s WHERE id=%s;",
                        (ordno, _now_ist(), attempt["id"]),
                    )
                elif found == set() and self._attempt_age_sec(attempt) >= self.ATTEMPT_NOT_FOUND_AFTER_SEC:
                    # Both books read, neither holds it, and any IOC it could
                    # have been is long dead: it never reached the broker. Left
                    # open it would block entries - and a restart's exit - for good.
                    LOGGER.warning(
                        "Order intent %s (%s %s %s) is in neither broker book after %.0fs; "
                        "closing it as NOT_FOUND.", client_ref, attempt.get("phase"),
                        attempt.get("leg"), attempt.get("symbol"), self.ATTEMPT_NOT_FOUND_AFTER_SEC,
                    )
                    await self.db.execute_async(
                        "UPDATE botdata.order_attempts SET status=%s,updated_at=%s WHERE id=%s;",
                        ("NOT_FOUND", _now_ist(), attempt["id"]),
                    )
                    continue
            row_id = attempt.get("order_log_id")
            if ordno and attempt.get("phase") == "FLATTEN":
                try:
                    state = await self._broker_terminal_status(ordno)
                    if state:
                        symbol = str(attempt.get("symbol") or "")
                        qty = await self._position_qty(symbol) if symbol else None
                        if qty == 0:
                            await self.db.execute_async(
                                "UPDATE botdata.order_attempts SET status=%s,updated_at=%s WHERE id=%s;",
                                (state, _now_ist(), attempt["id"]),
                            )
                        elif qty is None:
                            unresolved = True
                        # A terminal order with residual exposure may be retried
                        # by position reconciliation. Keep this attempt open until
                        # the broker actually reports the symbol flat.
                        continue
                except Exception as exc:
                    LOGGER.warning("Flatten attempt reconciliation failed for %s: %s", ordno, exc)
            if ordno and row_id and attempt.get("phase") in {"ENTRY", "EXIT"}:
                leg = str(attempt.get("leg") or "")
                is_exit = attempt.get("phase") == "EXIT"
                try:
                    if is_exit:
                        current = await self._fetch_order_log_row(int(row_id))
                        active_ordno = (current or {}).get(_leg_columns(leg, True)["orderno"])
                        if active_ordno and str(active_ordno) != ordno:
                            state = await self._broker_terminal_status(ordno)
                            if state:
                                await self.db.execute_async(
                                    "UPDATE botdata.order_attempts SET status=%s,updated_at=%s WHERE id=%s;",
                                    (state, _now_ist(), attempt["id"]),
                                )
                                continue
                            unresolved = True
                            continue
                    if is_exit:
                        await self._set_leg_exit_orderno(int(row_id), leg, ordno)
                    else:
                        await self._set_leg_orderno(int(row_id), leg, ordno)
                    await self._reconcile_exact_order(
                        row_id=int(row_id), leg=leg, ordno=ordno, is_exit=is_exit
                    )
                    row = await self._fetch_order_log_row(int(row_id))
                    state_key = (
                        f"{leg}_exit_state" if is_exit else ("state1" if leg == "buy" else "state2")
                    )
                    state = str((row or {}).get(state_key) or "").upper()
                    if state in {"COMPLETE", "REJECTED", "CANCELED", "CANCELLED"}:
                        await self.db.execute_async(
                            "UPDATE botdata.order_attempts SET status=%s,updated_at=%s WHERE id=%s;",
                            ("CANCELED" if state == "CANCELLED" else state, _now_ist(), attempt["id"]),
                        )
                        continue
                except Exception as exc:
                    LOGGER.warning("Attempt reconciliation failed for %s: %s", attempt["client_ref"], exc)
            unresolved = True
        if unresolved:
            emit, folded = _THROTTLE.should_log("unresolved_attempts")
            if emit:
                LOGGER.critical("Unresolved broker order attempt exists; entries and automatic "
                                "flatten are blocked.%s", log_throttle.suffix(folded))
        else:
            _THROTTLE.reset("unresolved_attempts")
        return unresolved

    @staticmethod
    def _attempt_age_sec(attempt: Dict[str, Any]) -> float:
        """Seconds since an attempt row was written; 0 when it carries no time."""
        created = attempt.get("created_at")
        if not isinstance(created, datetime):
            return 0.0
        return (_now_ist() - ist.to_ist(created)).total_seconds()

    # ──────────────────────────────────────────────────────────────────────
    #  Broker wrappers
    # ──────────────────────────────────────────────────────────────────────
    def _exchange_for(self, symbol: str) -> str:
        """The derivatives exchange `symbol` trades on, read off its own root.

        Not off the held index: reconciliation and rollback flatten symbols
        while flat, when `_index` names nothing - and a stray SENSEX leg looked
        up on NFO can never be found, so it could never be closed.
        """
        inst = instruments.of_symbol(symbol)
        return inst.fo_exchange if inst is not None else self.order_exchange

    def _contract_details(self, symbol: str) -> Dict[str, Any]:
        """Resolve one exact contract and its session reference data."""
        exchange = self._exchange_for(symbol)
        key = (exchange, symbol)
        cache = getattr(self, "_contract_cache", None)
        if cache is not None and key in cache:
            return cache[key]
        try:
            found = self.api.searchscrip(exchange=exchange, searchtext=symbol)
            values = found.get("values") if isinstance(found, dict) else found
            match = next((row for row in values if isinstance(row, dict) and row.get("tsym") == symbol), None) if isinstance(values, list) else None
            if not match or not match.get("token"):
                raise ValueError("exact broker contract/token unavailable")
            metadata = self.api.get_security_info(exchange=exchange, token=str(match["token"]))
            if not isinstance(metadata, dict) or str(metadata.get("stat") or "").lower() != "ok":
                raise ValueError("broker contract details unavailable")
            if metadata.get("tsym") not in (None, symbol):
                raise ValueError("broker contract details mismatch")
            lot = int(metadata["ls"])
            tick = Decimal(str(metadata["ti"]))
            if lot <= 0 or tick <= 0 or not tick.is_finite():
                raise ValueError("invalid broker lot or tick")
            try:
                freeze = int(float(str(metadata.get("frzqty"))))
            except (TypeError, ValueError, OverflowError):
                freeze = 0
            if freeze < lot:
                # No usable freeze quantity: one lot per order is always
                # inside it. Refusing instead would leave an open position
                # with no way to be closed.
                LOGGER.warning("%s: broker freeze quantity %r unusable; slicing orders "
                               "at one lot.", symbol, metadata.get("frzqty"))
                freeze = lot
            details = {"token": str(match["token"]), "lot": lot,
                       "max_qty": freeze // lot * lot, "tick": tick,
                       "uc": metadata.get("uc"), "lc": metadata.get("lc")}
        except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
            raise OrderPreflightError(f"invalid contract details for {symbol}: {exc}") from exc
        except Exception as exc:
            raise OrderPreflightError(f"contract lookup failed for {symbol}") from exc
        if cache is not None:
            cache[key] = details
        return details

    def _place_broker_order(
        self,
        side: str,
        symbol: str,
        *,
        qty: Optional[int] = None,
        reduce_only: Optional[bool] = None,
        remarks: Optional[str] = None,
    ) -> Dict[str, object] | None:
        order_qty = qty if qty is not None else self.order_quantity
        if type(order_qty) is not int:
            raise OrderPreflightError(f"{symbol}: order quantity must be an integer")
        exchange = self._exchange_for(symbol)
        details = self._contract_details(symbol)
        if order_qty <= 0 or order_qty % details["lot"] or order_qty > details["max_qty"]:
            raise OrderPreflightError(
                f"{symbol}: quantity {order_qty} must be a lot multiple up to broker freeze {details['max_qty']}"
            )
        quote = self._broker_quote(symbol, exchange, details["token"])
        if not reduce_only:
            # Entries only: an exit is never refused for a wide book.
            self._check_entry_spread(symbol, quote, details["tick"])
        try:
            tick = details["tick"]
            top = Decimal(str(quote["sp1" if side == "B" else "bp1"]))
            if top <= 0 or not top.is_finite():
                raise ValueError("invalid top of book")
            # Shoonya's API accepts LMT, not MKT. Cross the spread with a
            # bounded 2% buffer; IOC prevents an old exit from filling later.
            buffered = top * (Decimal("1.02") if side == "B" else Decimal("0.98"))
            rounding = ROUND_CEILING if side == "B" else ROUND_FLOOR
            limit = (buffered / tick).to_integral_value(rounding=rounding) * tick
            band_key = "uc" if side == "B" else "lc"
            band = quote.get(band_key) or details.get(band_key)
            if band not in (None, ""):
                bound = Decimal(str(band))
                if bound <= 0:
                    raise ValueError("invalid circuit band")
                limit = min(limit, bound) if side == "B" else max(limit, bound)
            if limit <= 0 or not limit.is_finite():
                raise ValueError("invalid limit price")
        except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
            raise OrderPreflightError(f"cannot price {symbol}: invalid broker quote") from exc
        base_kwargs = {
            "buy_or_sell": side,
            "product_type": "M",
            "exchange": exchange,
            "tradingsymbol": symbol,
            "quantity": order_qty,
            "discloseqty": 0,
            "price_type": "LMT",
            "price": float(limit),
            "retention": "IOC",
            "remarks": remarks,
        }
        return self.api.place_order(**base_kwargs)

    def _broker_quote(self, symbol: str, exchange: str, token: str) -> Dict[str, Any]:
        """The broker's current quote for exactly `symbol`, or OrderPreflightError."""
        try:
            quote = self.api.get_quotes(exchange=exchange, token=token)
        except Exception as exc:
            raise OrderPreflightError(f"cannot price {symbol}: quote lookup failed") from exc
        if not isinstance(quote, dict) or str(quote.get("stat") or "").lower() != "ok":
            raise OrderPreflightError(f"cannot price {symbol}: broker quote unavailable")
        if quote.get("tsym") not in (None, symbol):
            raise OrderPreflightError(f"cannot price {symbol}: quote contract mismatch")
        return quote

    def _check_entry_spread(self, symbol: str, quote: Dict[str, Any], tick: Decimal) -> None:
        """Refuse to open a leg on a one-sided book, or one wider than
        `MAX_ENTRY_SPREAD_PCT` of the mid (one tick is always allowed).

        The limit crosses the spread by up to 2%, so on a thin ITM leg a stale
        or stub quote would otherwise be filled as if it were the market.
        """
        try:
            bid, ask = Decimal(str(quote["bp1"])), Decimal(str(quote["sp1"]))
            ok = bid.is_finite() and ask.is_finite() and 0 < bid <= ask
        except (KeyError, TypeError, ValueError, ArithmeticError):
            ok = False
        if not ok:
            raise OrderPreflightError(
                f"{symbol}: entry refused, no two-sided book "
                f"(bid {quote.get('bp1')!r}, ask {quote.get('sp1')!r})"
            )
        spread, mid = ask - bid, (ask + bid) / 2
        allowed = max(Decimal(str(self.MAX_ENTRY_SPREAD_PCT)) * mid, tick)
        if spread > allowed:
            raise OrderPreflightError(
                f"{symbol}: entry refused, spread {spread} (bid {bid} / ask {ask}) is wider "
                f"than {allowed:.2f} ({self.MAX_ENTRY_SPREAD_PCT:.1%} of mid)"
            )

    def _check_entry_book(self, symbol: str) -> None:
        """`_check_entry_spread` on a fresh quote, before any order of the hedge is sent."""
        details = self._contract_details(symbol)
        quote = self._broker_quote(symbol, self._exchange_for(symbol), details["token"])
        self._check_entry_spread(symbol, quote, details["tick"])

    @staticmethod
    def _valid_orderno(value: Any) -> Optional[str]:
        if value is None:
            return None
        orderno = str(value).strip()
        if not orderno or len(orderno) > 64 or not re.fullmatch(r"[A-Za-z0-9_-]+", orderno):
            return None
        return orderno

    @staticmethod
    def _broker_rows(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            for key in ("data", "orders", "trades"):
                rows = payload.get(key)
                if isinstance(rows, list):
                    return [row for row in rows if isinstance(row, dict)]
        return []

    async def _recover_orderno_by_client_ref(self, client_ref: str) -> Optional[str]:
        """Recover one unambiguous broker order; ambiguity deliberately fails."""
        return self._sole_order(client_ref, await self._client_ref_orders(client_ref))

    async def _client_ref_orders(self, client_ref: str) -> Optional[set[str]]:
        """Order numbers the broker's books hold under `client_ref`.

        None when nothing matched and a book could not be read: absence is
        only evidence once both books have actually answered.
        """
        matches: set[str] = set()
        complete = True
        for getter_name in ("get_order_book", "get_trade_book"):
            getter = getattr(self.api, getter_name, None)
            if not getter:
                complete = False
                continue
            try:
                payload = await asyncio.wait_for(
                    asyncio.to_thread(getter), timeout=self.BROKER_RECONCILE_TIMEOUT_SEC
                )
            except Exception as exc:
                LOGGER.warning("%s reconciliation failed for %s: %s", getter_name, client_ref, exc)
                complete = False
                continue
            complete = complete and isinstance(payload, list)
            for row in self._broker_rows(payload):
                ref = str(row.get("remarks") or row.get("remarks1") or row.get("client_ref") or "")
                if ref != client_ref:
                    continue
                ordno = self._valid_orderno(
                    row.get("norenordno") or row.get("orderno") or row.get("order_number")
                )
                if ordno:
                    matches.add(ordno)
        return matches if (matches or complete) else None

    @staticmethod
    def _sole_order(client_ref: str, found: Optional[set[str]]) -> Optional[str]:
        if found and len(found) == 1:
            return next(iter(found))
        if found:
            LOGGER.critical("Ambiguous broker orders for client_ref=%s: %s", client_ref, sorted(found))
        return None

    async def _broker_terminal_record(self, ordno: str) -> Optional[Dict[str, Any]]:
        try:
            payload = await asyncio.wait_for(
                asyncio.to_thread(self.api.single_order_history, ordno),
                timeout=self.BROKER_RECONCILE_TIMEOUT_SEC,
            )
        except Exception as exc:
            LOGGER.warning("Order history unavailable for %s: %s", ordno, exc)
            return None
        exact = [row for row in self._broker_rows(payload)
                 if self._valid_orderno(row.get("norenordno") or row.get("orderno")) == ordno]
        for row in exact:
            state = str(row.get("status") or "").upper()
            if state in {"COMPLETE", "FILLED", "CANCELED", "CANCELLED", "REJECTED"}:
                return row
        return None

    async def _broker_terminal_status(self, ordno: str) -> Optional[str]:
        row = await self._broker_terminal_record(ordno)
        if not row:
            return None
        state = str(row["status"]).upper()
        return {"FILLED": "COMPLETE", "CANCELLED": "CANCELED"}.get(state, state)

    async def _create_order_attempt(
        self, *, row_id: Optional[int], phase: str, leg: str, attempt: int,
        symbol: str, side: str, qty: int,
    ) -> tuple[int, str]:
        # Sent as the order's `remarks` and matched back exactly to recover a
        # lost acknowledgement, so it must survive the broker untouched: 20
        # alphanumerics, inside the tag limit Indian broker APIs commonly
        # enforce. The row, phase and leg live on the attempt row itself.
        code = {"ENTRY": "E", "EXIT": "X", "FLATTEN": "F"}.get(phase, phase[:1])
        client_ref = f"NB{code}{leg[:1].upper()}{uuid.uuid4().hex[:16]}"
        now = _now_ist()
        rows = await self.db.execute_async(
            """
            INSERT INTO botdata.order_attempts
                (created_at,updated_at,order_log_id,client_ref,phase,leg,attempt_no,
                 symbol,side,requested_qty,status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'INTENT') RETURNING id;
            """,
            (now, now, row_id, client_ref, phase, leg, attempt, symbol, side, qty),
            fetch=True,
        )
        if not rows:
            raise RuntimeError("failed to persist broker order intent")
        return int(rows[0]["id"]), client_ref

    def _load_incomplete_execution_state(self) -> None:
        rows = self.db.execute(
            """
            SELECT symbol1,symbol2,lifecycle_state
            FROM botdata.order_logs AS o
            WHERE lifecycle_state IN ('ENTRY_PENDING','EXIT_PENDING','VERIFYING_FLAT','QUARANTINED')
               OR (
                    lifecycle_state='OPEN' AND NOT EXISTS (
                        SELECT 1 FROM botdata.trade_logs AS t
                        WHERE t.is_in_trade=TRUE
                          AND t.buy_orderno=o.buy_orderno
                          AND t.sell_orderno=o.sell_orderno
                    )
               )
            ORDER BY id DESC LIMIT 1;
            """,
            fetch=True,
        ) or []
        if not rows:
            return
        row = rows[0]
        buy_sym, sell_sym = row.get("symbol1"), row.get("symbol2")
        self._watch_symbols.update(str(sym) for sym in (buy_sym, sell_sym) if sym)
        self._set_quarantine(buy_sym, sell_sym, reason=f"restart_{row.get('lifecycle_state')}")

    async def _submit_order_strict(
        self, *, row_id: Optional[int], phase: str, leg: str, attempt: int,
        side: str, symbol: str, qty: int, reduce_only: Optional[bool] = None,
    ) -> str:
        """Persist intent, submit once, recover uncertain ack, then verify persistence."""
        if qty <= 0 or side not in {"B", "S"} or not symbol:
            raise RuntimeError(f"invalid order intent {side=} {symbol=} {qty=}")
        attempt_id, client_ref = await self._create_order_attempt(
            row_id=row_id, phase=phase, leg=leg, attempt=attempt,
            symbol=symbol, side=side, qty=qty,
        )
        ret: Any = None
        submit_error: Optional[Exception] = None
        try:
            ret = await asyncio.to_thread(
                self._place_broker_order, side, symbol, qty=qty,
                reduce_only=reduce_only, remarks=client_ref,
            )
        except OrderPreflightError as exc:
            await self.db.execute_async(
                "UPDATE botdata.order_attempts SET status='REJECTED',updated_at=%s,error=%s WHERE id=%s;",
                (_now_ist(), str(exc), attempt_id),
            )
            raise
        except Exception as exc:
            submit_error = exc

        stat_ok = isinstance(ret, dict) and str(ret.get("stat") or "").lower() == "ok"
        ordno = self._valid_orderno(
            (ret or {}).get("norenordno") or (ret or {}).get("orderno")
            if isinstance(ret, dict) else None
        )
        if not (stat_ok and ordno):
            # Even a clean Not_Ok can follow an upstream timeout. Never assume
            # non-acceptance and never resubmit before broker reconciliation.
            ordno = await self._recover_orderno_by_client_ref(client_ref)
        if not ordno:
            error = str(submit_error or ret or "missing/invalid broker order number")[:1000]
            await self.db.execute_async(
                "UPDATE botdata.order_attempts SET status='UNKNOWN',updated_at=%s,error=%s WHERE id=%s;",
                (_now_ist(), error, attempt_id),
            )
            raise UncertainOrderError(
                f"order acknowledgement uncertain for {client_ref}: {error}"
            ) from submit_error

        try:
            saved = await self.db.execute_async(
                """UPDATE botdata.order_attempts
                   SET orderno=%s,status='ACKNOWLEDGED',updated_at=%s
                   WHERE id=%s RETURNING orderno;""",
                (ordno, _now_ist(), attempt_id),
                fetch=True,
            )
            if not saved or str(saved[0].get("orderno") or "") != ordno:
                raise RuntimeError(f"failed to verify persisted order number {ordno}")
        except Exception as exc:
            raise UncertainOrderError(f"accepted order {ordno} could not be persisted: {exc}") from exc
        return ordno

    # -------- Cancel / emergency flatten ----------
    async def _cancel_order_if_possible(self, orderno: Optional[str]) -> bool:
        if not orderno:
            return False
        try:
            if hasattr(self.api, "cancel_order"):
                ret = await asyncio.to_thread(self.api.cancel_order, orderno=orderno)
                ok = isinstance(ret, dict) and str(ret.get("stat", "")).lower() == "ok"
                LOGGER.info("Cancel order %s -> %s", orderno, ("OK" if ok else ret))
                return ok
        except Exception as exc:
            LOGGER.debug("cancel_order failed for %s: %s", orderno, exc)
        return False

    async def _flatten_to_zero(self, symbol: str, *, reason: str) -> bool:
        try:
            contract = await asyncio.to_thread(self._contract_details, symbol)
            max_qty = contract["max_qty"]
        except OrderPreflightError as exc:
            LOGGER.error("Cannot flatten %s: %s", symbol, exc)
            return False
        initial_qty = await self._position_qty(symbol)
        if initial_qty is None:
            LOGGER.error("Cannot flatten %s: broker quantity unknown", symbol)
            return False
        # Every progressing IOC can fill as little as one lot. Allow that
        # worst case plus a fixed budget for no-fill or rejected attempts.
        max_attempts = ((abs(initial_qty) + contract["lot"] - 1) // contract["lot"]
                        + self.EMERGENCY_FLATTEN_RETRIES)
        for attempt in range(1, max_attempts + 1):
            qty = await self._position_qty(symbol)
            if qty is None:
                LOGGER.error("Flatten %s stopped: broker quantity unknown", symbol)
                return False

            if qty == 0:
                return True

            side = "S" if qty > 0 else "B"
            chunk = min(abs(qty), max_qty)
            try:
                LOGGER.warning(
                    "Flatten %s attempt %d: net=%s -> sending %s x%d IOC limit (reason=%s)",
                    symbol, attempt, qty, ("SELL" if side == "S" else "BUY"), chunk, reason,
                )
                ordno = await self._submit_order_strict(
                    row_id=self._active_orderlog_row_id,
                    phase="FLATTEN", leg="position", attempt=attempt,
                    side=side, symbol=symbol, qty=chunk, reduce_only=True,
                )
                deadline = time.monotonic() + 10.0
                while True:
                    terminal = await self._broker_terminal_record(ordno)
                    if terminal:
                        break
                    if time.monotonic() >= deadline:
                        LOGGER.critical("Flatten order %s still unresolved; refusing another order for %s", ordno, symbol)
                        return False
                    await asyncio.sleep(self.EMERGENCY_FLATTEN_SLEEP_SEC)
                try:
                    raw_fill = terminal.get("fillshares")
                    if raw_fill in (None, ""):
                        # The broker omits the field on an order that traded
                        # nothing. A COMPLETE order traded all of it. Either
                        # way the position check below must agree before the
                        # next order is sized.
                        status = str(terminal.get("status") or "").upper()
                        filled = chunk if status in {"COMPLETE", "FILLED"} else 0
                    else:
                        filled = int(float(str(raw_fill)))
                    if filled < 0 or filled > chunk:
                        raise ValueError("fill outside requested quantity")
                except (TypeError, ValueError, OverflowError) as exc:
                    LOGGER.critical("Flatten %s order %s has unknown fill: %s", symbol, ordno, exc)
                    return False
                expected = qty + (filled if side == "B" else -filled)
                settle_deadline = time.monotonic() + self.ENTRY_POSITION_VERIFY_TIMEOUT_SEC
                while True:
                    observed = await self._position_qty(symbol)
                    if observed == expected:
                        break
                    if time.monotonic() >= settle_deadline:
                        LOGGER.critical(
                            "Flatten %s order %s fill/position mismatch: expected %s, got %s",
                            symbol, ordno, expected, observed,
                        )
                        return False
                    await asyncio.sleep(self.ENTRY_POSITION_VERIFY_POLL_SEC)
            except Exception as exc:
                LOGGER.error("Flatten %s IOC limit failed: %s", symbol, exc)
                if isinstance(exc, UncertainOrderError):
                    LOGGER.critical(
                        "Flatten acknowledgement uncertain for %s; refusing blind retry.", symbol
                    )
                    return False
                if isinstance(exc, OrderPreflightError):
                    return False

            await asyncio.sleep(self.EMERGENCY_FLATTEN_SLEEP_SEC)

        qty = await self._position_qty(symbol)
        if qty == 0:
            return True
        LOGGER.error("Flatten %s failed; net=%s (reason=%s)", symbol, qty, reason)
        return False

    async def _emergency_flatten(self, symbol: str) -> None:
        if not await self._flatten_to_zero(symbol, reason="emergency_flatten"):
            LOGGER.warning("Emergency flatten attempts exhausted for %s", symbol)

    # ──────────────────────────────────────────────────────────────────────
    #  Entry (hedged)
    # ──────────────────────────────────────────────────────────────────────
    async def _place_hedge_seq(
        self, final_symbols: Dict[str, str], pending: Dict[str, Any]
    ) -> Optional[tuple[str, str, Optional[str], Optional[str]]]:
        buy_sym = final_symbols.get("buy")
        sell_sym = final_symbols.get("sell")
        sell_side = (final_symbols.get("sell_side") or "S").upper()
        if not buy_sym or not sell_sym:
            raise RuntimeError("final_symbols missing buy/sell legs")

        if await self._has_nonterminal_orders(buy_sym, sell_sym):
            LOGGER.warning("Non-terminal order exists for (%s, %s) - skipping new entry", buy_sym, sell_sym)
            raise RuntimeError("Pair has non-terminal orders")

        # Check both legs before buying the hedge. A broker freeze or lot-size
        # rejection on leg two would otherwise leave leg one exposed.
        expected_lot = instruments.quantity_of(self._index, self.lot_size)
        for sym in (buy_sym, sell_sym):
            details = await asyncio.to_thread(self._contract_details, sym)
            if details["lot"] != expected_lot or self.order_quantity > details["max_qty"]:
                raise OrderPreflightError(
                    f"{sym}: {self._order_lots} lots ({self.order_quantity} units) "
                    f"conflicts with broker lot {details['lot']} or freeze {details['max_qty']}"
                )
            # Both books, before the call is bought: a put too wide to sell
            # would otherwise be found only after the first leg had filled.
            await asyncio.to_thread(self._check_entry_book, sym)

        # Broker preflight can outlast the frozen signal's fill window. Check
        # the same intent and a current spot tick before starting any orders.
        try:
            active = self.strategy.entry_intent(now_ist())
        except Exception as exc:
            LOGGER.error("Entry intent recheck failed: %s", exc)
            return None
        if not pending.get("key") or not active or active.get("key") != pending["key"]:
            LOGGER.warning("Entry intent expired during broker preflight; skipping orders.")
            return None
        if self._live_price_for(self._index) is None:
            LOGGER.warning("Fresh %s spot price lost during broker preflight; skipping orders.", self._index)
            return None

        # Always create a fresh row for this entry sequence
        row_id = await self._ensure_order_log_row(buy_sym, sell_sym, sell_side=sell_side)
        self._active_orderlog_row_id = row_id
        self._sell_entry_side = sell_side

        # ---- BUY leg (the call) -------------------------------------------
        # Bought first: if the put cannot be sold - a margin shortfall, say -
        # `_enter_leg` flattens the call, and no naked short is ever left.
        buy_ordno = await self._enter_leg(
            row_id, "buy", "B", buy_sym, self.BUY_WAIT_TIMEOUT_SEC, label="BUY",
        )

        # ---- SELL leg (the put) -------------------------------------------
        sell_ordno = await self._enter_leg(
            row_id, "sell", sell_side, sell_sym, self.SELL_WAIT_TIMEOUT_SEC,
            label="SELL" if sell_side == "S" else "BUY", also_flatten=(buy_sym,),
        )

        return buy_sym, sell_sym, (str(buy_ordno) if buy_ordno is not None else None), (str(sell_ordno) if sell_ordno is not None else None)

    async def _enter_leg(
        self,
        row_id: int,
        leg: str,
        side: str,
        symbol: str,
        timeout_sec: float,
        *,
        label: str,
        also_flatten: tuple[str, ...] = (),
    ) -> str:
        """Place one entry leg and confirm it filled; roll it back and raise if not.

        The acknowledged order is bound to its row before anything waits on it.
        A leg that does not complete is cancelled and, once the broker reports
        it terminal, flattened - together with `also_flatten`, the legs placed
        before it, unless this leg is a short that could not be confirmed flat:
        then those are kept as its hedge. An outcome that cannot be confirmed raises
        `UncertainOrderError` instead: rolling back an order that may still
        fill would race the broker.
        """
        LOGGER.info("%s leg: %s x%d on %s", label, symbol, self.order_quantity, self.order_exchange)
        placed_at = _now_ist()  # capture BEFORE placing order
        ordno = await self._submit_order_strict(
            row_id=row_id, phase="ENTRY", leg=leg, attempt=1,
            side=side, symbol=symbol, qty=int(self.order_quantity),
        )
        try:
            await self._set_leg_orderno(row_id, leg, ordno)
        except Exception as exc:
            raise UncertainOrderError(
                f"{leg.upper()} order {ordno} could not be bound to its log row"
            ) from exc

        try:
            await self._wait_leg_status(
                row_id=row_id,
                leg=leg,
                ordno=ordno,
                expect="COMPLETE",
                timeout_sec=timeout_sec,
                poll_sec=self.POLL_INTERVAL_SEC,
                not_ok=self.NOT_OK_STATES,
                after_ts=placed_at,
            )
            await self._wait_leg_exchordid(
                row_id=row_id,
                leg=leg,
                ordno=ordno,
                timeout_sec=self.EXCHORDID_WAIT_TIMEOUT_SEC,
                poll_sec=self.POLL_INTERVAL_SEC,
                not_ok=self.NOT_OK_STATES,
            )
            LOGGER.info("%s leg COMPLETE (exchordid captured)", label)
        except Exception as exc:
            LOGGER.error("%s leg did not complete: %s", label, exc)
            await self._cancel_order_if_possible(ordno)
            try:
                terminal = await self._wait_leg_terminal(
                    row_id=row_id,
                    leg=leg,
                    ordno=ordno,
                    timeout_sec=timeout_sec,
                    poll_sec=self.POLL_INTERVAL_SEC,
                )
            except Exception as exc2:
                LOGGER.warning("%s leg terminal wait failed: %s", leg.upper(), exc2)
                terminal = None
            if terminal is None:
                raise UncertainOrderError(f"{leg.upper()} order {ordno} is still unresolved") from exc
            flat = await self._flatten_to_zero(symbol, reason=f"entry_{leg}_failed")
            if flat or side == "B":
                for placed in also_flatten:
                    await self._flatten_to_zero(placed, reason=f"entry_{leg}_failed_cleanup")
            elif also_flatten:
                # A short that will not close keeps its long: selling the
                # hedge now would leave the short naked.
                LOGGER.critical("%s short %s not confirmed flat; keeping %s as its hedge.",
                                label, symbol, ", ".join(also_flatten))
            raise RuntimeError(f"Aborting hedge: {leg.upper()} leg not completed")
        return ordno

    async def _validate_entry_execution(
        self,
        *,
        row_id: int,
        buy_sym: str,
        sell_sym: str,
        sell_side: str,
    ) -> None:
        """Fail closed unless fills and broker positions exactly match the entry.

        Order COMPLETE and an exchange order id are necessary, but neither alone
        proves that the requested quantity became the intended net position.
        Any missing or mismatched value rolls both symbols back to zero.  The
        caller consequently never writes an open trade for uncertain exposure.
        """
        expected_qty = int(self.order_quantity)
        if expected_qty <= 0:
            raise RuntimeError(f"invalid entry quantity configured: {expected_qty}")
        if sell_side not in {"B", "S"}:
            raise RuntimeError(f"invalid entry sell_side during quantity validation: {sell_side!r}")
        expected_buy_net = expected_qty
        expected_sell_net = expected_qty if sell_side == "B" else -expected_qty

        try:
            row = await self._fetch_order_log_row(row_id)
        except Exception as exc:
            # A database read failure is uncertainty, never permission to open.
            LOGGER.error("Could not read entry fills for row %s: %s", row_id, exc)
            row = None
        buy_fill = row.get("buy_fill_qty") if row else None
        sell_fill = row.get("sell_fill_qty") if row else None
        fill_ok = buy_fill == expected_qty and sell_fill == expected_qty
        if not fill_ok and row:
            # A COMPLETE push can omit fillshares. Confirm against the exact
            # broker orders before unwinding a fully filled hedge.
            for leg, key in (("buy", "buy_orderno"), ("sell", "sell_orderno")):
                ordno = self._valid_orderno(row.get(key))
                if ordno:
                    await self._reconcile_exact_order(row_id=row_id, leg=leg, ordno=ordno, is_exit=False)
            row = await self._fetch_order_log_row(row_id)
            buy_fill = row.get("buy_fill_qty") if row else None
            sell_fill = row.get("sell_fill_qty") if row else None
            fill_ok = buy_fill == expected_qty and sell_fill == expected_qty

        buy_net: Optional[int] = None
        sell_net: Optional[int] = None
        if fill_ok:
            deadline = time.monotonic() + max(0.0, self.ENTRY_POSITION_VERIFY_TIMEOUT_SEC)
            while True:
                buy_net = await self._position_qty(buy_sym)
                sell_net = await self._position_qty(sell_sym)
                if buy_net == expected_buy_net and sell_net == expected_sell_net:
                    break
                if time.monotonic() >= deadline:
                    break
                await asyncio.sleep(max(0.01, self.ENTRY_POSITION_VERIFY_POLL_SEC))
        else:
            # A definitive fill mismatch cannot converge into a valid fill record;
            # avoid holding bad exposure for the position-book grace period.
            buy_net = await self._position_qty(buy_sym)
            sell_net = await self._position_qty(sell_sym)

        position_ok = buy_net == expected_buy_net and sell_net == expected_sell_net
        if fill_ok and position_ok:
            LOGGER.info(
                "Entry quantities verified: %s fill/net=%s/%s, %s fill/net=%s/%s",
                buy_sym, buy_fill, buy_net, sell_sym, sell_fill, sell_net,
            )
            return

        detail = (
            f"entry quantity mismatch: expected fills={expected_qty}/{expected_qty}, "
            f"recorded fills={buy_fill}/{sell_fill}; expected broker net="
            f"{expected_buy_net}/{expected_sell_net}, broker net={buy_net}/{sell_net}"
        )
        LOGGER.critical("%s. Rolling both legs back to zero.", detail)

        buy_flat, sell_flat = await self._rollback_entry_symbols(
            buy_sym, sell_sym, reason="entry_quantity_mismatch"
        )
        if not (buy_flat and sell_flat):
            LOGGER.critical(
                "Entry rollback could not confirm flat: %s=%s, %s=%s. "
                "Quarantine will block further entries.",
                buy_sym, buy_flat, sell_sym, sell_flat,
            )
        raise RuntimeError(detail)

    async def _rollback_entry_symbols(
        self, buy_sym: Optional[str], sell_sym: Optional[str], *, reason: str
    ) -> tuple[bool, bool]:
        """Best-effort rollback that never lets one leg prevent the other."""
        async def flatten_safely(symbol: Optional[str]) -> bool:
            if not symbol:
                return False
            try:
                return await self._flatten_to_zero(symbol, reason=reason)
            except Exception as exc:
                LOGGER.exception("Entry rollback raised while flattening %s: %s", symbol, exc)
                return False

        # The sold leg first. If it cannot be confirmed flat the bought leg is
        # left alone as its hedge: closing it would turn a failed rollback into
        # a naked short. Quarantine and reconciliation take it from there.
        sell_flat = await flatten_safely(sell_sym)
        # If symbols are ever accidentally identical, the first call has already
        # flattened the shared position; avoid issuing a redundant broker query.
        if buy_sym and buy_sym == sell_sym:
            return sell_flat, sell_flat
        if sell_sym and not sell_flat:
            LOGGER.critical("Rollback: %s not confirmed flat; keeping %s open as its hedge.",
                            sell_sym, buy_sym)
            return False, False
        buy_flat = await flatten_safely(buy_sym)
        return buy_flat, sell_flat

    def _entry_cooldown_active(self) -> bool:
        now = time.time()
        if (now - self._last_entry_fail_ts) < self.ENTRY_FAIL_COOLDOWN_SEC:
            left = self.ENTRY_FAIL_COOLDOWN_SEC - (now - self._last_entry_fail_ts)
            LOGGER.info("Entry cooling down (%.1fs left).", left)
            return True
        if self._entry_fail_count >= self.ENTRY_FAIL_BUDGET_PER_MIN and (now - self._last_entry_fail_ts) < 60:
            LOGGER.warning("Entry retry budget reached for this minute; skipping this cycle.")
            return True
        return False

    # ──────────────────────────────────────────────────────────────────────
    #  Trade actions
    # ──────────────────────────────────────────────────────────────────────
    async def _enter_trade(
        self, direction: str, entry_price: float, pending: Dict[str, Any]
    ) -> bool:
        """Open the synthetic long future the strategy asked for.

        `direction` is v3's own side, always LONG. It is translated to the
        directional label `GetSymbol` uses (`bias_tag`) purely to select legs:
        that label reads 'HH' as bullish. `lots` lots per leg (v3 trades one).
        """
        if self.entering:
            LOGGER.debug("[LIVE] Entry already in progress - skip")
            return False

        index = str(pending.get("index") or instruments.CHART_INDEX)
        fetcher = self._fetcher_for(pending.get("index"))
        if fetcher is None:
            LOGGER.error(
                "[LIVE] v3 gave the slot to %s but this process has no leg "
                "builder for it (has: %s). Entry skipped rather than routed to "
                "another index's contracts.",
                index, ", ".join(sorted(self.symbol_fetchers)) or "none",
            )
            return False
        # Take one snapshot of the legs and use only that. `GetSymbol` publishes
        # legs and the bias they were built for in a single assignment, so
        # re-reading the fetcher mid-entry could pair new bias with old legs.
        fs = getattr(fetcher, "final_symbols", None)
        if isinstance(fs, dict):
            fs = dict(fs)
        if isinstance(fs, dict) and fs.get("index") not in (None, index):
            LOGGER.error(
                "[LIVE] %s's leg builder published %s legs - entry skipped.",
                index, fs.get("index"),
            )
            return False
        if isinstance(fs, dict):
            stamp = fs.get("built_at")
            if (isinstance(stamp, bool) or not isinstance(stamp, (int, float))
                    or not 0 <= time.monotonic() - stamp <= self.LEGS_MAX_AGE_SEC):
                LOGGER.warning("[LIVE] %s legs are stale or undated - entry skipped.", index)
                return False
        self.symbol = (fs or {}).get("sell") or getattr(fetcher, "final_symbol", None)
        if not self.symbol:
            # LIVE cannot invent a symbol - refusing is correct. What was wrong
            # was doing it at DEBUG, which `main.py`'s INFO handler discards:
            # the bot then declines every entry with no trace at all. Report it
            # at WARNING, name the cause, and escalate once it is clear this is
            # a standing fault rather than a one-poll race with the updater.
            reason = (
                getattr(fetcher, "legs_blocked_reason", None)
                or "reason not reported by the symbol fetcher"
            )
            now = time.monotonic()
            if self._legs_missing_since is None:
                self._legs_missing_since = now
            blocked_for = now - self._legs_missing_since
            emit, folded = _THROTTLE.should_log("live_no_legs")
            if emit:
                if blocked_for >= self.LEGS_MISSING_ALARM_SEC:
                    LOGGER.error(
                        "[LIVE] Option legs still not ready after %.0fs (%s). "
                        "EVERY entry is being skipped - the bot cannot trade "
                        "until this clears.%s",
                        blocked_for, reason, log_throttle.suffix(folded),
                    )
                else:
                    LOGGER.warning(
                        "[LIVE] Option legs not ready (%s) - entry SKIPPED; will "
                        "retry next poll.%s",
                        reason, log_throttle.suffix(folded),
                    )
            return False
        self._legs_missing_since = None
        _THROTTLE.reset("live_no_legs")
        if isinstance(fs, dict):
            for key in ("buy", "sell"):
                sym = fs.get(key)
                if sym:
                    self._watch_symbols.add(str(sym))

        tag = bias_tag(direction)
        stop_points = float(pending.get("stop_loss_points") or 0.0)
        self.direction = direction
        self.entry_price = entry_price
        self.stop_price = (
            (entry_price - stop_points if direction == "LONG" else entry_price + stop_points)
            if stop_points > 0
            else None
        )

        if not isinstance(fs, dict) or not fs.get("buy") or not fs.get("sell"):
            LOGGER.error("[LIVE] Hedged legs not ready - entry skipped.")
            return False
        # Read the bias out of the SAME snapshot the legs came from.
        leg_bias = fs.get("bias")
        if leg_bias != tag:
            LOGGER.warning(
                "[LIVE] Leg bias mismatch (strategy wants %s=%s, legs built for %s) "
                "- entry skipped; will retry once the legs are rebuilt.",
                direction, tag, leg_bias,
            )
            return False
        sell_side = (fs.get("sell_side") or "").upper()
        sell_sym = fs.get("sell")
        if sell_side not in {"S", "B"}:
            LOGGER.error("[LIVE] Invalid sell_side '%s' - entry skipped.", sell_side)
            return False
        if sell_sym and str(sell_sym).endswith(("F", "FUT")):  # NFO ...F, BFO ...FUT
            expected = "B" if direction == "LONG" else "S"
            if sell_side != expected:
                LOGGER.error(
                    "[LIVE] Futures side mismatch (direction=%s expected=%s got=%s) - entry skipped.",
                    direction, expected, sell_side,
                )
                return False
        else:
            if sell_side != "S":
                LOGGER.error(
                    "[LIVE] Options sell_side must be 'S' (got=%s) - entry skipped.",
                    sell_side,
                )
                return False
        acquired = False
        entered = False
        previous_index = getattr(self, "_index", None)
        legs = fs
        self.entering = True
        try:
            acquired = await self._try_acquire_entry_lock()
            if not acquired:
                return False

            if await self._has_nonterminal_orders(legs["buy"], legs["sell"]):
                LOGGER.warning("⏭️  Pair already has non-terminal orders – skipping entry")
                return False

            # Refuse to stack on top of exposure that is already there. Stray
            # quantity is normally cleared by the flat-path reconciliation, but
            # that now runs after the decider so a stop is never delayed - which
            # leaves a window where an entry could double up on an orphan leg.
            # Entry requires positive proof of flatness. Unknown broker state is
            # never permission to add exposure.
            for leg in (legs["buy"], legs["sell"]):
                qty = await self._position_qty(leg)
                if qty is None:
                    LOGGER.error("%s broker quantity is unknown; entry blocked.", leg)
                    self._set_quarantine(legs["buy"], legs["sell"], reason="position_unknown")
                    return False
                if qty != 0:
                    LOGGER.error(
                        "⏭️  %s already shows quantity %s at the broker – entry "
                        "skipped so the strategy cannot double its exposure.",
                        leg, qty,
                    )
                    self._set_quarantine(legs["buy"], legs["sell"], reason="orphan_position")
                    return False

            # Claim the index immediately before the first order goes out.
            # `order_exchange` and `order_quantity` read it, and it decides
            # which exchange this entry routes to and how many units it is.
            # Nothing above this line places an order, and the `finally` puts
            # it back if the entry does not open - so a failed BANKNIFTY entry
            # can never leave the manager sizing a NIFTY position at 30.
            self._index = index
            placed = await self._place_hedge_seq(legs, pending)
            if placed is None:
                return False
            buy_sym, sell_sym, buy_ordno, sell_ordno = placed

            if self._active_orderlog_row_id is None:
                raise RuntimeError("entry order-log row missing during quantity validation")
            await self._validate_entry_execution(
                row_id=self._active_orderlog_row_id,
                buy_sym=buy_sym,
                sell_sym=sell_sym,
                sell_side=(self._sell_entry_side or "").upper(),
            )
            self._entry_fail_count = 0
            self._last_entry_fail_ts = 0.0

            book = str(pending.get("book") or CARRY_BOOK)
            self.trade_row_id = await self._commit_open_trade_atomic(
                self._active_orderlog_row_id,
                entry_price, self.direction, self.stop_price,
                buy_sym, sell_sym,
                buy_ordno or None, sell_ordno or None,
                self._sell_entry_side,
                book,
                index,
            )
            self._book = book
            self._entry_day = now_ist().date()
            self._held_quantity = self.order_quantity
            self._buy_orderno, self._sell_orderno = buy_ordno, sell_ordno
            self.in_trade = True
            entered = True
            # WARNING: main.py's handlers keep nothing below it, and a live
            # entry must be in the day's log.
            LOGGER.warning(
                "[LIVE] ENTER %s %s on %s, with %s (%s x%d) @ %.2f | stop %s (%s, %.2f pts) "
                "| buy#%s sell#%s | book=%s | candidates=%s",
                self.direction,
                sell_sym,
                index,
                buy_sym,
                self.order_exchange,
                self.order_quantity,
                entry_price,
                "-" if self.stop_price is None else f"{self.stop_price:.2f}",
                pending.get("source"),
                stop_points,
                buy_ordno or "-",
                sell_ordno or "-",
                book,
                pending.get("priority_candidates") or "-",
            )

        except Exception as exc:
            self._last_entry_fail_ts = time.time()
            self._entry_fail_count += 1
            LOGGER.error("[LIVE] Hedged entry failed/rolled back: %s", exc)
            buy_sym = legs.get("buy") if isinstance(legs, dict) else None
            sell_sym = legs.get("sell") if isinstance(legs, dict) else None
            # This outer rollback closes every exceptional gap: failure while
            # validating, a lost order-log row, or a trade-log insert failure.
            # Rechecking a leg already flattened by an inner handler is harmless
            # and preferable to leaving uncertain broker exposure behind.
            row_id = self._active_orderlog_row_id
            if isinstance(exc, UncertainTradeCommitError):
                # Held until the ledger answers - see `_resolve_uncertain_open_commit`.
                self._uncertain_execution_id = exc.execution_id
            if isinstance(exc, (UncertainOrderError, UncertainTradeCommitError)):
                LOGGER.critical(
                    "Entry/order commit outcome unresolved; suppressing rollback until exact reconciliation."
                )
            else:
                flat = await self._rollback_entry_symbols(buy_sym, sell_sym, reason="entry_transaction_failed")
                if row_id is not None and all(flat):
                    # Broker confirmed flat: the attempt is over. Left
                    # ENTRY_PENDING it reads as live exposure to every restart
                    # and keeps the shutdown from archiving the ledger.
                    try:
                        await self.db.execute_async(
                            "UPDATE botdata.order_logs SET lifecycle_state='ABORTED' "
                            "WHERE id=%s AND lifecycle_state='ENTRY_PENDING';",
                            (row_id,),
                        )
                    except Exception as db_exc:  # noqa: BLE001 - the restart recovery still covers it
                        LOGGER.error("Could not mark failed entry row %s aborted: %s", row_id, db_exc)
            self._set_quarantine(buy_sym, sell_sym, reason="entry_failed")
            self._active_orderlog_row_id = None
            self._buy_orderno = None
            self._sell_orderno = None
            self._sell_entry_side = None
        finally:
            if not entered:
                self._index = previous_index
            if acquired:
                await self._release_entry_lock()
            self.entering = False
        return entered

    async def _realized_pnl(self, row_id: int, buy_sym: str, sell_sym: str) -> Optional[float]:
        """Rupees the closed position made on its own fills, before charges.

        None whenever any fill cannot be accounted for - a guess is worse than
        no number. Entry fills come off the order-log row, written in the
        session the position opened: broker order history is per session, so
        an overnight carry's entry could not be read back today. Exit fills are
        every EXIT and FLATTEN order this row sent, read from the broker's order
        history, so a partial IOC and the order for its residual both count.
        Each leg must net to zero across those fills, or the record is
        incomplete and nothing is claimed.
        """
        row = await self._fetch_order_log_row(row_id)
        if not row:
            return None
        qty = int(self.order_quantity)
        sell_side = (self._sell_entry_side or "S").upper()
        fills: List[tuple] = []
        for sym, side, filled, avg in (
            (buy_sym, "B", row.get("buy_fill_qty"), row.get("buy_avg_price")),
            (sell_sym, sell_side, row.get("sell_fill_qty"), row.get("sell_avg_price")),
        ):
            if filled is None or int(filled) != qty:
                return None
            fills.append((sym, side, qty, avg))
        attempts = await self.db.execute_async(
            """SELECT orderno, symbol, side, requested_qty FROM botdata.order_attempts
               WHERE order_log_id=%s AND phase IN ('EXIT','FLATTEN') AND orderno IS NOT NULL
               ORDER BY id;""",
            (row_id,), fetch=True,
        ) or []
        for attempt in attempts:
            record = await self._broker_terminal_record(str(attempt["orderno"]))
            if record is None:
                return None
            raw = record.get("fillshares")
            if raw in (None, ""):
                status = str(record.get("status") or "").upper()
                filled = int(attempt["requested_qty"]) if status in {"COMPLETE", "FILLED"} else 0
            else:
                filled = int(float(str(raw)))
            fills.append((attempt["symbol"], attempt["side"], filled, record.get("avgprc")))
        net = {buy_sym: 0, sell_sym: 0}
        cash = 0.0
        for sym, side, filled, avg in fills:
            if sym not in net or filled < 0:
                return None
            if not filled:
                continue
            price = float(str(avg)) if avg not in (None, "") else 0.0
            if not 0 < price < float("inf"):
                return None
            sign = 1 if side == "B" else -1
            net[sym] += sign * filled
            cash -= sign * filled * price
        if any(net.values()):
            return None
        return round(cash, 2)

    async def _commit_closed_trade_atomic(
        self, *, order_log_id: int, win: int, exit_price: float,
        realized_pnl: Optional[float] = None,
    ) -> None:
        if self.trade_row_id is None:
            raise RuntimeError("cannot close trade without trade_row_id")
        trade_id = self.trade_row_id
        conn = await self.db.get_raw_connection_async()
        transaction_error: Optional[Exception] = None
        committed = False
        try:
            def commit_transaction() -> None:
                with conn.cursor() as cur:
                    cur.execute(
                        """UPDATE botdata.trade_logs
                           SET is_in_trade=FALSE,trade_state='CLOSED',win=%s,exit_price=%s,exit_time=%s,
                               realized_pnl=%s
                           WHERE id=%s AND is_in_trade=TRUE RETURNING id;""",
                        (win, exit_price, _now_ist(), realized_pnl, trade_id),
                    )
                    if not cur.fetchone():
                        raise RuntimeError(f"open trade row {trade_id} was not available for close")
                    cur.execute(
                        """UPDATE botdata.order_logs SET lifecycle_state='CLOSED'
                           WHERE id=%s AND lifecycle_state IN ('EXIT_PENDING','QUARANTINED')
                           RETURNING id;""",
                        (order_log_id,),
                    )
                    if not cur.fetchone():
                        raise RuntimeError(f"order row {order_log_id} was not exit-pending during close")
                conn.commit()

            await asyncio.to_thread(commit_transaction)
            committed = True
        except Exception as exc:
            transaction_error = exc
            try:
                await asyncio.to_thread(conn.rollback)
            except Exception:
                pass
        finally:
            try:
                await self.db.put_raw_connection_async(conn, close=transaction_error is not None)
            except Exception as pool_exc:
                LOGGER.error("Failed returning CLOSE transaction connection to pool: %s", pool_exc)
        if committed:
            return
        try:
            recovered = await self.db.execute_async(
                """SELECT t.id FROM botdata.trade_logs AS t
                   JOIN botdata.order_logs AS o ON o.id=%s
                   WHERE t.id=%s AND t.is_in_trade=FALSE AND t.trade_state='CLOSED'
                     AND o.lifecycle_state='CLOSED';""",
                (order_log_id, trade_id), fetch=True,
            )
        except Exception as recovery_exc:
            raise UncertainTradeCommitError(
                f"cannot determine CLOSE transaction outcome for trade {trade_id}: {recovery_exc}"
            ) from transaction_error
        if recovered:
            return
        raise RuntimeError(f"CLOSE transaction rolled back: {transaction_error}") from transaction_error

    async def _exit_leg_with_retries(
        self,
        *,
        buy_sym: str,
        sell_sym: str,
        leg: str,                # "sell" (cover short) | "buy" (close long)
        action_side: str,        # "B" or "S"
        symbol: str,
        after_ts: Optional[datetime],
        per_try_timeout: float,
        row_id: Optional[int],
    ) -> bool:
        """
        STRICT exit confirmation:
          - Capture exit exchordid (hard gate).
          - Wait for EXIT state COMPLETE after our timestamp (hard gate).
          - Retries with backoff; emergency flatten if all fail.

        Fix: capture ts_exit_attempt BEFORE placing the exit order and ensure
             waiter guard >= that attempt time to avoid accepting stale completes.
        """
        max_tries = max(1, int(self.EXIT_MAX_RETRIES))
        backoff = max(0.5, float(self.EXIT_RETRY_BACKOFF_SEC))
        row_id_val = row_id

        last_exc: Optional[Exception] = None
        for attempt in range(1, max_tries + 1):
            exit_ordno: Optional[str] = None
            try:
                qty = await self._position_qty(symbol)
                if qty == 0:
                    LOGGER.info("Exit leg %s already flat: %s", leg.upper(), symbol)
                    return True
                if qty is None:
                    raise RuntimeError(
                        f"broker quantity unknown for EXIT {leg.upper()} {symbol}; blind exit suppressed"
                    )

                if row_id_val is None:
                    row_id_val = await self._resolve_order_log_row_id(buy_sym, sell_sym)
                if row_id_val is None:
                    row_id_val = await self._ensure_order_log_row(
                        buy_sym, sell_sym, sell_side=self._sell_entry_side
                    )
                if row_id_val is None:
                    raise RuntimeError(f"order_logs row not available for EXIT {leg.upper()} leg")

                existing = await self._fetch_order_log_row(row_id_val)
                if existing:
                    exit_cols = _leg_columns(leg, True)
                    existing_ordno = existing[exit_cols["orderno"]]
                    existing_state = existing[exit_cols["state"]]
                    if existing_ordno:
                        st = (existing_state or "").upper()
                        if st in {"OPEN", "PENDING", "TRIGGER_PENDING"} or st == "":
                            LOGGER.info(
                                "Existing exit order %s (%s) for %s leg; waiting",
                                existing_ordno, (st or "UNKNOWN"), leg.upper(),
                            )
                            try:
                                await self._wait_leg_status(
                                    row_id=row_id_val,
                                    leg=leg,
                                    ordno=existing_ordno,
                                    expect="COMPLETE",
                                    timeout_sec=per_try_timeout,
                                    poll_sec=self.POLL_INTERVAL_SEC,
                                    not_ok=self.NOT_OK_STATES,
                                    after_ts=None,
                                    is_exit=True,
                                )
                                await self._wait_leg_exchordid(
                                    row_id=row_id_val,
                                    leg=leg,
                                    ordno=existing_ordno,
                                    timeout_sec=self.EXIT_EXCHORDID_WAIT_TIMEOUT_SEC,
                                    poll_sec=self.POLL_INTERVAL_SEC,
                                    not_ok=self.NOT_OK_STATES,
                                    is_exit=True,
                                )
                                # The quantity read above predates this fill.
                                qty = (await self._await_flat(symbol))[symbol]
                                if qty == 0:
                                    LOGGER.info("Exit leg %s COMPLETE via existing order", leg.upper())
                                    return True
                                LOGGER.warning("Exit leg %s still has broker quantity %s after completed order",
                                               leg.upper(), qty)
                            except Exception as exc:
                                LOGGER.warning("Existing exit order %s not complete: %s", existing_ordno, exc)
                                await self._cancel_order_if_possible(existing_ordno)
                                try:
                                    terminal = await self._wait_leg_terminal(
                                        row_id=row_id_val,
                                        leg=leg,
                                        ordno=existing_ordno,
                                        timeout_sec=min(10.0, per_try_timeout),
                                        poll_sec=self.POLL_INTERVAL_SEC,
                                        is_exit=True,
                                    )
                                except Exception:
                                    terminal = None
                                if terminal is None:
                                    raise UncertainOrderError(
                                        f"existing EXIT {leg} order {existing_ordno} is unresolved"
                                    ) from exc
                                raise
                        if st == "COMPLETE":
                            await self._wait_leg_exchordid(
                                row_id=row_id_val,
                                leg=leg,
                                ordno=existing_ordno,
                                timeout_sec=self.EXIT_EXCHORDID_WAIT_TIMEOUT_SEC,
                                poll_sec=self.POLL_INTERVAL_SEC,
                                not_ok=self.NOT_OK_STATES,
                                is_exit=True,
                            )
                            qty = (await self._await_flat(symbol))[symbol]
                            if qty == 0:
                                LOGGER.info("Exit leg %s already COMPLETE in logs", leg.upper())
                                return True
                            LOGGER.warning("Exit leg %s remains open (%s) after logged COMPLETE; "
                                           "sizing residual", leg.upper(), qty)

                # Sized from the newest settled reading, never one taken
                # before an order that has since filled.
                if qty is None:
                    raise RuntimeError(
                        f"broker quantity unknown for EXIT {leg.upper()} {symbol}; blind exit suppressed"
                    )
                order_side = "S" if qty > 0 else "B"
                order_qty = abs(qty)
                if order_side != action_side:
                    LOGGER.warning(
                        "Exit side override for %s leg %s: action=%s qty=%s -> side=%s",
                        leg.upper(), symbol, action_side, qty, order_side,
                    )

                # A multi-lot exit uses the exact broker net after each IOC
                # fill, and slices it if today's freeze quantity is smaller.
                contract = await asyncio.to_thread(self._contract_details, symbol)
                if order_qty > contract["lot"]:
                    return await self._flatten_to_zero(symbol, reason=f"exit_{leg}")

                LOGGER.info(
                    "Attempt %d/%d EXIT: %s %s x%d",
                    attempt, max_tries,
                    ("BUY" if order_side == "B" else "SELL"), symbol, order_qty,
                )

                ts_exit_attempt = _now_ist()
                exit_ordno = await self._submit_order_strict(
                    row_id=row_id_val, phase="EXIT", leg=leg, attempt=attempt,
                    side=order_side, symbol=symbol, qty=order_qty, reduce_only=True,
                )

                if row_id_val and exit_ordno:
                    try:
                        await self._set_leg_exit_orderno(row_id_val, leg, str(exit_ordno))
                    except Exception as exc:
                        raise UncertainOrderError(
                            f"EXIT order {exit_ordno} could not be bound to its log row"
                        ) from exc
                    self._active_orderlog_row_id = row_id_val

                guard_ts = ts_exit_attempt if (after_ts is None or after_ts < ts_exit_attempt) else after_ts
                await self._wait_leg_status(
                    row_id=row_id_val,
                    leg=leg,
                    ordno=exit_ordno,
                    expect="COMPLETE",
                    timeout_sec=per_try_timeout,
                    poll_sec=self.POLL_INTERVAL_SEC,
                    not_ok=self.NOT_OK_STATES,
                    after_ts=guard_ts,
                    is_exit=True,
                )
                await self._wait_leg_exchordid(
                    row_id=row_id_val,
                    leg=leg,
                    ordno=exit_ordno,
                    timeout_sec=self.EXIT_EXCHORDID_WAIT_TIMEOUT_SEC,
                    poll_sec=self.POLL_INTERVAL_SEC,
                    not_ok=self.NOT_OK_STATES,
                    is_exit=True,
                )
                LOGGER.info("Exit leg %s COMPLETE (and exchordid captured) on attempt %d", leg.upper(), attempt)
                return True

            except Exception as exc:
                last_exc = exc
                if exit_ordno:
                    await self._cancel_order_if_possible(exit_ordno)
                    try:
                        terminal = await self._wait_leg_terminal(
                            row_id=row_id_val,
                            leg=leg,
                            ordno=exit_ordno,
                            timeout_sec=min(10.0, per_try_timeout),
                            poll_sec=self.POLL_INTERVAL_SEC,
                            is_exit=True,
                        )
                    except Exception:
                        terminal = None
                    if terminal is None:
                        raise UncertainOrderError(
                            f"EXIT {leg} order {exit_ordno} is unresolved after cancel"
                        ) from exc
                LOGGER.error("Exit leg %s attempt %d failed: %s", leg.upper(), attempt, exc)
                if isinstance(exc, UncertainOrderError):
                    LOGGER.critical(
                        "EXIT %s outcome unknown; refusing retry and emergency flatten.", leg.upper()
                    )
                    raise
                if attempt < max_tries:
                    sleep_s = backoff * (2 ** (attempt - 1))
                    LOGGER.info("Retrying %s leg in %.1fs", leg.upper(), sleep_s)
                    await asyncio.sleep(sleep_s)

        LOGGER.error("All exit attempts failed for %s leg %s. Triggering emergency flatten.", leg.upper(), symbol)
        await self._emergency_flatten(symbol)
        if last_exc:
            raise RuntimeError(f"All exit attempts failed for {leg.upper()} leg {symbol}") from last_exc
        return False

    async def _exit_trade(self, reason: str, win: int, exit_price: float) -> None:
        # Prevent duplicate/overlapping exits
        if self.exiting:
            LOGGER.debug("[LIVE] Exit already in progress – skip")
            return

        acquired = False
        self.exiting = True
        try:
            acquired = await self._try_acquire_exit_lock()
            if not acquired:
                return

            # An earlier order whose outcome is not yet known (an exit or
            # flatten that raised UncertainOrderError) may still fill. A new
            # order sized off a book that has not caught up with it could
            # reverse the position, so nothing is sent until the broker has
            # answered for it; the caller retries on its next poll. Restart
            # recovery has always waited on the same check.
            if await self._reconcile_unresolved_attempts():
                emit, folded = _THROTTLE.should_log("exit_held_unresolved")
                if emit:
                    LOGGER.critical("[LIVE] Exit (%s) held: an earlier order is unresolved at "
                                    "the broker; retrying once it is.%s",
                                    reason, log_throttle.suffix(folded))
                return
            _THROTTLE.reset("exit_held_unresolved")

            # Prefer the current open trade record; fall back to order_logs only if needed.
            buy_sym = sell_sym = None

            trow = await self.db.execute_async(
                """
                SELECT buy_symbol, sell_symbol
                FROM   botdata.trade_logs
                WHERE  is_in_trade = TRUE AND execution_mode='LIVE'
                ORDER  BY id DESC
                LIMIT  1;
                """,
                fetch=True,
            )
            if trow:
                buy_sym, sell_sym = trow[0]["buy_symbol"], trow[0]["sell_symbol"]

            if not (buy_sym and sell_sym):
                orow = await self.db.execute_async(
                    """
                    SELECT symbol1 AS buy_symbol, symbol2 AS sell_symbol
                    FROM   botdata.order_logs
                    WHERE  symbol1 IS NOT NULL OR symbol2 IS NOT NULL
                    ORDER  BY id DESC
                    LIMIT  1;
                    """,
                    fetch=True
                )
                if orow:
                    buy_sym, sell_sym = orow[0]["buy_symbol"], orow[0]["sell_symbol"]

            if not (buy_sym and sell_sym):
                LOGGER.error("Cannot exit: buy/sell symbols not found.")
                return

            row_id = await self._resolve_order_log_row_id(buy_sym, sell_sym)
            if row_id is None:
                row_id = await self._ensure_order_log_row(buy_sym, sell_sym, sell_side=self._sell_entry_side)
            self._active_orderlog_row_id = row_id
            # Guarded so a retry after a CLOSE whose acknowledgement was lost
            # cannot reopen the ledger: the close's own recovery check reads
            # these two states, and overwriting them looped the exit forever.
            await self.db.execute_async(
                "UPDATE botdata.order_logs SET lifecycle_state='EXIT_PENDING' "
                "WHERE id=%s AND lifecycle_state IS DISTINCT FROM 'CLOSED';",
                (row_id,),
            )
            if self.trade_row_id is not None:
                await self.db.execute_async(
                    "UPDATE botdata.trade_logs SET trade_state='EXIT_PENDING' WHERE id=%s AND is_in_trade=TRUE;",
                    (self.trade_row_id,),
                )

            sell_closed = False
            buy_closed = False
            exit_uncertain = False

            # Determine entry side for sell leg to decide exit action
            sell_entry_side = (self._sell_entry_side or "").upper()
            if not sell_entry_side:
                # Fallback inference: futures get direction-based side; options are shorts
                if sell_sym.endswith(("F", "FUT")):
                    sell_entry_side = "B" if (self.direction or "LONG") == "LONG" else "S"
                else:
                    sell_entry_side = "S"
            sell_exit_side = "B" if sell_entry_side == "S" else "S"

            # SELL leg first (cover short) — STRICT
            try:
                sell_closed = await self._exit_leg_with_retries(
                    buy_sym=buy_sym,
                    sell_sym=sell_sym,
                    leg="sell",
                    action_side=sell_exit_side,
                    symbol=sell_sym,
                    after_ts=None,            # per-attempt guard inside the method
                    per_try_timeout=self.SELL_WAIT_TIMEOUT_SEC,
                    row_id=row_id,
                )
            except Exception as exc:
                LOGGER.error("Final failure to exit SELL leg: %s (emergency flatten already attempted).", exc)
                exit_uncertain = isinstance(exc, UncertainOrderError)

            # BUY leg next — STRICT
            try:
                if exit_uncertain or not sell_closed:
                    raise UncertainOrderError("sell leg not confirmed flat; hedge exit suppressed")
                buy_closed = await self._exit_leg_with_retries(
                    buy_sym=buy_sym,
                    sell_sym=sell_sym,
                    leg="buy",
                    action_side="S",          # SELL to close long option leg
                    symbol=buy_sym,
                    after_ts=None,            # per-attempt guard inside the method
                    per_try_timeout=self.BUY_WAIT_TIMEOUT_SEC,
                    row_id=row_id,
                )
            except Exception as exc:
                LOGGER.error("Final failure to exit BUY leg: %s (emergency flatten already attempted).", exc)
                exit_uncertain = exit_uncertain or isinstance(exc, UncertainOrderError)

            if not sell_closed or not buy_closed:
                exit_uncertain = True

            # Final RMS sanity check for both legs, after the position book has
            # had its settle window: straight after a COMPLETE it can still
            # show the leg, and flattening that reading reverses the position.
            if not exit_uncertain:
                net = await self._await_flat(buy_sym, sell_sym)
                qty_buy, qty_sell = net[buy_sym], net[sell_sym]
                if (qty_buy is not None and qty_buy != 0) or (qty_sell is not None and qty_sell != 0):
                    LOGGER.warning("RMS still shows open qty after exit (buy=%s, sell=%s). Forcing final flatten.", qty_buy, qty_sell)
                    # Short first, so the hedge is never the leg left alone:
                    # the long is closed only once the short is confirmed flat.
                    if await self._flatten_to_zero(sell_sym, reason="emergency_flatten"):
                        await self._emergency_flatten(buy_sym)
                    else:
                        LOGGER.critical("Emergency flatten of %s failed; keeping %s as its hedge.",
                                        sell_sym, buy_sym)

            # Broker truth is the final gate. Unknown is not flat, and an
            # emergency order is not proof of closure; re-read after cleanup.
            book = await self._position_book()
            qty_buy = None if book is None else book.get(buy_sym, 0)
            qty_sell = None if book is None else book.get(sell_sym, 0)
            confirmed_flat = qty_buy == 0 and qty_sell == 0

            if sell_closed and buy_closed and confirmed_flat:
                try:
                    realized = await self._realized_pnl(row_id, buy_sym, sell_sym)
                except Exception as exc:  # noqa: BLE001 - the close must still be booked
                    LOGGER.error("[LIVE] Realized P&L unreadable for row %s: %s", row_id, exc)
                    realized = None
                if realized is not None:
                    # Profit is what the fills made, not where the index went.
                    win = 1 if realized > 0 else 0
                else:
                    LOGGER.warning("[LIVE] Realized P&L unknown for %s/%s (a fill could not be "
                                   "read back); win is judged on the index instead.",
                                   buy_sym, sell_sym)
                await self._commit_closed_trade_atomic(
                    order_log_id=row_id, win=win, exit_price=exit_price or 0.0,
                    realized_pnl=realized,
                )
                LOGGER.warning("[LIVE] EXIT (flat) %s/%s @ %.2f - %s | win=%d | realized %s "
                               "(fills, before charges)",
                               buy_sym, sell_sym, exit_price, reason, win,
                               "unknown" if realized is None else f"Rs {realized:,.2f}")

                self._clear_position()
                self._buy_orderno = None
                self._sell_orderno = None
                self._active_orderlog_row_id = None
                self._sell_entry_side = None
                self._resume_requires_verification = False
                self._resumed_trade_state = None
                self._held_quantity = None
            else:
                LOGGER.error(
                    "Exit incomplete; buy_closed=%s sell_closed=%s broker_net=%s/%s. "
                    "Holding trade state open and quarantined for reconciliation.",
                    buy_closed, sell_closed, qty_buy, qty_sell,
                )
                self._set_quarantine(buy_sym, sell_sym, reason="exit_not_confirmed_flat")
                await self.db.execute_async(
                    "UPDATE botdata.order_logs SET lifecycle_state='QUARANTINED' WHERE id=%s;",
                    (row_id,),
                )

        finally:
            if acquired:
                await self._release_exit_lock()
            self.exiting = False

    # ──────────────────────────────────────────────────────────────────────
    #  Strategy loop
    # ──────────────────────────────────────────────────────────────────────
    async def _get_price_with_fallback(self, context: str) -> Optional[float]:
        """
        Return a fresh websocket price, or None while the feed is unavailable.
        """
        lp = self._read_fresh_price()
        if lp is not None:
            return lp

        # A cached tick is never evidence that a live stop has been crossed.
        if context == "stop check":
            emit, folded = _THROTTLE.should_log("stop_deferred")
            if emit:
                LOGGER.warning("[%s] Live price unavailable; stop decision deferred.%s",
                               context, log_throttle.suffix(folded))
            return None

        LOGGER.error("[%s] No fresh websocket price available.", context)
        return None

    async def _exit_for_strategy(self, reason: str, exit_price: float) -> None:
        """Exit adapter for `StrategyDecider`, which does not classify wins."""
        await self._exit_trade(reason, self._win(exit_price), exit_price)

    async def _enter_for_strategy(
        self, direction: str, price: float, pending: Dict[str, Any]
    ) -> bool:
        """Entry adapter: applies only the broker-health guards, no strategy rules."""
        if self.entering or self._entry_cooldown_active():
            return False
        if now_ist().year not in self._holiday_years:
            emit, folded = _THROTTLE.should_log("missing_holiday_year")
            if emit:
                LOGGER.error(
                    "[LIVE] No trading/expiry holidays configured for %d; new entries blocked "
                    "until config.py is updated. Open positions can still exit.%s",
                    now_ist().year, log_throttle.suffix(folded),
                )
            return False
        if self._past_entry_cutoff(pending):
            return False
        return await self._enter_trade(direction, price, pending)

    def _market_open(self) -> bool:
        """True while F&O orders can trade; outside it an order is only a rejection."""
        return self.MARKET_OPEN <= now_ist().time() < self.MARKET_CLOSE

    async def _cycle_once(self) -> None:
        # Risk management runs FIRST. Reconciliation makes unbounded broker
        # calls (`get_positions` has no timeout), and a hung one ahead of this
        # would delay a stop, a reversal or the EOD flatten for as long as the
        # broker takes to answer. The decider reads only local state, so nothing
        # here depends on reconciliation having run.
        if not await self._resolve_uncertain_open_commit():
            return
        if self._resume_requires_verification and self._resumed_trade_state == "EXIT_PENDING":
            # A restart before the bell would otherwise fire rejected orders
            # every poll until 09:15.
            if not self._market_open():
                return
            if await self._reconcile_unresolved_attempts():
                return
            price = await self._held_price_getter("carry exit")
            if price is not None:
                await self._exit_trade("restart_exit_recovery", self._win(price), price)
            return
        if self._resume_requires_verification and not await self._verify_resumed_trade():
            return
        if self._quarantine_active and not self.in_trade:
            await self._reconcile_positions()
            return
        if self.exiting:
            return
        await self.decider.step(
            in_trade=self.in_trade,
            direction=self.direction,
            price_getter=self._held_price_getter,
            enter=self._enter_for_strategy,
            exit_=self._exit_for_strategy,
            live_price_getter=self.fresh_price_getter,
            book=self._book,
            entry_day=self._entry_day,
            stop_price=self.stop_price,
            index=self._index,
            index_price_getter=self._index_price_with_fallback,
            index_live_price_getter=self._live_price_for,
        )
        # Housekeeping last: stray-position cleanup when flat, and the
        # detection-only warning when a trade is open.
        await self._reconcile_positions()


class TradeManagerTest(_TradeManager):
    """Paper-trading manager (TEST mode). Never places broker orders.

    Every decision - direction, entry timing, stop level, exit, the legs - comes
    from the frozen v3 strategy via `StrategyDecider` and the published legs.
    This class only records what the strategy decided into
    `botdata.trade_logs`. It
    has no risk rules of its own
    beyond the same manager-level session cutoff as LIVE
    (`no_new_trades_after` in `config.py`), so paper trading rehearses the same
    operational window; a daily loss limit or a reversal rule layered on top
    would still make the bot trade something other than the strategy that was
    validated.

    `close_all_positions_by` deliberately does not apply. v3 holds into the
    next session's 09:15 open, so a manager that flattened at 15:15 would not
    be trimming the strategy, it would be deleting it.

    Option legs are *not* a precondition here, unlike LIVE. See `_enter_trade`.
    """

    TAG = "TEST"
    _LOG_PREFIX = "[TEST] "
    _LOOP_ERROR_TRACEBACK = True

    #: Stands in for the option leg when the plumbing has not published one, so
    #: a paper trade is recorded rather than dropped. Deliberately not a real
    #: tradeable symbol: nothing in TEST reaches the broker, and a rehearsal
    #: booked against this name is obvious in `botdata.trade_logs`.
    PAPER_FALLBACK_SYMBOL = "PAPER-NO-LEGS"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._ensure_tables()
        self._load_open_trade_state()

    # ──────────────────────────────────────────────────────────────────────
    #  Persisted state
    # ──────────────────────────────────────────────────────────────────────
    def _load_open_trade_state(self) -> None:
        row = self.db.execute(
            """
            SELECT id, symbol, direction, fill_price, stop_price, book,
                   index_name, log_time
            FROM   botdata.trade_logs
            WHERE  is_in_trade = TRUE AND execution_mode='TEST'
            ORDER  BY id DESC
            LIMIT  1;
            """,
            fetch=True,
        )
        if not row:
            return
        self._restore_position(row[0])

        LOGGER.warning(
            "⚠️  [TEST] Resuming open trade id=%s %s %s on %s @ %.2f | stop %s | leg=%s opened=%s",
            self.trade_row_id, self.direction, self.symbol, self._index, self.entry_price,
            "-" if self.stop_price is None else f"{self.stop_price:.2f}",
            self._book, self._entry_day,
        )

    # ──────────────────────────────────────────────────────────────────────
    #  Price
    # ──────────────────────────────────────────────────────────────────────
    async def _get_price_with_fallback(self, context: str) -> Optional[float]:
        """Return a fresh websocket price, or None while the feed is unavailable."""
        try:
            value = self.fresh_price_getter() if callable(self.fresh_price_getter) else None
            lp = float(value) if value is not None else None
        except Exception as exc:
            LOGGER.error("[TEST][%s] Fresh price getter failed: %s", context, exc)
            lp = None
        if lp is not None and 0 < lp < float("inf"):
            return lp
        if context == "stop check":
            return None

        LOGGER.error("[TEST][%s] No fresh websocket price available.", context)
        return None

    # ──────────────────────────────────────────────────────────────────────
    #  Trade rows
    # ──────────────────────────────────────────────────────────────────────
    async def _insert_trade_row(
        self,
        price: float,
        symbol: str,
        direction: str,
        stop_price: Optional[float],
        book: str,
        index_name: str,
        sell_symbol: Optional[str] = None,
        buy_symbol: Optional[str] = None,
    ) -> int:
        # The legs are written with the row, as LIVE does: the symbol columns
        # are immutable once the row exists, and the console's held-legs view
        # reads them from here.
        res = await self.db.execute_async(
            """
            INSERT INTO botdata.trade_logs
                  (log_time, symbol, direction, fill_price,
                   stop_price, exit_price, is_in_trade, win, execution_mode,
                   trade_state, book, index_name, sell_symbol, buy_symbol)
            VALUES (%s,        %s,     %s,         %s,
                    %s,        NULL,    TRUE,        NULL, 'TEST',
                    'OPEN',     %s,   %s,         %s,          %s)
            RETURNING id;
            """,
            (_now_ist(), symbol, direction, price, stop_price, book, index_name,
             sell_symbol, buy_symbol),
            fetch=True,
        )
        return int(res[0]["id"])

    async def _close_trade_row(self, win: int, exit_price: float) -> bool:
        """Close the open row. True if this call is what closed it.

        `trade_state` is written here as well as `is_in_trade`. The insert
        stamps 'OPEN' (see `_insert_trade_row`), so a close that cleared only
        the boolean left the row advertising an open position for the rest of
        its life. LIVE has always written both - see
        `TradeManagerLive._commit_closed_trade_atomic` - and TEST diverging
        from it is what let an exited overnight hold still read as OPEN.

        The `is_in_trade = TRUE` guard makes this the same claim-the-position
        update LIVE makes, so `RETURNING id` answers whether the row really was
        open. Returning that instead of assuming it keeps the caller from
        reporting an exit the database never took.
        """
        if self.trade_row_id is None:
            return False
        res = await self.db.execute_async(
            """
            UPDATE botdata.trade_logs
            SET is_in_trade = FALSE,
                trade_state = 'CLOSED',
                win         = %s,
                exit_price  = %s,
                exit_time   = %s
            WHERE id = %s AND is_in_trade = TRUE
            RETURNING id;
            """,
            (win, exit_price, _now_ist(), self.trade_row_id),
            fetch=True,
        )
        return bool(res)

    # ──────────────────────────────────────────────────────────────────────
    #  Paper orders (log only)
    # ──────────────────────────────────────────────────────────────────────
    def _paper_order(self, side: str, symbol: str) -> None:
        LOGGER.info("📄 [PAPER] %s %s ×%d", side, symbol, self.lot_size)

    async def _enter_trade(
        self, direction: str, entry_price: float, pending: dict[str, Any]
    ) -> bool:
        index = str(pending.get("index") or instruments.CHART_INDEX)
        fetcher = self._fetcher_for(pending.get("index"))
        if fetcher is None:
            LOGGER.error(
                "[TEST] v3 gave the slot to %s but this process has no leg "
                "builder for it (has: %s). Entry skipped rather than traded on "
                "another index.",
                index, ", ".join(sorted(self.symbol_fetchers)) or "none",
            )
            return False
        fs = getattr(fetcher, "final_symbols", None)
        if isinstance(fs, dict):
            fs = dict(fs)  # one snapshot: legs and their bias travel together
        self.symbol = (fs or {}).get("sell") or getattr(fetcher, "final_symbol", None)
        if not self.symbol:
            # Paper mode rehearses the *strategy*, not the option plumbing, so a
            # missing leg must not delete the trade: a TEST run that silently
            # records nothing is indistinguishable from a strategy that decided
            # not to trade, which is precisely how a dead price feed went
            # unnoticed. Book it against a clearly-marked synthetic symbol and
            # say so, loudly enough to be seen at INFO.
            self.symbol = self.PAPER_FALLBACK_SYMBOL
            emit, folded = _THROTTLE.should_log("test_no_legs")
            if emit:
                LOGGER.warning(
                    "[TEST] Option legs not ready (%s); booking the paper trade "
                    "against %r so the strategy is still rehearsed. LIVE would "
                    "have SKIPPED this entry.%s",
                    getattr(fetcher, "legs_blocked_reason", None)
                    or "reason not reported by the symbol fetcher",
                    self.PAPER_FALLBACK_SYMBOL,
                    log_throttle.suffix(folded),
                )
        else:
            _THROTTLE.reset("test_no_legs")

        # Paper mode records the engine's direction regardless, because its job
        # is to rehearse the strategy rather than the option plumbing. A stale
        # leg snapshot is still worth surfacing here: in LIVE it blocks the entry.
        tag = bias_tag(direction)
        leg_bias = (fs or {}).get("bias")
        if leg_bias is not None and leg_bias != tag:
            LOGGER.warning(
                "[TEST] Leg bias mismatch (strategy wants %s=%s, legs built for %s); "
                "LIVE would skip this entry.",
                direction, tag, leg_bias,
            )

        stop_points = float(pending.get("stop_loss_points") or 0.0)
        self.direction = direction
        self.entry_price = entry_price
        self.stop_price = (
            entry_price - stop_points if direction == "LONG" else entry_price + stop_points
        ) if stop_points > 0 else None

        self._paper_order("B" if direction == "LONG" else "S", self.symbol)

        book = str(pending.get("book") or CARRY_BOOK)
        self.trade_row_id = await self._insert_trade_row(
            entry_price, self.symbol, self.direction, self.stop_price, book, index,
            sell_symbol=(fs or {}).get("sell") or None, buy_symbol=(fs or {}).get("buy") or None,
        )
        self._book = book
        self._index = index
        self._entry_day = now_ist().date()
        self.in_trade = True
        LOGGER.warning(
            "[TEST] ENTER %s %s on %s, with %s @ %.2f | stop %s (%s, %.2f pts) | book=%s | candidates=%s",
            self.direction, self.symbol, index, (fs or {}).get("buy") or "-", entry_price,
            "-" if self.stop_price is None else f"{self.stop_price:.2f}",
            pending.get("source"), stop_points, book,
            pending.get("priority_candidates") or "-",
        )
        return True

    async def _enter_for_strategy(
        self, direction: str, price: float, pending: dict[str, Any]
    ) -> bool:
        """Entry adapter: applies the same manager-level cutoff as LIVE."""
        if self._past_entry_cutoff(pending):
            return False
        return await self._enter_trade(direction, price, pending)

    async def _exit_trade(self, reason: str, exit_price: float) -> None:
        if not self.in_trade or not self.symbol:
            return
        win = self._win(exit_price)

        # The database first, then the two log lines that claim it happened.
        # A failure here propagates: `_main_loop` logs it and the next poll
        # retries, with in-memory state untouched, rather than announcing an
        # exit against a row that is still open.
        closed = await self._close_trade_row(win, exit_price)
        if not closed:
            # The row is not open - already closed, or gone from under us. There
            # is nothing left to close, so the manager goes flat to match it,
            # but this is never routine and must not pass silently.
            LOGGER.warning(
                "[TEST] trade row %s was not open when closing it (%s); the row "
                "already reads flat, so the manager is going flat to match it.",
                self.trade_row_id, reason,
            )
        self._paper_order("S" if self.direction == "LONG" else "B", self.symbol)
        LOGGER.warning("[TEST] EXIT  %s @ %.2f - %s | win=%d", self.symbol, exit_price, reason, win)
        self._clear_position()

    # ──────────────────────────────────────────────────────────────────────
    #  Main loop
    # ──────────────────────────────────────────────────────────────────────
    def _live_price_for(self, index: str) -> Optional[float]:
        """A current tick for one index, or None.

        Never falls back to another index's price: an entry measures its stop
        from the fill, so pricing a BANKNIFTY entry off NIFTY's tick would put
        the stop at a level that never traded on the contract being bought.
        """
        getter = self.index_live_price_getters.get(index)
        if getter is None:
            # Single-index deployment, or the chart index in a book: the
            # manager's own fresh getter already means this index.
            if index == instruments.CHART_INDEX or not self.index_live_price_getters:
                getter = self.fresh_price_getter
            else:
                return None
        try:
            value = getter() if callable(getter) else None
            value = float(value) if value is not None else None
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("[TEST] live price for %s failed: %s", index, exc)
            return None
        return value if value is not None and 0 < value < float("inf") else None

    async def _index_price_with_fallback(self, index: str, context: str) -> Optional[float]:
        """`_get_price_with_fallback`, for a named index."""
        price = self._live_price_for(index)
        if price and price > 0:
            return float(price)
        if context == "stop check":
            return None
        if index == instruments.CHART_INDEX or not self.index_live_price_getters:
            return await self._get_price_with_fallback(context)
        LOGGER.error("[TEST][%s] no price available for %s.", context, index)
        return None

    async def _cycle_once(self) -> None:
        await self.decider.step(
            in_trade=self.in_trade,
            direction=self.direction,
            price_getter=self._held_price_getter,
            enter=self._enter_for_strategy,
            exit_=self._exit_trade,
            live_price_getter=self.fresh_price_getter,
            book=self._book,
            entry_day=self._entry_day,
            stop_price=self.stop_price,
            index=self._index,
            index_price_getter=self._index_price_with_fallback,
            index_live_price_getter=self._live_price_for,
        )

    async def _held_price_getter(self, context: str) -> Optional[float]:
        """Price for whichever index the open position is on.

        The exit and the stop are both judged against the held contract's own
        index, so this resolves through `_index` rather than the chart index.
        """
        if self._index:
            return await self._index_price_with_fallback(self._index, context)
        return await self._get_price_with_fallback(context)
