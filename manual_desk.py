# manual_desk.py
"""The manual trading desk: one-click synthetic longs and shorts beside the v3 algo.

BUY opens the position v3 trades - a synthetic long future, the in-the-money
call bought and the put of that strike sold, built by `getdata.GetSymbol` -
SHORT opens its mirror - the in-the-money put bought and the call of that
strike sold (`DeskLegs`) - and SELL squares either off. Every order goes through the
algo's own managers (`trade_manager.TradeManagerLive` / `TradeManagerTest`),
unmodified: the hedge sequence, IOC limits, fill and position verification,
rollback, quarantine and restart recovery are the ones v3 runs on. Only the
decision changes hands - the operator's click replaces `StrategyDecider`.

Nothing the algo owns is shared:

  * **Ledger.** Rows land in the `manualdata` schema, a structural copy of
    `botdata.trade_logs` / `order_logs` / `order_attempts`. `ManualLedger`
    points the managers' SQL at it, so the algo's tables, backup and
    truncate never see a manual row.
  * **Broker product (LIVE).** The broker nets a position per symbol *per
    product*. The algo trades NRML and treats every NRML quantity on a symbol
    it watches as its own: it flattens what it does not hold, and an exit
    sizes off the broker's net. A manual NRML leg on the algo's strike would
    be closed by the algo, or closed *with* it. So the desk trades MIS and
    reads only MIS rows (`ManualBroker`), and the algo never reads them. MIS
    is intraday: the desk squares off at `square_off_at`, ahead of the
    broker's own MIS cut.
  * **Everything else.** Its own leg builders, database pool, advisory locks,
    order-update writer and trade thread. A stuck manual order cannot hold a
    connection or a lock the algo needs.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import re
import threading
import time
import uuid
from array import array
from collections import deque
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
import dataclasses
from dataclasses import dataclass
from datetime import datetime, timezone
from datetime import time as dt_time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from flask import Response, jsonify, request, stream_with_context
from psycopg2 import sql

import db_connector
import hhll_store
import hhll_tick
import instruments
import ist
from config import DATA_SETTINGS, TRADE_SETTINGS
from data_handler import shoonya_candles
from getdata import GetSymbol, LegSelectionError
from strategy_logic import SESSION_OPEN_TIME, bias_tag, is_momentum_book, now_ist
from trade_manager import LOGGER as TRADE_LOGGER, TradeManagerLive, TradeManagerTest, _OrderUpdateSignal

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ManualSettings:
    #: Broker product for desk orders: MIS. Never NRML - see the module notes.
    product: str = "I"
    #: BUY and SHORT are accepted from the open until `square_off_at`; an open position
    #: is squared off from `square_off_at` until the close, ahead of the
    #: broker's own MIS square-off (~15:20 at Shoonya).
    entry_from: dt_time = dt_time(9, 15)
    square_off_at: dt_time = dt_time(15, 10)
    market_close: dt_time = dt_time(15, 30)
    #: A sanity cap on one click. The broker's freeze quantity is checked
    #: separately by the order path before anything is sent.
    max_lots: int = 50
    #: The desk's starting cash and the margin one lot takes, in rupees. The
    #: ticket shows the cash plus every realised P&L of the mode's ledger; the
    #: broker's own margin check is still the one an order meets.
    account_cash: float = 200_000.0
    margin_per_lot: float = 170_000.0
    #: A click not picked up within this is dropped, never sent late.
    command_ttl_sec: float = 5.0
    #: The desk's poll. Cheap - it reads local state - and it is the latency
    #: between a click and the first order.
    poll_interval: float = 0.2
    #: Minimum gap between automatic square-off attempts that did not go flat.
    square_off_retry_sec: float = 5.0


MANUAL_SETTINGS = ManualSettings()

SCHEMA = "manualdata"
#: The algo tables the desk keeps its own copy of.
LEDGER_TABLES = ("trade_logs", "order_logs", "order_attempts")
#: `trade_logs.book` of a manual position.
BOOK = "manual"
#: Prefix of every desk thread's name, which is how desk log lines are told
#: apart from the algo's (`_DeskLogTag`).
THREAD_NAME = "manual-desk"
CANDLE_MINUTES = (1, 5, 15)
#: The click that opens each side.
OPEN_ACTIONS = {"BUY": "LONG", "SHORT": "SHORT"}


class DeskBusy(RuntimeError):
    """An order is already queued or being carried out."""


# --------------------------------------------------------------------------- #
#  Ledger                                                                      #
# --------------------------------------------------------------------------- #
_BOTDATA = re.compile(r"\bbotdata\.")


def ensure_manual_schema(db, schema: str = SCHEMA) -> None:
    """Create the desk's copy of the algo's order ledger. Additive and idempotent.

    Built from `db_connector`'s own table, index, function and trigger specs,
    so a column the algo's ledger gains is gained here at the next start.
    """
    tables = {table.name: table for table in db_connector.TABLES}
    target = sql.Identifier(schema)
    conn = db.get_raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                        ("lune-schema", schema))
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(target))
            for name in LEDGER_TABLES:
                columns = tables[name].columns
                cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                    target, sql.Identifier(name), sql.SQL(", ").join(
                        sql.SQL("{} {}").format(sql.Identifier(c.name), sql.SQL(c.create_definition))
                        for c in columns)))
                for column in columns:
                    cur.execute(sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}").format(
                        target, sql.Identifier(name), sql.Identifier(column.name),
                        sql.SQL(column.add_definition)))
            for index in db_connector.INDEXES:
                if index.table in LEDGER_TABLES:
                    cur.execute(sql.SQL("CREATE {}INDEX IF NOT EXISTS {} ON {}.{} {}").format(
                        sql.SQL("UNIQUE ") if index.unique else sql.SQL(""),
                        sql.Identifier(index.name), target, sql.Identifier(index.table),
                        sql.SQL(index.key)))
            for definition in db_connector.FUNCTIONS.values():
                cur.execute(_BOTDATA.sub(f"{schema}.", definition).replace(
                    "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1))
            # The premium each leg of a paper trade would have filled at (the broker's best
            # ask for a buy, best bid for a sell), so every trade's charges are its own.
            cur.execute(sql.SQL(
                "CREATE TABLE IF NOT EXISTS {}.leg_fills (trade_id integer NOT NULL, phase text NOT NULL, "
                "symbol text NOT NULL, side text NOT NULL, qty integer NOT NULL, premium double precision NOT NULL, "
                "quoted_at timestamptz NOT NULL, PRIMARY KEY (trade_id, phase, symbol))").format(target))
            for trigger, table, function in db_connector.TRIGGERS:
                if table in LEDGER_TABLES:
                    cur.execute(sql.SQL(
                        "CREATE OR REPLACE TRIGGER {} BEFORE UPDATE ON {}.{} "
                        "FOR EACH ROW EXECUTE FUNCTION {}.{}()").format(
                        sql.Identifier(trigger), target, sql.Identifier(table),
                        target, sql.Identifier(function)))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        db.put_raw_connection(conn)


class ManualLedger:
    """A `TimescaleDBConnector` whose `botdata.` tables are the desk's own.

    The managers and the order-update writer address the ledger as
    `botdata.<table>` in plain SQL strings; every statement through here, raw
    connections included, is retargeted at `schema` before it runs. Anything
    that is not a plain string is refused rather than run unretargeted.
    """

    def __init__(self, db, schema: str = SCHEMA) -> None:
        self.db = db
        self.schema = schema
        self._schema_lock = threading.Lock()
        self._schema_ready = False

    def retarget(self, statement: str) -> str:
        if not isinstance(statement, str):
            raise TypeError("the manual ledger only runs plain SQL strings")
        return _BOTDATA.sub(f"{self.schema}.", statement)

    def ensure_schema(self) -> None:
        with self._schema_lock:
            if not self._schema_ready:
                ensure_manual_schema(self.db, self.schema)
                self._schema_ready = True

    def execute(self, statement: str, params=None, *, fetch: bool = False):
        return self.db.execute(self.retarget(statement), params, fetch=fetch)

    async def execute_async(self, statement: str, params=None, *, fetch: bool = False):
        return await asyncio.to_thread(self.execute, statement, params, fetch=fetch)

    def get_raw_connection(self):
        return _LedgerConnection(self.db.get_raw_connection(), self.retarget)

    def put_raw_connection(self, conn, *, close: bool = False) -> None:
        self.db.put_raw_connection(getattr(conn, "raw", conn), close=close)

    async def get_raw_connection_async(self):
        return await asyncio.to_thread(self.get_raw_connection)

    async def put_raw_connection_async(self, conn, *, close: bool = False) -> None:
        await asyncio.to_thread(self.put_raw_connection, conn, close=close)


class _LedgerConnection:
    def __init__(self, raw, retarget: Callable[[str], str]) -> None:
        self.raw = raw
        self._retarget = retarget

    def cursor(self, *args, **kwargs):
        return _LedgerCursor(self.raw.cursor(*args, **kwargs), self._retarget)

    def __getattr__(self, name: str):
        return getattr(self.raw, name)


class _LedgerCursor:
    def __init__(self, raw, retarget: Callable[[str], str]) -> None:
        self.raw = raw
        self._retarget = retarget

    def execute(self, statement, params=None):
        return self.raw.execute(self._retarget(statement), params)

    def __enter__(self):
        # Whatever the driver's cursor hands out on entry is wrapped too.
        return _LedgerCursor(self.raw.__enter__(), self._retarget)

    def __exit__(self, *exc):
        return self.raw.__exit__(*exc)

    def __getattr__(self, name: str):
        return getattr(self.raw, name)


# --------------------------------------------------------------------------- #
#  Broker                                                                      #
# --------------------------------------------------------------------------- #
class ManualBroker:
    """The broker account as the desk sees it: its own product and nothing else.

    Every order goes out as `product`. The position book is cut to `product`
    rows before the managers' parser reads it; that parser keeps NRML rows
    only (the algo's product), so the desk's rows are handed to it labelled
    as such. A row the parser could not read is passed through untouched, so
    an unreadable book still reads as unknown, never as flat.
    """

    def __init__(self, api, product: str = MANUAL_SETTINGS.product) -> None:
        self.api = api
        self.product = str(product).upper()

    def place_order(self, **order):
        return self.api.place_order(**{**order, "product_type": self.product})

    def get_positions(self):
        payload = self.api.get_positions()
        if isinstance(payload, list):
            return self._own(payload)
        if isinstance(payload, dict):
            return {key: self._own(rows) if isinstance(rows, list) else rows
                    for key, rows in payload.items()}
        return payload

    def _own(self, rows: list) -> list:
        kept = []
        for row in rows:
            if not isinstance(row, dict) or str(row.get("stat") or "ok").lower() != "ok":
                kept.append(row)
            elif str(row.get("prd") or "").upper() == self.product:
                kept.append({**row, "prd": "M"})
        return kept

    def __getattr__(self, name: str):
        return getattr(self.api, name)


# --------------------------------------------------------------------------- #
#  Log tagging                                                                 #
# --------------------------------------------------------------------------- #
class _DeskLogTag(logging.Filter):
    """Marks every record logged on a desk thread "[MANUAL]".

    The managers' messages say "[LIVE]"/"[TEST]" whoever runs them, so
    without this a manual entry reads exactly like the algo's in the bot log.
    Warnings are also kept for the desk, which reports the last one as the
    reason a click did not fill.
    """

    def __init__(self) -> None:
        super().__init__()
        self.sink: Callable[[str], None] = lambda _message: None

    @classmethod
    def install(cls, sink: Callable[[str], None]) -> None:
        """Tag desk records from here on, and hand their warnings to `sink`."""
        global _LOG_TAG
        if _LOG_TAG is None:
            _LOG_TAG = cls()
            for logger in (TRADE_LOGGER, logging.getLogger()):
                logger.addFilter(_LOG_TAG)
        _LOG_TAG.sink = sink

    def filter(self, record: logging.LogRecord) -> bool:
        if threading.current_thread().name.startswith(THREAD_NAME) and not getattr(record, "manual_desk", False):
            record.manual_desk = True
            record.msg = f"[MANUAL] {record.msg}"
            if record.levelno >= logging.WARNING:
                try:
                    self.sink(record.getMessage())
                except Exception:  # noqa: BLE001 - logging must never raise
                    pass
        return True


_LOG_TAG: Optional[_DeskLogTag] = None


# --------------------------------------------------------------------------- #
#  HH/LL pivots and the automatic desk                                         #
# --------------------------------------------------------------------------- #
#: Pivots served per index to the chart (the database keeps every one).
PIVOT_KEEP = 500


#: A desk entry follows the engine only while the engine's entry is this fresh:
#: a late signal is skipped, never chased.
ENTRY_TTL_MS = 3000
#: The desk prices its paper fills from its own tape, never from a REST quote;
#: a tick older than this is no price at all.
TICK_FRESH_SEC = 5.0
#: The indices the automatic trader enters until one is chosen in Settings (`hhll.state` row "desk"). Every
#: index's pivots are still drawn.
AUTO_ORDER = ("FINNIFTY",)
#: How often the settings hhll_tick has adopted are looked for (`hhll.state`).
SETTINGS_POLL_SEC = 1.0
#: The pivot threshold, x cost, Settings accepts.
FIXED_MIN, FIXED_MAX = 0.5, 100.0


class TickTrader:
    """The desk's automatic trader: `hhll_tick`'s engine on the desk's own tape.

    One `hhll_tick.Book` per index, on the settings hhll_tick has adopted (`hhll.state`,
    polled once a second off the websocket thread), so every desk trade is a trade of the
    engine that is replayed, reviewed and improved after every close. R is a multiple of
    each index's own all-in cost; pivots come from ticks only. With a store attached
    (`attach`) each book's pivots go to `hhll.pivots`, the chart reads them from there, and
    a book holds only its working state; without one (tests) everything stays in memory
    on the prior settings.

    Holding: the position closes once the held index's book is off its side
    (reverse, stop, lock, square-off). A position the engine did not open (a
    click) is closed only by a fresh opposite entry. Flat: the first index in
    `self.order` whose book has just opened is opened, once per entry.
    New settings reach an index's book only while the desk is not holding it.

    Always on, on `self.order`. A failed order halts it only while a position is held (a failed
    exit: closed by a click or the square-off); once flat it trades the next entry again.

    A book rebuilt for new settings replays the whole session. With `since` and the settings
    thread running (`start`), that replay runs on the settings thread, never on the websocket
    thread that carries every tick and order update: the old book trades on meanwhile (but
    is not entered), and the new one is caught up to the same tick before it replaces it.
    """

    def __init__(self, names, history: Callable[[str], tuple[list, list, int]], lots: int = 1,
                 since: Optional[Callable[[str, int], tuple[list, list]]] = None) -> None:
        self.names = tuple(n for n in names if n in hhll_tick.SPECS)
        self.history = history               # name -> (epoch ms, prices, last seq) of today's ticks so far
        self.since = since                   # (name, seq) -> (epoch ms, prices) of today's ticks from that seq
        self._lock = threading.RLock()
        self.lots = int(lots)
        self.order = AUTO_ORDER
        self.halted: Optional[str] = None
        self.inflight: Optional[str] = None
        self._entered: Dict[str, int] = {}   # name -> entry ms of the engine position already taken
        self._managed: set = set()           # held index whose position the engine manages
        self._held: Optional[str] = None
        self._swap: set = set()
        self._fed: Dict[str, int] = {}       # name -> last tape seq its book has seen
        self._store = None
        self._seen = None                    # (settings version, day) last read
        self._polling: Optional[threading.Thread] = None
        self._stopped = threading.Event()
        self._kick = threading.Event()       # wakes the settings thread: a book is waiting for its rebuild
        self._points: Dict[str, tuple] = {}  # name -> (key, rows): the chart's pivots, while the DB has the book's latest
        self.woke = _OrderUpdateSignal()     # a book's position changed: the desk loop need not wait out its poll
        self.params = hhll_tick._prior()
        self._load(hhll_tick.today_ist())

    # -- the engine ----------------------------------------------------------
    def _replay(self, name: str, params) -> tuple["hhll_tick.Book", int]:
        """A book of `params` that has seen today's ticks so far, and the seq of the last one."""
        live = self._store is not None
        b = hhll_tick.Book(hhll_tick.SPECS[name], params, on_pivot=self._pivot if live else None, keep=not live)
        ms, px, last = self.history(name)
        for m, x in zip(ms, px):
            b.on_tick(x, m)
        b.trades.clear()
        return b, last

    def _book(self, name: str, params) -> "hhll_tick.Book":
        b, self._fed[name] = self._replay(name, params)
        return b

    @staticmethod
    def _same_for(a, b, name: str) -> bool:
        """Whether settings `a` and `b` trade `name` identically: every field but the other
        indices' own thresholds (`index_pivots`), which a book never reads."""
        own = lambda p: dataclasses.replace(p, index_pivots=tuple(x for x in p.index_pivots if x[0] == name))
        return own(a) == own(b)

    def _off_thread(self) -> bool:
        """Whether rebuilds run on the settings thread (else inline, on the tick: tests)."""
        return self.since is not None and self._polling is not None and self._polling.is_alive()

    def _rebuild_pending(self) -> None:
        """Settings thread: rebuild each book waiting for new settings that the desk is not
        holding. The replay runs outside the lock while the old book keeps taking ticks; the
        new book is then caught up to the old one's last tick and swapped in under the lock."""
        with self._lock:
            params, day = self.params, self.day
            todo = []
            for name in self.names:
                if name not in self._swap or name == self._held:
                    continue
                b = self.books.get(name)
                if b is None or self._same_for(b.p, params, name):
                    if b is not None:
                        b.p = params              # nothing this book reads changed: no replay
                    self._swap.discard(name)
                else:
                    todo.append(name)
        for name in todo:
            nb, last = self._replay(name, params)
            with self._lock:
                if (params != self.params or day != self.day or hhll_tick.today_ist() != day
                        or name not in self._swap or name == self._held):
                    continue                      # newer settings, a new day, or held: a later pass
                fed = self._fed.get(name, -1)
                if fed > last:
                    ms, px = self.since(name, last + 1)
                    need = fed - last
                    if len(ms) < need:
                        LOGGER.warning("[MANUAL] %s rebuild: %d of %d ticks to catch up; retrying",
                                       name, len(ms), need)
                        continue
                    for m, x in zip(ms[:need], px[:need]):
                        nb.on_tick(x, m)
                    nb.trades.clear()
                was = self.books[name].pos
                self.books[name] = nb
                self._fed[name] = max(fed, last)
                self._swap.discard(name)
                if nb.pos is not was:
                    self.woke.notify()

    def _pivot(self, b, pv) -> None:
        self._store.writer.put_pivot(hhll_store.pivot_row(b.spec.name, hhll_tick.day_of(pv.conf_ms), b.chain, pv))

    def _load(self, day) -> None:
        self.day = day
        self.books = {n: self._book(n, self.params) for n in self.names}
        self._swap.clear()

    def attach(self, store) -> None:
        """Trade on the engine's stored settings and keep pivots in `store` (hhll_tick.STORE)."""
        day = hhll_tick.today_ist()
        version = store.state_version()
        params, _ = hhll_tick.current_params(day)
        trade = store.load_state("desk").get("trade")
        with self._lock:
            self._store, self._seen, self.params = store, (version, day), params
            if trade in self.names:
                self.order = (trade,)
            self._load(day)

    def start(self) -> None:
        """Poll the engine's settings once a second (needs `attach`)."""
        if self._store is None or (self._polling is not None and self._polling.is_alive()):
            return
        self._stopped.clear()
        self._polling = threading.Thread(target=self._poll_loop, name=f"{THREAD_NAME}-settings", daemon=True)
        self._polling.start()

    def stop(self) -> None:
        self._stopped.set()
        self._kick.set()

    def _poll_loop(self) -> None:
        failing = False
        while not self._stopped.is_set():
            self._kick.wait(SETTINGS_POLL_SEC)
            self._kick.clear()
            if self._stopped.is_set():
                return
            try:
                self.poll_settings()
                if self.since is not None:
                    self._rebuild_pending()
                failing = False
            except Exception as exc:  # noqa: BLE001 - the database is away; next second
                if not failing:
                    LOGGER.warning("[MANUAL] engine settings unreadable: %s", exc)
                failing = True
                self._stopped.wait(SETTINGS_POLL_SEC)   # a kick per tick must not become a retry per tick

    def poll_settings(self) -> None:
        """Pick up settings hhll_tick has adopted, and the next session's at a new day."""
        if self._store is None:
            return
        day = hhll_tick.today_ist()
        key = (self._store.state_version(), day)
        if key == self._seen:
            return
        p, _ = hhll_tick.current_params(day)
        with self._lock:
            self._seen = key
            if p != self.params:
                LOGGER.warning("[MANUAL] auto trader adopts %s", json.dumps(hhll_tick.diff(self.params, p)))
                self.params = p
                self._swap = set(self.names)

    def on_tick(self, name: str, price: float, stamp: float, seq: Optional[int] = None) -> None:
        """One tape tick; `seq` is its place in the session (a rebuilt book has seen up to some seq)."""
        with self._lock:
            today = hhll_tick.today_ist()
            if today != self.day:
                self._load(today)
            b = self.books.get(name)
            if b is None:
                return
            if name in self._swap and name != self._held:
                if self._off_thread():
                    self._kick.set()          # the settings thread replays; this book trades on meanwhile
                else:
                    self._swap.discard(name)
                    if b.p != self.params:
                        if self._same_for(b.p, self.params, name):
                            b.p = self.params
                        else:
                            self.books[name] = self._book(name, self.params)   # history already has this tick
                            return
            ms = round(stamp * 1000)
            if seq is not None:
                if seq <= self._fed.get(name, -1):
                    return
                self._fed[name] = seq
            elif b.last_ms is not None and ms <= b.last_ms:
                return
            was = b.pos
            b.on_tick(float(price), ms)
            if b.pos is not was:
                self.woke.notify()

    def points(self, name: str) -> Dict[str, list]:
        """The chart's pivots of `name`: its book's chain today, from the database."""
        with self._lock:
            b = self.books.get(name)
            store, day, r = self._store, self.day, b.chain if getattr(b, "zz", None) else None
            mem = [] if b is None or store is not None else [
                (p.kind, p.price, p.ms, p.conf_price, p.conf_ms, p.leg) for p in list(b.pivots)[-PIVOT_KEEP:]]
            latest = b.pivots[-1] if b is not None and b.pivots else None
        if store is not None and r is not None:
            # Read again only when the book has printed a pivot since: the cached rows are kept
            # only once they end on the book's own latest pivot (the writer has stored it).
            key = (day, r, None if latest is None else (latest.kind, latest.price, latest.conf_ms))
            hit = self._points.get(name)
            if hit is not None and hit[0] == key:
                rows = hit[1]
            else:
                rows = store.pivots(day, name, r, PIVOT_KEEP)
                last = rows[-1] if rows else None
                if latest is not None and last is not None and (last[0], last[1], last[4]) == key[2]:
                    self._points[name] = (key, rows)
        else:
            rows = mem
        out: Dict[str, list] = {"hh_points": [], "ll_points": []}
        for kind, price, ms, conf_price, conf_ms, _ in rows:
            out["hh_points" if kind == "HH" else "ll_points"].append({
                "time": ist.isoformat(_utc(ms)), "extreme_at": int(ms), "price": price,
                "confirmed": ist.isoformat(_utc(conf_ms)),
                "confirmed_at": int(conf_ms), "confirmed_price": conf_price})
        return out

    # -- the trader ----------------------------------------------------------
    @staticmethod
    def _side(b) -> Optional[str]:
        return None if b is None or b.pos is None else ("LONG" if b.pos.side > 0 else "SHORT")

    def desired(self, held: Optional[str], held_index: Optional[str], in_window: bool) -> Optional[tuple]:
        """The next auto action as (action, index, lots), or None to wait."""
        with self._lock:
            self._held = held_index if held is not None else None
            if held is None:
                self.halted = None
            if self.halted is not None or self.inflight is not None:
                return None
            now = time.time() * 1000
            if held is not None:
                b = self.books.get(held_index)
                if b is None:
                    return None
                side = self._side(b)
                if side == held:
                    if held_index not in self._managed:
                        self._managed.add(held_index)
                        self._entered[held_index] = b.pos.ms
                    if self._entered.get(held_index) == b.pos.ms:
                        return None
                    return ("SELL", held_index, self.lots)   # two pivots since our entry: the first was the exit
                fresh_flip = side is not None and now - b.pos.ms <= ENTRY_TTL_MS
                if held_index in self._managed or fresh_flip:
                    return ("SELL", held_index, self.lots)
                return None
            self._managed.clear()
            if not in_window:
                return None
            for n in self.order:
                b = self.books.get(n)
                if b is None or b.pos is None or now - b.pos.ms > ENTRY_TTL_MS or self._entered.get(n) == b.pos.ms:
                    continue
                if n in self._swap:
                    continue                  # its book is on superseded settings until the rebuild lands
                self._entered[n] = b.pos.ms
                self._managed = {n}
                return ({"LONG": "BUY", "SHORT": "SHORT"}[self._side(b)], n, self.lots)
            return None

    def issued(self, order_id: str) -> None:
        with self._lock:
            self.inflight = order_id

    def settle(self, order_id: str, ok: bool, message: str) -> None:
        with self._lock:
            if self.inflight != order_id:
                return
            self.inflight = None
            if not ok:
                self.halted = message

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "lots": self.lots, "halted": self.halted, "trade": self.order[0],
                "fixed": {n: self.params.fixed_of(n) for n in self.names},
                "engine": "hhll_tick", "params": dataclasses.asdict(self.params),
                "threshold_pts": {n: round(b.zz.R, 2) for n, b in self.books.items() if getattr(b, "zz", None)},
                "cost_pts": {n: round(getattr(b, "cost", 0.0), 2) for n, b in self.books.items()},
                "engine_side": {n: self._side(b) for n, b in self.books.items()},
            }


def _utc(ms: int):
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


# --------------------------------------------------------------------------- #
#  Decisions                                                                   #
# --------------------------------------------------------------------------- #
class ManualDecider:
    """The desk's stand-in for `StrategyDecider`: the operator decides.

    One order at a time. A click is queued by the web thread and carried out
    on the manager's own loop, through the manager's own entry and exit, so a
    manual order meets every guard an algo order does. It also stands in as
    the manager's strategy: `_place_hedge_seq` re-reads the entry intent just
    before the first order goes out, and here that is the click being worked.
    """

    def __init__(self, manager) -> None:
        self.manager = manager
        self.settings: ManualSettings = manager.desk_settings
        self._lock = threading.Lock()
        self._queued: Optional[Dict[str, Any]] = None
        self._working: Optional[Dict[str, Any]] = None
        self._last: Optional[Dict[str, Any]] = None
        self._intent: Optional[Dict[str, Any]] = None
        self._notes: deque[str] = deque(maxlen=20)
        self._next_square_off = 0.0
        self._paused: Optional[str] = None      # why new entries were last held back (logged once)

    # -- web thread --------------------------------------------------------
    def submit(self, action: str, index: Optional[str] = None, lots: Optional[int] = None) -> Dict[str, Any]:
        with self._lock:
            self._expire_locked()
            if self._queued is not None or self._working is not None:
                raise DeskBusy("an order is already being worked")
            order = {"id": uuid.uuid4().hex[:12], "action": action, "index": index, "lots": lots,
                     "queued_ist": ist.isoformat(), "_at": time.monotonic()}
            self._queued = order
        return _public(order)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            self._expire_locked()
            return {"queued": _public(self._queued), "working": _public(self._working),
                    "last": _public(self._last)}

    def _expire_locked(self) -> None:
        """Drop a click the loop has not picked up in time - it never will be sent.

        The loop stops reading clicks while it reconciles (a quarantine, a
        resumed position it has not verified, an exit in flight), so this is
        also how such a click is answered at all.
        """
        order = self._queued
        if order is not None and time.monotonic() - order["_at"] > self.settings.command_ttl_sec:
            self._queued = None
            self._last = {**order, "ok": False, "done_ist": ist.isoformat(), "message": self._expired_message()}

    def _expired_message(self) -> str:
        return (f"Expired: not picked up within {self.settings.command_ttl_sec:.0f}s "
                "(the desk was busy or reconciling); nothing was sent")

    def note(self, message: str) -> None:
        self._notes.append(message)

    # -- the manager's strategy surface --------------------------------------
    def entry_intent(self, now=None) -> Optional[Dict[str, Any]]:
        return self._intent

    def blocked(self) -> Optional[str]:
        """Why no new desk entry may open now (the v3 night trade is held), or None."""
        check = getattr(self.manager, "entry_paused", None)
        reason = check() if callable(check) else None
        if reason != self._paused:
            if reason is not None:
                LOGGER.warning("[MANUAL] New entries paused: %s.", reason)
            elif self._paused is not None:
                LOGGER.warning("[MANUAL] New entries resumed: v3 is flat.")
            self._paused = reason
        return reason

    # -- the manager's loop --------------------------------------------------
    async def step(self, *, in_trade: bool, direction=None, price_getter, enter, exit_,
                   index_live_price_getter=None, **_ignored) -> None:
        now = now_ist()
        if in_trade and self._square_off_due(now):
            await self._auto_square_off(price_getter, exit_)
            in_trade = self.manager.in_trade

        # Twice at most: an automatic square-off, then the entry its reversal wants, in one pass.
        for _ in range(2):
            self._drive_auto(now, direction if in_trade else None)
            order = await self._work(in_trade, price_getter, enter, exit_, index_live_price_getter)
            if not (order and order["ok"] and order.get("auto") and order["action"] not in OPEN_ACTIONS):
                return
            in_trade = self.manager.in_trade

    async def _work(self, in_trade, price_getter, enter, exit_, index_live_price_getter) -> Optional[Dict[str, Any]]:
        """Carry out the queued order, if any; it comes back with its outcome under "ok"."""
        with self._lock:
            order, self._queued = self._queued, None
            self._working = order
        if order is None:
            return None
        ok, message = False, "Interrupted before it finished"
        try:
            if time.monotonic() - order["_at"] > self.settings.command_ttl_sec:
                message = self._expired_message()
            elif order["action"] in OPEN_ACTIONS:
                ok, message = await self._open(order, in_trade, enter, index_live_price_getter)
            else:
                ok, message = await self._sell(in_trade, price_getter, exit_)
        except Exception as exc:  # noqa: BLE001 - reported to the operator and the log
            LOGGER.exception("[MANUAL] %s failed", order["action"])
            message = f"Failed: {exc}"
        finally:
            with self._lock:
                self._working = None
                self._last = {**order, "ok": ok, "message": message, "done_ist": ist.isoformat()}
            if order.get("auto") and self.manager.auto is not None:
                self.manager.auto.settle(order["id"], ok, message)
            LOGGER.warning("[MANUAL] %s %s -> %s: %s", order["action"], order.get("index") or "",
                           "done" if ok else "not done", message)
        return {**order, "ok": ok}

    def _drive_auto(self, now, held) -> None:
        """Queue the automatic trader's next step, as a click would be queued.

        Only when the desk is idle: nothing queued, no entry or exit in flight,
        not reconciling, and not cooling down from a failed entry.
        """
        auto = self.manager.auto
        if auto is None:
            return
        m = self.manager
        if getattr(m, "entering", False) or getattr(m, "exiting", False) or getattr(m, "_quarantine_active", False):
            return
        if held is None and getattr(m, "ENTRY_FAIL_COOLDOWN_SEC", 0.0) - (time.time() - getattr(m, "_last_entry_fail_ts", 0.0)) > 0:
            return
        in_window = self._in_window(now) and (held is not None or self.blocked() is None)
        step = auto.desired(held, m._index if held is not None else None, in_window)
        if step is None:
            return
        action, index, lots = step
        order = {"id": uuid.uuid4().hex[:12], "action": action, "index": index, "lots": lots,
                 "queued_ist": ist.isoformat(), "_at": time.monotonic(), "auto": True}
        with self._lock:
            # A click from the console wins the slot; the trader waits its turn.
            if self._queued is not None or self._working is not None:
                return
            auto.issued(order["id"])
            self._queued = order

    def _in_window(self, now) -> bool:
        return self.settings.entry_from <= now.time() < self.settings.square_off_at

    def _market_open(self, now) -> bool:
        return self.settings.entry_from <= now.time() < self.settings.market_close

    def _square_off_due(self, now) -> bool:
        if not self._market_open(now):
            return False
        held_since = self.manager._entry_day
        return now.time() >= self.settings.square_off_at or (held_since is not None and held_since < now.date())

    async def _auto_square_off(self, price_getter, exit_) -> None:
        if time.monotonic() < self._next_square_off:
            return
        self._next_square_off = time.monotonic() + self.settings.square_off_retry_sec
        LOGGER.warning("[MANUAL] Squaring off %s: MIS positions close by %s.",
                       self.manager._index, self.settings.square_off_at.strftime("%H:%M"))
        await exit_("manual_eod_square_off", await self._exit_price(price_getter))

    def _take_notes(self) -> Optional[str]:
        last = self._notes[-1] if self._notes else None
        self._notes.clear()
        return last

    async def _open(self, order, in_trade, enter, price_of) -> tuple[bool, str]:
        m = self.manager
        index, lots = order["index"], order["lots"]
        direction = OPEN_ACTIONS[order["action"]]
        if in_trade:
            return False, f"Already holding {m._index}; square it off first"
        fetcher = m.symbol_fetchers.get(index)
        if fetcher is None:
            return False, f"No leg builder for {index}"
        if not self._in_window(now_ist()):
            return False, (f"Manual entries are taken {self.settings.entry_from:%H:%M}-"
                           f"{self.settings.square_off_at:%H:%M} IST")
        paused = self.blocked()
        if paused is not None:
            return False, f"Entries paused: {paused}"
        if getattr(m, "entering", False):
            return False, "An entry is already in progress"
        cooldown = getattr(m, "ENTRY_FAIL_COOLDOWN_SEC", 0.0) - (time.time() - getattr(m, "_last_entry_fail_ts", 0.0))
        if cooldown > 0:
            return False, f"Cooling down after a failed entry; retry in {cooldown:.0f}s"
        price = price_of(index) if callable(price_of) else None
        if price is None and m.last_price_getter is not None:
            price = m.last_price_getter(index)
        if price is None:
            return False, f"No fresh {index} price; nothing was sent"

        # Built now, from this tick, for this click's side: the legs are the
        # ones this click trades. The manager refuses legs of the other side.
        fetcher.side = direction
        await asyncio.to_thread(fetcher.update_symbol)
        legs = getattr(fetcher, "final_symbols", None)
        if not isinstance(legs, dict) or not legs.get("buy") or not legs.get("sell"):
            reason = getattr(fetcher, "legs_blocked_reason", None) or "the leg builder published none"
            return False, f"No {index} legs: {reason}"

        m.lots = int(lots)
        self._notes.clear()
        self._intent = {"key": order["id"], "index": index, "book": BOOK, "source": "manual",
                        "stop_loss_points": 0.0, "lots": lots}
        try:
            entered = await enter(direction, float(price), self._intent)
        finally:
            self._intent = None
        if entered:
            verb = "Bought" if direction == "LONG" else "Shorted"
            return True, f"{verb} {lots} lot{'s' if lots != 1 else ''} {index} @ {float(price):,.2f}"
        return False, self._take_notes() or "Entry not filled; see the bot log"

    async def _sell(self, in_trade, price_getter, exit_) -> tuple[bool, str]:
        m = self.manager
        if not in_trade:
            return False, "No open manual position"
        index = m._index
        if not self._market_open(now_ist()):
            return False, "Market closed; the position is squared off from the next open"
        price = await self._exit_price(price_getter)
        self._notes.clear()
        await exit_("manual_square_off", price)
        if not m.in_trade:
            return True, f"Squared off {index} @ {price:,.2f}"
        return False, self._take_notes() or "Exit not confirmed flat; the position is held for reconciliation"

    async def _exit_price(self, price_getter) -> float:
        """The held index's fresh price, else its last tick, else the entry.

        Only the ledger's exit price depends on it. A square-off is never
        refused for want of a quote: the position is real either way.
        """
        m = self.manager
        price = await price_getter("manual exit")
        if price is None and m.last_price_getter is not None:
            price = m.last_price_getter(m._index)
        return float(price if price is not None else (m.entry_price or 0.0))


#: An index option's premium is never this large a share of its index (weekly ATM ~0.3-1.5%).
MAX_PREMIUM_SHARE = 0.15

#: Seconds before each retry of a leg quote. The broker intermittently answers an option's quote
#: with its index's (cause unknown: not the token, the same contract quotes fine moments later).
LEG_QUOTE_WAITS = (0.3, 1.0, 3.0)


def _leg_premium(symbol: str, side: str, quote: Any, spot: Optional[float]) -> float:
    """The price one leg would fill at: best ask for a buy, best bid for a sell, else the last trade.
    Refuses a quote of another contract, or one priced like the index rather than an option."""
    if not isinstance(quote, dict) or str(quote.get("stat") or "").lower() != "ok":
        raise ValueError(f"no quote: {quote!r}")
    if quote.get("tsym") not in (None, symbol):
        raise ValueError(f"quote is for {quote.get('tsym')!r}, not {symbol}: {quote!r}")
    premium = float(quote.get("sp1" if side == "B" else "bp1") or 0) or float(quote.get("lp") or 0)
    if not 0 < premium < float("inf"):
        raise ValueError(f"no usable price: {quote!r}")
    if spot and premium > MAX_PREMIUM_SHARE * spot:
        raise ValueError(f"premium {premium} is not an option price (index {spot}): {quote!r}")
    return premium


#: Most a trade's 4 leg quotes may differ from its index move, as a share of the index,
#: before they are taken as a junk book and the index move is used instead.
MAX_QUOTE_DRIFT = 0.005


def _quoted(trade: Dict[str, Any], fills: list) -> bool:
    """True when a closed trade has all 4 leg quotes - a buy and a sell at entry and at
    exit - and each is an option price, not the index's own quote."""
    entry = float(trade["entry"])
    ceiling = MAX_PREMIUM_SHARE * entry
    if not (len(fills) == 4
            and {(f["phase"], f["side"]) for f in fills} == {("entry", "B"), ("entry", "S"), ("exit", "B"), ("exit", "S")}
            and all(0 < float(f["premium"]) < ceiling for f in fills)):
        return False
    # A junk book (a placeholder bid at the open, an illiquid strike) shows as legs that
    # moved nothing like the index did: typical gap 0.03% of the index, junk 0.7%+.
    legs = sum((1 if f["side"] == "S" else -1) * float(f["premium"]) for f in fills)
    move = float(trade["exit"]) - entry
    return abs(legs - (-move if trade["direction"] == "SHORT" else move)) <= MAX_QUOTE_DRIFT * entry


def _charges(name: str, trade: Dict[str, Any], row: Dict[str, Any], fills: list) -> tuple[float, str]:
    """Every Indian charge on the round trip at Shoonya (Rs 5 an executed option order, STT,
    exchange, SEBI, stamp, GST), on this trade's own 4 option orders.

    "quotes": each order at the premium its leg was quoted at when the trade opened and closed.
    "model": a leg quote is missing or not an option price (e.g. the index's own quote was
    recorded), so the ATM premium is modelled as hhll_tick does."""
    spec = hhll_tick.SPECS.get(name)
    if spec is None or not trade.get("lots"):
        return 0.0, "none"
    if _quoted(trade, fills):
        total = 0
        for f in fills:
            qty = int(f["qty"])
            c = hhll_tick.order_charges(spec.exch, f["side"] == "B", qty, float(f["premium"]),
                                        math.ceil(qty / spec.freeze))
            total += sum(c.values())
        return float(total), "quotes"
    side = -1 if trade["direction"] == "SHORT" else 1
    e_ms = int(row["log_time"].timestamp() * 1000)
    x_ms = int((row.get("exit_time") or row["log_time"]).timestamp() * 1000)
    return hhll_tick.round_trip_charges(spec, int(trade["lots"]), side, float(trade["entry"]), float(trade["exit"]),
                                        e_ms, x_ms)["total"], "model"


def _settle(name: str, trade: Dict[str, Any], row: Dict[str, Any], fills: list) -> Dict[str, Any]:
    """A closed trade's gross points and ₹, its charges, and what is left after them (in place).

    With all 4 leg quotes recorded, the ₹ is what the legs would have paid - each sell's
    bid less each buy's ask - so the option spread and the synthetic's drift off the index
    are in it. Without them (or for v3's night book, which records none) it is the index move."""
    qty = trade["quantity"]
    if qty and _quoted(trade, fills):
        trade["pnl"] = round(sum((1 if f["side"] == "S" else -1) * float(f["premium"]) * int(f["qty"])
                                 for f in fills), 2)
        trade["points"] = round(trade["pnl"] / qty, 2)
    else:
        move = trade["exit"] - trade["entry"]
        trade["points"] = round(-move if trade["direction"] == "SHORT" else move, 2)
        trade["pnl"] = round(trade["points"] * qty, 2)
    charges, trade["charges_basis"] = _charges(name, trade, row, fills)
    trade["charges"] = round(charges, 2)
    trade["net_pnl"] = round(trade["pnl"] - trade["charges"], 2)
    trade["net_points"] = round(trade["net_pnl"] / qty, 2) if qty else trade["points"]
    return trade


#: What `_settle` adds to a trade.
_SETTLED = ("points", "pnl", "charges_basis", "charges", "net_pnl", "net_points")


def trade_figures(row: Dict[str, Any], fills=()) -> Dict[str, Any]:
    """One closed ledger row's figures - points, ₹, charges and the net after them -
    exactly as the console's books figure them, for the performance history. `row`
    carries typed values (datetimes, floats); `fills` are its recorded leg quotes."""
    name, trade = ManualDesk._trade(row)
    return _settle(name, trade, row, list(fills))


def _public(order: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return None if order is None else {k: v for k, v in order.items() if not k.startswith("_")}


# --------------------------------------------------------------------------- #
#  Managers                                                                    #
# --------------------------------------------------------------------------- #
class _DeskManager:
    """What the desk adds to an algo manager: its decider, entry window,
    locks, lot count and thread names. Everything else is the algo's."""

    _ENTRY_LOCK = ("tm_manual_entry", 1)
    _EXIT_LOCK = ("tm_manual_exit", 2)

    def __init__(self, *, desk_settings: ManualSettings = MANUAL_SETTINGS,
                 last_price_getter: Optional[Callable[[str], Optional[float]]] = None,
                 auto: Optional["TickTrader"] = None,
                 entry_blocker: Optional[Callable[[], Optional[str]]] = None,
                 **kwargs) -> None:
        self.desk_settings = desk_settings
        self.last_price_getter = last_price_getter
        self.auto = auto
        # Says why no new entry may open (the v3 night trade is held), or None.
        self.entry_blocker = entry_blocker
        super().__init__(strategy_service=None, poll_interval=desk_settings.poll_interval, **kwargs)
        self.decider = self.strategy = ManualDecider(self)

    def entry_paused(self) -> Optional[str]:
        """`entry_blocker`'s answer; a blocker that raises pauses entries (exits are never held)."""
        blocker = self.entry_blocker
        if blocker is None:
            return None
        try:
            return blocker()
        except Exception as exc:  # noqa: BLE001 - an unknown v3 state never lets an entry through
            LOGGER.error("[MANUAL] v3 position check failed (%s); entries paused.", exc)
            return f"v3 position check failed: {exc}"

    def _past_entry_cutoff(self, pending: Dict[str, Any]) -> bool:
        opens, cutoff = self.desk_settings.entry_from, self.desk_settings.square_off_at
        if not opens <= now_ist().time() < cutoff:
            LOGGER.warning("[MANUAL] Manual entries are taken %s-%s IST; entry refused.",
                           opens.strftime("%H:%M"), cutoff.strftime("%H:%M"))
            return True
        paused = self.entry_paused()
        if paused is not None:
            LOGGER.warning("[MANUAL] Entry refused: %s.", paused)
            return True
        return False

    async def _nap(self) -> None:
        """Until the automatic trader's book changes side, or the poll interval."""
        if self.auto is None:
            return await super()._nap()
        await self.auto.woke.wait(getattr(self, "_woke_seen", 0), self.poll_interval)
        self._woke_seen = self.auto.woke.seq    # read before the next cycle: a change during it ends the next nap at once

    def start(self) -> None:
        async def run() -> None:
            # Named so `_DeskLogTag` can tell the desk's broker and database
            # calls from the algo's, which run on another loop's executor.
            asyncio.get_running_loop().set_default_executor(
                ThreadPoolExecutor(thread_name_prefix=f"{THREAD_NAME}-io"))
            await self._main_loop()
        asyncio.run(run())


class ManualTradeManagerLive(_DeskManager, TradeManagerLive):
    """The algo's live manager, on the desk's ledger, broker product and decider."""


class ManualTradeManagerTest(_DeskManager, TradeManagerTest):
    """The algo's paper manager, on the desk's ledger and decider.

    Paper rows also carry the lot count and both legs, which the algo's paper
    rows never needed: the desk sizes every entry by hand.
    """

    _held_quantity: Optional[int] = None
    _legs: tuple = (None, None)          # (bought leg, sold leg) of the open synthetic

    def _load_open_trade_state(self) -> None:
        super()._load_open_trade_state()
        if self.in_trade:
            rows = self.db.execute("SELECT quantity, buy_symbol, sell_symbol FROM botdata.trade_logs WHERE id=%s;",
                                   (self.trade_row_id,), fetch=True) or []
            self._held_quantity = rows[0].get("quantity") if rows else None
            self._legs = (rows[0].get("buy_symbol"), rows[0].get("sell_symbol")) if rows else (None, None)

    async def _exit_trade(self, reason: str, exit_price: float) -> None:
        held = self.in_trade and bool(self.symbol)
        tid, legs, qty = self.trade_row_id, self._legs, self._held_quantity
        await super()._exit_trade(reason, exit_price)
        if held and tid and not self.in_trade:
            self._quote_legs(tid, "exit", legs, qty)

    def _quote_legs(self, trade_id: int, phase: str, legs: tuple, qty: Optional[int]) -> None:
        """Record, off the desk's loop, what each leg would have filled at now (for its charges)."""
        if qty:
            threading.Thread(target=self._record_legs, args=(trade_id, phase, legs, int(qty)),
                             daemon=True, name=f"{THREAD_NAME}-legs").start()

    _tokens: Dict[str, tuple] = {}

    def _spot_now(self, symbol: str) -> Optional[float]:
        inst = instruments.of_symbol(symbol)
        getter = self.last_price_getter if inst is not None else None
        return getter(inst.name) if getter else None

    def _leg_token(self, symbol: str) -> tuple[str, str]:
        """(exchange, token) of one option contract, from the broker's scrip master (cached)."""
        if symbol not in self._tokens:
            inst = instruments.of_symbol(symbol)
            if inst is None:
                raise ValueError(f"no index for {symbol}")
            found = self.api.searchscrip(exchange=inst.fo_exchange, searchtext=symbol)
            rows = found.get("values") if isinstance(found, dict) else found
            match = next((r for r in rows or () if isinstance(r, dict) and r.get("tsym") == symbol and r.get("token")), None)
            if match is None:
                raise ValueError(f"{symbol} not in the scrip master")
            self._tokens[symbol] = (inst.fo_exchange, str(match["token"]))
        return self._tokens[symbol]

    def _leg_quote(self, symbol: str, side: str) -> float:
        """One leg's premium, asked again on a bad reply, with the token looked up afresh each time."""
        for tries, wait in enumerate((*LEG_QUOTE_WAITS, None), 1):
            exchange, token = self._leg_token(symbol)
            quote = self.api.get_quotes(exchange=exchange, token=token)
            try:
                premium = _leg_premium(symbol, side, quote, spot=self._spot_now(symbol))
            except ValueError as exc:
                if wait is None:
                    raise ValueError(f"asked {exchange}|{token}, {tries} tries: {exc}") from exc
                self._tokens.pop(symbol, None)
                time.sleep(wait)
                continue
            if tries > 1:
                LOGGER.warning("[MANUAL] %s leg quote good on try %d (asked %s|%s)", symbol, tries, exchange, token)
            return premium
        raise AssertionError("unreachable")

    def _record_legs(self, trade_id: int, phase: str, legs: tuple, qty: int) -> None:
        for symbol, opens in zip(legs, ("B", "S")):
            if not symbol:
                continue
            side = opens if phase == "entry" else ("S" if opens == "B" else "B")
            try:
                premium = self._leg_quote(symbol, side)
                self.db.execute(
                    "INSERT INTO botdata.leg_fills (trade_id, phase, symbol, side, qty, premium, quoted_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                    (trade_id, phase, symbol, side, qty, premium, ist.now()))
            except Exception as exc:  # noqa: BLE001 - the trade stands; its charges fall back to the model
                LOGGER.warning("[MANUAL] %s %s leg quote for trade %s failed: %s", phase, symbol, trade_id, exc)

    async def _insert_trade_row(self, price, symbol, direction, stop_price, book, index_name,
                                **_legs) -> int:
        # The desk reads its own legs below; the algo's leg kwargs are not used.
        legs = getattr(self._fetcher_for(index_name), "final_symbols", None) or {}
        buy = legs.get("buy") if legs.get("sell") == symbol else None
        quantity = instruments.quantity_of(index_name, self.lot_size) * int(self.lots)
        res = await self.db.execute_async(
            """
            INSERT INTO botdata.trade_logs
                  (log_time, symbol, direction, fill_price, stop_price, exit_price,
                   is_in_trade, win, execution_mode, trade_state, book, index_name,
                   quantity, buy_symbol, sell_symbol, sell_entry_side)
            VALUES (%s, %s, %s, %s, %s, NULL, TRUE, NULL, 'TEST', 'OPEN', %s, %s, %s, %s, %s, 'S')
            RETURNING id;
            """,
            (ist.now(), symbol, direction, price, stop_price, book, index_name,
             quantity, buy, symbol),
            fetch=True,
        )
        self._held_quantity = quantity
        self._legs = (buy, symbol)
        trade_id = int(res[0]["id"])
        self._quote_legs(trade_id, "entry", self._legs, quantity)
        return trade_id

    def _clear_position(self) -> None:
        super()._clear_position()
        self._held_quantity = None
        self._legs = (None, None)


# --------------------------------------------------------------------------- #
#  Market data for the desk's chart                                            #
# --------------------------------------------------------------------------- #
#: Ticks per index the tape keeps in memory once the database has them: enough for a
#: chart stream to catch up from. Anything older is read back from `hhll.ticks`.
TAPE_KEEP = 4096


def _tape_open(now: dt_time) -> bool:
    """Whether a tick is a tick of the session."""
    return SESSION_OPEN_TIME <= now <= DATA_SETTINGS.session_close


class TickTape:
    """Every spot tick of today's session, per index: stored in the database, the latest in memory.

    Fed from the websocket's own thread, so `on_tick` only parses, appends and queues the
    tick for the store's background writer. Each tick gets its seq, its place in that
    index's session. With a store (`persist_to`) memory holds only the last `keep` ticks
    plus any the database has not committed yet; older ones are read back from the
    database. Without one (tests) the whole session stays in memory.
    Ticks before the 09:15 open are dropped: the subscription snapshot the
    broker sends then still carries yesterday's close.
    `generation` moves at each day rollover so a stream can tell a fresh day
    from more of the same one.
    """

    def __init__(self, spot_keys: Mapping[str, str], keep: int = TAPE_KEEP) -> None:
        self._by_key = {key: name for name, key in spot_keys.items()}
        self._by_token: Dict[str, Optional[str]] = {}
        for name, key in spot_keys.items():
            token = key.split("|", 1)[-1]
            # A token two exchanges share cannot be attributed without `e`.
            self._by_token[token] = None if token in self._by_token else name
        self._lock = threading.RLock()
        self.cond = threading.Condition(self._lock)
        # One condition per index on the same lock: a chart stream waits on its own index's
        # alone, so the other instruments' ticks do not wake it.
        self._waiters = {name: threading.Condition(self._lock) for name in spot_keys}
        #: This process's tape: a stream resumes from a seq only on the tape that numbered it.
        self.epoch = uuid.uuid4().hex[:12]
        #: Called as (index, price, epoch seconds, seq) after each tick is stored.
        self.observers: list = []
        self.keep = max(1, int(keep))
        self.store = None
        self._day = ist.today()
        self.generation = 0
        self._ms = {name: array("q") for name in spot_keys}
        self._prices = {name: array("d") for name in spot_keys}
        self._base = {name: 0 for name in spot_keys}     # seq of the first tick held in memory
        self._saved = {name: -1 for name in spot_keys}   # highest seq of today the database has

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._ms)

    def waiter(self, name: str) -> threading.Condition:
        """The condition notified on each tick of `name` and at a day rollover; it shares
        `cond`'s lock, so `size`/`generation` read under it are consistent."""
        return self._waiters[name]

    def persist_to(self, store) -> None:
        """Store every tick in `store` from now on, continuing today's seqs there (a restart
        mid-session carries on where the last run stopped). Called before the first tick."""
        with self.cond:
            day = self._day
            for name in self._ms:
                n = store.next_seq(day, name)
                held = list(zip(self._ms[name], self._prices[name]))
                self._base[name], self._saved[name] = n, n - 1
                for i, (m, p) in enumerate(held):
                    store.writer.put_tick((day, name, n + i, m, p))
            self.store = store
            store.writer.on_ticks.append(self._on_saved)

    def detach(self) -> None:
        """Stop queueing ticks (shutdown); memory keeps the session from here on."""
        with self.cond:
            if self.store is not None:
                try:
                    self.store.writer.on_ticks.remove(self._on_saved)
                except ValueError:
                    pass
            self.store = None

    def _on_saved(self, day, name: str, seq: int) -> None:
        with self.cond:
            if day == self._day and name in self._saved and seq > self._saved[name]:
                self._saved[name] = seq
                self._trim(name)

    def _trim(self, name: str) -> None:
        """Drop the oldest ticks the database already holds, beyond `keep` (caller holds `cond`)."""
        ms = self._ms[name]
        if self.store is None or len(ms) <= 2 * self.keep:
            return
        drop = min(self._saved[name] + 1 - self._base[name], len(ms) - self.keep)
        if drop > 0:
            del ms[:drop]
            del self._prices[name][:drop]
            self._base[name] += drop

    def on_tick(self, tick: Any) -> None:
        if not isinstance(tick, dict):
            return
        try:
            price = float(tick.get("lp"))
        except (TypeError, ValueError):
            return
        if not 0 < price < float("inf"):
            return
        token = str(tick.get("tk") or "").strip()
        exch = str(tick.get("e") or "").strip()
        name = self._by_key.get(f"{exch}|{token}") if exch else self._by_token.get(token)
        if name is None or not _tape_open(ist.now().time()):
            return
        ms = int(time.time() * 1000)
        with self.cond:
            today = ist.today()
            if today != self._day:
                self._day = today
                self.generation += 1
                for name_ in self._ms:
                    self._ms[name_] = array("q")
                    self._prices[name_] = array("d")
                    self._base[name_], self._saved[name_] = 0, -1
                for waiter in self._waiters.values():
                    waiter.notify_all()
            seq = self._base[name] + len(self._ms[name])
            self._ms[name].append(ms)
            self._prices[name].append(price)
            if self.store is not None:
                self.store.writer.put_tick((self._day, name, seq, ms, price))
            self._trim(name)
            self._waiters[name].notify_all()
            self.cond.notify_all()
        for observer in self.observers:
            try:
                observer(name, price, ms / 1000, seq)
            except Exception:  # noqa: BLE001 - one observer must not stop the tape
                LOGGER.exception("[MANUAL] tape observer failed")

    def size(self, name: str) -> int:
        """Ticks of today so far (the next tick's seq)."""
        return self._base[name] + len(self._ms[name])

    def read(self, name: str, start: int = 0) -> tuple[int, int, list[int], list[float]]:
        """(generation, total, epoch ms from seq `start`, prices from seq `start`).
        Ticks no longer in memory come from the database (outside the lock)."""
        with self.cond:
            gen, day, base, store = self.generation, self._day, self._base[name], self.store
            total = base + len(self._ms[name])
            i = max(0, start - base)
            ms, px = self._ms[name][i:].tolist(), self._prices[name][i:].tolist()
        if start < base:
            _, dm, dp = store.ticks(day, name, start, base)
            ms, px = dm + ms, dp + px
        return gen, total, ms, px

    def last(self, name: str) -> Optional[float]:
        with self.cond:
            prices = self._prices.get(name)
            return prices[-1] if prices else None

    def fresh(self, name: str, max_age: float = TICK_FRESH_SEC) -> Optional[float]:
        """The last tick, only while it is younger than `max_age` seconds; never a quote."""
        with self.cond:
            ms, prices = self._ms.get(name), self._prices.get(name)
            if not ms or time.time() * 1000 - ms[-1] > max_age * 1000:
                return None
            return prices[-1]

    def history(self, name: str) -> tuple[list, list, int]:
        """Today's ticks so far (epoch ms, prices) and the last one's seq: the database's part
        (everything this and any earlier run of today stored), then memory's."""
        with self.cond:
            day, base, store = self._day, self._base[name], self.store
            k = max(0, self._saved[name] + 1 - base)      # first held tick the database lacks
            ms, px = self._ms[name][k:].tolist(), self._prices[name][k:].tolist()
            last = base + len(self._ms[name]) - 1
        if store is not None and base + k > 0:
            _, dm, dp = store.ticks(day, name, 0, base + k)
            ms, px = dm + ms, dp + px
        return ms, px, last


def _resume_point(tape: TickTape, name: str, last_id: Optional[str]) -> Optional[tuple[int, int]]:
    """(generation, ticks already sent) when `last_id` names a point of this tape's current
    day for `name`; None (send the whole session) otherwise."""
    try:
        epoch, gen, sent = str(last_id or "").strip().split(".")
        gen, sent = int(gen), int(sent)
    except ValueError:
        return None
    with tape.cond:
        if epoch != tape.epoch or gen != tape.generation or not 0 <= sent <= tape.size(name):
            return None
    return gen, sent


#: Price scales the compact encoding tries: paise.
TICK_SCALES = (100,)


def _tick_payload(name: str, reset: bool, start: int, times: list, prices: list, compact: bool) -> Dict[str, Any]:
    """One tick-stream message. `from` is the seq of its first tick, so a client can tell a
    gap. Compact (`enc` "d1"): times as the first epoch ms (`t0`) and the steps after it
    (`dt`); prices as integers of 1/`ps` (`q0`, `dq`) when every price is exactly one, so
    `q / ps` gives back the very same float; else as they are (`p`). A whole session is
    ~3x smaller."""
    payload: Dict[str, Any] = {"index": name, "reset": reset, "from": start}
    if not compact:
        payload.update(t=times, p=prices)
        return payload
    payload["enc"] = "d1"
    if not times:
        payload.update(dt=[], p=[])
        return payload
    payload["t0"] = times[0]
    payload["dt"] = [b - a for a, b in zip(times, times[1:])]
    for scale in TICK_SCALES:
        q = [round(x * scale) for x in prices]
        if all(c / scale == x for c, x in zip(q, prices)):
            payload.update(ps=scale, q0=q[0], dq=[b - a for a, b in zip(q, q[1:])])
            break
    else:
        payload["p"] = prices
    return payload


class CandleCache:
    """1, 5 and 15 minute candles per index, from the broker, fetched lazily.

    Only a chart that is being looked at costs a call: at most one per bar
    boundary (after a short grace for the broker to close the bar) and one
    per `max_age_sec` in between, which picks up the bar still forming. The
    desk's chart draws the forming bar from ticks itself.

    Only today's bars are served: before the bell, or on a holiday, the chart
    is empty rather than showing an earlier session as if it were today's.
    """

    GRACE_SEC = 3.0

    def __init__(self, api, spot_tokens: Mapping[str, tuple[str, str]], *, max_age_sec: float = 30.0) -> None:
        self.api = api
        self.spot_tokens = dict(spot_tokens)
        self.max_age_sec = max_age_sec
        self._lock = threading.Lock()
        self._fetch_lock = threading.Lock()
        self._cache: Dict[tuple[str, int], tuple[int, float, list]] = {}

    def _fresh(self, key: tuple[str, int], now: float) -> Optional[list]:
        entry = self._cache.get(key)
        if entry is None:
            return None
        bucket, fetched_at, bars = entry
        current = int((now - self.GRACE_SEC) // (key[1] * 60))
        return bars if bucket == current and now - fetched_at < self.max_age_sec else None

    def get(self, name: str, minutes: int) -> list:
        key = (name, minutes)
        with self._lock:
            bars = self._fresh(key, time.time())
        if bars is not None:
            return bars
        with self._fetch_lock:
            now = time.time()
            with self._lock:
                bars = self._fresh(key, now)
            if bars is not None:
                return bars
            try:
                bars = self._fetch(name, minutes)
            except Exception:
                with self._lock:
                    stale = self._cache.get(key)
                if stale is None:
                    raise
                LOGGER.warning("[MANUAL] %dm %s candle refresh failed; serving the last set.",
                               minutes, name, exc_info=True)
                return stale[2]
            with self._lock:
                self._cache[key] = (int((now - self.GRACE_SEC) // (minutes * 60)), now, bars)
            return bars

    def _fetch(self, name: str, minutes: int) -> list:
        exch, tok = self.spot_tokens[name]
        df = shoonya_candles(self.api, exch, tok, ist.midnight(), interval=minutes)
        if df.empty:
            return []
        df = df[df["time"].dt.date == ist.today()]
        return [{"time": row.time.strftime(ist.FEED_TIME_FORMAT), "open": float(row.open),
                 "high": float(row.high), "low": float(row.low), "close": float(row.close)}
                for row in df.itertuples()]


# --------------------------------------------------------------------------- #
#  Legs                                                                        #
# --------------------------------------------------------------------------- #
class DeskLegs(GetSymbol):
    """`GetSymbol` for the desk: v3's synthetic long, and its mirror short.

    `side` is the side of the click being worked; the decider sets it just
    before it asks for legs. A short is the long turned over: the put bought
    and the call sold, at the in-the-money put strike closest to the money,
    on the same expiry. The put goes first (`_place_hedge_seq` buys the `buy`
    leg first), so a short is never left naked either. The algo's builders
    are plain `GetSymbol` and stay long-only.
    """

    side: str = "LONG"

    def _itm_put_strike(self, spot: float, expiry=None) -> int:
        """The lowest listed strike strictly above `spot`: `_itm_call_strike` mirrored."""
        listed = self._listed_itm_strike(spot, expiry, below=False)
        if listed is not None:
            return listed
        step = self.strike_step
        return int(math.floor(spot / step) * step + step)

    def _build_legs(self, current_price: float, pivot_type: str) -> Dict[str, Any]:
        if pivot_type != "LL":
            return super()._build_legs(current_price, pivot_type)
        expiry = self._entry_expiry(ist.today())
        strike = self._itm_put_strike(current_price, expiry)
        code = expiry.strftime("%d%b%y").upper()
        return {
            "buy": self._format_symbol(code, "P", strike),
            "sell": self._format_symbol(code, "C", strike),
            "sell_side": "S",
            "bias": "LL",
            "index": self.index_name,
            "exchange": self.fo_exch,
            "quantity": self.quantity,
            "atm_strike": self._atm_strike(current_price, expiry),
            "strike": strike,
            "expiry": expiry.isoformat(),
        }


# --------------------------------------------------------------------------- #
#  The desk                                                                    #
# --------------------------------------------------------------------------- #
class ManualDesk:
    #: Set while stop() runs: the web threads outlive the pool, and their requests are refused quietly.
    closing = False

    """Builds and runs the desk; serves it to the console.

    Lifecycle, from `main.py`: construct, `connect()`, `register_routes(app)`,
    feed `tape.on_tick` from the websocket, `start(order_writer)` once the
    websocket is up, `stop()` before the algo's own shutdown.
    """

    def __init__(
        self,
        *,
        api,
        mode: str,
        spot_tokens: Mapping[str, tuple[str, str]],
        spot_keys: Mapping[str, str],
        price_getters: Mapping[str, Callable[[], Optional[float]]],
        ledger_db=None,
        settings: ManualSettings = MANUAL_SETTINGS,
        fetchers: Optional[Mapping[str, Any]] = None,
        tick_store=None,
        night_db=None,
    ) -> None:
        """`tick_store`: an `hhll_store.Store` (not yet connected) that every tick, pivot and the
        engine's settings live in; None keeps the session in memory (tests).

        `night_db`: the v3 algo's own connector (`botdata`), read only, so the console's
        `/books` shows the night book beside this desk's day book. Never written to."""
        self.mode = str(mode).strip().upper()
        if self.mode not in {"TEST", "LIVE"}:
            raise ValueError(f"invalid trade mode {mode!r}")
        self.api = api
        self.settings = settings
        self.price_getters = dict(price_getters)
        self.names = tuple(name for name in instruments.NAMES if name in spot_tokens)
        self.ledger = ManualLedger(ledger_db if ledger_db is not None
                                   else db_connector.TimescaleDBConnector(minconn=1, maxconn=6))
        self.tick_store = tick_store
        self.night_db = night_db
        self.tape = TickTape({name: spot_keys[name] for name in self.names})
        self.auto = TickTrader(self.names, self.tape.history, since=lambda n, start: self.tape.read(n, start)[2:])
        self.tape.observers.append(self.auto.on_tick)
        self.candles = CandleCache(api, {name: spot_tokens[name] for name in self.names})
        self.fetchers = dict(fetchers) if fetchers is not None else {
            name: self._fetcher(api, name, spot_tokens[name][1]) for name in self.names}
        self.manager = None
        self.thread: Optional[threading.Thread] = None
        self.order_writer = None
        #: Says why no new entry may open, or None. `main.py` points it at the v3
        #: manager before `start`: the desk takes no day trade while the v3 night
        #: trade is held.
        self.entry_blocker: Optional[Callable[[], Optional[str]]] = None
        self._settled: Dict[tuple, Dict[str, Any]] = {}   # a closed trade's figures, by everything they depend on

    def _fetcher(self, api, name: str, token: str) -> DeskLegs:
        """A leg builder of the desk's own: same contracts as the algo's, built on demand."""
        common = dict(
            instrument=instruments.get(name),
            option_roll_days_before_expiry=TRADE_SETTINGS.option_roll_days_before_expiry,
            future_roll_days_before_expiry=TRADE_SETTINGS.future_roll_days_before_expiry,
            expiry_holidays=TRADE_SETTINGS.expiry_holidays,
        )
        fetcher = DeskLegs(api, self.price_getters[name], **common)
        fetcher.tok = str(token)
        # Legs are built only for a click, for the side that click opens.
        fetcher.set_pivot_type_getter(lambda: bias_tag(fetcher.side))
        return fetcher

    def connect(self) -> None:
        connect = getattr(self.ledger.db, "connect", None)
        if callable(connect):
            connect()
        self.ledger.ensure_schema()
        store = self.tick_store
        if store is not None:
            store.connect()
            hhll_tick.use_store(store)
            hhll_tick.migrate_files()            # the old files, before any seq is taken
            store.archive_closed_days(ist.today())
            self.tape.persist_to(store)
            self.auto.attach(store)

    def _new_manager(self):
        chart = instruments.CHART_INDEX if instruments.CHART_INDEX in self.fetchers else self.names[0]
        manager_type = ManualTradeManagerTest if self.mode == "TEST" else ManualTradeManagerLive
        # Entries, exits and stops are priced from the desk's own ticks only: no REST quote.
        ticks = {name: (lambda n=name: self.tape.fresh(n)) for name in self.names}
        return manager_type(
            api=self.api if self.mode == "TEST" else ManualBroker(self.api, self.settings.product),
            db_connector=self.ledger,
            symbol_fetcher=self.fetchers[chart],
            symbol_fetchers=self.fetchers,
            index_live_price_getters=ticks,
            live_price_getter=ticks.get(chart),
            fresh_price_getter=ticks.get(chart),
            desk_settings=self.settings,
            last_price_getter=self.tape.last,
            auto=self.auto,
            entry_blocker=self.entry_blocker,
        )

    def start(self, order_writer=None) -> None:
        """Start the desk's manager. `order_writer` is its order-update writer."""
        self.closing = False
        self.order_writer = order_writer
        self.manager = self._new_manager()
        _DeskLogTag.install(self.manager.decider.note)
        self.thread = threading.Thread(target=self.manager.start, daemon=True, name=THREAD_NAME)
        self.thread.start()
        self.auto.start()
        LOGGER.warning("[MANUAL] Desk started in %s mode (%s).", self.mode,
                       "paper" if self.mode == "TEST" else f"broker product {self.settings.product}")

    def stop(self, timeout: float = 90.0) -> bool:
        """Stop the manager, then its writer, then the pool. False if it would not stop."""
        self.closing = True
        if self.thread is not None and self.thread.is_alive():
            self.manager.stop()
            self.thread.join(timeout=timeout)
            if self.thread.is_alive():
                LOGGER.critical("[MANUAL] Desk manager did not stop; leaving its ledger open.")
                return False
        drained = True
        if self.order_writer is not None:
            drained = bool(self.order_writer.stop())
            if not drained:
                LOGGER.critical("[MANUAL] Desk order-update writer did not drain.")
        close = getattr(self.ledger.db, "close", None)
        if drained and callable(close):
            close()
        self.auto.stop()
        if self.tick_store is not None:
            # Ticks after this moment stay in memory only: the process is going down.
            self.tape.detach()
            if not self.tick_store.close():
                LOGGER.critical("[MANUAL] Not every tick/pivot reached the database.")
        return drained

    def busy_reason(self) -> Optional[str]:
        """Why the desk is not flat - a position, an entry or exit in flight, an
        order queued - or None. Any thread. The v3 night trade waits on it."""
        m = self.manager
        if m is None:
            return None
        reason = m.busy_reason()
        if reason is not None:
            return f"the HH/LL desk is {reason}"
        orders = m.decider.snapshot()
        if orders["queued"] is not None or orders["working"] is not None:
            return "the HH/LL desk has an order in flight"
        return None

    # -- reads for the console ------------------------------------------------
    def submit(self, action: str, index: Optional[str], lots: Any) -> Dict[str, Any]:
        """Queue one click. Raises ValueError for a bad order, DeskBusy for a second one."""
        action = str(action or "").strip().upper()
        if action not in {*OPEN_ACTIONS, "SELL"}:
            raise ValueError("action must be BUY, SHORT or SELL")
        if self.manager is None or self.thread is None or not self.thread.is_alive():
            raise RuntimeError("the manual desk is not running")
        if action in OPEN_ACTIONS:
            index = str(index or "").strip().upper()
            if index not in self.fetchers:
                raise ValueError(f"unknown index {index!r}")
            if isinstance(lots, bool) or not isinstance(lots, int) or not 1 <= lots <= self.settings.max_lots:
                raise ValueError(f"lots must be a whole number from 1 to {self.settings.max_lots}")
            return self.manager.decider.submit(action, index, lots)
        return self.manager.decider.submit("SELL")

    def _fills(self, ids: list) -> Dict[int, list]:
        """Each closed trade's recorded leg quotes, by trade id."""
        fills: Dict[int, list] = {}
        if ids:
            for f in self.ledger.execute(
                    "SELECT trade_id, phase, symbol, side, qty, premium FROM botdata.leg_fills WHERE trade_id = ANY(%s);",
                    (ids,), fetch=True) or []:
                fills.setdefault(f["trade_id"], []).append(f)
        return fills

    @staticmethod
    def _trade(row: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        name = row.get("index_name") or instruments.CHART_INDEX
        lot = instruments.quantity_of(name, TRADE_SETTINGS.lot_size)
        qty = int(row.get("quantity") or lot)
        return name, {
            "id": row["id"], "index": name, "direction": row.get("direction"),
            "entry": row.get("fill_price"), "exit": row.get("exit_price"),
            "quantity": qty, "lots": qty // lot if lot else None,
            "opened_ist": ist.isoformat(row["log_time"]) if row.get("log_time") else None,
            "closed_ist": ist.isoformat(row["exit_time"]) if row.get("exit_time") else None,
            "state": row.get("trade_state"), "open": bool(row.get("is_in_trade")),
            "buy": row.get("buy_symbol"), "sell": row.get("sell_symbol") or row.get("symbol"),
        }

    _earlier: tuple = (None, 0.0)       # (IST day, net ₹ of every closed trade before it)

    def _settle(self, name: str, trade: Dict[str, Any], row: Dict[str, Any], fills: list) -> Dict[str, Any]:
        """`_settle`, cached: the console reads the day's trades every second, and a closed
        trade's figures change only with its prices, quantity, times or recorded leg quotes."""
        key = (trade["id"], name, trade["direction"], trade["entry"], trade["exit"], trade["quantity"],
               trade["lots"], row.get("log_time"), row.get("exit_time"),
               tuple(sorted((f["phase"], f["symbol"], f["side"], f["qty"], f["premium"]) for f in fills)))
        figures = self._settled.get(key)
        if figures is None:
            done = _settle(name, dict(trade), row, fills)
            figures = {k: done[k] for k in _SETTLED}
            if len(self._settled) > 5000:
                self._settled.clear()
            self._settled[key] = figures
        trade.update(figures)
        return trade

    def _earlier_net(self) -> float:
        """Net ₹ (after charges) of every earlier day's closed trades, so the desk's cash
        carries over from one session to the next. Those days are closed: cached per day."""
        today = ist.today()
        if self._earlier[0] != today:
            rows = self.ledger.execute(
                """
                SELECT id, log_time, exit_time, index_name, direction, fill_price, exit_price, quantity
                FROM   botdata.trade_logs
                WHERE  execution_mode = %s AND is_in_trade = FALSE
                  AND  fill_price IS NOT NULL AND exit_price IS NOT NULL
                  AND (COALESCE(exit_time, log_time) AT TIME ZONE %s)::date < %s;
                """,
                (self.mode, ist.IST_NAME, today), fetch=True,
            ) or []
            fills = self._fills([r["id"] for r in rows])
            net = 0.0
            for row in rows:
                name, trade = self._trade(row)
                net += self._settle(name, trade, row, fills.get(row["id"], []))["net_pnl"]
            self._earlier = (today, net)
        return self._earlier[1]

    def state(self) -> Dict[str, Any]:
        rows = self.ledger.execute(
            """
            SELECT id, log_time, exit_time, index_name, direction, fill_price, exit_price,
                   quantity, is_in_trade, trade_state, buy_symbol, sell_symbol, symbol
            FROM   botdata.trade_logs
            WHERE  execution_mode = %s
              AND (is_in_trade = TRUE OR (COALESCE(exit_time, log_time) AT TIME ZONE %s)::date = %s)
            ORDER  BY id DESC
            LIMIT  100;
            """,
            (self.mode, ist.IST_NAME, ist.today()), fetch=True,
        ) or []
        fills = self._fills([r["id"] for r in rows if not r.get("is_in_trade")])
        book = self._tally(rows, lambda name, trade, row: self._settle(name, trade, row, fills.get(row["id"], [])))
        position, trades, net_pnl = book["position"], book["trades"], book["net_pnl"]
        manager = self.manager
        return {
            "mode": self.mode,
            "running": bool(self.thread and self.thread.is_alive()),
            "product": "PAPER" if self.mode == "TEST" else {"I": "MIS", "M": "NRML"}.get(
                self.settings.product, self.settings.product),
            "window": {"entry_from": self.settings.entry_from.strftime("%H:%M"),
                       "square_off_at": self.settings.square_off_at.strftime("%H:%M")},
            "max_lots": self.settings.max_lots,
            "account": {"cash": round(self.settings.account_cash + self._earlier_net() + net_pnl, 2),
                        "margin_per_lot": self.settings.margin_per_lot},
            "lot_sizes": {name: instruments.get(name).quantity for name in self.names},
            "exchanges": {name: instruments.get(name).fo_exchange for name in self.names},
            "position": position,
            "auto": self.auto.status(),
            "trades": trades,
            "closed": book["closed"],
            "net_points": book["net_points"],
            "net_pnl": net_pnl,
            "charges": book["charges"],
            "orders": manager.decider.snapshot() if manager is not None else None,
            "busy": bool(manager is not None and (getattr(manager, "entering", False)
                                                   or getattr(manager, "exiting", False))),
            "quarantined": bool(getattr(manager, "_quarantine_active", False)),
            "as_of_ist": ist.isoformat(),
        }

    def _tally(self, rows, settle: Callable[[str, Dict[str, Any], Dict[str, Any]], Any]) -> Dict[str, Any]:
        """A ledger's rows as the console's trades: the open position, every trade, and the
        closed ones' net figures (`settle` adds a closed trade's, in place)."""
        position, trades = None, []
        net_points = net_pnl = charges = 0.0
        for row in rows:
            name, trade = self._trade(row)
            if trade["open"]:
                if position is None:
                    position = trade
                    # The desk's last tick of the held index, for an open
                    # P&L on a chart that is showing another one.
                    trade["mark"] = self.tape.last(name) if name in self.tape.names else None
            elif trade["exit"] is not None and trade["entry"] is not None:
                settle(name, trade, row)
                net_points += trade["net_points"]
                net_pnl += trade["net_pnl"]
                charges += trade["charges"]
            trades.append(trade)
        return {"position": position, "trades": trades,
                "closed": sum(1 for t in trades if not t["open"] and t.get("points") is not None),
                "net_points": round(net_points, 4), "net_pnl": round(net_pnl, 2), "charges": round(charges, 2)}

    # -- the console's one view: DAY (this desk) and NIGHT (v3) -----------------
    #: Which book a trade is in. DAY: this desk - HH/LL, intraday, squared off by
    #: `square_off_at`. NIGHT: v3 - entered once the desk is flat, sold at the next open.
    DAY, NIGHT = "DAY", "NIGHT"
    #: Where `main.py` archives v3's ledger before truncating it on a flat stop.
    ARCHIVES = Path(__file__).resolve().parent / "backups"

    @staticmethod
    def _night_counts(row: Dict[str, Any]) -> bool:
        """Whether a v3 row belongs to the night book. The night book started fresh with the
        carry: closed trades of the retired momentum setup (A) are left out of every figure
        (they stay in the ledger and its archives). A held position always shows."""
        return bool(row.get("is_in_trade")) or not is_momentum_book(row.get("book"))

    def _archived_night_rows(self, today_only: bool = True) -> list:
        """Closed v3 trades of this mode from the archives `main._backup_and_clear` writes
        before it truncates v3's ledger on a flat stop. `today_only`: those closed today -
        the night trade sold at today's open must not vanish from the day's view because
        the bot was restarted after it. Else those closed before today: the account's."""
        today, rows = ist.today(), []
        pattern = f"trade_logs_{ist.date_key()}*.csv" if today_only else "trade_logs_*.csv"
        for path in sorted(self.ARCHIVES.glob(pattern)):
            with open(path, newline="", encoding="utf-8") as stream:
                for r in csv.DictReader(stream):
                    exit_time, log_time = ist.parse_ist(r.get("exit_time")), ist.parse_ist(r.get("log_time"))
                    if (str(r.get("execution_mode") or "").upper() != self.mode
                            or is_momentum_book(r.get("book"))
                            or str(r.get("is_in_trade")).lower() not in {"false", "f", "0"}
                            or exit_time is None or log_time is None
                            or (exit_time.date() != today if today_only else exit_time.date() >= today)
                            or not r.get("exit_price") or not r.get("fill_price")):
                        continue
                    rows.append({**r, "id": int(r["id"]), "log_time": log_time, "exit_time": exit_time,
                                 "fill_price": float(r["fill_price"]), "exit_price": float(r["exit_price"]),
                                 "quantity": int(r["quantity"]) if r.get("quantity") else None,
                                 "index_name": r.get("index_name") or None, "is_in_trade": False})
        return rows

    def night_state(self) -> Dict[str, Any]:
        """Today's v3 trades (closed today, or still held), figured exactly as the desk's
        own - same charges model, same net - from the algo's ledger and today's
        archive of it. Read only."""
        rows = self.night_db.execute(
            """
            SELECT id, log_time, exit_time, index_name, direction, fill_price, exit_price,
                   quantity, is_in_trade, trade_state, buy_symbol, sell_symbol, symbol, book
            FROM   botdata.trade_logs
            WHERE  execution_mode = %s
              AND (is_in_trade = TRUE OR (COALESCE(exit_time, log_time) AT TIME ZONE %s)::date = %s)
            ORDER  BY id DESC
            LIMIT  100;
            """,
            (self.mode, ist.IST_NAME, ist.today()), fetch=True,
        ) or []
        rows = self._with_archived([r for r in rows if self._night_counts(r)], today_only=True)
        # v3 records no leg quotes, so its charges are always the modelled ones.
        return self._tally(rows, lambda name, trade, row: _settle(name, trade, row, []))

    def _with_archived(self, rows: list, *, today_only: bool) -> list:
        """`rows` and the archived ones. An archive written by a backup whose truncate then
        rolled back repeats live rows, and a later archive of the same day (`.r1`) can
        repeat an earlier one's: each trade is counted once."""
        rows = list(rows)
        seen = {(int(r["id"]), r["log_time"]) for r in rows}
        for r in self._archived_night_rows(today_only):
            if (r["id"], r["log_time"]) not in seen:
                seen.add((r["id"], r["log_time"]))
                rows.append(r)
        return rows

    _night_earlier: tuple = (None, 0.0)   # (IST day, net ₹ of every v3 trade closed before it)

    def _night_earlier_net(self) -> float:
        """Net ₹ (after charges) of every v3 trade of this mode closed before today, in its
        ledger or archived, so the account carries the night book's history as the desk's
        cash carries the day book's. Those days are closed: cached per day."""
        today = ist.today()
        if self._night_earlier[0] != today:
            rows = self.night_db.execute(
                """
                SELECT id, log_time, exit_time, index_name, direction, fill_price, exit_price, quantity, book
                FROM   botdata.trade_logs
                WHERE  execution_mode = %s AND is_in_trade = FALSE
                  AND  fill_price IS NOT NULL AND exit_price IS NOT NULL
                  AND (COALESCE(exit_time, log_time) AT TIME ZONE %s)::date < %s;
                """,
                (self.mode, ist.IST_NAME, today), fetch=True,
            ) or []
            net = 0.0
            for row in self._with_archived([r for r in rows if self._night_counts(r)], today_only=False):
                name, trade = self._trade(row)
                net += _settle(name, trade, row, [])["net_pnl"]
            self._night_earlier = (today, net)
        return self._night_earlier[1]

    @staticmethod
    def _mark_open(position: Optional[Dict[str, Any]]) -> None:
        """An open position's P&L at its mark, in points and rupees (in place); None unmarked."""
        if position is None:
            return
        mark, entry = position.get("mark"), position.get("entry")
        if mark is None or entry is None:
            position["open_points"] = position["open_pnl"] = None
            return
        pts = float(entry) - float(mark) if position["direction"] == "SHORT" else float(mark) - float(entry)
        position["open_points"] = round(pts, 2)
        position["open_pnl"] = round(pts * position["quantity"], 2)

    def books(self) -> Dict[str, Any]:
        """Both books for the console, every trade tagged with its book.

        Each book is read on its own: one ledger that cannot be read never blanks the
        other. An unreadable book is None - never shown as flat - and the totals then
        cover only the books that answered (`day`/`night` say which)."""
        def read(label: str, fn: Callable[[], Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            try:
                return fn()
            except Exception:  # noqa: BLE001 - reported to the console as unavailable
                if not self.closing:
                    LOGGER.exception("[MANUAL] %s book unavailable", label)
                return None

        day = read("day", self.state)
        night = read("night", self.night_state) if self.night_db is not None else None
        trades: list = []
        for tag, book in ((self.DAY, day), (self.NIGHT, night)):
            if book is None:
                continue
            self._mark_open(book["position"])
            for trade in book.pop("trades"):
                trade["book"] = tag
                trades.append(trade)
        # IST isoformat strings, one offset: they sort as the instants they are.
        trades.sort(key=lambda t: t["opened_ist"] or "", reverse=True)
        answered = [b for b in (day, night) if b is not None]
        held = [b["position"] for b in answered if b["position"] is not None]
        total = (lambda key: round(sum(b[key] for b in answered), 2)) if answered else (lambda key: None)
        open_pnl = (None if not answered or any(p["open_pnl"] is None for p in held)
                    else round(sum(p["open_pnl"] for p in held), 2))

        def account() -> Dict[str, Any]:
            """The whole account, realised only: the starting capital and every realised net
            of both books (all days, after charges). An open position is not in it."""
            start = self.settings.account_cash
            day_all = round(day["account"]["cash"] - start, 2)     # the desk's cash: every day net, today's too
            night_all = round(self._night_earlier_net() + night["net_pnl"], 2)
            realised = round(start + day_all + night_all, 2)
            return {"start": start, "day_realised": day_all, "night_realised": night_all, "balance": realised}

        return {
            "mode": self.mode,
            "day": day,
            "night": night,
            "trades": trades,
            # None when no book answered: no figure at all rather than a zero.
            "closed": total("closed"),
            "net_pnl": total("net_pnl"),
            "charges": total("charges"),
            # None while a held position is unmarked: never a zero that reads as "costing nothing".
            "open_pnl": open_pnl,
            # Complete only with both books: None rather than a balance missing one of them.
            "account": read("account", account) if day is not None and night is not None else None,
            "as_of_ist": ist.isoformat(),
        }

    # -- routes ----------------------------------------------------------------
    def register_routes(self, app) -> None:
        names = self.names

        def chosen() -> str:
            name = (request.args.get("index") or "").strip().upper()
            return name if name in names else (instruments.CHART_INDEX if instruments.CHART_INDEX in names else names[0])

        @app.get("/manual/state")
        def manual_state():
            try:
                return jsonify(self.state())
            except Exception:  # noqa: BLE001
                if not self.closing:
                    LOGGER.exception("[MANUAL] state unavailable")
                return jsonify(error="Manual ledger unavailable"), 503

        @app.get("/books")
        def books_today():
            # The console's one trades view: the day book (this desk) and the night book (v3).
            return jsonify(self.books())

        @app.post("/manual/order")
        def manual_order():
            # Orders only from this console. A cross-site page cannot send JSON
            # without a preflight this app never answers, and a same-site one
            # must name this host as its origin.
            origin = request.headers.get("Origin")
            if origin and urlparse(origin).netloc != request.host:
                return jsonify(error="Cross-origin orders are refused"), 403
            if not request.is_json:
                return jsonify(error="Send the order as JSON"), 415
            body = request.get_json(silent=True)
            if not isinstance(body, dict):
                return jsonify(error="Send the order as a JSON object"), 400
            try:
                order = self.submit(body.get("action"), body.get("index"), body.get("lots"))
            except DeskBusy as exc:
                return jsonify(error=str(exc)), 409
            except ValueError as exc:
                return jsonify(error=str(exc)), 400
            except RuntimeError as exc:
                return jsonify(error=str(exc)), 503
            return jsonify(order=order), 202

        @app.post("/manual/settings")
        def manual_settings():
            # This console only, as for orders. hhll_tick applies the thresholds within a second.
            origin = request.headers.get("Origin")
            if origin and urlparse(origin).netloc != request.host:
                return jsonify(error="Cross-origin changes are refused"), 403
            body = request.get_json(silent=True)
            body = body if isinstance(body, dict) else {}
            trade, fixed = body.get("trade"), body.get("fixed")
            if not isinstance(fixed, dict) or set(fixed) != set(self.auto.names):
                return jsonify(error="Send a threshold for every instrument"), 400
            for name, value in fixed.items():
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not FIXED_MIN <= value <= FIXED_MAX:
                    return jsonify(error=f"{name}: the threshold must be {FIXED_MIN} to {FIXED_MAX:.0f}"), 400
            if trade not in self.auto.names:
                return jsonify(error="Choose an instrument to trade"), 400
            store = self.auto._store
            if store is None:
                return jsonify(error="The settings store is not connected"), 503
            store.save_state({"fixed": {n: float(v) for n, v in fixed.items()}}, "request")
            store.save_state({"trade": trade}, "desk")
            with self.auto._lock:
                self.auto.order = (trade,)
            return jsonify(fixed=fixed, trade=trade), 202

        @app.get("/manual/health")
        def manual_health():
            # The retune check hhll_tick stores after each finished session and each settings change.
            store = self.auto._store
            if store is None:
                return jsonify(error="The settings store is not connected"), 503
            try:
                return jsonify(store.load_state("health") or {})
            except Exception:  # noqa: BLE001
                LOGGER.exception("[MANUAL] retune check unreadable")
                return jsonify(error="Retune check unavailable"), 503

        @app.get("/manual/pivots")
        def manual_pivots():
            try:
                return jsonify(self.auto.points(chosen()))
            except Exception:  # noqa: BLE001
                if not self.closing:
                    LOGGER.exception("[MANUAL] pivots unavailable")
                return jsonify(error="Pivots unavailable"), 503

        @app.get("/manual/candles")
        def manual_candles():
            try:
                minutes = int(request.args.get("tf", "5"))
            except ValueError:
                minutes = 0
            if minutes not in CANDLE_MINUTES:
                return jsonify(error=f"tf must be one of {CANDLE_MINUTES}"), 400
            name = chosen()
            try:
                return jsonify(index=name, tf=minutes, candles=self.candles.get(name, minutes))
            except Exception:  # noqa: BLE001
                LOGGER.exception("[MANUAL] %dm %s candles unavailable", minutes, name)
                return jsonify(error="Candles unavailable"), 503

        @app.get("/manual/tick_stream")
        def manual_tick_stream():
            # Every message carries `id: <tape epoch>.<generation>.<ticks sent>`. A reconnect sends
            # it back (Last-Event-ID; a reopened stream passes it as `from`), and the stream goes on
            # from there instead of resending the whole session. `enc=d1` asks for the compact
            # encoding (`_tick_payload`); without it the payload is the plain one.
            name = chosen()
            tape = self.tape
            compact = request.args.get("enc") == "d1"
            resume = _resume_point(tape, name, request.headers.get("Last-Event-ID") or request.args.get("from"))
            waiter = tape.waiter(name)

            def events():
                generation, sent = resume if resume is not None else (None, 0)
                wrote = 0.0
                while True:
                    with waiter:
                        waiter.wait_for(
                            lambda: tape.generation != generation or tape.size(name) != sent, timeout=1.0)
                        # A new day (or a new client) starts from the top.
                        gen_now = tape.generation
                        reset = gen_now != generation
                    try:
                        # Outside the lock: older ticks are read from the database.
                        now_gen, total, times, prices = tape.read(name, 0 if reset else sent)
                    except Exception:  # noqa: BLE001 - the client reconnects and starts again
                        if not self.closing:
                            LOGGER.warning("[MANUAL] %s tick stream ended: ticks unreadable", name, exc_info=True)
                        return
                    if now_gen != gen_now:
                        generation = None          # the day turned meanwhile: start it from the top
                        continue
                    if reset or times:
                        start = 0 if reset else sent
                        generation, sent, wrote = now_gen, total, time.monotonic()
                        payload = _tick_payload(name, reset, start, times, prices, compact)
                        yield (f"id: {tape.epoch}.{now_gen}.{total}\n"
                               f"data: {json.dumps(payload, separators=(',', ':'))}\n\n")
                    elif time.monotonic() - wrote >= 1.0:
                        # Keeps proxies open, and ends the thread with its tab.
                        wrote = time.monotonic()
                        yield ": \n\n"

            return Response(stream_with_context(events()), mimetype="text/event-stream",
                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
