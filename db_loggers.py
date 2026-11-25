# db_loggers.py
"""Writers that persist order updates and pivot/symbol state in `botdata`.

    WebsocketOrderUpdateLogger -> botdata.ws_orderupdates  (every order update)
    ForecastLogger             -> botdata.forecast_logs    (pivot/symbol state)
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import sys
import threading
from datetime import datetime
from typing import Any, Dict, Optional

import ist

_now_ist = ist.now

#: How long a shutdown waits for a writer to finish the rows it already holds.
#: These are queued rows against a local Postgres, so a healthy drain is
#: milliseconds; the bound only exists so a wedged writer cannot hold the
#: process open past the 15:30 stop.
DRAIN_TIMEOUT_SECONDS = 5.0

#: Queued in place of a row to tell a writer's loop that it has reached the end
#: of the work it will ever be given. It travels *through* the queue rather than
#: around it, so everything queued before the stop is written first.
_END_OF_STREAM = object()


class _DrainableWriter:
    """Shared stop/drain contract for the writers that own a DB connection.

    Before this, shutdown closed the connection pool while these loops were
    still draining, and every row they had already accepted died against a
    closed pool - 84 `DB insert failed` lines in one 15:30 stop on 2026-08-17,
    and the rows themselves lost between the backup's SELECT and the TRUNCATE.
    Stopping the writers first turns that into an ordered handover: accept
    nothing new, finish what was accepted, then let the pool close.
    """

    TABLE: str

    def __init__(self, db_connector) -> None:
        self.db = db_connector
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue[Any] | None = None
        self._end_task: asyncio.Task[None] | None = None
        self._accept_lock = threading.Lock()
        # Read from the websocket thread on every tick, set from the shutdown
        # thread; `threading.Event` is the primitive that is safe for both.
        self._closing = threading.Event()
        self._drained = threading.Event()
        self._drained.set()  # nothing running yet is trivially drained
        if self.db is not None:
            try:
                self.db.ensure_schema()
            except Exception as exc:
                logging.error("Failed ensuring columns on %s: %s", self.TABLE, exc)

    @property
    def closing(self) -> bool:
        return self._closing.is_set()

    def stop(self, timeout: float = DRAIN_TIMEOUT_SECONDS) -> bool:
        """Stop accepting rows and wait for the queued ones to be written.

        Safe to call from any thread, more than once. Returns True when the
        writer finished; False means it is still busy and the caller should
        expect its remaining rows to be lost.
        """
        with self._accept_lock:
            self._closing.set()
            loop = self._loop
            if loop is None or self._drained.is_set():
                return True
            try:
                loop.call_soon_threadsafe(self._signal_end_of_stream)
            except RuntimeError:
                # The loop is already closed, so nothing is still writing.
                return True
        return self._drained.wait(timeout)

    def _signal_end_of_stream(self) -> None:
        """Append the stop marker after every accepted row, even when full."""
        if self._queue is None:
            self._drained.set()
        elif self._end_task is None:
            self._end_task = asyncio.create_task(self._queue.put(_END_OF_STREAM))

    def _execute(self, sql: str, params: tuple) -> None:
        """One insert. A failed row is logged and dropped; the writer goes on."""
        try:
            self.db.execute(sql, params)
        except Exception as exc:
            logging.error("DB insert failed on %s: %s", self.TABLE, exc)


class _QueuedWriter(_DrainableWriter):
    """A writer that records every message it is handed, one row each, in order.

    Every queued item is a real message that arrived, so there is no dedup and
    no heartbeat: each one gets written. The bounded queue exists only to cap
    memory if the DB stalls; past it the oldest pending row is dropped, with a
    warning, so the newest state is never the casualty.
    """

    QUEUE_MAXSIZE: int
    #: What the full-queue warning calls the thing that fell behind.
    QUEUE_FULL_WARNING: str

    def _hand_off(self, received_at: datetime, *row: Any) -> None:
        """Queue one row. Runs ON this writer's loop (`call_soon_threadsafe`).

        Deliberately does *not* check `closing`. By the time this runs the
        message has already been accepted, and `stop()` schedules the
        end-of-stream marker through the same FIFO - so every hand-off made
        before the stop is queued ahead of it. Dropping here instead lost 20 of
        50 rows in test.
        """
        queue = self._queue
        if queue is None:
            return
        if queue.full():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            logging.warning(
                "%s: %s; dropped the oldest pending row.",
                self.TABLE, self.QUEUE_FULL_WARNING,
            )
        queue.put_nowait((received_at, *row))

    def _insert_row(self, ts: datetime, *row: Any) -> None:
        raise NotImplementedError

    async def start_logging_async(self, queue_maxsize: int | None = None) -> None:
        """Drain the queue and write one row per item, in order."""
        with self._accept_lock:
            if self.closing:
                return
            self._queue = asyncio.Queue(
                maxsize=self.QUEUE_MAXSIZE if queue_maxsize is None else queue_maxsize
            )
            self._drained.clear()
            self._loop = asyncio.get_running_loop()
        written = 0
        try:
            while True:
                item = await self._queue.get()
                if item is _END_OF_STREAM:
                    logging.info(
                        "%s: writer stopped after draining %d queued row(s).",
                        self.TABLE, written,
                    )
                    return
                await asyncio.to_thread(self._insert_row, *item)
                written += 1
        finally:
            self._drained.set()

    def start_logging(self, queue_maxsize: int | None = None) -> None:
        """Blocking entry point: runs the writer on its own event loop."""
        asyncio.run(self.start_logging_async(queue_maxsize))


# =========================================================================== #
#  Order updates -> botdata.ws_orderupdates   (was ws_orderupdate_logger.py)
#
#  `order_logs`/`order_attempts` (main.py's `_process_order_update`) only ever
#  persist a narrow, orderno-matched projection onto a *known* leg, and
#  silently drop an update entirely if its orderno can't be matched to one.
#  This section exists so `botdata.ws_orderupdates` can be trusted as the full
#  record regardless - every message, matched or not, exactly as the broker
#  sent it.
# =========================================================================== #

_STATUS = {
    "NEWACK": "PENDING", "NEW_ACK": "PENDING", "PENDING": "PENDING",
    "NEW": "OPEN", "OPEN": "OPEN",
    "COMPLETE": "COMPLETE", "FILLED": "COMPLETE",
    "REJECTED": "REJECTED",
    "CANCELLED": "CANCELED", "CANCELED": "CANCELED",
    "TRIGGER PENDING": "TRIGGER_PENDING", "TRIGGER_PENDING": "TRIGGER_PENDING",
}


def _normalize_status(raw: str) -> str:
    """The broker's order status in the bot's vocabulary, or INVALID_STATUS_TYPE."""
    return _STATUS.get((raw or "").strip().upper(), "INVALID_STATUS_TYPE")


def _to_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(float(str(value)))
    except Exception:
        return None


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        parsed = float(str(value))
        return parsed if math.isfinite(parsed) else None
    except Exception:
        return None


def order_update_fields(msg: Dict[str, Any]) -> tuple[str, Any, Any, str, Optional[int], Optional[float]]:
    """`(tsym, orderno, exchordid, status, fill_qty, avg_price)` of one "om" message.

    The broker has spelled each of these more than one way. Both the raw
    `ws_orderupdates` record and `main.py`'s order-ledger writer read them
    through here, so the same order's status reads identically in both.
    """
    return (
        str(msg.get("tsym") or msg.get("trading_symbol") or ""),
        msg.get("norenordno") or msg.get("orderno") or msg.get("order_number"),
        msg.get("exchordid") or msg.get("exch_order_id") or msg.get("exchordno"),
        _normalize_status(str(msg.get("status") or msg.get("orderstatus") or "")),
        _to_int(
            msg.get("fillshares")
            or msg.get("filled_qty")
            or msg.get("fillqty")
            or msg.get("fill_qty")
            or msg.get("trdqty")
            or msg.get("trade_qty")
        ),
        _to_float(
            msg.get("avgprc")
            or msg.get("avg_price")
            or msg.get("avgprice")
            or msg.get("averageprice")
        ),
    )


class WebsocketOrderUpdateLogger(_QueuedWriter):
    """Logs every raw order-update message to `botdata.ws_orderupdates`.

    Register `on_order_update` with `LiveWebSocket.add_order_update_listener`.
    It runs synchronously on the websocket's own background thread, so it
    only queues the message and returns; the DB write happens on this logger's own
    asyncio loop. Unlike ticks, order-update messages are each a complete,
    self-contained update (not a delta), so there is no running state to
    merge - every message is queued and written exactly as received.
    """

    TABLE = "botdata.ws_orderupdates"
    #: Order-update volume is a handful to a few dozen messages per trade, so
    #: this is a generous cushion, not a routine limit.
    QUEUE_MAXSIZE = 500
    QUEUE_FULL_WARNING = "order-update queue full (DB writes falling behind)"

    # ---- Websocket-thread hook: hand off, no I/O here ---------------------- #
    def on_order_update(self, message: Dict[str, Any] | None = None) -> None:
        if not message:
            return
        # Copy now, on the caller's thread: `message` is the broker library's
        # own dict and must not be read again later from another thread.
        with self._accept_lock:
            loop = self._loop
            if loop is not None and not self.closing:
                loop.call_soon_threadsafe(self._hand_off, _now_ist(), dict(message))

    # ---- DB write ---------------------------------------------------------- #
    def _insert_row(self, ts: datetime, msg: Dict[str, Any]) -> None:
        if self.db is None:
            return
        tsym, orderno, exchordid, status, fill_qty, avg_price = order_update_fields(msg)
        self._execute(
            f"""
            INSERT INTO {self.TABLE}
                (log_time, msg_type, tsym, orderno, exchordid, status, fill_qty,
                 avg_price, raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb);
            """,
            (
                ts,
                msg.get("t"),
                tsym or None,
                str(orderno) if orderno else None,
                str(exchordid) if exchordid else None,
                status,
                fill_qty,
                avg_price,
                json.dumps(msg),
            ),
        )


# =========================================================================== #
#  Pivot/symbol state -> botdata.forecast_logs        (was print_log.py)
# =========================================================================== #


def _to_ist(ts: str | None) -> Optional[datetime]:
    """Read a feed timestamp as aware IST, or None if it is unusable.

    Candle and pivot times are IST wall clock. Current builds label them
    `+05:30`; older ones published the same digits with a `Z` suffix, so
    `ist.parse_feed_time` reads that `Z` as IST rather than honouring it as UTC
    - honouring it shifted every logged pivot forward by 5h30m, recording a
    15:25 bar as 20:55.
    """
    return ist.parse_feed_time(ts)


def _state_key(
    data: Dict[str, Any], buy_leg: Optional[str], sell_leg: Optional[str]
) -> tuple[Any, ...]:
    """Stable DB-facing identity; display formatting must not create events."""
    return (
        data.get("hh_count"),
        data.get("ll_count"),
        data.get("pivot_type"),
        data.get("pivot_price"),
        data.get("pivot_time"),
        buy_leg,
        sell_leg,
    )


def _is_transient_leg_regression(
    previous: Optional[tuple[Any, ...]], current: tuple[Any, ...]
) -> bool:
    """Ignore same-pivot complete->empty transitions caused by feed deltas."""
    if previous is None or previous[:5] != current[:5]:
        return False
    previous_has_legs = previous[5] is not None and previous[6] is not None
    current_has_no_legs = current[5] is None and current[6] is None
    return previous_has_legs and current_has_no_legs


class ForecastLogger(_DrainableWriter):
    """Writes the strategy's pivot/symbol state to **botdata.forecast_logs**.

    The pivots come from `pivot_provider` - the in-process
    `PivotSnapshotService.latest_pivots` - rather than a loopback request to
    the Flask app's `/realtime_pivots`: that serialised ~13 KB of JSON through
    a TCP socket and parsed it back once a second to read three numbers
    (6.37 ms via loopback vs 0.39 ms direct).
    """

    TABLE = "botdata.forecast_logs"
    #: The state is looked at no more often than this. It changes at a candle close or a
    #: leg rebuild; looking on every tick of every subscription (~20 a second) only
    #: re-read the same pivots, and cost the websocket thread a loop wake-up per tick.
    MIN_INTERVAL_SEC = 1.0

    def __init__(self, symbol_fetcher, db_connector=None, *, pivot_provider) -> None:
        super().__init__(db_connector)
        self.symbol_fetcher = symbol_fetcher
        self._pivot_provider = pivot_provider

        # Set once `start_logging_async` is actually running (see there).
        # `notify_tick` runs on the websocket's own thread and must never
        # touch anything but the event, via the thread-safe primitive below,
        # since everything else in this class assumes it only ever runs on
        # this logger's own asyncio loop.
        self._wake_event: asyncio.Event | None = None
        # A wake-up is already on its way to the loop: later ticks need not send another.
        self._wake_pending = False

    # ---- Websocket-thread hook: wake the loop, do no work here ------------- #
    def notify_tick(self, _tick: Dict[str, Any] | None = None) -> None:
        """Wake the logging loop to check for a pivot/symbol change.

        Called synchronously from `LiveWebSocket`'s own background thread
        (via `add_price_listener`) on every tick. A tick is used purely as
        "something happened, worth a look". Only ever
        the current pivot/symbol state is read once woken, so unlike the tick
        feed there is nothing to lose by coalescing several wake-ups that
        land before the loop gets to them - a plain `Event` is enough, no
        queue needed.
        """
        loop = self._loop
        event = self._wake_event
        if loop is not None and event is not None and not self.closing and not self._wake_pending:
            self._wake_pending = True
            loop.call_soon_threadsafe(event.set)

    def _signal_end_of_stream(self) -> None:
        """Wake the loop so it observes `closing` and returns.

        This writer is state-driven rather than queue-driven - it writes only
        when the pivot/symbol state changed - so there is no backlog to drain,
        only a wait to interrupt before the pool closes underneath it.
        """
        event = self._wake_event
        if event is None:
            self._drained.set()
            return
        event.set()

    # ---- Insert a row with separate buy/sell legs -------------------------- #
    def _insert_row(
        self,
        ts: datetime,
        data: Dict[str, Any],
        buy_leg: str | None,
        sell_leg: str | None,
    ) -> None:
        if self.db is None:
            return
        self._execute(
            f"""
            INSERT INTO {self.TABLE}
                (log_time, hh_count, ll_count,
                 pivot_type, pivot_price, pivot_time, buy_leg, sell_leg)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                ts,
                data.get("hh_count"),
                data.get("ll_count"),
                data.get("pivot_type"),
                data.get("pivot_price"),
                data.get("pivot_time"),
                buy_leg,
                sell_leg,
            ),
        )

    # ---- Helper: read legs from symbol_fetcher & build display ------------- #
    def _legs_from_symbol_fetcher(
        self, forecast_bias: Optional[str] = None
    ) -> tuple[Optional[str], Optional[str], str]:
        """
        Returns (buy_leg, sell_leg, display_text)
        - Prefers new `final_symbols` dict with {'buy','sell'} (a live/pending trade)
        - While flat, falls back to a `(preview)` of what the current forecast
          bias would trade, computed via `preview_legs`. This table
          (`botdata.forecast_logs`) is a display/audit log only - order
          placement always reads `symbol_fetcher.final_symbols` directly, never
          this table - so writing the preview here is safe and is what keeps
          buy_leg/sell_leg populated instead of NULL for most of the session.
        - Falls back to legacy `final_symbol` (stored as buy_leg) if neither applies
        """
        final_symbols = getattr(self.symbol_fetcher, "final_symbols", None)
        if isinstance(final_symbols, dict):
            buy = final_symbols.get("buy")
            sell = final_symbols.get("sell")
            return buy, sell, f"Buy: {buy or '—'} | Sell: {sell or '—'}"

        preview_fn = getattr(self.symbol_fetcher, "preview_legs", None)
        if callable(preview_fn):
            try:
                preview = preview_fn(forecast_bias)
            except Exception as exc:
                logging.debug("Symbol preview failed: %s", exc)
                preview = None
            if preview:
                buy = preview.get("buy")
                sell = preview.get("sell")
                return buy, sell, f"Buy: {buy or '—'} | Sell: {sell or '—'} (preview, no position)"

        # Legacy single symbol path
        single = getattr(self.symbol_fetcher, "final_symbol", None)
        return single, None, f"Buy: {single or '—'} | Sell: —"

    # ------------------------------------------------------------------- #
    #  🔄 Event-driven logging loop                                       #
    # ------------------------------------------------------------------- #
    async def start_logging_async(self) -> None:
        """Write a DB row only when the pivot/symbol state actually changed.

        Woken by `notify_tick` on every websocket tick instead of polling on
        a fixed timer, so a change is caught as soon as the feed gives us a
        reason to look - not up to a second late. No price is read or stored
        here; this table is purely pivot/symbol state, so it is
        gated on that alone, and an exact repeat writes nothing.
        """
        with self._accept_lock:
            if self.closing:
                return
            self._wake_event = asyncio.Event()
            self._drained.clear()
            self._loop = asyncio.get_running_loop()
        try:
            await self._log_until_closed()
        finally:
            self._drained.set()

    async def _log_until_closed(self) -> None:
        last_written: tuple[Any, ...] | None = None
        while True:
            await self._wake_event.wait()
            # In this order, on the loop: a tick arriving from here on schedules a new wake-up.
            self._wake_pending = False
            self._wake_event.clear()
            if self.closing:
                logging.info("%s: writer stopped.", self.TABLE)
                return

            now = _now_ist()
            text, data = await asyncio.to_thread(self._fetch)
            buy_leg, sell_leg, legs_disp = await asyncio.to_thread(
                self._legs_from_symbol_fetcher, data.get("pivot_bias")
            )
            if sys.stdout.isatty():
                print(f"\r{ist.format_log(now)} | {text} | Updated Symbols: {legs_disp}",
                      end="", flush=True)

            state = _state_key(data, buy_leg, sell_leg)
            if not _is_transient_leg_regression(last_written, state) and state != last_written:
                last_written = state
                # off-load blocking DB write
                await asyncio.to_thread(self._insert_row, now, data, buy_leg, sell_leg)
            # Ticks meanwhile are coalesced into one wake-up; a stop still ends the next wait.
            await asyncio.sleep(self.MIN_INTERVAL_SEC)

    def start_logging(self) -> None:
        """Blocking entry point: runs the writer on its own event loop."""
        asyncio.run(self.start_logging_async())

    def _fetch(self) -> tuple[str, Dict[str, Any]]:
        try:
            piv = self._pivot_provider() or {}
        except Exception as exc:
            logging.error(f"Failed to fetch forecast data: {exc}")
            return "Error fetching forecast data.", {}

        hh_cnt = len(piv.get("hh_points", []))
        ll_cnt = len(piv.get("ll_points", []))

        # combine HH and LL into a single list of points
        points = (
            [{**p, "type": "HH", "price": p.get("high")} for p in piv.get("hh_points", [])]
            + [{**p, "type": "LL", "price": p.get("low")} for p in piv.get("ll_points", [])]
        )
        # `type` is a *chart* label; `bias` is the directional label `GetSymbol`
        # reads, and the two are not the same alphabet - a long is drawn low
        # and selected with 'HH'. The marker carries its own bias, so the leg
        # preview below never has to guess which convention it is holding.

        # Sort by the actual instant, not the raw string. The fallback has to
        # be aware too: `_to_ist` returns aware IST, and comparing that against
        # a naive `datetime.min` raises TypeError mid-sort.
        undated = datetime.min.replace(tzinfo=ist.IST)
        points.sort(key=lambda x: _to_ist(x.get("time")) or undated)

        counts = f"F-HH Count: {hh_cnt}, F-LL Count: {ll_cnt}"
        if not points:
            return f"{counts} | Last Forecast Pivot: None", {
                "hh_count": hh_cnt,
                "ll_count": ll_cnt,
                "pivot_type": None,
                "pivot_bias": None,
                "pivot_price": None,
                "pivot_time": None,
            }

        last = points[-1]
        p_type, p_price = last["type"], last["price"]
        p_time_dt = _to_ist(last.get("time"))
        p_time_str = ist.format_log(p_time_dt) if p_time_dt else str(last.get("time"))
        disp = (
            f"{counts} | Last Forecast Pivot: {p_type}, Price: {p_price:.2f}, "
            f"Time: {p_time_str}"
        )
        return disp, {
            "hh_count": hh_cnt,
            "ll_count": ll_cnt,
            "pivot_type": p_type,
            "pivot_bias": last.get("bias") or p_type,
            "pivot_price": p_price,
            "pivot_time": p_time_dt,
        }
