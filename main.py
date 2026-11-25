from __future__ import annotations

import asyncio
import atexit
import logging
import os
import queue
import signal
import sys
import threading
import time
from pathlib import Path
from types import FrameType
from typing import Any, Callable, Optional

import waitress

# ──────────────────────────────────────────────────────────────────────────────
#  TRADE MODE: change here only ("TEST" or "LIVE")
# ──────────────────────────────────────────────────────────────────────────────
TRADE_MODE = "TEST"  # <<< Change to "LIVE" when you're ready
HHLL_REVERSAL_TICKS = 16  # Fast confirmation: 16 ticks x 0.05 = 0.80 index points
HHLL_CHARGES_INR = 300.0  # Round-trip statutory and broker charges (Rs 300 INR)
HHLL_MIN_LEG = 25.0       # Big leg length threshold for profitability after 300 charges

import ist
from shoonya_api import ShoonyaApiPy
import instruments
from getdata import GetSymbol
from live_server import PivotSnapshotService, create_app
from live_websocket import LiveWebSocket
from manual_desk import ManualDesk
import hhll_store
import db_loggers
from db_loggers import (
    ForecastLogger,
    WebsocketOrderUpdateLogger,
    order_update_fields,
)
from data_handler import DataHandler
from db_connector import DatabaseNotFoundError, TimescaleDBConnector
from shoonya_login import STATE_DIR, TransientLoginError, _InterProcessLock
from database_backup import backup_and_truncate_tables, has_unresolved_orders, tables_to_truncate
from strategy_logic import MarketData, StrategyEngine, bias_tag
from config import TRADE_SETTINGS

from trade_manager import LEG_COLUMNS, ORDER_UPDATES, TradeManagerLive, TradeManagerTest
from hhll_algo import HHLLPaperAlgo, attach as hhll_attach
from hhll_live import ShoonyaFutureRouter, assert_flat, resolve_front_future

# ──────────────────────────────────────────────────────────────────────────────
#  Logging configuration: use script’s dir so logs/ is always created there
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"


def configure_logging() -> None:
    """Point the root logger at today's bot log.

    Called from `main()` rather than at import time. `market_hours_runner`
    launches this file as a subprocess, so nothing in production imports it -
    but the checks do, and at import time this would append to the *live* bot
    log and seize root logging for the rest of the test run.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name != "nt":
        LOG_DIR.chmod(0o700)
    log_file = LOG_DIR / f"bot_{ist.date_key()}.txt"

    formatter = ist.ISTFormatter("%(asctime)s | %(levelname)s | %(message)s")

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)

    # File handler (append mode)
    file_handler = logging.FileHandler(str(log_file), mode="a", encoding="utf-8")
    if os.name != "nt":
        log_file.chmod(0o600)
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(formatter)

    logging.basicConfig(
        level=logging.WARNING,
        handlers=[console_handler, file_handler],
        force=True,
    )
    # websocket-client repeats the same exception after invoking our error
    # callback; that callback owns the bounded, actionable outage record.
    logging.getLogger("websocket").setLevel(logging.CRITICAL)
# ──────────────────────────────────────────────────────────────────────────────


# Order-update volume is a handful of messages per trade (PENDING/OPEN/
# COMPLETE per leg), so this bound is never meant to actually be reached in
# normal operation - it exists only as a last-resort cap against unbounded
# memory growth if something is deeply wrong (e.g. a message storm).
_ORDER_UPDATE_QUEUE_MAXSIZE = 10_000

#: Queued behind the last real update to tell the order-update worker it has
#: reached the end of its work. Travels through the queue so everything already
#: accepted is written first. Mirrors `db_loggers._END_OF_STREAM`.
_END_OF_ORDER_UPDATES = object()


def _make_order_update_logger(db: TimescaleDBConnector):
    """
    Strict orderno-based order-writer for botdata.order_logs.

    • ENTRY:  norenordno == buy_orderno  → state1/time1/buy_exchordid
              norenordno == sell_orderno → state2/time2/sell_exchordid
    • EXIT:   norenordno == buy_exit_orderno  → buy_exit_state/time1_exit/buy_exit_exchordid
              norenordno == sell_exit_orderno → sell_exit_state/time2_exit/sell_exit_exchordid

    If a broker update arrives before the manager binds its order number to
    order_logs, use the persisted order_attempts record to identify the exact
    row and leg. An unrelated account order must never claim an empty slot.

    Threading: the returned callable is registered as an order-update listener
    on `LiveWebSocket`, which calls it synchronously on its own single message-
    read thread (see `event_handler_order_update`). The actual DB work below
    does 6-9 unwrapped `db.execute()` round trips per message, which would
    otherwise stall that thread - and therefore stall price-tick processing
    too, since the same thread reads both - for the duration. So the returned
    callable only enqueues; a single dedicated worker thread drains the queue
    and does the real work, strictly FIFO (order-state transitions must apply
    in arrival order) with exactly one consumer.
    """

    db.ensure_schema()

    def _attach_ordno_if_missing(*, tsym: str, ordno: str) -> None:
        def find_attempt():
            return db.execute(
                """SELECT order_log_id, phase, leg FROM botdata.order_attempts
                   WHERE orderno=%s LIMIT 1;""",
                (ordno,), fetch=True,
            )

        attempts = find_attempt()
        if not attempts and tsym:
            # An update can precede the HTTP acknowledgement. The intent row
            # exists before submission; wait briefly only for that exact race.
            pending = db.execute(
                """SELECT 1 FROM botdata.order_attempts
                   WHERE symbol=%s AND orderno IS NULL
                     AND status IN ('INTENT','UNKNOWN')
                     AND created_at > NOW() - INTERVAL '2 minutes'
                   LIMIT 1;""",
                (tsym,), fetch=True,
            )
            if pending:
                for _ in range(2):
                    time.sleep(0.2)
                    attempts = find_attempt()
                    if attempts:
                        break
        if not attempts:
            return
        attempt = attempts[0]
        row_id = attempt.get("order_log_id")
        col = {
            ("ENTRY", "buy"): "buy_orderno",
            ("ENTRY", "sell"): "sell_orderno",
            ("EXIT", "buy"): "buy_exit_orderno",
            ("EXIT", "sell"): "sell_exit_orderno",
        }.get((attempt.get("phase"), attempt.get("leg")))
        if row_id is None or col is None:
            return
        db.execute(
            f"UPDATE botdata.order_logs SET {col}=COALESCE({col}, %s) "
            f"WHERE id=%s AND ({col} IS NULL OR {col}=%s);",
            (ordno, row_id, ordno),
        )

    def _process_order_update(message: dict, received_at) -> None:
        """
        WS → DB bridge (strict orderno matching).

        Runs on the dedicated worker thread below, never on the websocket's
        own thread - see the threading note in this function's docstring.
        """
        msg = message if isinstance(message, dict) else {}
        tsym, ordno, xoid, status, fill_qty, avg_price = order_update_fields(msg)

        if not ordno:
            return
        if status == "INVALID_STATUS_TYPE":
            logging.warning("Ignoring unsupported broker order status %r for %s",
                            msg.get("status") or msg.get("orderstatus"), ordno)
            return
        ordno = str(ordno)
        xoid = str(xoid) if xoid else None

        # Ensure the ordno is attached somewhere (handles races)
        row = db.execute(
            """
            SELECT id FROM botdata.order_logs
            WHERE buy_orderno=%s OR sell_orderno=%s
               OR buy_exit_orderno=%s OR sell_exit_orderno=%s
            ORDER BY id DESC LIMIT 1;
            """,
            (ordno, ordno, ordno, ordno),
            fetch=True,
        )
        if not row:
            _attach_ordno_if_missing(tsym=tsym, ordno=ordno)

        # ENTRY BUY, ENTRY SELL, EXIT BUY, EXIT SELL - whichever leg carries
        # this order number. A terminal state is never overwritten.
        for is_exit in (False, True):
            for leg in ("buy", "sell"):
                cols = LEG_COLUMNS[leg, is_exit]
                db.execute(
                    f"""
                    UPDATE botdata.order_logs
                       SET {cols['state']}=%s,
                           {cols['time']}=%s,
                           {cols['exchordid']} = COALESCE({cols['exchordid']}, %s),
                           {cols['fill_qty']} = COALESCE(%s, {cols['fill_qty']}),
                           {cols['avg_price']} = COALESCE(%s, {cols['avg_price']})
                     WHERE {cols['orderno']}=%s
                       AND UPPER(COALESCE({cols['state']},'')) NOT IN ('COMPLETE','REJECTED','CANCELED','CANCELLED');
                    """,
                    (status, received_at, xoid, fill_qty, avg_price, ordno),
                )

        # Immutable attempt ledger: only broker-owned outcome fields change.
        db.execute(
            """
            UPDATE botdata.order_attempts
               SET status=%s, updated_at=%s,
                   exchordid=COALESCE(exchordid,%s),
                   filled_qty=COALESCE(%s,filled_qty),
                   avg_price=COALESCE(%s,avg_price)
             WHERE orderno=%s
               AND UPPER(COALESCE(status,'')) NOT IN ('COMPLETE','REJECTED','CANCELED','CANCELLED');
            """,
            (status, received_at, xoid, fill_qty, avg_price, ordno),
        )

    # One consumer, strict FIFO: PENDING -> OPEN -> COMPLETE for the same
    # order must be applied in arrival order, which a single dedicated
    # worker thread draining one queue gives for free.
    order_update_queue: "queue.Queue[Any]" = queue.Queue(maxsize=_ORDER_UPDATE_QUEUE_MAXSIZE)
    closing = threading.Event()
    drained = threading.Event()
    failed = threading.Event()
    accept_lock = threading.Lock()

    def _order_update_worker() -> None:
        written = 0
        try:
            while True:
                item = order_update_queue.get()
                if item is _END_OF_ORDER_UPDATES:
                    logging.info(
                        "order_logs writer stopped after draining %d update(s).",
                        written,
                    )
                    return
                for attempt in range(3):
                    try:
                        _process_order_update(*item)
                        written += 1
                        ORDER_UPDATES.notify()   # wake the leg waiters now, not on their next poll
                        break
                    except Exception:
                        if attempt == 2:
                            raise
                        time.sleep(0.2)
        except Exception:
            failed.set()
            logging.critical("Order-update writer stopped after repeated database errors", exc_info=True)
        finally:
            drained.set()

    worker_thread = threading.Thread(
        target=_order_update_worker, daemon=True, name="order-update-writer"
    )
    worker_thread.start()

    def _enqueue_order_update(message: dict) -> None:
        """Runs on the websocket's own thread - must stay cheap and never do I/O."""
        msg = message if isinstance(message, dict) else {}
        with accept_lock:
            if closing.is_set() or failed.is_set():
                return
            try:
                order_update_queue.put_nowait((dict(msg), ist.now()))
            except queue.Full:
                # Never block the websocket read thread behind database I/O.
                failed.set()
                logging.critical("Order-update queue full (%d pending); stopping trading.",
                                 order_update_queue.qsize())

    def _stop(timeout: float = db_loggers.DRAIN_TIMEOUT_SECONDS) -> bool:
        """Finish the queued updates before the pool closes underneath them.

        Same contract as `db_loggers._DrainableWriter.stop`, so `main`'s
        shutdown can treat every writer the same way. This one matters most:
        these rows are the order ledger, not diagnostics.
        """
        with accept_lock:
            if not closing.is_set():
                closing.set()
                if not drained.is_set():
                    try:
                        order_update_queue.put(_END_OF_ORDER_UPDATES, timeout=timeout)
                    except queue.Full:
                        return False
        return drained.wait(timeout) and not failed.is_set()

    _enqueue_order_update.stop = _stop  # type: ignore[attr-defined]
    _enqueue_order_update.thread = worker_thread  # type: ignore[attr-defined]
    _enqueue_order_update.failed = failed  # type: ignore[attr-defined]
    _enqueue_order_update.TABLE = "botdata.order_logs"  # type: ignore[attr-defined]
    return _enqueue_order_update


async def _update_symbol_periodically(name: str, fetcher: GetSymbol) -> None:
    """Keep one index's legs current despite delays on another index."""
    while True:
        try:
            await asyncio.to_thread(fetcher.update_symbol)
        except Exception as exc:  # noqa: BLE001
            logging.warning("[%s] symbol update failed: %s", name, exc)
        await asyncio.sleep(1)


async def _watch_trade_thread(
    thread: threading.Thread, shutting_down: Callable[[], bool],
    *, label: str = "Trade manager thread", failed: threading.Event | None = None,
) -> None:
    while not shutting_down():
        if not thread.is_alive() or (failed is not None and failed.is_set()):
            logging.critical("%s exited unexpectedly", label)
            raise RuntimeError(f"{label} exited unexpectedly")
        await asyncio.sleep(1)


async def _bootstrap() -> None:
    mode = (TRADE_MODE or "").strip().upper()
    if mode not in {"TEST", "LIVE"}:
        raise SystemExit(f"Invalid TRADE_MODE: {TRADE_MODE!r} (use 'TEST' or 'LIVE')")
    logging.info(f"🚀 Starting in TRADE_MODE = {mode}")

    api = ShoonyaApiPy()
    if not (resp := api.login_and_search()) or resp.get("stat") != "Ok":
        # Surface the reason: a permanent failure needs a human, a transient one
        # will clear on its own, and the runner restarts us either way.
        detail = resp.get("emsg") if isinstance(resp, dict) else resp
        kind = "PERMANENT" if isinstance(resp, dict) and resp.get("permanent") else "transient"
        raise SystemExit(f"Shoonya login failed ({kind}) – aborting. {detail}")
    if not getattr(api, "websocket_login_status", False):
        ws_ret = api.ensure_websocket_session()
        if not ws_ret or ws_ret.get("stat") != "Ok":
            detail = ws_ret.get("emsg") if isinstance(ws_ret, dict) else ws_ret
            raise SystemExit(f"Shoonya WebSocket session login failed – aborting. {detail}")

    # WebSocket first
    live_ws = LiveWebSocket(api)

    # One leg builder per v3 index. Each resolves its own spot token,
    # builds its own tradingsymbols on its own exchange, and refuses to publish
    # a leg the broker does not list - so an index whose expiry calendar has
    # moved simply stops competing instead of trading a contract that does not
    # exist.
    #
    # Each leg builder reads only a fresh price for its own subscription key:
    # its tick, or its REST quote while the ticks are out (see LiveWebSocket).
    # The keys are known only after spot-token resolution, so the getters are
    # late-bound through `spot_keys`.
    spot_keys: dict[str, str] = {}

    def _index_fresh_price(index_name: str):
        def getter():
            key = spot_keys.get(index_name)
            return live_ws.get_fresh_price_for(key) if key else None
        return getter

    fresh_prices = {name: _index_fresh_price(name) for name in instruments.NAMES}

    symbol_fetchers: dict[str, GetSymbol] = {}
    spot_tokens: dict[str, tuple[str, str]] = {}
    for _name in instruments.NAMES:
        _inst = instruments.get(_name)
        # Expiry weekdays come from `config.INDEX_EXPIRY` via the instrument.
        _fetcher = GetSymbol(
            api,
            fresh_prices[_name],
            instrument=_inst,
            option_roll_days_before_expiry=TRADE_SETTINGS.option_roll_days_before_expiry,
            future_roll_days_before_expiry=TRADE_SETTINGS.future_roll_days_before_expiry,
            expiry_holidays=TRADE_SETTINGS.expiry_holidays,
        )
        try:
            _exch, _tok = _fetcher.fetch_exch_tok()
        except Exception as exc:  # noqa: BLE001 - a broker that cannot name one
            # index makes the portfolio date unusable.
            logging.error(
                "⚠️  Could not resolve %s's spot feed: %s. That index will not "
                "participate in v3's portfolio.",
                _name, exc,
            )
            continue
        spot_tokens[_name] = (_exch, _tok)
        spot_keys[_name] = instruments.subscription_key(_inst, _tok)
        symbol_fetchers[_name] = _fetcher
    missing_feeds = [name for name in instruments.NAMES if name not in symbol_fetchers]
    if missing_feeds:
        raise SystemExit(
            "v3 requires every portfolio feed; unresolved: " + ", ".join(missing_feeds)
        )

    # The chart index drives the single-symbol routes (`/data`, the candle
    # chart) and is the fallback for a trade row that predates the book.
    chart_index = (instruments.CHART_INDEX if instruments.CHART_INDEX in symbol_fetchers
                   else next(iter(symbol_fetchers)))
    symbol_fetcher = symbol_fetchers[chart_index]
    exch, tok = spot_tokens[chart_index]

    # Headroom for the trade manager, DB writers, and dashboard routes.
    db_connector = TimescaleDBConnector(minconn=2, maxconn=12)
    await asyncio.to_thread(db_connector.connect)

    # ── Backup & truncate on any clean exit ───────────────────────────────────
    backup_done: bool = False

    # Every writer that owns rows destined for `db_connector`. Populated once
    # the loggers are constructed below; `_backup_and_clear` is defined first
    # because it has to be registered with `atexit` before anything can fail.
    db_writers: list = []

    def _stop_db_writers() -> bool:
        """Let the writers finish their queued rows before the pool closes.

        Without this the pool closed underneath them and every row they had
        already accepted died against it - 84 `DB insert failed` lines in the
        2026-08-17 15:30 stop alone, and those rows lost from the backup too,
        because they were still in flight when the SELECT ran.
        """
        drained = True
        for writer in db_writers:
            name = getattr(writer, "TABLE", None) or type(writer).__name__
            try:
                if not writer.stop():
                    drained = False
                    logging.warning(
                        "%s did not finish writing within %.0fs; its remaining "
                        "queued rows are lost.",
                        name, db_loggers.DRAIN_TIMEOUT_SECONDS,
                    )
            except Exception as exc:  # noqa: BLE001 - shutdown must continue
                drained = False
                logging.error("Failed stopping %s: %s", name, exc)
        return drained

    # Declared before the exit hook that reads it: a startup that fails before
    # the pipeline is built still runs `_backup_and_clear`, and an unbound name
    # there would replace the real error with a NameError.
    tm: TradeManagerTest | TradeManagerLive | None = None
    trade_thread: threading.Thread | None = None
    manual_desk: ManualDesk | None = None

    def _stop_trader() -> bool:
        # The manual desk first: it owns its own pool and writer, and a desk
        # that will not stop must keep the algo's resume tables too.
        if manual_desk is not None and not manual_desk.stop():
            logging.critical("Manual desk did not stop; keeping resume tables intact.")
            return False
        if trade_thread is None or not trade_thread.is_alive():
            return True
        if tm is not None:
            tm.stop()
        trade_thread.join(timeout=90)
        if trade_thread.is_alive():
            logging.critical("Trade manager did not stop; keeping resume tables intact.")
            return False
        return True

    def _backup_and_clear(*, trader_stopped: bool | None = None) -> None:
        nonlocal backup_done
        if backup_done:
            return
        backup_done = True
        if trader_stopped is None:
            trader_stopped = _stop_trader()
        if not trader_stopped:
            logging.critical("Skipping backup and truncate while the trade manager is still active.")
            return
        # Before anything reads or truncates: the writers must be finished, or
        # the backup races rows that are still being written.
        if not _stop_db_writers():
            logging.critical("Skipping backup and truncate because a DB writer did not drain.")
            return
        date_str = ist.date_key()
        backup_dir = BASE_DIR / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

        try:
            # See `tables_to_truncate` for why each condition retains.
            open_rows = db_connector.execute(
                """SELECT 1 FROM botdata.trade_logs
                   WHERE is_in_trade = TRUE LIMIT 1;""",
                fetch=True,
            )
            unresolved_orders = has_unresolved_orders(db_connector)
            retain_trade_tables = bool(open_rows) or unresolved_orders
            tables = tables_to_truncate(
                open_trade=bool(open_rows),
                unresolved_orders=unresolved_orders,
            )
            reports = backup_and_truncate_tables(
                db_connector, tables, backup_dir, date_str
            )
            for table, row_count, path in reports:
                if path is None:
                    logging.info("No rows in %s to back up", table)
                else:
                    logging.info(
                        "Backed up and validated %d rows from %s in %s",
                        row_count, table, path,
                    )

            if retain_trade_tables:
                logging.warning(
                    "⚠️  %s — truncated forecast_logs/ws_orderupdates "
                    "only. trade_logs/order_logs/order_attempts kept so the "
                    "position can be resumed.",
                    "Open trade present"
                    if open_rows
                    else "Unresolved order lifecycle present",
                )
            else:
                logging.info(
                    "✅ Truncated forecast_logs, ws_orderupdates, "
                    "trade_logs, order_logs, order_attempts on exit"
                )
        except Exception as exc:
            logging.error("Error during backup + truncate: %s", exc)
        finally:
            db_connector.close()

    atexit.register(_backup_and_clear)

    # ── Graceful shutdown (Ctrl-C / SIGTERM) ──────────────────────────────────
    shutting_down = False

    async def _finish_shutdown() -> None:
        trader_stopped = await asyncio.to_thread(_stop_trader)
        try:
            await live_ws.stop()
        except Exception as exc:
            logging.error("WebSocket shutdown failed: %s", exc)
        try:
            _backup_and_clear(trader_stopped=trader_stopped)
        finally:
            for task in asyncio.all_tasks():
                if task is not asyncio.current_task():
                    task.cancel()

    def graceful_shutdown(signum: int, frame: Optional[FrameType]) -> None:
        nonlocal shutting_down
        if shutting_down:
            return
        shutting_down = True
        logging.info("🔴 Received signal %s — shutting down gracefully…", signum)
        asyncio.create_task(_finish_shutdown())

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, graceful_shutdown)

    # ── Live data pipeline ────────────────────────────────────────────────────
    # One 5-minute broker feed per index, for the chart.
    data_handlers: dict[str, DataHandler] = {}
    for _name, (_exch, _tok) in spot_tokens.items():
        _handler = DataHandler(api, _exch, _tok)
        _handler.start_live_updates()
        data_handlers[_name] = _handler
    data_handler = data_handlers[chart_index]

    # The frozen v3 strategy and its single portfolio slot. It keeps its own
    # candles - the history in backups/, every new one from Shoonya, through
    # the spot feeds resolved above - and is refreshed in the
    # background after every candle close. Everything downstream - the chart's
    # markers, the option legs, and every entry and exit - reads from this one
    # engine, so the bot cannot show one thing and trade another.
    strategy_service = StrategyEngine(source=MarketData(api, spot_tokens))
    strategy_service.start()
    # Registered after `_backup_and_clear`, so it runs first: a refresh in
    # progress finishes writing v3's data before the process exits.
    atexit.register(strategy_service.stop)

    # One snapshot service per index, so the console can chart any of the six
    # and see that index's own bars and its own gate verdicts. The
    # chart index's service is the one the forecast logger shares, so the log
    # and the default chart still read the same signals the strategy trades on.
    pivot_services = {
        # The fresh getter, not the plain one: the console scores the gates
        # against the tick on the bar still forming, and a stale price there
        # would show an operator a reading the market has already left.
        name: PivotSnapshotService(handler, strategy_service.view(name), fresh_prices[name])
        for name, handler in data_handlers.items()
    }
    pivot_service = pivot_services[chart_index]

    def _strategy_bias_getter(index_name: str):
        """The directional label `GetSymbol` should build option legs for.

        v3 is long-only, so the answer is only ever LONG or "leave the legs
        alone". Legs are asked for from the first carry candle (14:45) through
        the night fill window (15:11-15:14), because `GetSymbol` needs lead
        time and an entry whose legs are not ready at the fill moment is an
        entry that does not happen.

        Asked per index, and answered for *every* index that could still win the
        slot rather than only the one that eventually does: which index wins is
        not known until the signal bar closes, and legs that start building
        then are legs that arrive after the fill window has passed.
        """
        def getter() -> Optional[str]:
            try:
                return (bias_tag("LONG")
                        if strategy_service.wants_long_legs(index_name) else None)
            except Exception as exc:  # noqa: BLE001
                logging.warning("Strategy bias getter for %s failed: %s", index_name, exc)
                return None
        return getter

    for _name, _fetcher in symbol_fetchers.items():
        _fetcher.set_pivot_type_getter(_strategy_bias_getter(_name))

    # The manual desk: one-click synthetic longs and shorts on its own ledger, pool,
    # leg builders and broker product (see manual_desk.py). It reads the same
    # spot feeds; nothing of the algo's is handed to it. Its ticks, the HH/LL engine's
    # pivots and settings, and the HH/LL algo's ledger live in the `hhll` schema, on a
    # pool of their own (hhll_store).
    manual_desk = ManualDesk(
        api=api, mode=mode, spot_tokens=spot_tokens, spot_keys=spot_keys,
        price_getters=fresh_prices,
        tick_store=hhll_store.Store(TimescaleDBConnector(minconn=1, maxconn=4, auto_provision=False)),
        # Read only: the console's `/books` shows v3's night trades beside the desk's day trades.
        night_db=db_connector,
    )
    await asyncio.to_thread(manual_desk.connect)

    app = create_app(
        api,
        exch,
        tok,
        live_ws.get_fresh_price,
        data_handler,
        db_connector,
        trade_mode=mode,
        pivot_service=pivot_service,
        pivot_services=pivot_services,
        price_getters=fresh_prices,
        chart_index=chart_index,
        symbol_fetchers=symbol_fetchers,
        # In the strategy's order, so the console's feed strip matches its legs.
        feed_health=lambda: live_ws.health(
            {name: spot_keys[name] for name in instruments.NAMES if name in spot_keys}),
        add_price_listener=live_ws.add_price_listener,
        price_keys=spot_keys,
    )
    manual_desk.register_routes(app)
    live_ws.add_price_listener(manual_desk.tape.on_tick)

    # The HH/LL desk (day trades, 09:15-15:10) and the v3 night trade never
    # overlap. v3 fills its night trade only once the desk is flat, and the desk
    # opens no new trade while v3 holds it (until the next 09:15 open's exit).
    # Exits are never held back by either.
    def _v3_busy() -> Optional[str]:
        reason = None if tm is None else tm.busy_reason()
        return None if reason is None else f"v3 night trade is {reason}"

    strategy_service.set_entry_blocker(manual_desk.busy_reason)
    manual_desk.entry_blocker = _v3_busy
    # Waitress, not Flask's dev server: that one closes the connection after
    # every response, so a phone on Tailscale paid a fresh TCP handshake for
    # each of the console's several polls a second. Every open price/tick
    # stream holds a thread, hence the headroom.
    threading.Thread(
        target=lambda: waitress.serve(app, host="127.0.0.1", port=5000, threads=32,
                                      channel_timeout=60, _quiet=True),
        daemon=True,
    ).start()

    # Subscribe after server/data pipeline is up
    order_update_writer = _make_order_update_logger(db_connector)
    db_writers.append(order_update_writer)
    # The chart index is first, so the primary fresh-price getter serves the
    # same instrument as the chart.
    _subscriptions = [spot_keys[chart_index]] + [
        key for name, key in spot_keys.items() if name != chart_index
    ]
    # HH/LL algo (see below): its signal and its orders run on the front-month
    # NIFTY future, so that contract's ticks are subscribed too.
    hhll_contract = hhll_router = None
    try:
        hhll_contract = resolve_front_future(api)
        if mode == "LIVE":
            assert_flat(api, hhll_contract.tsym)
            hhll_router = ShoonyaFutureRouter(api, hhll_contract)
        _subscriptions.append(f"{hhll_contract.exchange}|{hhll_contract.token}")
    except Exception as exc:  # the bot itself keeps running without the algo
        hhll_contract = hhll_router = None
        logging.critical("HH/LL algo NOT started: %s", exc)
    logging.info("Subscribing spot feeds: %s", ", ".join(
        f"{name}={spot_keys[name]}" for name in spot_keys))
    # Only the index spot feeds are read as fresh prices (the legs, the algo's
    # entries, the console); the HH/LL future trades on its ticks alone, so it
    # is never quoted over REST.
    live_ws.start(_subscriptions, order_update_handler=order_update_writer,
                  rest_keys=list(spot_keys.values()))

    # HH/LL algo: short on a confirmed HH, long on a confirmed LL, one lot of the
    # front-month NIFTY future. TEST fills on the future's ticks at the
    # confirming price; LIVE sends LMT IOC orders (hhll_live) and halts on any
    # failed or unknown order.
    if hhll_contract is not None:
        hhll_key = f"{hhll_contract.exchange}|{hhll_contract.token}"
        hhll_algo = HHLLPaperAlgo(
            lot_size=hhll_contract.lot,
            tick_size=float(hhll_contract.tick),
            reversal_ticks=HHLL_REVERSAL_TICKS,
            min_leg=HHLL_MIN_LEG,
            charges=HHLL_CHARGES_INR,
            instrument="future",
            mode=mode,
            router=hhll_router,
            sink=manual_desk.tick_store.writer.put_ledger,
        )
        hhll_attach(live_ws, hhll_algo, key=hhll_key)
        logging.info("HH/LL algo (%s) on %s %s", mode, hhll_contract.tsym, hhll_key)

    forecast_logger = ForecastLogger(
        symbol_fetcher,
        db_connector,
        pivot_provider=pivot_service.latest_pivots,
    )
    # Event-driven: a tick wakes the logger's own loop instead of it polling
    # on a fixed timer. `notify_tick` is thread-safe (see db_loggers.py) since
    # it fires on the websocket's own background thread.
    live_ws.add_price_listener(forecast_logger.notify_tick)
    threading.Thread(target=forecast_logger.start_logging, daemon=True).start()
    db_writers.append(forecast_logger)

    # The complete, unfiltered raw order-update ("om") record - every message,
    # matched to a known leg or not. order_logs/order_attempts only keep a
    # narrow orderno-matched projection and silently drop anything that
    # doesn't match; this is the full record. See db_loggers.py.
    ws_order_logger = WebsocketOrderUpdateLogger(db_connector)
    live_ws.add_order_update_listener(ws_order_logger.on_order_update)
    threading.Thread(target=ws_order_logger.start_logging, daemon=True).start()
    db_writers.append(ws_order_logger)

    # ── Trade logic (mode-based) ──────────────────────────────────────────────
    manager_type = TradeManagerTest if mode == "TEST" else TradeManagerLive
    tm = manager_type(
        api=api,
        db_connector=db_connector,
        symbol_fetcher=symbol_fetcher,
        symbol_fetchers=symbol_fetchers,
        index_live_price_getters=fresh_prices,
        live_price_getter=live_ws.get_fresh_price,
        fresh_price_getter=live_ws.get_fresh_price,
        strategy_service=strategy_service,
    )
    trade_thread = threading.Thread(target=tm.start, daemon=True)
    trade_thread.start()
    logging.info("🧪 TEST mode: TradeManagerTest started (paper)." if mode == "TEST"
                 else "💡 LIVE mode: TradeManagerLive started.")

    # The desk's own order-update writer, on its own ledger, listening before
    # the desk can place its first order.
    manual_order_writer = _make_order_update_logger(manual_desk.ledger)
    live_ws.add_order_update_listener(manual_order_writer)
    manual_desk.start(order_writer=manual_order_writer)

    async with asyncio.TaskGroup() as tg:
        for name, fetcher in symbol_fetchers.items():
            tg.create_task(_update_symbol_periodically(name, fetcher))
        tg.create_task(_watch_trade_thread(trade_thread, lambda: shutting_down))
        tg.create_task(_watch_trade_thread(
            order_update_writer.thread, lambda: shutting_down,
            label="Order-update writer", failed=order_update_writer.failed,
        ))
        tg.create_task(_watch_trade_thread(
            manual_desk.thread, lambda: shutting_down, label="Manual desk thread"))
        tg.create_task(_watch_trade_thread(
            manual_order_writer.thread, lambda: shutting_down,
            label="Manual order-update writer", failed=manual_order_writer.failed,
        ))


def main() -> None:
    # Keep the lock until atexit has finished backing up. A second copy must
    # never write to, or truncate, the same order ledger concurrently.
    instance_lock = _InterProcessLock(STATE_DIR / "lune_instance.lock", timeout=0)
    try:
        instance_lock.__enter__()
    except TransientLoginError as exc:
        raise SystemExit("Another Lune bot instance is already running") from exc
    atexit.register(instance_lock.__exit__, None, None, None)
    configure_logging()
    try:
        asyncio.run(_bootstrap())
    except DatabaseNotFoundError as exc:
        logging.critical("Database startup check failed: %s", exc)
        raise SystemExit(str(exc)) from None
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass


if __name__ == "__main__":
    main()
