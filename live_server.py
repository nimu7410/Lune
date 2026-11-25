# live_server.py
"""The operator-facing Flask app: the UI route, the JSON routes, the services
behind them, and the app factory that mounts them.

`trading_ui.py`, `realtime_pivot_flask.py`, `getdb_data.py`,
`pivot_service.py`, `resource_monitor.py` and `pnl_history.py` were folded in
here. Each had this module as its only caller (`main.py` aside, which already
imports it), so they live beside the routes they serve rather than in separate
files.
"""

import csv
import gzip
import logging
import os
import statistics
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import psutil
from flask import Flask, Response, jsonify, render_template, request, stream_with_context

import instruments
import ist
from config import CHART_SETTINGS, DATA_SETTINGS
from manual_desk import trade_figures
from strategy_logic import IndexView, is_momentum_book, parse_bar_timestamp


# --------------------------------------------------------------------------- #
#  Chart snapshots (was pivot_service.py)                                      #
# --------------------------------------------------------------------------- #
#: A strategy service that cannot name the replay it serves: nothing is cached.
_UNVERSIONED = object()


class PivotSnapshotService:
    """Serves the chart the *strategy's own* entry signals.

    The markers drawn here are read straight out of the v3 replay the bot
    trades from, so the chart cannot show something the bot did not act on, or
    hide something it did. Each sits on the bar whose close produced the
    signal; v3 fills it at the open of the bar after.

    v3 is long-only, so every marker is a buy and they are all
    served in `ll_points` - the list the console renders as a long. `hh_points`
    stays in the payload, and stays empty, because the chart consumes the
    route's shape and an absent key would read as a failed fetch.
    """

    def __init__(self, data_handler, strategy_service=None, price_getter=None) -> None:
        self.data_handler = data_handler
        self.strategy_service = strategy_service
        self.price_getter = price_getter
        self._cache_lock = threading.Lock()
        self._cache_entry: Optional[tuple[Any, List[Dict[str, Any]]]] = None
        # (strategy snapshot, candle revision, pivots within those candles). The
        # console asks for every index's pivots each second; they change only
        # when the strategy publishes a new replay or the candles move.
        self._pivots_entry: Optional[tuple[Any, Any, Dict[str, List[Dict[str, object]]]]] = None

    def attach_price_getter(self, price_getter) -> None:
        """Late-bind this index's tick feed, for scoring the forming bar.

        It must be a *fresh* getter - one that answers None rather than serving
        a stale tick. A live gate reading drawn on a price that has stopped
        moving is the one thing this panel must never show.
        """
        self.price_getter = price_getter

    @staticmethod
    def _empty_pivots() -> Dict[str, List[Dict[str, Any]]]:
        return {"hh_points": [], "ll_points": []}

    def _candles(self) -> List[Dict[str, Any]]:
        """Current bars, cached against the handler's revision counter.

        Callers poll at 1-2 Hz but 5m bars only change every ~5 minutes, so
        without the cache almost every request would rebuild a byte-identical
        result and burn GIL time the trade loop needs.
        """
        with self._cache_lock:
            known = self._cache_entry[0] if self._cache_entry else None
            getter = getattr(self.data_handler, "get_latest_data_if_changed", None)
            if callable(getter):
                revision, candles = getter(known)
            else:
                candles = self.data_handler.get_latest_data()
                revision = tuple(
                    (c.get("time"), c.get("open"), c.get("high"), c.get("low"), c.get("close"))
                    for c in candles
                )
                if known == revision:
                    candles = None

            if candles is not None:
                self._cache_entry = (revision, list(candles))
            elif self._cache_entry is None:
                self._cache_entry = (revision, list(self.data_handler.get_latest_data()))
            return self._cache_entry[1]

    def _all_pivots(self) -> Dict[str, List[Dict[str, object]]]:
        if self.strategy_service is None:
            return self._empty_pivots()
        try:
            return deepcopy(self.strategy_service.pivot_points())
        except Exception as exc:  # noqa: BLE001 - the chart must not take the bot down
            logging.error("Failed reading strategy signals: %s", exc)
            return self._empty_pivots()

    def _gate_verdicts(self) -> Dict[str, Dict[str, Any]]:
        """Recent entry-gate verdicts from the strategy, or {} if unavailable."""
        getter = getattr(self.strategy_service, "gate_verdicts", None)
        if not callable(getter):
            return {}
        try:
            return getter() or {}
        except Exception as exc:  # noqa: BLE001 - the chart must not take the bot down
            logging.error("Failed reading entry-gate verdicts: %s", exc)
            return {}

    def _live_verdict(self) -> Optional[Dict[str, Any]]:
        """The gates scored against the tick, on the bar still forming.

        A forecast, not a decision - see `IndexView.live_verdict`. Absent
        whenever it would be fiction: no tick feed, a stale tick, or a strategy
        that predates the method.
        """
        getter = getattr(self.strategy_service, "live_verdict", None)
        if not callable(getter) or not callable(self.price_getter):
            return None
        try:
            return getter(self.price_getter())
        except Exception as exc:  # noqa: BLE001 - the chart must not take the bot down
            logging.error("Failed scoring the live entry gates: %s", exc)
            return None

    def _latest_verdict(self) -> Optional[Dict[str, Any]]:
        """The gate verdict for the most recently completed bar.

        v3 scores every bar and takes a trade on very few of them, so the
        interesting reading is almost always "why did the last bar decline",
        not "how did the last accepted signal score". The verdicts are stored
        newest-last, so this is the final entry.
        """
        verdicts = self._gate_verdicts()
        return next(reversed(verdicts.values()), None) if verdicts else None

    @staticmethod
    def _within(
        pivots: Dict[str, List[Dict[str, object]]],
        candles: List[Dict[str, Any]],
    ) -> Dict[str, List[Dict[str, object]]]:
        """Keep only pivots that fall inside the candles being served.

        The runtime accumulates markers across the whole run, but the feed only
        publishes the current day. Without this the chart would plot weeks of
        markers floating beside a single session of candles.
        """
        stamps = [s for s in (parse_bar_timestamp(c.get("time")) for c in candles) if s is not None]
        if not stamps:
            return {"hh_points": [], "ll_points": []}
        first, last = min(stamps), max(stamps)

        def keep(points: List[Dict[str, object]]) -> List[Dict[str, object]]:
            return [
                point for point in points
                if (stamp := parse_bar_timestamp(point.get("time"))) is not None
                and first <= stamp <= last
            ]

        return {
            "hh_points": keep(pivots.get("hh_points", [])),
            "ll_points": keep(pivots.get("ll_points", [])),
        }

    def _strategy_snapshot(self) -> Any:
        """The strategy replay the pivots are read from, or `_UNVERSIONED` when
        the service cannot say (a stand-in without one: never cached)."""
        if not isinstance(self.strategy_service, IndexView):
            return _UNVERSIONED
        return self.strategy_service.engine.snapshot

    def latest_pivots(self) -> Dict[str, List[Dict[str, object]]]:
        """The strategy's published signals for the currently served candles."""
        snapshot = self._strategy_snapshot()
        candles = self._candles()
        with self._cache_lock:
            revision = self._cache_entry[0] if self._cache_entry else None
            hit = self._pivots_entry
        if (snapshot is not _UNVERSIONED and hit is not None and hit[0] is snapshot
                and hit[1] == revision):
            return deepcopy(hit[2])
        pivots = self._within(self._all_pivots(), candles)
        if snapshot is not _UNVERSIONED and snapshot is self._strategy_snapshot():
            with self._cache_lock:
                self._pivots_entry = (snapshot, revision, deepcopy(pivots))
        return pivots

    def latest_snapshot(self) -> Dict[str, Any]:
        """Raw candles plus the strategy's pivots, for the chart routes.

        `signal` carries the entry-gate verdict for the most recently completed
        bar - which gate declined it, or that it was accepted. It rides along
        here rather than on a route of its own so the console keeps polling
        once.
        """
        candles = list(self._candles())
        pivots = self._within(self._all_pivots(), candles)
        return {
            "candles": candles,
            "pivots": pivots,
            "signal": self._latest_verdict(),
        }

    def latest_engine(self) -> Dict[str, Any]:
        """This index's whole engine reading: last signal plus gate verdict.

        The console reports every index's engine, not only the charted one: v3
        gives its single slot to the first qualifying index in its waterfall, so "is
        tonight's carry clear on SENSEX" is a question a NIFTY chart cannot
        answer. That is why the last published signal travels with the verdict
        here instead of being read off the charted index's own markers.
        """
        return {
            "signal": self._latest_verdict(),
            "live": self._live_verdict(),
            "last": self.latest_pivot_summary(),
        }

    def latest_pivot_summary(self) -> Optional[Dict[str, Any]]:
        """The most recently published signal: {"type", "price", "time", "leg"}."""
        pivots = self.latest_pivots() or {}
        points: List[Dict[str, object]] = [
            {"type": kind, "price": point.get("price", point.get(extreme)),
             "time": point.get("time"), "leg": point.get("leg")}
            for kind, key, extreme in (("HH", "hh_points", "high"), ("LL", "ll_points", "low"))
            for point in pivots.get(key, [])
        ]

        dated = [
            (stamp, point) for point in points
            if (stamp := parse_bar_timestamp(point.get("time"))) is not None
            and point.get("price") is not None
        ]
        if not dated:
            return None

        dated.sort(key=lambda item: item[0])
        latest = dated[-1][1]
        return {
            "type": latest.get("type"),
            "price": float(latest["price"]),
            "time": latest.get("time"),
            # Which of v3's two setups opened it. The console needs it to tell a
            # carry that was actually taken from one the day merely ran past.
            "leg": latest.get("leg"),
        }


# --------------------------------------------------------------------------- #
#  Resource usage (was resource_monitor.py)                                    #
#                                                                              #
#  What the bot process costs, and what the PostgreSQL server costs. Kept      #
#  deliberately cheap: everything the dashboard polls is either a cached       #
#  psutil handle (microseconds) or a TTL-cached pair of catalog queries, so    #
#  the panel that reports load does not itself become load.                   #
# --------------------------------------------------------------------------- #

# Process-table scans are the expensive part on Windows, so we hold the handles
# and only re-scan occasionally to catch new/exited backends.
_DB_PROC_REFRESH_SEC = 30.0
# The dashboard polls every few seconds; this keeps repeated hits off the DB.
_DB_STATS_TTL_SEC = 2.0


def _safe(fn, default=None):
    """psutil raises on processes that vanish or are owned by another user."""
    try:
        return fn()
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, OSError):
        return default
    except Exception as exc:  # noqa: BLE001
        logging.debug("resource probe failed: %s", exc)
        return default


class ResourceMonitor:
    """Snapshot of bot-process and database-server resource usage."""

    def __init__(self, db=None, *, db_process_names: tuple[str, ...] = ("postgres",)) -> None:
        self.db = db
        self._db_process_names = tuple(n.lower() for n in db_process_names)

        self._proc = psutil.Process(os.getpid())
        # cpu_percent() reports usage since the previous call on the same
        # handle, so prime it once here; the first real reading is then a true
        # interval measurement rather than 0.0.
        _safe(lambda: self._proc.cpu_percent(None))
        self._cpu_count = psutil.cpu_count(logical=True) or 1

        self._lock = threading.Lock()
        self._db_procs: List[psutil.Process] = []
        self._db_procs_at = 0.0
        self._db_stats_cache: Optional[Dict[str, Any]] = None
        self._db_stats_at = 0.0

    def _bot(self) -> Dict[str, Any]:
        cpu = _safe(lambda: self._proc.cpu_percent(None), 0.0) or 0.0
        mem = _safe(lambda: self._proc.memory_info())
        created = _safe(lambda: self._proc.create_time())
        return {
            "cpu_percent": round(cpu, 1),                       # of one core
            "cpu_percent_of_machine": round(cpu / self._cpu_count, 1),
            "memory_mb": round(mem.rss / (1024 * 1024), 1) if mem else None,
            "threads": _safe(lambda: self._proc.num_threads()),
            "uptime_sec": int(time.time() - created) if created else None,
            "pid": self._proc.pid,
        }

    def _refresh_db_procs_if_stale(self) -> None:
        now = time.monotonic()
        if self._db_procs and (now - self._db_procs_at) < _DB_PROC_REFRESH_SEC:
            return

        found: List[psutil.Process] = []
        for proc in psutil.process_iter(["name"]):
            name = (proc.info.get("name") or "").lower()
            if any(name.startswith(target) for target in self._db_process_names):
                # Prime each handle so the next reading is an interval average.
                _safe(lambda p=proc: p.cpu_percent(None))
                found.append(proc)
        self._db_procs = found
        self._db_procs_at = now

    def _db_processes(self) -> Dict[str, Any]:
        with self._lock:
            self._refresh_db_procs_if_stale()
            procs = list(self._db_procs)

        cpu = 0.0
        rss = 0
        alive = 0
        for proc in procs:
            value = _safe(lambda p=proc: p.cpu_percent(None))
            if value is None:
                continue          # backend exited or is not ours to read
            mem = _safe(lambda p=proc: p.memory_info())
            cpu += value
            rss += mem.rss if mem else 0
            alive += 1

        return {
            "cpu_percent": round(cpu, 1),
            "cpu_percent_of_machine": round(cpu / self._cpu_count, 1),
            "memory_mb": round(rss / (1024 * 1024), 1) if alive else None,
            "processes": alive,
        }

    def _db_stats(self) -> Dict[str, Any]:
        if self.db is None:
            return {"available": False, "reason": "no database connector"}

        now = time.monotonic()
        with self._lock:
            cached = self._db_stats_cache
            if cached is not None and (now - self._db_stats_at) < _DB_STATS_TTL_SEC:
                return cached

        stats: Dict[str, Any] = {"available": True}
        try:
            rows = self.db.execute(
                """
                SELECT pg_database_size(current_database())            AS size_bytes,
                       pg_size_pretty(pg_database_size(current_database())) AS size_pretty,
                       (SELECT count(*) FROM pg_stat_activity
                         WHERE datname = current_database())            AS connections,
                       (SELECT count(*) FROM pg_stat_activity
                         WHERE datname = current_database()
                           AND state = 'active')                        AS active_queries;
                """,
                fetch=True,
            )
            if rows:
                stats.update(dict(rows[0]))

            # n_live_tup is a maintained estimate, so this stays O(1) even as
            # forecast_logs grows through the session.
            tables = self.db.execute(
                """
                SELECT relname AS table_name, n_live_tup AS rows
                FROM   pg_stat_user_tables
                WHERE  schemaname = 'botdata'
                ORDER  BY relname;
                """,
                fetch=True,
            ) or []
            stats["tables"] = [
                {"name": r["table_name"], "rows": int(r["rows"] or 0)} for r in tables
            ]
        except Exception as exc:  # noqa: BLE001
            logging.debug("DB resource stats unavailable: %s", exc)
            stats = {"available": False, "reason": "query failed"}

        with self._lock:
            self._db_stats_cache = stats
            self._db_stats_at = now
        return stats

    def _pool(self) -> Dict[str, Any]:
        """Connection-pool occupancy, best effort — psycopg2 exposes no API."""
        pool = getattr(self.db, "_pool", None) if self.db is not None else None
        if pool is None:
            return {"available": False}
        used = _safe(lambda: len(pool._used))
        free = _safe(lambda: len(pool._pool))
        if used is None or free is None:
            return {"available": False}
        return {
            "available": True,
            "in_use": used,
            "idle": free,
            "max": getattr(self.db, "maxconn", None),
        }

    def snapshot(self) -> Dict[str, Any]:
        return {
            "bot": self._bot(),
            "database": {**self._db_processes(), **self._db_stats(), "pool": self._pool()},
            "cpu_count": self._cpu_count,
            "system_cpu_percent": round(_safe(lambda: psutil.cpu_percent(None), 0.0) or 0.0, 1),
        }


# --------------------------------------------------------------------------- #
#  Closed-trade performance (was pnl_history.py)                               #
#                                                                              #
#  From the live ledger and the shutdown archives.                             #
# --------------------------------------------------------------------------- #
BACKUPS = Path(__file__).resolve().parent / "backups"


@lru_cache(maxsize=1)
def archived_trades(files):
    rows = []
    for name, _mtime, _size in files:
        with open(name, newline="", encoding="utf-8") as stream:
            rows.extend(csv.DictReader(stream))
    return rows


#: The two books, by the ledger a row came from: DAY is the HH/LL desk
#: (`manualdata`, intraday), NIGHT is v3 (`botdata` and its archives, held overnight).
BOOKS = ("DAY", "NIGHT")


def _number(value) -> Optional[float]:
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return x if x == x and abs(x) != float("inf") else None


def performance(rows, start, end, instrument="", mode="ALL", book="ALL"):
    """Closed trades' net ₹ (after charges) by entry day, instrument and book, each
    figured exactly as the console's books figure it (`manual_desk.trade_figures`).
    A row's book is its `_book` key ("DAY"/"NIGHT"); one without it is v3's, as every
    archive row is. A DAY row's recorded leg quotes ride in its `_fills`."""
    # Archive and DB can overlap after a backup whose transaction rolled back.
    unique = {}
    for row in rows:
        stamp = ist.parse_ist(row.get("log_time"))
        if stamp is None:
            raise ValueError("Trade has no valid timestamp")
        row_book = row.get("_book") or "NIGHT"
        # Two ledgers, two id sequences: the book is part of a trade's identity.
        key = (row_book, row.get("execution_id") or (str(row.get("id")), stamp.isoformat()))
        unique[key] = (row, stamp.date().isoformat(), row_book)
    daily, by_instrument, books = defaultdict(list), defaultdict(list), defaultdict(list)
    available = set()
    values = []
    for row, day, row_book in unique.values():
        if not start <= day <= end:
            continue
        row_mode = str(row.get("execution_mode") or "UNKNOWN").upper()
        if mode != "ALL" and row_mode != mode:
            continue
        if book != "ALL" and row_book != book:
            continue
        # The night book started fresh with the carry: the retired momentum setup's
        # trades stay in the ledger and archives but out of these figures.
        if row_book == "NIGHT" and is_momentum_book(row.get("book")):
            continue
        if str(row.get("is_in_trade")).lower() not in {"false", "f", "0"}:
            continue
        if row.get("exit_price") in (None, ""):
            continue
        direction = str(row.get("direction") or "").upper()
        if direction not in {"LONG", "SHORT"}:
            continue
        # The index the trade was on; a row from before the multi-index book was NIFTY's.
        name = str(row.get("index_name") or instruments.CHART_INDEX)
        available.add(name)
        if instrument and name != instrument:
            continue
        entry, exit_price = _number(row.get("fill_price")), _number(row.get("exit_price"))
        if entry is None or exit_price is None:
            raise ValueError("Non-finite trade price")
        quantity = _number(row.get("quantity"))
        trade = trade_figures({
            **row, "log_time": ist.parse_ist(row.get("log_time")), "exit_time": ist.parse_ist(row.get("exit_time")),
            "index_name": name, "direction": direction, "fill_price": entry, "exit_price": exit_price,
            "quantity": int(quantity) if quantity else None,
        }, row.get("_fills") or ())
        values.append(trade)
        daily[day].append(trade)
        by_instrument[name].append(trade)
        books[row_book].append(trade)

    def summary(items):
        return {"pnl": round(sum(t["net_pnl"] for t in items), 2),
                "charges": round(sum(t["charges"] for t in items), 2),
                "trades": len(items), "wins": sum(t["net_pnl"] > 0 for t in items)}

    return {**summary(values), "start": start, "end": end, "mode": mode, "book": book,
            "available_instruments": sorted(available),
            "daily": [{"date": day, **summary(items)} for day, items in sorted(daily.items())],
            "instruments": [{"instrument": name, **summary(items)} for name, items in sorted(by_instrument.items())],
            "books": [{"book": name, **summary(books[name])} for name in BOOKS if name in books]}


def register_pnl_history(app, db):
    @app.get("/pnl_history")
    def pnl_history():
        try:
            start = date.fromisoformat(request.args.get("start", ist.today().replace(day=1).isoformat()))
            end = date.fromisoformat(request.args.get("end", ist.today().isoformat()))
            if start > end:
                raise ValueError("Start date must not follow end date")
            mode = request.args.get("mode", app.config["TRADE_MODE"]).upper()
            if mode not in {"ALL", "LIVE", "TEST", "UNKNOWN"}:
                raise ValueError("Invalid execution mode")
            book = request.args.get("book", "ALL").upper()
            if book not in {"ALL", *BOOKS}:
                raise ValueError("Invalid book")
        except ValueError:
            return jsonify(error="Choose valid dates, an execution mode and a book."), 400
        try:
            if db is None:
                raise RuntimeError("Database unavailable")
            ledgers = []
            # NIGHT: v3's ledger (and its archives below). DAY: the HH/LL desk's, never
            # truncated. Both or nothing: a history missing one book would read as complete.
            for schema, name in (("botdata", "NIGHT"), ("manualdata", "DAY")):
                if book not in ("ALL", name):
                    continue
                rows = db.execute(
                    f"SELECT * FROM {schema}.trade_logs WHERE (log_time AT TIME ZONE %s)::date BETWEEN %s AND %s",
                    (ist.IST_NAME, start, end), fetch=True)
                if rows is None:
                    raise RuntimeError("Ledger query failed")
                if name == "DAY" and rows:
                    # The desk records each leg's quote: its charges are its own, as in its book.
                    fills: Dict[Any, list] = {}
                    for f in db.execute(
                            "SELECT trade_id, phase, symbol, side, qty, premium FROM manualdata.leg_fills "
                            "WHERE trade_id = ANY(%s)", ([r["id"] for r in rows],), fetch=True) or []:
                        fills.setdefault(f["trade_id"], []).append(f)
                    rows = [{**row, "_fills": fills.get(row["id"], [])} for row in rows]
                ledgers.extend({**row, "_book": name} for row in rows)
            files = tuple((str(p), p.stat().st_mtime_ns, p.stat().st_size)
                          for p in sorted(BACKUPS.glob("trade_logs_*.csv")))
            archived = archived_trades(files) if book in ("ALL", "NIGHT") else []
            result = performance([*archived, *ledgers], start.isoformat(), end.isoformat(),
                                 request.args.get("instrument", "").strip().upper(), mode, book)
            return jsonify(result)
        except Exception:
            app.logger.exception("PNL history unavailable")
            return jsonify(error="Trade history is unavailable. Please retry."), 503


# --------------------------------------------------------------------------- #
#  Broker latency probe                                                        #
# --------------------------------------------------------------------------- #
class BrokerLatencyProbe:
    """Round-trip time from this machine to the Shoonya API.

    Measured on ``GetQuotes`` — a read-only route the console already depends
    on — so the figure is the latency of the path that actually carries the
    trading day. An ICMP ping to the edge would answer faster than the API
    does and would tell an operator nothing about whether an order can go out.

    Two rules keep the probe cheaper than the thing it measures:

    * **TTL-gated.** A dashboard polling every two seconds still costs the
      broker one request per ``ttl_sec``.
    * **Never queued.** If a probe is already in flight, a second caller takes
      the last published sample rather than waiting behind the network. The
      panel that reports latency must never itself become latency.

    A failed probe is kept in the history as ``None`` instead of being dropped,
    so a run of timeouts reads as a run of failures rather than as an unbroken
    line of the last good number.
    """

    def __init__(self, api, exchange, token, *, ttl_sec: float = 5.0,
                 history: int = 32) -> None:
        self._api = api
        self._exchange = exchange
        self._token = token
        self._ttl = ttl_sec

        self._lock = threading.Lock()          # guards the published snapshot
        self._probe_lock = threading.Lock()    # at most one probe in flight
        self._samples: deque[float | None] = deque(maxlen=history)
        self._last_ms: float | None = None
        self._last_error: str | None = None
        self._probed_at = 0.0

    @property
    def available(self) -> bool:
        return self._api is not None and self._token is not None

    def _measure(self) -> None:
        """One timed call. Runs outside the snapshot lock, never re-entered."""
        started = time.perf_counter()
        latency_ms: float | None = None
        error: str | None = None
        try:
            result = self._api.get_quotes(self._exchange, self._token)
            elapsed = (time.perf_counter() - started) * 1000.0
            if result is None:
                # `_checked` already logged the broker's message; from here it
                # is a reachable-but-refusing endpoint, which is not a latency.
                error = "rejected"
            else:
                latency_ms = round(elapsed, 1)
        except Exception as exc:  # noqa: BLE001
            logging.debug("broker_ping probe failed: %s", exc)
            error = type(exc).__name__

        with self._lock:
            self._samples.append(latency_ms)
            self._last_ms = latency_ms
            self._last_error = error
            self._probed_at = time.monotonic()

    def snapshot(self) -> dict:
        if not self.available:
            return {"available": False, "reason": "broker session unavailable"}

        with self._lock:
            stale = (time.monotonic() - self._probed_at) >= self._ttl

        # Non-blocking: whoever loses the race reads the previous sample. A
        # slow broker therefore slows the number, never the dashboard.
        if stale and self._probe_lock.acquire(blocking=False):
            try:
                self._measure()
            finally:
                self._probe_lock.release()

        with self._lock:
            samples = list(self._samples)
            latency_ms = self._last_ms
            error = self._last_error

        good = [value for value in samples if value is not None]
        return {
            "available": True,
            "ok": latency_ms is not None,
            "latency_ms": latency_ms,
            "error": error,
            "history": samples,
            "median_ms": round(statistics.median(good), 1) if good else None,
            "peak_ms": max(good) if good else None,
            "samples": len(samples),
            "failures": len(samples) - len(good),
            "as_of_ist": ist.isoformat(),
        }


# --------------------------------------------------------------------------- #
#  Operator UI route (was trading_ui.py)                                       #
# --------------------------------------------------------------------------- #
def create_trading_ui_route(app, indices: Sequence[str] = (), chart_index: str = "") -> None:
    """Register the single, consolidated operator interface."""

    timeframe = (
        CHART_SETTINGS.timeframe_label
        or f"{DATA_SETTINGS.candle_interval_minutes}m"
    )
    # Which indices the console may switch between, and which it opens on. Only
    # those whose spot feed actually resolved are offered: a name in the picker
    # that answers nothing would read as a broken chart.
    names = list(indices) or [chart_index or instruments.CHART_INDEX]
    opens_on = chart_index if chart_index in names else names[0]

    @app.route("/")
    def trading_ui():
        return render_template(
            "index.html",
            timeframe=timeframe,
            poll_ms=CHART_SETTINGS.realtime_poll_ms,
            indices=names,
            chart_index=opens_on,
            trade_mode=app.config.get("TRADE_MODE", "ALL"),
        )


# --------------------------------------------------------------------------- #
#  Pivot JSON routes (was realtime_pivot_flask.py)                             #
# --------------------------------------------------------------------------- #
def create_realtime_pivot_routes(
    app,
    data_handler,
    pivot_service: PivotSnapshotService,
    pivot_services: Mapping[str, PivotSnapshotService] | None = None,
    chart_index: str = "",
) -> None:
    """Expose the strategy's candles and confirmed HH/LL pivots as JSON.

    The markers come from the detector used by the execution engine. A pivot is
    published only after confirmation and remains anchored to its extreme bar.
    Confirmed HH pivots imply SHORT; confirmed LL pivots imply LONG.

    v3 evaluates six indices in waterfall order, so the console can chart any
    of them: `?index=BANKNIFTY` picks that index's own feed and its own gate
    verdicts. An unknown or absent name serves the default rather than 404ing -
    the chart poller must never be able to leave the operator with a blank page
    because a query string went stale across a deploy.
    """

    services = dict(pivot_services or {})

    def _chosen() -> PivotSnapshotService:
        return services.get((request.args.get("index") or "").strip().upper(), pivot_service)

    @app.route("/realtime_pivots")
    def realtime_pivots():
        return jsonify(_chosen().latest_pivots())

    @app.route("/realtime_pivot_snapshot")
    def realtime_pivot_snapshot():
        return jsonify(_chosen().latest_snapshot())

    # Every index at once, keyed by name. The console's engine bay is switched
    # independently of the chart - v3 allocates one slot across all six, so
    # which of them is clear tonight is not a question the charted index can
    # answer - and three verdicts are a few hundred bytes, so they ride one poll
    # rather than three.
    engines = dict(services) or {
        (chart_index or instruments.CHART_INDEX): pivot_service
    }

    @app.route("/entry_gates")
    def entry_gates():
        return jsonify({name: svc.latest_engine() for name, svc in engines.items()})


# --------------------------------------------------------------------------- #
#  Option legs                                                                 #
# --------------------------------------------------------------------------- #
def held_legs(db, chart_index: str = "") -> Optional[Dict[str, Any]]:
    """The open position's legs as the ledger recorded them, or None when flat.

    Raises when the ledger cannot be read: an unanswered ledger is not a flat
    book, and the caller must be able to tell the two apart.
    """
    rows = db.execute(
        """
        SELECT index_name, symbol, buy_symbol, sell_symbol
        FROM   botdata.trade_logs
        WHERE  is_in_trade = TRUE
        ORDER  BY log_time DESC
        LIMIT  1;
        """,
        fetch=True,
    ) or []
    if not rows:
        return None
    row = rows[0]
    # A row with no index predates the multi-index book, and every one of
    # those is the chart index's (see `trade_logs.index_name`).
    return {
        "index": row.get("index_name") or chart_index or instruments.CHART_INDEX,
        "sell": row.get("sell_symbol") or row.get("symbol"),
        "buy": row.get("buy_symbol"),
    }


def option_leg_rows(fetchers: Mapping[str, Any], held: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One row per tradeable index, in the strategy's order.

    The traded legs outrank the built ones on the held index: once filled, the
    position is what is on the book, whatever the builder prices next.
    """
    rows = []
    for name in instruments.NAMES:
        inst = instruments.get(name)
        row: Dict[str, Any] = {
            "index": name,
            "exchange": inst.fo_exchange,
            "lot": inst.quantity,
            "expiry": f"{inst.expiry.option_cadence} {inst.expiry.option_weekday}",
        }
        fetcher = fetchers.get(name)
        if held and held["index"] == name:
            row.update(state="held", sell=held["sell"], buy=held["buy"], note=None)
        elif fetcher is None:
            row.update(state="offline", sell=None, buy=None, note="no leg builder for this index")
        else:
            row.update(fetcher.legs_status())
        rows.append(row)
    return rows


def create_app(api, exch, tok, live_price_getter, data_handler, db, trade_mode: str | None = None,
               pivot_service: PivotSnapshotService | None = None,
               pivot_services: Mapping[str, PivotSnapshotService] | None = None,
               price_getters: Mapping[str, Callable[[], float | None]] | None = None,
               chart_index: str = "",
               symbol_fetchers: Mapping[str, Any] | None = None,
               feed_health: Callable[[], dict] | None = None,
               add_price_listener: Callable[[Callable[[Any], None]], None] | None = None,
               price_keys: Mapping[str, str] | None = None):
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    app = Flask(__name__)
    app.config["TRUSTED_HOSTS"] = [
        "127.0.0.1", "localhost", "free-a1", "free-a1.tail57c420.ts.net", "100.69.232.2",
    ]
    app.config["TSDB_CONN"] = db

    @app.after_request
    def _gzip(response):
        # The console is read from a phone over Tailscale on mobile data: the
        # scripts and the once-a-second JSON shrink ~4x. Streams pass as-is.
        if (response.status_code != 200 or response.mimetype == "text/event-stream"
                or "Content-Encoding" in response.headers
                or "gzip" not in request.headers.get("Accept-Encoding", "")):
            return response
        response.direct_passthrough = False
        data = response.get_data()
        if len(data) < 1024:
            return response
        # Level 1: within ~5% of level 5's size on this JSON, at half the CPU.
        response.set_data(gzip.compress(data, 1))
        response.headers["Content-Encoding"] = "gzip"
        response.vary.add("Accept-Encoding")
        return response
    app.config["SHOONYA_API"] = api
    app.config["TRADE_MODE"] = (trade_mode or "").strip().upper() or "UNKNOWN"
    register_pnl_history(app, db)

    # Accept an injected service so in-process consumers (e.g. ForecastLogger)
    # share one snapshot cache with the routes instead of each building their own.
    if pivot_service is None:
        pivot_service = PivotSnapshotService(data_handler)

    @app.route("/data")
    def data():
        return jsonify(data_handler.get_latest_data())

    def _index_price() -> Optional[float]:
        # Same index the chart is showing, from that index's own subscription:
        # a BANKNIFTY chart under a NIFTY last price is worse than no price.
        getter = (price_getters or {}).get(
            (request.args.get("index") or "").strip().upper(), live_price_getter)
        if not callable(getter):
            return None
        try:
            price = getter()
        except Exception as exc:  # noqa: BLE001
            logging.debug("live_price fetch failed: %s", exc)
            return None
        return price if price is not None and price > 0 else None

    # Every websocket tick bumps a counter and wakes the streams, so the console
    # prints each new LTP as it lands instead of whatever a poll happened to
    # catch. The read thread only pays for a counter and a notify. A stream
    # whose index has a known `EXCH|TOKEN` (`price_keys`) waits on that token
    # alone, so the other instruments' ticks do not wake it; one without waits
    # on every tick. A token two exchanges share wakes both: harmless.
    keys = dict(price_keys or {})
    tick_conds = {key.split("|", 1)[-1]: threading.Condition() for key in keys.values()}
    tick_seqs = dict.fromkeys(tick_conds, 0)
    ticks = {"seq": 0}
    tick_cond = threading.Condition()

    def _on_tick(tick: Any = None) -> None:
        token = str(tick.get("tk") or "").strip() if isinstance(tick, dict) else ""
        cond = tick_conds.get(token)
        if cond is not None:
            with cond:
                tick_seqs[token] += 1
                cond.notify_all()
        with tick_cond:
            ticks["seq"] += 1
            tick_cond.notify_all()

    def _stream_waiter() -> tuple[threading.Condition, Callable[[], int]]:
        """The condition this request's stream waits on, and its tick counter."""
        name = (request.args.get("index") or "").strip().upper()
        if name not in (price_getters or {}):
            name = chart_index          # `_index_price` falls back to the chart index too
        key = keys.get(name)
        token = key.split("|", 1)[-1] if key else None
        if token in tick_conds:
            return tick_conds[token], lambda: tick_seqs[token]
        return tick_cond, lambda: ticks["seq"]

    if add_price_listener is not None:
        add_price_listener(_on_tick)

    @app.route("/live_price_stream")
    def live_price_stream():
        cond, counter = _stream_waiter()

        def events():
            last, seq, wrote = None, -1, 0.0
            while True:
                price = _index_price()
                if price is not None and price != last:
                    last, wrote = price, time.monotonic()
                    yield f"data: {price}\n\n"
                elif time.monotonic() - wrote >= 1.0:
                    # A quiet second: keeps idle proxies open and surfaces a
                    # closed tab on the write, so the thread ends with its client.
                    wrote = time.monotonic()
                    yield ": \n\n"
                with cond:
                    # The 1 s timeout re-reads anyway: a REST quote standing in
                    # for dead ticks arrives without a tick to announce it.
                    cond.wait_for(lambda: counter() != seq, timeout=1.0)
                    seq = counter()

        return Response(stream_with_context(events()), mimetype="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.route("/live_price")
    def live_price():
        price = _index_price()
        if price is None:
            return jsonify({"price": None}), 503

        return jsonify({
            "price": price,
            "as_of_ist": ist.isoformat(),
        })

    @app.route("/pivot_status")
    def pivot_status():
        """The type ("HH"/"LL") of the most recent forecast pivot, or ""."""
        try:
            pivots = pivot_service.latest_pivots()
            points = (
                [{"time": p["time"], "type": "HH"} for p in pivots.get("hh_points", [])]
                + [{"time": p["time"], "type": "LL"} for p in pivots.get("ll_points", [])]
            )
            # Same IST reading as every other consumer of these timestamps, so
            # "most recent pivot" cannot mean one thing here and another in
            # db_loggers or the snapshot service. Unparseable points sort to the
            # front rather than raising and turning the route into a 500.
            undated = datetime.min.replace(tzinfo=ist.IST)
            points.sort(key=lambda x: ist.parse_feed_time(x["time"]) or undated)
            return jsonify({"forecasted_type": points[-1]["type"] if points else ""})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/account_balance")
    def account_balance():
        broker = app.config.get("SHOONYA_API")
        if broker is None:
            return jsonify({"error": "API unavailable"}), 503

        try:
            res = broker.get_limits()
        except Exception as exc:  # noqa: BLE001
            logging.error("account_balance: get_limits failed: %s", exc)
            return jsonify({"error": "fetch_failed"}), 502

        if not isinstance(res, dict):
            return jsonify({"error": "invalid_response"}), 502

        # Shoonya returns {"stat":"Ok", ...fields}
        if res.get("stat") and str(res.get("stat")).lower() != "ok":
            return jsonify({"error": res.get("emsg") or "not_ok"}), 502

        def _to_float(val):
            try:
                return float(val)
            except Exception:
                return None

        return jsonify({
            "cash": _to_float(res.get("cash")),
            "collateral": _to_float(res.get("collateral")),
            "margin_used": _to_float(res.get("marginused") or res.get("cnc_marginused")),
        })

    # `exch`/`tok` are what the rest of the bot quotes, so the console reports
    # the latency of the exact call it depends on rather than of a stand-in.
    broker_probe = BrokerLatencyProbe(api, exch, tok)

    @app.route("/broker_ping")
    def broker_ping():
        try:
            return jsonify(broker_probe.snapshot())
        except Exception as exc:  # noqa: BLE001
            logging.error("broker_ping failed: %s", exc)
            return jsonify({"available": False, "reason": "unavailable"}), 503

    resource_monitor = ResourceMonitor(db)

    @app.route("/resource_usage")
    def resource_usage():
        try:
            return jsonify(resource_monitor.snapshot())
        except Exception as exc:  # noqa: BLE001
            logging.error("resource_usage failed: %s", exc)
            return jsonify({"error": "unavailable"}), 503

    @app.route("/net_pnl")
    def net_pnl_today():
        if db is None:
            return jsonify({"error": "DB unavailable"}), 503

        # "Today" is the IST trading day, and the zone is passed rather than
        # hardcoded so it cannot drift from the one the rest of the bot uses.
        rows = db.execute(
            """
            SELECT log_time, direction, fill_price, exit_price, is_in_trade
            FROM   botdata.trade_logs
            WHERE  is_in_trade = TRUE
               OR (COALESCE(exit_time, log_time) AT TIME ZONE %s)::date = %s;
            """,
            (ist.IST_NAME, ist.today()),
            fetch=True,
        ) or []

        net_points = 0.0
        closed_trades = 0
        open_component = None
        open_direction = None
        open_entry = None
        latest_price = None

        for row in rows:
            direction = (row.get("direction") or "").upper()
            entry = row.get("fill_price")
            exit_price = row.get("exit_price")
            in_trade = bool(row.get("is_in_trade"))
            if entry is None or direction not in {"LONG", "SHORT"}:
                continue

            if not in_trade and exit_price is not None:
                closed_trades += 1
                net_points += (exit_price - entry) if direction == "LONG" else (entry - exit_price)
                continue

            if in_trade:
                # Recorded before the price lookup, not inside it: an open
                # position has a side and an entry whether or not a live tick
                # is available to mark it to market, and "LONG, unrealised
                # unknown" is the reading an operator needs in that gap.
                open_direction = direction
                open_entry = entry

                if latest_price is None and callable(live_price_getter):
                    try:
                        latest_price = live_price_getter()
                    except Exception:
                        latest_price = None

                if latest_price is not None and latest_price > 0:
                    open_pts = (latest_price - entry) if direction == "LONG" else (entry - latest_price)
                    net_points += open_pts
                    open_component = open_pts

        return jsonify({
            "mode": app.config["TRADE_MODE"],
            "net_pnl_points": round(net_points, 2),
            "closed_trades": closed_trades,
            "open_trade_points": None if open_component is None else round(open_component, 2),
            "open_direction": open_direction,
            "open_entry": None if open_entry is None else round(open_entry, 2),
            "as_of_ist": ist.isoformat(),
        })

    @app.route("/feed_health")
    def feed_health_route():
        if feed_health is None:
            return jsonify({"available": False}), 503
        try:
            return jsonify({"available": True, **feed_health()})
        except Exception as exc:  # noqa: BLE001
            logging.error("feed_health failed: %s", exc)
            return jsonify({"available": False}), 503

    @app.route("/option_legs")
    def option_legs():
        held, ledger = None, db is not None
        if ledger:
            try:
                held = held_legs(db, chart_index)
            except Exception as exc:  # noqa: BLE001 - the legs still render without it
                logging.error("option_legs: reading the open position failed: %s", exc)
                ledger = False
        return jsonify({
            "legs": option_leg_rows(symbol_fetchers or {}, held),
            "held": held,
            # False when the open position could not be read: every row then
            # shows what was built, which is not proof that nothing is held.
            "ledger": ledger,
            "as_of_ist": ist.isoformat(),
        })

    create_trading_ui_route(app, indices=list(pivot_services or ()), chart_index=chart_index)
    create_realtime_pivot_routes(
        app, data_handler, pivot_service=pivot_service, pivot_services=pivot_services,
        chart_index=chart_index,
    )
    return app
