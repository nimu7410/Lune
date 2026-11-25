# tests/order_path_fakes.py
"""Stand-ins that let the *real* trade managers run with no broker and no database.

`check_order_paths.py` drives `TradeManagerTest` and `TradeManagerLive`
unmodified - the production classes, the production `StrategyDecider`, the
production SQL. Everything they talk to lives here:

  * `FakeDB`      - an in-memory `botdata` schema. It answers the exact
                    statements the managers issue, so a change to that SQL
                    surfaces as a check failure rather than passing silently.
  * `FakeBroker`  - order placement, the position book, limits and order
                    history. It also plays the part of the *WebSocket*: live
                    order state (`state1`/`buy_exchordid`/fill columns) is
                    never written by the trade manager, it is written by
                    `main.py`'s order-update handler when the broker pushes an
                    update. `ws=False` reproduces that push going missing.
  * `FakeSymbols` - `GetSymbol` as the managers see it: a synthetic long's legs.
  * `ScriptedRuntime` / `ScriptedService` - a strategy that wants a specific
                    trade at a specific moment, so the order path can be tested
                    without waiting for a real v3 signal.
  * `v3_market` / `live_view` / `write_live_root` - v3's signals on its
                    recorded sessions, as a live process sees them candle by
                    candle.

Nothing here touches the production database, the broker, or the running bot.
"""

from __future__ import annotations

import copy
import functools
import logging
import re
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import numpy as np
import pandas as pd

import instruments
import ist
import strategy_logic

BAR_MINUTES = 5

# Columns every `botdata.order_logs` row carries. The managers read these by
# name off the returned dicts, so a missing key would be an AttributeError-like
# failure rather than the NULL the real schema returns.
ORDER_LOG_COLUMNS = (
    "log_time", "symbol1", "symbol2", "state1", "state2",
    "order_type1", "order_type2", "time1", "time2",
    "buy_orderno", "sell_orderno", "buy_exchordid", "sell_exchordid",
    "buy_fill_qty", "sell_fill_qty", "buy_avg_price", "sell_avg_price",
    "buy_exit_state", "sell_exit_state", "time1_exit", "time2_exit",
    "buy_exit_orderno", "sell_exit_orderno",
    "buy_exit_exchordid", "sell_exit_exchordid",
    "buy_exit_fill_qty", "sell_exit_fill_qty",
    "buy_exit_avg_price", "sell_exit_avg_price",
    "lifecycle_state",
)

TRADE_LOG_COLUMNS = (
    "log_time", "symbol", "direction", "fill_price", "stop_price", "exit_price", "exit_time",
    "is_in_trade", "win", "buy_orderno", "sell_orderno", "buy_symbol",
    "sell_symbol", "sell_entry_side", "trade_state", "execution_id",
    "execution_mode", "book", "index_name", "quantity", "realized_pnl",
)

ATTEMPT_COLUMNS = (
    "created_at", "updated_at", "order_log_id", "client_ref", "phase", "leg",
    "attempt_no", "symbol", "side", "requested_qty", "status", "orderno", "error",
)

TERMINAL_STATES = {"COMPLETE", "REJECTED", "CANCELED", "CANCELLED"}


class UnsupportedStatement(RuntimeError):
    """The manager issued SQL this harness does not model.

    Raised rather than ignored: a statement the harness has never seen means
    the order path changed shape, and quietly returning `[]` would let a broken
    path report success.
    """


# --------------------------------------------------------------------------- #
#  Database
# --------------------------------------------------------------------------- #
class FakeDB:
    """In-memory `botdata`, addressed by the statements the managers send."""

    def __init__(self) -> None:
        self.order_logs: list[dict[str, Any]] = []
        self.trade_logs: list[dict[str, Any]] = []
        self.order_attempts: list[dict[str, Any]] = []
        self.statements: list[str] = []
        self.unsupported: list[str] = []
        self._lock = threading.RLock()
        self._next = {"order_logs": 1, "trade_logs": 1, "order_attempts": 1}
        # Set to raise on the next matching statement, to rehearse a database
        # failure mid-entry.
        self.fail_on: Optional[str] = None

    # -- helpers ----------------------------------------------------------
    def _new_id(self, table: str) -> int:
        value = self._next[table]
        self._next[table] = value + 1
        return value

    def _rows_for_pair(self, buy_sym, sell_sym) -> list[dict[str, Any]]:
        return [
            row for row in self.order_logs
            if (row.get("symbol1") or "") == (buy_sym or "")
            and (row.get("symbol2") or "") == (sell_sym or "")
        ]

    def order_log(self, row_id: int) -> Optional[dict[str, Any]]:
        return next((r for r in self.order_logs if r["id"] == row_id), None)

    @property
    def open_trades(self) -> list[dict[str, Any]]:
        return [r for r in self.trade_logs if r.get("is_in_trade")]

    # -- connector surface -------------------------------------------------
    def ensure_schema(self) -> None:
        return None

    async def execute_async(self, statement, params=None, *, fetch=False):
        return self.execute(statement, params, fetch=fetch)

    async def get_raw_connection_async(self):
        return FakeConnection(self)

    async def put_raw_connection_async(self, conn, *, close: bool = False) -> None:
        return None

    def get_raw_connection(self):
        return FakeConnection(self)

    def put_raw_connection(self, conn, *, close: bool = False) -> None:
        return None

    # -- the dispatcher ----------------------------------------------------
    def execute(self, statement: str, params=None, *, fetch: bool = False):
        query = " ".join(str(statement).split())
        params = tuple(params or ())
        with self._lock:
            self.statements.append(query)
            if self.fail_on and self.fail_on in query:
                raise RuntimeError(f"simulated database failure on {self.fail_on!r}")
            rows = self._dispatch(query, params)
        return rows if fetch else None

    def _dispatch(self, q: str, p: tuple) -> list[dict[str, Any]]:
        # Advisory locks. The harness is single-process, so the lock is always
        # free; `_try_acquire_entry_lock` reads `ok` off a dict row.
        if "pg_try_advisory_lock" in q:
            return [{"ok": True}]
        if "pg_advisory_unlock" in q:
            return []

        if q.startswith("INSERT INTO botdata.order_logs"):
            return self._insert_order_log(p)
        if q.startswith("INSERT INTO botdata.order_attempts"):
            return self._insert_attempt(p)
        if q.startswith("INSERT INTO botdata.trade_logs"):
            return self._insert_trade_log(q, p)
        if q.startswith("UPDATE botdata.order_attempts"):
            return self._update_attempt(q, p)
        if q.startswith("UPDATE botdata.order_logs"):
            return self._update_order_log(q, p)
        if q.startswith("UPDATE botdata.trade_logs"):
            return self._update_trade_log(q, p)
        if "FROM botdata.order_attempts" in q:
            return self._select_attempts(q, p)
        if "FROM botdata.trade_logs AS t JOIN botdata.order_logs AS o" in q:
            # The CLOSE-commit recovery probe: did both ledgers close?
            order_log_id, trade_id = p
            trade, order = self._trade(trade_id), self.order_log(order_log_id)
            done = (trade is not None and order is not None and not trade.get("is_in_trade")
                    and trade.get("trade_state") == "CLOSED"
                    and order.get("lifecycle_state") == "CLOSED")
            return [{"id": trade_id}] if done else []
        if "FROM botdata.order_logs" in q:
            return self._select_order_logs(q, p)
        if "FROM botdata.trade_logs" in q:
            return self._select_trade_logs(q, p)

        self.unsupported.append(q)
        raise UnsupportedStatement(q)

    # -- inserts -----------------------------------------------------------
    def _insert_order_log(self, p: tuple) -> list[dict[str, Any]]:
        log_time, symbol1, symbol2, order_type1, order_type2 = p
        row = {"id": self._new_id("order_logs")}
        row.update({name: None for name in ORDER_LOG_COLUMNS})
        row.update({
            "log_time": log_time, "symbol1": symbol1, "symbol2": symbol2,
            "order_type1": order_type1, "order_type2": order_type2,
            "lifecycle_state": "ENTRY_PENDING",
        })
        self.order_logs.append(row)
        return [{"id": row["id"]}]

    def _insert_attempt(self, p: tuple) -> list[dict[str, Any]]:
        (created, updated, order_log_id, client_ref, phase, leg,
         attempt_no, symbol, side, qty) = p
        row = {"id": self._new_id("order_attempts")}
        row.update({name: None for name in ATTEMPT_COLUMNS})
        row.update({
            "created_at": created, "updated_at": updated,
            "order_log_id": order_log_id, "client_ref": client_ref,
            "phase": phase, "leg": leg, "attempt_no": attempt_no,
            "symbol": symbol, "side": side, "requested_qty": qty,
            "status": "INTENT",
        })
        self.order_attempts.append(row)
        return [{"id": row["id"]}]

    def _insert_trade_log(self, q: str, p: tuple) -> list[dict[str, Any]]:
        row = {"id": self._new_id("trade_logs")}
        row.update({name: None for name in TRADE_LOG_COLUMNS})
        if "'TEST'," in q:
            (log_time, symbol, direction, price, stop_price, book, index_name,
             sell_symbol, buy_symbol) = p
            row.update({
                "log_time": log_time, "symbol": symbol, "direction": direction,
                "fill_price": price, "stop_price": stop_price, "is_in_trade": True,
                "execution_mode": "TEST", "trade_state": "OPEN", "book": book,
                "index_name": index_name, "sell_symbol": sell_symbol, "buy_symbol": buy_symbol,
            })
        else:
            (log_time, symbol, direction, fill_price, stop_price, buy_ordno,
             sell_ordno, buy_symbol, sell_symbol, sell_entry_side,
             execution_id, book, index_name, quantity) = p
            row.update({
                "log_time": log_time, "symbol": symbol, "direction": direction,
                "fill_price": fill_price, "stop_price": stop_price,
                "is_in_trade": True, "buy_orderno": buy_ordno,
                "sell_orderno": sell_ordno, "buy_symbol": buy_symbol,
                "sell_symbol": sell_symbol, "sell_entry_side": sell_entry_side,
                "trade_state": "OPEN", "execution_id": execution_id,
                "execution_mode": "LIVE", "book": book, "index_name": index_name,
                "quantity": quantity,
            })
        self.trade_logs.append(row)
        return [{"id": row["id"]}]

    # -- updates -----------------------------------------------------------
    def _update_attempt(self, q: str, p: tuple) -> list[dict[str, Any]]:
        if "status='REJECTED'" in q:
            updated, error, attempt_id = p
            row = self._attempt(attempt_id)
            if row:
                row.update({"status": "REJECTED", "updated_at": updated, "error": error})
            return []
        if "status='UNKNOWN'" in q:
            updated, error, attempt_id = p
            row = self._attempt(attempt_id)
            if row:
                row.update({"status": "UNKNOWN", "updated_at": updated, "error": error})
            return []
        if "status='ACKNOWLEDGED'" in q:
            orderno, updated, attempt_id = p
            row = self._attempt(attempt_id)
            if row:
                row.update({"orderno": orderno, "status": "ACKNOWLEDGED", "updated_at": updated})
            return [{"orderno": row["orderno"]}] if row else []
        if "SET status=%s" in q:
            status, updated, attempt_id = p
            row = self._attempt(attempt_id)
            if row:
                row.update({"status": status, "updated_at": updated})
            return []
        self.unsupported.append(q)
        raise UnsupportedStatement(q)

    def _attempt(self, attempt_id) -> Optional[dict[str, Any]]:
        return next((r for r in self.order_attempts if r["id"] == attempt_id), None)

    def _update_order_log(self, q: str, p: tuple) -> list[dict[str, Any]]:
        # `_set_leg_orderno`: write once, keep the first.
        match = re.search(r"SET (\w+)=COALESCE\(\1, %s\) WHERE id=%s", q)
        if match:
            column = match.group(1)
            value, row_id = p
            row = self.order_log(row_id)
            if row is None:
                return []
            if row.get(column) is None:
                row[column] = value
            return [{"ordno": row[column]}] if "RETURNING" in q else []

        # `_set_leg_exit_orderno`: a changed order number invalidates the
        # exchange id captured for the previous one.
        match = re.search(r"SET (\w+) = CASE WHEN (\w+) IS DISTINCT FROM", q)
        if match:
            xoid_col, ord_col = match.group(1), match.group(2)
            ordno, *_checks, row_id = p
            row = self.order_log(row_id)
            if row is None:
                return []
            if row.get(ord_col) != ordno:
                for field in (xoid_col, ord_col.replace("orderno", "state"),
                              ("time1_exit" if ord_col.startswith("buy") else "time2_exit"),
                              ord_col.replace("orderno", "fill_qty"),
                              ord_col.replace("orderno", "avg_price")):
                    row[field] = None
            row[ord_col] = ordno
            return [{"ordno": row[ord_col]}]

        # `_reconcile_exact_order`: the REST fallback for a lost WS update.
        if "THEN" in q and "ELSE %s END" in q and "WHERE id=%s AND" in q:
            state_col = re.search(r"SET (\w+)=CASE", q).group(1)
            time_col = re.search(r", (\w+)=CASE", q).group(1)
            xoid_col = re.search(r", (\w+)=COALESCE\(\1,%s\)", q).group(1)
            fill_col = re.search(r",(\w+)=COALESCE\(%s,\1\)", q).group(1)
            avg_col = re.search(r", (\w+)=COALESCE\(%s,\1\)", q).group(1)
            ord_col = re.search(r"WHERE id=%s AND (\w+)=%s", q).group(1)
            state, stamp, xoid, fill, avg, row_id, ordno = p
            row = self.order_log(row_id)
            if row is None or str(row.get(ord_col) or "") != str(ordno):
                return []
            if str(row.get(state_col) or "").upper() not in TERMINAL_STATES:
                row[state_col] = state
                row[time_col] = stamp
            if row.get(xoid_col) is None:
                row[xoid_col] = xoid
            if fill is not None:
                row[fill_col] = fill
            if avg is not None:
                row[avg_col] = avg
            return []

        if "lifecycle_state='OPEN'" in q and "lifecycle_state='ENTRY_PENDING'" in q:
            (row_id,) = p
            row = self.order_log(row_id)
            if row is None or row.get("lifecycle_state") != "ENTRY_PENDING":
                return []
            row["lifecycle_state"] = "OPEN"
            return [{"id": row_id}]

        if "lifecycle_state='CLOSED'" in q and "IN ('EXIT_PENDING','QUARANTINED')" in q:
            (row_id,) = p
            row = self.order_log(row_id)
            if row is None or row.get("lifecycle_state") not in {"EXIT_PENDING", "QUARANTINED"}:
                return []
            row["lifecycle_state"] = "CLOSED"
            return [{"id": row_id}]

        if "lifecycle_state='CLOSED'" in q and "buy_orderno=%s AND sell_orderno=%s" in q:
            buy_ordno, sell_ordno = p
            for row in self.order_logs:
                if row.get("buy_orderno") == buy_ordno and row.get("sell_orderno") == sell_ordno:
                    row["lifecycle_state"] = "CLOSED"
            return []

        # A guard on the current state is honoured, as the real UPDATE would.
        match = re.search(
            r"SET lifecycle_state='(\w+)' WHERE id=%s"
            r"(?: AND lifecycle_state='(\w+)'| AND lifecycle_state IS DISTINCT FROM '(\w+)')?", q)
        if match:
            (row_id,) = p
            row = self.order_log(row_id)
            if row is not None:
                current = row.get("lifecycle_state")
                only_from, never_from = match.group(2), match.group(3)
                if (only_from is None or current == only_from) and \
                        (never_from is None or current != never_from):
                    row["lifecycle_state"] = match.group(1)
            return []

        self.unsupported.append(q)
        raise UnsupportedStatement(q)

    def _update_trade_log(self, q: str, p: tuple) -> list[dict[str, Any]]:
        # Both managers close a position with the same claim-the-row update:
        # is_in_trade FALSE *and* trade_state CLOSED, guarded on the row still
        # being open. Matched on what the statement does rather than on its
        # spacing, and applying only what the statement actually sets - a
        # branch here that helpfully filled in a trade_state the real UPDATE
        # never wrote is what hid an exited TEST trade still reading OPEN.
        squashed = q.replace(" ", "")
        if "is_in_trade=FALSE" in squashed and "trade_state='CLOSED'" in squashed:
            if "realized_pnl=%s" in squashed:
                win, exit_price, exit_time, realized_pnl, trade_id = p
            else:
                (win, exit_price, exit_time, trade_id), realized_pnl = p, None
            row = self._trade(trade_id)
            if row is None or not row.get("is_in_trade"):
                return []
            row.update({"is_in_trade": False, "trade_state": "CLOSED",
                        "win": win, "exit_price": exit_price, "exit_time": exit_time,
                        "realized_pnl": realized_pnl})
            return [{"id": trade_id}]
        if "trade_state='RECOVERY_FLAT'" in q:
            (trade_id,) = p
            row = self._trade(trade_id)
            if row is not None and row.get("is_in_trade"):
                row.update({"is_in_trade": False, "trade_state": "RECOVERY_FLAT"})
            return []
        match = re.search(r"SET trade_state='(\w+)' WHERE id=%s"
                          r"(?: AND (is_in_trade=TRUE|trade_state='(\w+)'))?", q)
        if match:
            (trade_id,) = p
            row = self._trade(trade_id)
            guard, only_from = match.group(2), match.group(3)
            if row is not None and (guard is None
                                    or (guard == "is_in_trade=TRUE" and row.get("is_in_trade"))
                                    or (only_from is not None and row.get("trade_state") == only_from)):
                row["trade_state"] = match.group(1)
            return []
        self.unsupported.append(q)
        raise UnsupportedStatement(q)

    def _trade(self, trade_id) -> Optional[dict[str, Any]]:
        return next((r for r in self.trade_logs if r["id"] == trade_id), None)

    # -- selects -----------------------------------------------------------
    def _select_attempts(self, q: str, p: tuple) -> list[dict[str, Any]]:
        if "WHERE order_log_id=%s AND phase IN ('EXIT','FLATTEN')" in q:  # `_realized_pnl`
            rows = [r for r in self.order_attempts if r.get("order_log_id") == p[0]
                    and r.get("phase") in ("EXIT", "FLATTEN") and r.get("orderno")]
            return [dict(r) for r in sorted(rows, key=lambda r: r["id"])]
        live = {"INTENT", "UNKNOWN", "ACKNOWLEDGED", "OPEN", "PENDING", "TRIGGER_PENDING"}
        rows = [r for r in self.order_attempts if str(r.get("status")) in live]
        return [dict(r) for r in sorted(rows, key=lambda r: r["id"])[:20]]

    def _select_order_logs(self, q: str, p: tuple) -> list[dict[str, Any]]:
        # Restart recovery: anything mid-flight, or an OPEN row with no trade.
        if "lifecycle_state IN ('ENTRY_PENDING','EXIT_PENDING','VERIFYING_FLAT','QUARANTINED')" in q:
            pending = {"ENTRY_PENDING", "EXIT_PENDING", "VERIFYING_FLAT", "QUARANTINED"}
            found = []
            for row in self.order_logs:
                state = row.get("lifecycle_state")
                if state in pending:
                    found.append(row)
                elif state == "OPEN" and not any(
                    t.get("is_in_trade")
                    and t.get("buy_orderno") == row.get("buy_orderno")
                    and t.get("sell_orderno") == row.get("sell_orderno")
                    for t in self.trade_logs
                ):
                    found.append(row)
            if not found:
                return []
            newest = max(found, key=lambda r: r["id"])
            return [{k: newest.get(k) for k in ("symbol1", "symbol2", "lifecycle_state")}]

        if "WHERE buy_orderno=%s OR sell_orderno=%s" in q:
            ordno = p[0]
            matches = [
                row for row in self.order_logs
                if ordno in (row.get("buy_orderno"), row.get("sell_orderno"),
                             row.get("buy_exit_orderno"), row.get("sell_exit_orderno"))
            ]
            return [{"id": max(matches, key=lambda r: r["id"])["id"]}] if matches else []

        if q.startswith("SELECT symbol1, symbol2 FROM botdata.order_logs WHERE id=%s"):
            row = self.order_log(p[0])
            return [{"symbol1": row["symbol1"], "symbol2": row["symbol2"]}] if row else []

        if "symbol1 AS buy_symbol" in q:
            rows = [r for r in self.order_logs if r.get("symbol1") or r.get("symbol2")]
            if not rows:
                return []
            newest = max(rows, key=lambda r: r["id"])
            return [{"buy_symbol": newest["symbol1"], "sell_symbol": newest["symbol2"]}]

        if "buy_exit_avg_price" in q:  # `_fetch_order_log_row`
            row = self.order_log(p[0])
            return [dict(row)] if row else []

        if q.startswith("SELECT state1, state2,"):  # `_has_nonterminal_orders`
            rows = self._rows_for_pair(p[0], p[1])
            if not rows:
                return []
            newest = max(rows, key=lambda r: r["id"])
            keys = ("state1", "state2", "buy_exit_state", "sell_exit_state",
                    "buy_orderno", "sell_orderno", "buy_exit_orderno", "sell_exit_orderno")
            return [{k: newest.get(k) for k in keys}]

        if q.startswith("SELECT id FROM botdata.order_logs"):
            rows = self._rows_for_pair(p[0], p[1])
            return [{"id": max(rows, key=lambda r: r["id"])["id"]}] if rows else []

        self.unsupported.append(q)
        raise UnsupportedStatement(q)

    def _select_trade_logs(self, q: str, p: tuple) -> list[dict[str, Any]]:
        if q.startswith("SELECT buy_symbol, sell_symbol"):
            rows = [r for r in self.open_trades if r.get("execution_mode") == "LIVE"]
            if not rows:
                return []
            newest = max(rows, key=lambda r: r["id"])
            return [{"buy_symbol": newest["buy_symbol"], "sell_symbol": newest["sell_symbol"]}]

        if "WHERE id=%s AND is_in_trade=TRUE" in q:
            row = self._trade(p[0])
            if row is None or not row.get("is_in_trade"):
                return []
            return [{k: row.get(k) for k in ("buy_symbol", "sell_symbol", "sell_entry_side")}]

        if "WHERE execution_id=%s" in q:
            rows = [r for r in self.trade_logs if r.get("execution_id") == p[0]]
            return [{"id": rows[0]["id"]}] if rows else []

        if "execution_mode='TEST'" in q or "execution_mode='LIVE'" in q:
            mode = "TEST" if "execution_mode='TEST'" in q else "LIVE"
            rows = [r for r in self.open_trades if r.get("execution_mode") == mode]
            rows.sort(key=lambda r: r["id"], reverse=True)
            limit = 2 if "LIMIT 2" in q else 1
            return [dict(r) for r in rows[:limit]]

        self.unsupported.append(q)
        raise UnsupportedStatement(q)


class FakeCursor:
    def __init__(self, db: FakeDB) -> None:
        self.db = db
        self._rows: list[dict[str, Any]] = []

    def execute(self, statement, params=None) -> None:
        self._rows = self.db.execute(statement, params, fetch=True) or []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self) -> None:
        self._rows = []


class FakeConnection:
    """Raw connection for the two atomic commits, with real commit/rollback."""

    def __init__(self, db: FakeDB) -> None:
        self.db = db
        self.commits = 0
        self.rollbacks = 0

    @contextmanager
    def cursor(self):
        cur = FakeCursor(self.db)
        try:
            yield cur
        finally:
            cur.close()

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None


# --------------------------------------------------------------------------- #
#  Broker (and the WebSocket that reports its orders)
# --------------------------------------------------------------------------- #
class FakeBroker:
    """Accepts orders, keeps a position book, and pushes order updates.

    The push is the important part. `TradeManagerLive` never writes an order's
    live state itself - it places the order and then *waits* for
    `botdata.order_logs.state1` / `buy_exchordid` to appear, which in production
    only happens when `main.py`'s order-update handler receives a WebSocket
    message. `ws=False` keeps the order accepted but the update silent, which is
    the shape of a stalled feed: the leg never confirms and the entry times out.
    """

    def __init__(
        self,
        db: FakeDB,
        *,
        ws: bool = True,
        reject: Iterable[str] = (),
        reject_side: Optional[str] = None,
        stat_not_ok: bool = False,
        positions: Optional[dict[str, int]] = None,
        fill_ratio: float = 1.0,
        free_margin: float = 250_000.0,
        limits_ok: bool = True,
        history: bool = True,
        freeze_qty: int = 650,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        # `history=False` removes the REST order-history fallback the manager
        # falls back on when a WebSocket update is lost. With both gone, an
        # order's outcome is genuinely unknowable and the entry must fail safe.
        self.history = history
        self.freeze_qty = freeze_qty
        self.db = db
        self.ws = ws
        # Order-update timestamps must land strictly after the manager's own
        # pre-placement stamp, or `_wait_leg_status`'s `after_ts` guard rejects
        # the update as stale. Both sides read the same clock.
        self.clock = clock or ist.now
        self.reject = set(reject)
        self.reject_side = reject_side
        self.stat_not_ok = stat_not_ok
        self.positions: dict[str, int] = dict(positions or {})
        self.fill_ratio = float(fill_ratio)
        self.free_margin = float(free_margin)
        self.limits_ok = limits_ok

        self.orders: list[dict[str, Any]] = []
        self.cancelled: list[str] = []
        self._seq = 0
        self.last_price = 24_350.0

    # -- placement ---------------------------------------------------------
    def searchscrip(self, exchange, searchtext):
        return {"values": [{"tsym": searchtext, "token": searchtext}]}

    def get_security_info(self, exchange, token):
        return {"stat": "Ok", "tsym": token, "ls": str(lot_of(token)), "ti": "0.05",
                "frzqty": str(self.freeze_qty)}

    def get_quotes(self, exchange, token):
        return {"stat": "Ok", "tsym": token, "ti": "0.05", "ls": "1",
                "bp1": "99.95", "sp1": "100.05"}

    def place_order(self, **kwargs) -> dict[str, Any]:
        symbol = kwargs["tradingsymbol"]
        side = kwargs["buy_or_sell"]
        qty = int(kwargs["quantity"])
        remarks = kwargs.get("remarks")
        # A partial fill is an *entry* condition. A flatten is the
        # recovery from one, and modelling it as also partial would leave the
        # rollback permanently unable to reach zero for reasons the bot does
        # not have.
        ratio = 1.0 if str(remarks).startswith("NBF") else self.fill_ratio

        self._seq += 1
        orderno = f"FAKE{self._seq:06d}"
        record = {
            "orderno": orderno, "symbol": symbol, "side": side, "qty": qty,
            "remarks": remarks, "status": "COMPLETE", "filled": 0,
        }

        if symbol in self.reject or (self.reject_side and side == self.reject_side):
            record["status"] = "REJECTED"
            self.orders.append(record)
            if self.ws:
                self._push(orderno, symbol, "REJECTED", 0, 0.0)
            return {"stat": "Ok", "norenordno": orderno}

        if self.stat_not_ok:
            # Accepted by the exchange but the acknowledgement is lost. The
            # manager must recover it from the order book, never resubmit.
            self.orders.append(record)
            filled = int(qty * ratio)
            record["filled"] = filled
            self._apply_position(symbol, side, filled)
            if self.ws:
                self._push(orderno, symbol, "COMPLETE", filled, self.last_price)
            return {"stat": "Not_Ok", "emsg": "acknowledgement lost"}

        self.orders.append(record)
        filled = int(qty * ratio)
        record["filled"] = filled
        self._apply_position(symbol, side, filled)
        if self.ws:
            self._push(orderno, symbol, "COMPLETE", filled, self.last_price)
        return {"stat": "Ok", "norenordno": orderno}

    def _apply_position(self, symbol: str, side: str, qty: int) -> None:
        delta = qty if side == "B" else -qty
        self.positions[symbol] = self.positions.get(symbol, 0) + delta

    # -- the WebSocket order-update handler, as main.py writes it ----------
    def _push(self, orderno: str, tsym: str, status: str, fill: int, avg: float) -> None:
        """Simulate immediate broker fills for trade-manager order-path checks.

        The production writer's exact attempt matching is checked separately
        in `test_shutdown_and_feed.py`; this fake pushes before HTTP returns.
        """
        rows = self.db.order_logs
        if not rows:
            return
        leg_columns = {
            "buy": ("buy_orderno", "state1", "time1", "buy_exchordid", "buy_fill_qty", "buy_avg_price"),
            "sell": ("sell_orderno", "state2", "time2", "sell_exchordid", "sell_fill_qty", "sell_avg_price"),
            "buy_exit": ("buy_exit_orderno", "buy_exit_state", "time1_exit",
                         "buy_exit_exchordid", "buy_exit_fill_qty", "buy_exit_avg_price"),
            "sell_exit": ("sell_exit_orderno", "sell_exit_state", "time2_exit",
                          "sell_exit_exchordid", "sell_exit_fill_qty", "sell_exit_avg_price"),
        }

        target: Optional[tuple[dict[str, Any], str]] = None
        for row in sorted(rows, key=lambda r: r["id"], reverse=True):
            for leg, columns in leg_columns.items():
                if row.get(columns[0]) == orderno:
                    target = (row, leg)
                    break
            if target:
                break

        if target is None:
            for row in sorted(rows, key=lambda r: r["id"], reverse=True):
                if row.get("symbol1") == tsym:
                    leg = "buy" if not row.get("buy_orderno") else (
                        "buy_exit" if not row.get("buy_exit_orderno") else None
                    )
                elif row.get("symbol2") == tsym:
                    leg = "sell" if not row.get("sell_orderno") else (
                        "sell_exit" if not row.get("sell_exit_orderno") else None
                    )
                else:
                    continue
                if leg is None:
                    continue
                row[leg_columns[leg][0]] = orderno
                target = (row, leg)
                break

        if target is None:
            return
        row, leg = target
        _ord, state_col, time_col, xoid_col, fill_col, avg_col = leg_columns[leg]
        row[state_col] = status
        row[time_col] = self.clock()
        if row.get(xoid_col) is None:
            row[xoid_col] = f"X{orderno}"
        row[fill_col] = fill
        row[avg_col] = avg

    # -- read APIs ---------------------------------------------------------
    def get_positions(self):
        return [{"tsym": sym, "netqty": qty} for sym, qty in self.positions.items()]

    def get_limits(self, product_type=None, segment=None, exchange=None):
        if not self.limits_ok:
            return {"stat": "Not_Ok", "emsg": "limits unavailable"}
        return {
            "stat": "Ok", "cash": f"{self.free_margin}", "collateral": "0",
            "marginused": "0", "pendordval": "0",
        }

    def single_order_history(self, orderno):
        if not self.history:
            return []
        record = next((o for o in self.orders if o["orderno"] == orderno), None)
        if record is None:
            return []
        return [{
            "norenordno": orderno, "status": record["status"],
            "exchordid": f"X{orderno}",
            "fillshares": record["filled"],
            "avgprc": self.last_price,
        }]

    def get_order_book(self):
        return [
            {"norenordno": o["orderno"], "remarks": o["remarks"],
             "status": o["status"], "tsym": o["symbol"]}
            for o in self.orders
        ]

    def get_trade_book(self):
        return self.get_order_book()

    def cancel_order(self, orderno):
        self.cancelled.append(orderno)
        return {"stat": "Ok"}


def lot_of(symbol: str) -> int:
    """The lot of the index a tradingsymbol belongs to, as the broker reports it.

    Longest root first, so NIFTYNXT50 is not read as NIFTY.
    """
    return (instruments.of_symbol(symbol) or instruments.get("NIFTY")).quantity


class FakeSymbols:
    """`GetSymbol` as the trade managers consume it, for one index.

    The legs are a synthetic long future, as `GetSymbol` builds them: the
    call bought and the put sold, one strike and expiry.
    """

    BUY_LEG = "NIFTY14AUG26C24400"
    SELL_LEG = "NIFTY14AUG26P24400"

    def __init__(self, *, ready: bool = True, bias: str = "HH",
                 sell_side: str = "S", reason: Optional[str] = None,
                 root: str = "NIFTY") -> None:
        self.ready = ready
        self.bias = bias
        self.sell_side = sell_side
        self.legs_blocked_reason = reason if not ready else None
        self.buy_leg = root + self.BUY_LEG[len("NIFTY"):]
        self.sell_leg = root + self.SELL_LEG[len("NIFTY"):]

    @property
    def final_symbols(self) -> Optional[dict[str, Any]]:
        if not self.ready:
            return None
        return {"buy": self.buy_leg, "sell": self.sell_leg,
                "sell_side": self.sell_side, "bias": self.bias,
                "built_at": time.monotonic()}

    @property
    def final_symbol(self) -> Optional[str]:
        return self.sell_leg if self.ready else None


# --------------------------------------------------------------------------- #
#  A strategy that wants a specific trade
# --------------------------------------------------------------------------- #
class ScriptedRuntime:
    """What the decider reads off a strategy, driven by the check instead of by bars."""

    def __init__(self) -> None:
        self.intent: Optional[dict[str, Any]] = None
        self.open_price: Optional[float] = None

    def entry_intent(self, now=None):
        return dict(self.intent) if self.intent else None


class ScriptedService:
    """Stands in for the `StrategyEngine` - one scripted intent, no allocation."""

    def __init__(self, runtime: Optional[ScriptedRuntime] = None) -> None:
        self.runtime = runtime or ScriptedRuntime()

    def entry_intent(self, now=None):
        return self.runtime.entry_intent(now)

    def session_open_price(self, index, day):
        return self.runtime.open_price


# --------------------------------------------------------------------------- #
#  Log capture
# --------------------------------------------------------------------------- #
class LogCapture(logging.Handler):
    """Collects records so a check can prove a refusal was actually announced.

    The defect this whole script exists for was a skipped entry logged at
    DEBUG, which `main.py`'s INFO handler discarded: a bot that refused to trade
    all day looked exactly like a strategy with no signal.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def clear(self) -> None:
        self.records = []

    def messages(self, minimum: int = logging.DEBUG) -> list[str]:
        out = []
        for record in self.records:
            if record.levelno < minimum:
                continue
            try:
                out.append(record.getMessage())
            except Exception:  # noqa: BLE001 - a bad format string is itself a defect
                out.append(f"<unformattable {record.name}>")
        return out

    def has(self, needle: str, minimum: int = logging.INFO) -> bool:
        return any(needle.lower() in m.lower() for m in self.messages(minimum))

    @contextmanager
    def attached(self, *loggers: logging.Logger):
        previous = [(lg, lg.level, lg.disabled) for lg in loggers]
        # `run_checks.py` silences the suite with a global `logging.disable`,
        # which is checked before a record ever reaches a handler. Lifting it
        # here is what lets a check assert that a refusal *was* announced; it
        # cannot hide one, because the capture is the only handler attached.
        globally_disabled = logging.root.manager.disable
        logging.disable(logging.NOTSET)
        for lg in loggers:
            lg.addHandler(self)
            lg.setLevel(logging.DEBUG)
            lg.disabled = False
        try:
            yield self
        finally:
            for lg, level, disabled in previous:
                lg.removeHandler(self)
                lg.setLevel(level)
                lg.disabled = disabled
            logging.disable(globally_disabled)


# --------------------------------------------------------------------------- #
#  v3 on recorded sessions
# --------------------------------------------------------------------------- #
#: Sessions from v3's supplied data that hold every kind of v3 trade, early
#: enough in the history that the market loads in seconds:
#:   2015-01-12  B signalled on the 14:45 candle, filled at the 14:50 open
#:   2015-02-10  A stopped out, then B filled at 14:55 and sold the next 09:15
#:   2015-06-29  a shock session: A filled at 14:15 and sold at the 14:55 open
#:   2015-07-06  a shock session again
SESSIONS = ("2015-01-12", "2015-02-10", "2015-06-29", "2015-07-06")
SESSIONS_THROUGH = "2015-07-31"


@functools.lru_cache(maxsize=1)
def v3_market():
    """v3's own market through `SESSIONS_THROUGH`. Shared: never mutate it."""
    return strategy_logic.V2.Market(data_root=strategy_logic.DATA_DIR, through=SESSIONS_THROUGH)


@functools.lru_cache(maxsize=1)
def v3_trades():
    """v3's own backtest over that market - what every live replay must match."""
    m = v3_market()
    return strategy_logic.V3.backtest(m, strategy_logic.V3.build_slots(m))


def day_index(m, day: str) -> int:
    return int(m.cal.get_loc(pd.Timestamp(day)))


def candle_close(day: str, candles: int) -> datetime:
    """The moment `candles` candles of `day` have closed, plus a beat."""
    return datetime.combine(date.fromisoformat(day), dt_time(9, 15)) + timedelta(
        minutes=BAR_MINUTES * candles, seconds=2)


def live_view(m, d: int, candles: int):
    """The market a live process holds once `candles` candles of session `d` closed.

    What `live_v3.market` builds from files, made in memory: history through
    the day before, then the day's first `candles` candles. Daily features are
    shifted one session, so the day's own are already final.
    """
    x = copy.copy(m)
    lo = max(0, d - 10)
    x.cal = m.cal[lo:d + 1]
    for name in "OHLC":
        arr = getattr(m, name)[:, lo:d + 1].copy()
        arr[:, -1, candles:] = np.nan
        setattr(x, name, arr)
    for name in ("atr", "sma", "pC", "available"):
        setattr(x, name, getattr(m, name)[:, lo:d + 1])
    x.valid = m.valid[:, lo:d + 1].copy()
    x.valid[:, -1] = np.isfinite(x.O[:, -1, 0])
    return x


def write_live_root(root: Path, day: str) -> Path:
    """`root` laid out like `backups/data_live` on the morning of `day`.

    The supplied history before `day` is linked, never copied. The daily
    reference is cut to the sessions before `day`, as it is live: the supplied
    one ends on 2026-09-04 and never holds the session being traded. Use a
    fresh root per session: `write_candles` leaves real files behind.
    """
    src = strategy_logic.DATA_DIR
    stamp = date.fromisoformat(day).strftime("%Y_%m_%d")
    for folder in strategy_logic.V2.BASE.DATA_DIRS.values():
        (root / folder).mkdir(parents=True, exist_ok=True)
        for f in (src / folder).glob("*_5m.csv"):
            if f.stem[:10] < stamp:
                (root / folder / f.name).symlink_to(f)
    for sub in ("daily_ref", "warmup_extra"):
        (root / sub).mkdir()
        for f in (src / sub).glob("*.csv"):
            ref = pd.read_csv(f)
            ref[ref.Date < day].to_csv(root / sub / f.name, index=False)
    return root


def write_candles(root: Path, day: str, candles: int) -> None:
    """`day`'s first `candles` candles for every index, as `live_v3.build` writes them."""
    src = strategy_logic.DATA_DIR
    stamp = date.fromisoformat(day).strftime("%Y_%m_%d")
    for folder in strategy_logic.V2.BASE.DATA_DIRS.values():
        today, out = src / folder / f"{stamp}_5m.csv", root / folder / f"{stamp}_5m.csv"
        out.unlink(missing_ok=True)
        if today.exists() and candles:
            out.write_text("\n".join(today.read_text().splitlines()[: candles + 1]) + "\n")


# --------------------------------------------------------------------------- #
#  Shoonya's candle API, from recorded sessions
# --------------------------------------------------------------------------- #
#: (exchange, token) of each index's spot feed, as main.py resolves them.
SPOT_TOKENS = {"NIFTY": ("NSE", "26000"), "BANKNIFTY": ("NSE", "26009"),
               "FINNIFTY": ("NSE", "26037"), "MIDCPNIFTY": ("NSE", "26074"),
               "NIFTYNXT50": ("NSE", "26013"), "SENSEX": ("BSE", "1")}


class FakeShoonya:
    """`get_time_price_series` as Shoonya answers it, from recorded sessions.

    One row per candle, newest first, the time as `dd-mm-YYYY HH:MM:SS` IST
    wall clock and every number a string; only candles that have started by
    the clock, the one still forming included; None for "no data". Sessions
    come from the tested history, then the live cache's own files.
    """

    def __init__(self, clock: Callable[[], datetime], *, skip: Iterable[datetime] = ()) -> None:
        self.clock = clock
        self.skip = set(skip)               # candles the broker leaves out
        self.calls: list[tuple[str, date]] = []
        self.names = {token: name for name, (_exchange, token) in SPOT_TOKENS.items()}

    def _session(self, name: str, day: date) -> pd.DataFrame:
        stamp = f"{day:%Y_%m_%d}_5m.csv"
        if name in strategy_logic.V2.BASE.DATA_DIRS:
            folder = strategy_logic.V2.BASE.DATA_DIRS[name]
            for root in (strategy_logic.DATA_DIR, strategy_logic.LIVE_DIR):
                path = root / folder / stamp
                if path.exists():
                    return pd.read_csv(path, parse_dates=["Datetime"])
            return pd.DataFrame(columns=["Datetime"])
        else:
            bars = pd.read_parquet(strategy_logic.LIVE_DIR / "indices_5m" / f"{name}.parquet")
        return bars[bars.Datetime.dt.date == day]

    def get_time_price_series(self, exchange, token, starttime, endtime=None, interval=5,
                              warn_on_error=True):
        name = self.names[str(token)]
        start = ist.from_epoch(starttime).replace(tzinfo=None)
        end = self.clock() if endtime is None else ist.from_epoch(endtime).replace(tzinfo=None)
        rows = []
        for day in pd.date_range(start.date(), end.date()):
            self.calls.append((name, day.date()))
            bars = self._session(name, day.date())
            rows += [bar for bar in bars.itertuples()
                     if start <= bar.Datetime <= min(end, self.clock())
                     and bar.Datetime.to_pydatetime() not in self.skip]
        if not rows:
            return None
        return [{"stat": "Ok", "time": bar.Datetime.strftime("%d-%m-%Y %H:%M:%S"),
                 "into": str(bar.Open), "inth": str(bar.High), "intl": str(bar.Low),
                 "intc": str(bar.Close), "intv": "0", "intoi": "0"}
                for bar in reversed(rows)]
