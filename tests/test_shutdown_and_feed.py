"""The two faults the 2026-08-17 log showed, other than the missed entry.

  WriterDrainTests   - 84 `DB insert failed` lines in one 15:30 stop, because
                       the connection pool closed while the writers were still
                       draining. The rows were lost, not just the log lines.
  MarketHoursTests   - 27 `no data` / `5m refresh returned no rows` warnings,
                       24 of them before the session had opened, because the
                       candle refresh polls every 5 minutes round the clock.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import db_loggers
import market_hours_runner
from config import DATA_SETTINGS
from data_handler import DataHandler
from db_loggers import ForecastLogger, WebsocketOrderUpdateLogger
from main import _update_symbol_periodically, _watch_trade_thread
from trade_manager import TradeManagerLive, TradeManagerTest
from tests.log_capture import capture_logs


class TradeShutdownTests(unittest.TestCase):
    def test_stop_finishes_both_manager_loops_after_current_cycle(self) -> None:
        for manager_type, cycle_name in (
            (TradeManagerLive, "_cycle_once"),
            (TradeManagerTest, "_cycle_once"),
        ):
            with self.subTest(manager=manager_type.__name__):
                manager = manager_type.__new__(manager_type)
                manager._stop_requested = threading.Event()
                manager.poll_interval = 0
                calls = []

                async def cycle():
                    calls.append(1)
                    manager.stop()

                setattr(manager, cycle_name, cycle)
                asyncio.run(manager._main_loop())
                self.assertEqual(calls, [1])

    def test_persistent_cycle_errors_stop_both_managers(self) -> None:
        for manager_type, cycle_name in (
            (TradeManagerLive, "_cycle_once"),
            (TradeManagerTest, "_cycle_once"),
        ):
            with self.subTest(manager=manager_type.__name__):
                manager = manager_type.__new__(manager_type)
                manager._stop_requested = threading.Event()
                manager.poll_interval = 0
                calls = []

                async def fail():
                    calls.append(1)
                    raise RuntimeError("persistent failure")

                setattr(manager, cycle_name, fail)
                with capture_logs(logging.getLogger()) as captured:
                    with self.assertRaisesRegex(RuntimeError, "persistent failure"):
                        asyncio.run(manager._main_loop())
                self.assertEqual(len(calls), 10)
                self.assertTrue(captured.has("10 consecutive cycles", logging.CRITICAL))

    def test_successful_cycle_resets_error_count(self) -> None:
        for manager_type, cycle_name in (
            (TradeManagerLive, "_cycle_once"),
            (TradeManagerTest, "_cycle_once"),
        ):
            with self.subTest(manager=manager_type.__name__):
                manager = manager_type.__new__(manager_type)
                manager._stop_requested = threading.Event()
                manager.poll_interval = 0
                calls = []

                async def cycle():
                    calls.append(1)
                    if len(calls) in (10, 20):
                        if len(calls) == 20:
                            manager.stop()
                        return
                    raise RuntimeError("intermittent failure")

                setattr(manager, cycle_name, cycle)
                with capture_logs(logging.getLogger()):
                    asyncio.run(manager._main_loop())
                self.assertEqual(len(calls), 20)


class TradeThreadWatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_dead_trade_thread_fails_unless_shutdown_started(self) -> None:
        thread = threading.Thread(target=lambda: None)
        with capture_logs(logging.getLogger()) as captured:
            with self.assertRaisesRegex(RuntimeError, "Trade manager thread exited unexpectedly"):
                await _watch_trade_thread(thread, lambda: False)
        self.assertTrue(captured.has("Trade manager thread exited unexpectedly", logging.CRITICAL))
        await _watch_trade_thread(thread, lambda: True)


class SymbolUpdateSchedulingTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_index_does_not_delay_other_legs(self) -> None:
        slow_started = threading.Event()
        release = threading.Event()
        fast_done = threading.Event()

        def slow():
            slow_started.set()
            release.wait(2)

        tasks = [
            asyncio.create_task(_update_symbol_periodically("SENSEX", SimpleNamespace(update_symbol=slow))),
            asyncio.create_task(_update_symbol_periodically("NIFTY", SimpleNamespace(update_symbol=fast_done.set))),
        ]
        try:
            self.assertTrue(await asyncio.to_thread(slow_started.wait, 1))
            self.assertTrue(await asyncio.to_thread(fast_done.wait, 1))
        finally:
            release.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


class _FakeDB:
    """Records writes and can be 'closed' the way the real pool is at shutdown."""

    def __init__(self) -> None:
        self.rows: list[tuple] = []
        self.closed = False
        self.writes_after_close = 0

    def ensure_schema(self) -> None:
        pass

    def execute(self, sql, params=None, fetch=False):
        if self.closed:
            self.writes_after_close += 1
            raise RuntimeError("connection pool is closed")
        self.rows.append(params)
        return []


def _await_started(writer, timeout: float = 5.0) -> None:
    """Block until the writer's loop and queue exist, as the message hook needs."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if writer._loop is not None and writer._queue is not None:
            return
        time.sleep(0.005)
    raise AssertionError(f"{type(writer).__name__} never started")


class WriterDrainTests(unittest.TestCase):
    """`stop()` must finish the accepted rows and refuse anything after."""

    def _run(self, writer, feed, count: int):
        thread = threading.Thread(target=writer.start_logging, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 1.0)
        _await_started(writer)
        for index in range(count):
            feed(index)
        return thread

    def test_order_writer_drains_every_queued_row_before_returning(self) -> None:
        db = _FakeDB()
        writer = WebsocketOrderUpdateLogger(db)
        self._run(writer, lambda i: writer.on_order_update({"norenordno": str(i)}), 50)

        self.assertTrue(writer.stop(timeout=5.0), "the writer did not finish draining")
        self.assertEqual(len(db.rows), 50, "queued rows were dropped by the stop")

    def test_nothing_is_written_after_the_pool_closes(self) -> None:
        """The exact 2026-08-17 sequence, in order."""
        db = _FakeDB()
        writer = WebsocketOrderUpdateLogger(db)
        self._run(writer, lambda i: writer.on_order_update({"norenordno": str(i)}), 20)

        self.assertTrue(writer.stop(timeout=5.0))
        db.closed = True  # what `_backup_and_clear` does next

        for index in range(20):
            writer.on_order_update({"norenordno": str(index)})
        time.sleep(0.1)

        self.assertEqual(db.writes_after_close, 0,
                         "a writer was still writing after the pool closed")

    def test_stop_is_idempotent_and_safe_before_the_loop_starts(self) -> None:
        for writer_type in (WebsocketOrderUpdateLogger,):
            with self.subTest(writer=writer_type.__name__):
                writer = writer_type(_FakeDB())
                self.assertTrue(writer.stop(timeout=0.1), "stop must not hang on an unstarted writer")
                self.assertTrue(writer.stop(timeout=0.1))
                asyncio.run(asyncio.wait_for(writer.start_logging_async(queue_maxsize=1), 0.2))
        forecast = ForecastLogger(object(), pivot_provider=lambda: {})
        self.assertTrue(forecast.stop(timeout=0.1))
        asyncio.run(asyncio.wait_for(forecast.start_logging_async(), 0.2))

    def test_order_update_writer_drains_too(self) -> None:
        db = _FakeDB()
        writer = WebsocketOrderUpdateLogger(db)
        self._run(writer, lambda i: writer.on_order_update({"norenordno": str(i)}), 25)

        self.assertTrue(writer.stop(timeout=5.0))
        self.assertEqual(len(db.rows), 25)

        db.closed = True
        writer.on_order_update({"norenordno": "late"})
        time.sleep(0.1)
        self.assertEqual(db.writes_after_close, 0)

    def test_full_queue_stop_keeps_pending_row_and_arrival_time(self) -> None:
        class PausingDB(_FakeDB):
            def __init__(self):
                super().__init__()
                self.started = threading.Event()
                self.release = threading.Event()

            def execute(self, sql, params=None, fetch=False):
                self.started.set()
                self.release.wait(3)
                return super().execute(sql, params, fetch)

        for writer_type, feed in (
            (WebsocketOrderUpdateLogger,
             lambda writer, n: writer.on_order_update({"norenordno": str(n), "status": "OPEN"})),
        ):
            with self.subTest(writer=writer_type.__name__):
                db = PausingDB()
                self.addCleanup(db.release.set)
                writer = writer_type(db)
                thread = threading.Thread(target=lambda: writer.start_logging(queue_maxsize=1), daemon=True)
                thread.start()
                self.addCleanup(thread.join, 1.0)
                _await_started(writer)

                received_at = dt.datetime(2026, 9, 24, 9, 0, tzinfo=dt.timezone.utc)
                clock = [received_at]
                with patch.object(db_loggers, "_now_ist", side_effect=lambda: clock[0]):
                    feed(writer, 1)
                    self.assertTrue(db.started.wait(1))
                    feed(writer, 2)
                    queued = threading.Event()
                    writer._loop.call_soon_threadsafe(queued.set)
                    self.assertTrue(queued.wait(1))
                    self.assertEqual(writer._queue.qsize(), 1)
                    clock[0] += dt.timedelta(minutes=1)

                    marker_called = threading.Event()
                    original = writer._signal_end_of_stream

                    def marked():
                        original()
                        marker_called.set()

                    writer._signal_end_of_stream = marked
                    stopped = []
                    stopper = threading.Thread(target=lambda: stopped.append(writer.stop(timeout=4)), daemon=True)
                    stopper.start()
                    self.assertTrue(marker_called.wait(1))
                    db.release.set()
                    stopper.join(4)
                    thread.join(4)

                self.assertEqual(stopped, [True])
                self.assertEqual(len(db.rows), 2)
                self.assertEqual([row[0] for row in db.rows], [received_at, received_at])

    def test_the_drain_announces_what_it_wrote(self) -> None:
        db = _FakeDB()
        writer = WebsocketOrderUpdateLogger(db)
        with capture_logs(logging.getLogger()) as captured:
            self._run(writer, lambda i: writer.on_order_update({"norenordno": str(i)}), 3)
            writer.stop(timeout=5.0)
        self.assertTrue(
            captured.has("writer stopped after draining", logging.INFO),
            f"the stop must be visible; got {captured.text()}",
        )


class MarketHoursTests(unittest.TestCase):
    """No candle can exist outside the session, so nothing should ask for one."""

    def test_runner_outlives_the_final_candle_refresh(self) -> None:
        self.assertEqual(market_hours_runner.END, DATA_SETTINGS.session_close)
        self.assertGreater(market_hours_runner.END, dt.time(15, 30))

    class _API:
        def __init__(self, rows=None) -> None:
            self.rows = rows if rows is not None else []
            self.calls = 0

        def get_time_price_series(self, **_kwargs):
            self.calls += 1
            return list(self.rows)

    def _handler(self, api, at: dt.datetime) -> DataHandler:
        handler = DataHandler(api, "NSE", "26000", now_fn=lambda: at)
        handler.boundary_retry_attempts = 3
        handler.boundary_retry_interval = 0.01
        return handler

    def test_the_session_window_is_the_one_from_config(self) -> None:
        handler = self._handler(self._API(), dt.datetime(2026, 8, 17, 12, 0))
        for hour, minute, expected in (
            (9, 14, False), (9, 15, False), (9, 20, True), (12, 0, True),
            (15, 35, True), (15, 36, False), (23, 0, False), (3, 0, False),
        ):
            with self.subTest(time=f"{hour:02d}:{minute:02d}"):
                moment = dt.datetime(2026, 8, 17, hour, minute)
                self.assertIs(handler._market_can_have_candles(moment), expected)

    def test_startup_outside_hours_makes_no_broker_call(self) -> None:
        """The 22:34 and 00:06 starts on 2026-08-17 both hit the API for nothing."""
        api = self._API()
        handler = self._handler(api, dt.datetime(2026, 8, 17, 22, 34))
        asyncio.run(handler._bootstrap_history())
        self.assertEqual(api.calls, 0)

    def test_startup_inside_hours_still_fetches(self) -> None:
        api = self._API()
        handler = self._handler(api, dt.datetime(2026, 8, 17, 10, 30))
        asyncio.run(handler._bootstrap_history())
        self.assertEqual(api.calls, 1)

    def test_first_completed_candle_gets_a_bounded_retry(self) -> None:
        api = self._API()
        handler = self._handler(api, dt.datetime(2026, 8, 17, 9, 20, 3))
        asyncio.run(handler._fetch_until_new_bar(None))
        self.assertEqual(api.calls, handler.boundary_retry_attempts)

    def test_a_late_bar_is_still_retried(self) -> None:
        """The case the retry budget actually exists for must survive."""
        rows = [{
            "time": "17-08-2026 09:15:00",
            "into": "24000", "inth": "24010", "intl": "23990", "intc": "24005",
        }]
        api = self._API(rows)
        handler = self._handler(api, dt.datetime(2026, 8, 17, 9, 25, 3))
        seen = dt.datetime(2026, 8, 17, 9, 15, tzinfo=handler._today_midnight().tzinfo)
        asyncio.run(handler._fetch_until_new_bar(seen))
        self.assertEqual(api.calls, handler.boundary_retry_attempts,
                         "a bar that is merely late must still be waited for")

    def test_just_closed_bar_is_not_reported_missing(self) -> None:
        # Shoonya timestamps a bar by its opening time. At 12:45, the 12:40
        # bar is present; the 12:45 successor may arrive later.
        rows = [{
            "time": "17-08-2026 12:40:00",
            "into": "24000", "inth": "24010", "intl": "23990", "intc": "24005",
        }]
        api = self._API(rows)
        handler = self._handler(api, dt.datetime(2026, 8, 17, 12, 45, 3))
        seen = dt.datetime(2026, 8, 17, 12, 40, tzinfo=handler._today_midnight().tzinfo)
        with capture_logs(logging.getLogger()) as captured:
            asyncio.run(handler._fetch_until_new_bar(seen))
        self.assertEqual(api.calls, handler.boundary_retry_attempts)
        self.assertEqual(captured.at_least(logging.WARNING), [])

    def test_an_empty_response_in_session_hours_still_warns(self) -> None:
        api = self._API()
        handler = self._handler(api, dt.datetime(2026, 8, 17, 12, 0))
        handler._five_min = [{"time": "x"}]  # the day has been producing candles
        with capture_logs(logging.getLogger()) as captured:
            asyncio.run(handler._refresh_once())
        self.assertTrue(captured.has("returned no rows", logging.WARNING))
        self.assertEqual(handler._five_min, [{"time": "x"}])

    def test_an_empty_response_before_the_open_does_not_warn(self) -> None:
        api = self._API()
        handler = self._handler(api, dt.datetime(2026, 8, 17, 9, 15, 3))
        handler.session_open = dt.time(9, 20)  # not open yet at 09:15:03
        with capture_logs(logging.getLogger()) as captured:
            asyncio.run(handler._refresh_once())
        self.assertEqual(captured.at_least(logging.WARNING), [],
                         f"expected silence before the open; got {captured.text()}")


class OrderLedgerWriterTests(unittest.TestCase):
    """`main`'s own order-update worker is a fourth writer with the same flaw.

    It matters most of the four: `order_logs` / `order_attempts` are the order
    ledger, not diagnostics, so an update lost to a closed pool is a fill the
    bot no longer has a record of.
    """

    @staticmethod
    def _writer(db):
        # Safe to import: `main` no longer configures logging at import time,
        # so this cannot append to the live bot log. See `configure_logging`.
        import main

        return main._make_order_update_logger(db)

    def test_unknown_order_cannot_claim_a_matching_symbol(self) -> None:
        class AttemptDB(_FakeDB):
            def __init__(self):
                super().__init__()
                self.attachments = []

            def execute(self, sql, params=None, fetch=False):
                if "SELECT order_log_id, phase, leg FROM botdata.order_attempts" in sql:
                    return ([{"order_log_id": 7, "phase": "EXIT", "leg": "sell"}]
                            if params == ("bot-order",) else [])
                if "SET sell_exit_orderno=COALESCE" in sql:
                    self.attachments.append(params)
                return super().execute(sql, params, fetch)

        db = AttemptDB()
        writer = self._writer(db)
        writer({"norenordno": "manual-order", "tsym": "SAME_SYMBOL", "status": "COMPLETE"})
        writer({"norenordno": "bot-order", "tsym": "SAME_SYMBOL", "status": "COMPLETE"})
        self.assertTrue(writer.stop(timeout=3))
        self.assertEqual(db.attachments, [("bot-order", 7, "bot-order")])

    def test_update_before_http_ack_waits_for_its_attempt(self) -> None:
        class AckRaceDB(_FakeDB):
            lookups = 0
            attached = False

            def execute(self, sql, params=None, fetch=False):
                if "SELECT order_log_id, phase, leg FROM botdata.order_attempts" in sql:
                    self.lookups += 1
                    return ([{"order_log_id": 9, "phase": "ENTRY", "leg": "buy"}]
                            if self.lookups > 1 else [])
                if "WHERE symbol=%s AND orderno IS NULL" in sql:
                    return [{"?column?": 1}]
                if "SET buy_orderno=COALESCE" in sql:
                    self.attached = True
                return super().execute(sql, params, fetch)

        db = AckRaceDB()
        writer = self._writer(db)
        writer({"norenordno": "early-order", "tsym": "BOT_SYMBOL", "status": "COMPLETE"})
        self.assertTrue(writer.stop(timeout=3))
        self.assertTrue(db.attached)

    def test_unsupported_status_does_not_hide_an_unresolved_attempt(self) -> None:
        db = _FakeDB()
        writer = self._writer(db)
        with capture_logs(logging.getLogger()) as captured:
            writer({"norenordno": "order-3", "status": "UNEXPECTED"})
            self.assertTrue(writer.stop(timeout=3))
        self.assertEqual(db.rows, [])
        self.assertTrue(captured.has("Ignoring unsupported broker order status", logging.WARNING))

    def test_nonfinite_fill_price_is_not_persisted(self) -> None:
        db = _FakeDB()
        writer = self._writer(db)
        writer({"norenordno": "order-4", "status": "COMPLETE", "avgprc": "nan"})
        self.assertTrue(writer.stop(timeout=3))
        updates = [params for params in db.rows if params and len(params) == 6]
        self.assertEqual(len(updates), 5)
        self.assertTrue(all(params[4] is None for params in updates))
        self.assertIsNone(db_loggers._to_float("inf"))

    def test_queued_updates_are_written_before_the_stop_returns(self) -> None:
        db = _FakeDB()
        writer = self._writer(db)
        for index in range(30):
            writer({"norenordno": str(index), "status": "COMPLETE", "tsym": "LEG"})

        self.assertTrue(writer.stop(timeout=5.0), "the ledger writer did not drain")
        before = len(db.rows)

        db.closed = True
        writer({"norenordno": "late", "status": "COMPLETE", "tsym": "LEG"})
        time.sleep(0.1)

        self.assertGreater(before, 0, "no updates were written at all")
        self.assertEqual(db.writes_after_close, 0,
                         "the ledger writer was still writing after the pool closed")

    def test_stop_is_exposed_for_the_shutdown_path(self) -> None:
        writer = self._writer(_FakeDB())
        self.assertTrue(callable(getattr(writer, "stop", None)),
                        "main's shutdown looks for .stop() on every writer")
        self.assertTrue(writer.stop(timeout=2.0))
        self.assertTrue(writer.stop(timeout=0.1), "stop must be idempotent")

    def test_transient_failure_retries_the_same_update(self) -> None:
        class FlakyDB(_FakeDB):
            failed_once = False

            def __init__(self):
                super().__init__()
                self.sql = []

            def execute(self, sql, params=None, fetch=False):
                if "SET state2=%s" in sql and not self.failed_once:
                    self.failed_once = True
                    raise RuntimeError("temporary database error")
                self.sql.append(sql)
                return super().execute(sql, params, fetch)

        db = FlakyDB()
        writer = self._writer(db)
        writer({"norenordno": "order-1", "status": "COMPLETE"})
        self.assertTrue(writer.stop(timeout=3))
        self.assertTrue(db.failed_once)
        self.assertTrue(any("UPDATE botdata.order_attempts" in sql for sql in db.sql))

    def test_persistent_failure_stops_the_writer_and_blocks_backup(self) -> None:
        class BrokenDB(_FakeDB):
            calls = 0

            def execute(self, sql, params=None, fetch=False):
                self.calls += 1
                raise RuntimeError("database unavailable")

        db = BrokenDB()
        writer = self._writer(db)
        with capture_logs(logging.getLogger()) as captured:
            writer({"norenordno": "order-2", "status": "COMPLETE"})
            writer.thread.join(3)
        self.assertFalse(writer.thread.is_alive())
        self.assertEqual(db.calls, 3)
        self.assertTrue(writer.failed.is_set())
        self.assertFalse(writer.stop(timeout=0.1))
        self.assertTrue(captured.has("Order-update writer stopped", logging.CRITICAL))
        with self.assertRaisesRegex(RuntimeError, "Order-update writer exited unexpectedly"):
            asyncio.run(_watch_trade_thread(
                writer.thread, lambda: False, label="Order-update writer", failed=writer.failed
            ))

    def test_queue_keeps_the_websocket_arrival_time(self) -> None:
        class PausingDB(_FakeDB):
            def __init__(self):
                super().__init__()
                self.started = threading.Event()
                self.release = threading.Event()

            def execute(self, sql, params=None, fetch=False):
                if params == ("first",) * 4 and not self.started.is_set():
                    self.started.set()
                    self.release.wait(2)
                return super().execute(sql, params, fetch)

        db = PausingDB()
        writer = self._writer(db)
        self.addCleanup(db.release.set)
        first = dt.datetime(2026, 9, 24, 9, 0, tzinfo=dt.timezone.utc)
        second = first + dt.timedelta(seconds=1)
        clock = [first]
        with patch("main.ist.now", side_effect=lambda: clock[0]):
            writer({"norenordno": "first", "status": "OPEN"})
            self.assertTrue(db.started.wait(1))
            clock[0] = second
            writer({"norenordno": "second", "status": "COMPLETE"})
            clock[0] = second + dt.timedelta(minutes=1)
            db.release.set()
            self.assertTrue(writer.stop(timeout=3))
        self.assertTrue(any(
            params and len(params) == 6 and params[-1] == "second" and params[1] == second
            for params in db.rows
        ))


class DrainTimeoutTests(unittest.TestCase):
    def test_the_timeout_is_bounded_so_shutdown_cannot_hang(self) -> None:
        self.assertGreater(db_loggers.DRAIN_TIMEOUT_SECONDS, 0)
        self.assertLessEqual(db_loggers.DRAIN_TIMEOUT_SECONDS, 15.0)


if __name__ == "__main__":
    unittest.main()
