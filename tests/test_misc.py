"""Small test suites that did not each justify their own file.

Consolidated from `test_db_connector.py`, `test_event_driven_forecast.py`,
the former strategy-logic tests and `test_shoonya_browser_launch.py`. The classes are
unchanged from where they came, except that the browser-launch check was
rewritten from a bare pytest function into a `TestCase` - written in a style
this project has no runner for, it had been collecting zero tests and had
therefore never actually run.

  DatabaseCredentialSafetyTests - DB credentials and timezone-aware columns
  PartialTickPriceTests         - a delta tick must not erase the last price
  ForecastStateTests            - which pivot/leg transitions are real events
  StrategyHistorySafetyTests    - warm-up epochs and duplicate-bar handling
  ServiceBrowserTests           - a browser the LocalSystem service can launch
  BrokerLatencyProbeTests       - the dashboard's ping must not cost what it measures
"""

import asyncio
import gzip
import logging
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import db_connector
import ist
from db_connector import DB_TIMEZONE, TimescaleDBConnector
from db_loggers import _is_transient_leg_regression, _state_key
from db_connector import TABLES
from live_server import BrokerLatencyProbe, create_app
from live_websocket import LiveWebSocket


class LoggingConfigTests(unittest.TestCase):
    def test_both_sinks_filter_explicit_info_loggers(self):
        import main

        with tempfile.TemporaryDirectory() as directory, \
                patch.object(main, "LOG_DIR", Path(directory)), \
                patch("main.logging.basicConfig") as basic_config:
            main.configure_logging()
            handlers = basic_config.call_args.kwargs["handlers"]
            try:
                self.assertEqual(basic_config.call_args.kwargs["level"], logging.WARNING)
                self.assertTrue(all(handler.level == logging.WARNING for handler in handlers))
            finally:
                for handler in handlers:
                    handler.close()


class DashboardHostTests(unittest.TestCase):
    def test_dashboard_accepts_only_local_and_configured_tailscale_hosts(self):
        app = create_app(
            None, "NSE", "1", lambda: 100.0, None, None,
            pivot_service=object(),
        )
        client = app.test_client()
        for host in ("127.0.0.1:5000", "localhost:5000", "free-a1:8080",
                     "free-a1.tail57c420.ts.net:8080", "100.69.232.2:8080"):
            self.assertEqual(client.get("/live_price", headers={"Host": host}).status_code, 200)
        self.assertEqual(
            client.get("/live_price", headers={"Host": "attacker.example:5000"}).status_code,
            400,
        )

    def test_large_responses_are_gzipped_for_clients_that_accept_it(self):
        app = create_app(None, "NSE", "1", lambda: 100.0, None, None, pivot_service=object())
        client = app.test_client()
        headers = {"Host": "localhost:5000", "Accept-Encoding": "gzip"}
        zipped = client.get("/static/Lune-v6.js", headers=headers)
        self.assertEqual("gzip", zipped.headers.get("Content-Encoding"))
        plain = client.get("/static/Lune-v6.js", headers={"Host": "localhost:5000"})
        self.assertNotIn("Content-Encoding", plain.headers)
        self.assertEqual(plain.data, gzip.decompress(zipped.data))
        # Too small to be worth it.
        self.assertNotIn("Content-Encoding", client.get("/live_price", headers=headers).headers)

    def test_price_stream_pushes_every_tick_for_its_own_index(self):
        prices = {"NIFTY": 100.0, "BANKNIFTY": 500.0}
        listeners = []
        app = create_app(
            None, "NSE", "1", lambda: None, None, None, pivot_service=object(),
            price_getters={name: (lambda n=name: prices[n]) for name in prices},
            add_price_listener=listeners.append,
        )
        self.assertEqual(1, len(listeners))
        response = app.test_client().get(
            "/live_price_stream?index=NIFTY", headers={"Host": "localhost:5000"}, buffered=False)
        self.assertEqual("text/event-stream", response.mimetype)
        chunks = response.response
        try:
            self.assertEqual(b"data: 100.0\n\n", next(chunks))
            # Three ticks inside a few ms: each one is written, none waits on
            # a poll interval. Another index's tick writes nothing here.
            got = []
            for price in (100.05, 100.1, 99.95):
                prices["BANKNIFTY"] += 1
                prices["NIFTY"] = price
                threading.Timer(0.01, listeners[0], args=({},)).start()
                started = time.monotonic()
                got.append(next(chunks))
                self.assertLess(time.monotonic() - started, 0.5)
            self.assertEqual([b"data: 100.05\n\n", b"data: 100.1\n\n", b"data: 99.95\n\n"], got)
            # A quiet second sends a keep-alive comment, not a repeated price.
            self.assertEqual(b": \n\n", next(chunks))
        finally:
            response.close()

    def test_net_pnl_includes_overnight_hold_and_today_close(self):
        class DB:
            def execute(self, query, params, *, fetch):
                self.assert_query = query
                self.assert_params = params
                return [
                    {"direction": "LONG", "fill_price": 100.0, "exit_price": None,
                     "is_in_trade": True},
                    {"direction": "LONG", "fill_price": 100.0, "exit_price": 115.0,
                     "is_in_trade": False},
                ]

        db = DB()
        app = create_app(None, "NSE", "1", lambda: 110.0, None, db,
                         pivot_service=object())
        response = app.test_client().get("/net_pnl", headers={"Host": "localhost:5000"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["net_pnl_points"], 25.0)
        self.assertEqual(response.json["closed_trades"], 1)
        self.assertEqual(response.json["open_trade_points"], 10.0)
        self.assertEqual(response.json["open_direction"], "LONG")
        self.assertIn("is_in_trade = TRUE", db.assert_query)
        self.assertIn("COALESCE(exit_time, log_time)", db.assert_query)
        self.assertEqual(db.assert_params, (ist.IST_NAME, ist.today()))


class OptionLegsRouteTests(unittest.TestCase):
    """The legs bay: one row per instrument, the held legs taking their index's."""

    class _Fetcher:
        def __init__(self, state, sell=None, buy=None):
            self.status = {"state": state, "sell": sell, "buy": buy, "note": None}

        def legs_status(self):
            return dict(self.status)

    def _get(self, db, fetchers):
        app = create_app(None, "NSE", "1", lambda: None, None, db, pivot_service=object(),
                         chart_index="NIFTY", symbol_fetchers=fetchers)
        response = app.test_client().get("/option_legs", headers={"Host": "localhost:5000"})
        self.assertEqual(response.status_code, 200)
        return response.json

    def test_rows_follow_the_instruments_and_the_ledger_outranks_the_builder(self):
        import instruments

        class DB:
            def execute(self, query, params=None, *, fetch):
                self.query = query
                return [{"index_name": None, "symbol": "NIFTY08SEP26P24500",
                         "sell_symbol": None, "buy_symbol": "NIFTY08SEP26P23800"}]

        db = DB()
        body = self._get(db, {
            "NIFTY": self._Fetcher("armed", "NIFTY08SEP26P24600", "NIFTY08SEP26P23900"),
            "SENSEX": self._Fetcher("armed", "SENSEX08SEP26P80000", "SENSEX08SEP26P78000"),
        })
        self.assertIn("is_in_trade = TRUE", db.query)
        self.assertTrue(body["ledger"])
        rows = {row["index"]: row for row in body["legs"]}
        self.assertEqual(list(instruments.NAMES), [row["index"] for row in body["legs"]])
        # A legacy row (no index, no sell column) is the chart index's sold leg.
        self.assertEqual(("held", "NIFTY08SEP26P24500", "NIFTY08SEP26P23800"),
                         (rows["NIFTY"]["state"], rows["NIFTY"]["sell"], rows["NIFTY"]["buy"]))
        self.assertEqual(("armed", "BFO"), (rows["SENSEX"]["state"], rows["SENSEX"]["exchange"]))
        self.assertEqual(instruments.get("SENSEX").quantity, rows["SENSEX"]["lot"])
        self.assertEqual("offline", rows["BANKNIFTY"]["state"])

    def test_an_unreadable_ledger_is_flagged_not_read_as_flat(self):
        class DB:
            def execute(self, *_args, **_kwargs):
                raise RuntimeError("pool exhausted")

        with patch("live_server.logging.error") as logged:
            body = self._get(DB(), {"NIFTY": self._Fetcher("armed", "S", "B")})
        self.assertTrue(logged.called)
        self.assertFalse(body["ledger"])
        self.assertIsNone(body["held"])
        self.assertEqual("armed", {row["index"]: row["state"] for row in body["legs"]}["NIFTY"])


class InstanceLockTests(unittest.TestCase):
    def test_second_instance_cannot_start_until_first_exits(self):
        from shoonya_login import TransientLoginError, _InterProcessLock

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "instance.lock"
            with _InterProcessLock(path, timeout=0):
                with self.assertRaises(TransientLoginError):
                    with _InterProcessLock(path, timeout=0):
                        pass
            with _InterProcessLock(path, timeout=0):
                pass


# --------------------------------------------------------------------------- #
#  was test_db_connector.py                                                    #
# --------------------------------------------------------------------------- #
class DatabaseCredentialSafetyTests(unittest.TestCase):
    """Resolution order for the database password.

    The private .env provides the default; an explicit argument or environment
    override takes precedence without putting credentials in source files.
    """

    def test_private_password_is_used_when_nothing_else_is_set(self):
        with patch.dict(os.environ, {}, clear=True):
            connector = TimescaleDBConnector(auto_provision=False)
        self.assertEqual(connector.cfg["password"], db_connector.DB_PASSWORD)

    def test_missing_password_fails_closed(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(db_connector, "DB_PASSWORD", None):
            with self.assertRaisesRegex(ValueError, "Database password is required"):
                TimescaleDBConnector(auto_provision=False)

    def test_explicit_password_wins_over_everything(self):
        with patch.dict(os.environ, {"TSDB_PASSWORD": "from-env"}, clear=True):
            connector = TimescaleDBConnector(password="test-only", auto_provision=False)
        self.assertEqual(connector.cfg["password"], "test-only")
        self.assertEqual(connector.cfg["options"], "-c timezone=Asia/Kolkata")

    def test_environment_password_overrides_the_module(self):
        """So another machine can point at a different instance without an edit."""
        with patch.dict(os.environ, {"TSDB_PASSWORD": "test-env-only"}, clear=True):
            connector = TimescaleDBConnector(auto_provision=False)
        self.assertEqual(connector.cfg["password"], "test-env-only")

    def test_the_runner_and_the_connector_share_one_password(self):
        """The runner and child must use the same private credential."""
        import market_hours_runner

        with patch.dict(os.environ, {}, clear=True):
            connector = TimescaleDBConnector(auto_provision=False)
        self.assertEqual(market_hours_runner.PG_PASSWORD, connector.cfg["password"])

    def test_application_timezone_is_explicitly_ist(self):
        self.assertEqual(DB_TIMEZONE, "Asia/Kolkata")

    def test_schema_has_no_timezone_naive_timestamp_columns(self):
        timestamp_columns = [
            column
            for table in TABLES
            for column in table.columns
            if "TIMESTAMP" in column.create_definition.upper()
        ]
        self.assertTrue(timestamp_columns)
        for column in timestamp_columns:
            self.assertIn("TIMESTAMPTZ", column.create_definition.upper())


# --------------------------------------------------------------------------- #
#  was test_event_driven_forecast.py                                           #
# --------------------------------------------------------------------------- #
class PartialTickPriceTests(unittest.TestCase):
    def _socket(self):
        socket = LiveWebSocket.__new__(LiveWebSocket)
        socket.latest_price = 24350.0
        socket.latest_price_ts = 10.0
        socket._price_handlers = []
        return socket

    def test_partial_tick_does_not_erase_last_price(self):
        socket = self._socket()
        socket.event_handler_feed_update({"t": "tf", "oi": "123"})
        self.assertEqual(socket.latest_price, 24350.0)
        self.assertEqual(socket.latest_price_ts, 10.0)

    def test_invalid_price_does_not_erase_last_price(self):
        socket = self._socket()
        socket.event_handler_feed_update({"t": "tf", "lp": ""})
        self.assertEqual(socket.latest_price, 24350.0)
        self.assertEqual(socket.latest_price_ts, 10.0)

    def test_infinite_price_does_not_replace_last_price(self):
        socket = self._socket()
        socket.event_handler_feed_update({"t": "tf", "lp": "inf"})
        self.assertEqual(socket.latest_price, 24350.0)
        self.assertEqual(socket.latest_price_ts, 10.0)

    def test_valid_price_replaces_last_price_and_timestamp(self):
        socket = self._socket()
        with patch("live_websocket.time.monotonic", return_value=20.0):
            socket.event_handler_feed_update({"t": "tf", "lp": "24351.25"})
        self.assertEqual(socket.latest_price, 24351.25)
        self.assertEqual(socket.latest_price_ts, 20.0)


class FeedHealthTests(unittest.TestCase):
    """The System bay's WebSocket probe: which source prices each index."""

    def test_each_index_names_its_price_source(self):
        socket = LiveWebSocket.__new__(LiveWebSocket)
        socket._lock = threading.Lock()
        socket.feed_opened = True
        socket._opens, socket._opened_at, socket._ticks = 3, 40.0, 120
        socket._price_stamps = {"NSE|1": 99.0, "NSE|2": 80.0, "NSE|3": 91.0}
        socket._rest_stamps = {"NSE|2": 97.0}
        names = {"NIFTY": "NSE|1", "BANKNIFTY": "NSE|2", "FINNIFTY": "NSE|3",
                 "SENSEX": "BSE|1"}
        with patch("live_websocket.time.monotonic", return_value=100.0):
            body = socket.health(names)
        self.assertEqual({"NIFTY": "ws", "BANKNIFTY": "rest", "FINNIFTY": "ws", "SENSEX": "none"},
                         {f["index"]: f["source"] for f in body["feeds"]})
        self.assertEqual((True, 60, 2, 120),
                         (body["connected"], body["uptime_s"], body["reconnects"], body["ticks"]))

        socket.feed_opened = False
        socket._price_stamps = {}
        with patch("live_websocket.time.monotonic", return_value=130.0):
            body = socket.health(names)
        self.assertIsNone(body["uptime_s"])
        self.assertEqual("stale", body["feeds"][1]["source"])    # its quote is 33 s old

    def test_route_reports_an_absent_monitor_as_unavailable(self):
        app = create_app(None, "NSE", "1", lambda: None, None, None, pivot_service=object())
        response = app.test_client().get("/feed_health", headers={"Host": "localhost:5000"})
        self.assertEqual((503, False), (response.status_code, response.json["available"]))


class TokenCollisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_exchange_less_collision_cannot_price_the_wrong_index(self):
        socket = LiveWebSocket.__new__(LiveWebSocket)
        socket._loop = asyncio.get_running_loop()
        socket._lock = threading.Lock()
        socket._prices = {}
        socket._price_stamps = {}
        socket._price_handlers = []
        socket.latest_price = None
        socket.latest_price_ts = None
        socket._rest_thread = object()  # no REST poller in a unit test

        async def no_connect():
            pass

        socket._start_and_supervise = no_connect
        socket.start(["NSE|123", "BSE|123"])
        await asyncio.sleep(0)
        socket.event_handler_feed_update({"tk": "123", "lp": "100"})
        self.assertEqual(socket._prices, {})
        self.assertIsNone(socket.latest_price)

        socket.event_handler_feed_update({"e": "NSE", "tk": "123", "lp": "101"})
        socket.event_handler_feed_update({"e": "BSE", "tk": "123", "lp": "202"})
        self.assertEqual(socket._prices, {"NSE|123": 101.0, "BSE|123": 202.0})
        self.assertEqual(socket.latest_price, 101.0)


class ForecastStateTests(unittest.TestCase):
    def test_same_pivot_complete_to_empty_legs_is_transient(self):
        data = {"hh_count": 1, "ll_count": 0, "pivot_type": "HH", "pivot_price": 24365.0, "pivot_time": "09:15"}
        previous = _state_key(data, "BUY", "SELL")
        current = _state_key(data, None, None)
        self.assertTrue(_is_transient_leg_regression(previous, current))

    def test_new_pivot_without_legs_is_a_real_event(self):
        old = {"hh_count": 1, "ll_count": 0, "pivot_type": "HH", "pivot_price": 24365.0, "pivot_time": "09:15"}
        new = {"hh_count": 1, "ll_count": 1, "pivot_type": "LL", "pivot_price": 24200.0, "pivot_time": "10:30"}
        self.assertFalse(_is_transient_leg_regression(_state_key(old, "BUY", "SELL"), _state_key(new, None, None)))


# --------------------------------------------------------------------------- #
#  was test_shoonya_browser_launch.py (rewritten so it actually runs)          #
# --------------------------------------------------------------------------- #
class ServiceBrowserTests(unittest.TestCase):
    """The Shoonya login needs a browser the *service account* can launch.

    The Lune service runs as LocalSystem, which has its own profile and does
    not see a per-user Playwright install. A machine-wide Edge is the fallback
    `shoonya_login` reaches for, so its absence is what turns 09:15 into a
    crash loop rather than a login.
    """

    @unittest.skipUnless(os.name == "nt", "Windows-only path check")
    def test_machine_wide_edge_exists_for_local_system_service(self):
        candidates = (
            Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"))
            / "Microsoft" / "Edge" / "Application" / "msedge.exe",
            Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
            / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        )
        self.assertTrue(
            any(path.is_file() for path in candidates),
            f"no machine-wide Edge found in {[str(c) for c in candidates]}",
        )


# --------------------------------------------------------------------------- #
#  Broker latency probe (live_server)                                          #
# --------------------------------------------------------------------------- #
class BrokerLatencyProbeTests(unittest.TestCase):
    """The console's Shoonya ping.

    The failure mode worth a test is not a wrong millisecond count - it is the
    probe becoming load of its own. An operator leaving the dashboard open, or
    two of them on two screens, must not turn a status panel into a stream of
    broker requests, and a broker that has stopped answering must not be able
    to wedge the route that reports it.
    """

    class _Broker:
        def __init__(self, *, fail=False, reject=False, delay=0.0):
            self.fail = fail
            self.reject = reject
            self.delay = delay
            self.calls = 0

        def get_quotes(self, exchange, token):
            self.calls += 1
            time.sleep(self.delay)
            if self.fail:
                raise TimeoutError("no route to host")
            return None if self.reject else {"stat": "Ok", "lp": "24500"}

    def test_ttl_gates_repeated_polling(self):
        broker = self._Broker()
        probe = BrokerLatencyProbe(broker, "NSE", "26000", ttl_sec=30.0)

        for _ in range(25):
            probe.snapshot()

        self.assertEqual(broker.calls, 1, "each poll hit the broker")

    def test_concurrent_pollers_share_one_probe(self):
        broker = self._Broker(delay=0.05)
        probe = BrokerLatencyProbe(broker, "NSE", "26000", ttl_sec=30.0)

        threads = [threading.Thread(target=probe.snapshot) for _ in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(broker.calls, 1, "callers queued behind the network")

    def test_failed_probe_is_a_gap_not_a_latency(self):
        broker = self._Broker(fail=True)
        probe = BrokerLatencyProbe(broker, "NSE", "26000", ttl_sec=0.0)

        for _ in range(3):
            snapshot = probe.snapshot()

        # None, never 0.0: a zero here renders on the dashboard as a perfect
        # link to a broker that is not answering at all.
        self.assertIsNone(snapshot["latency_ms"])
        self.assertIsNone(snapshot["median_ms"])
        self.assertFalse(snapshot["ok"])
        self.assertEqual(snapshot["failures"], snapshot["samples"])
        self.assertTrue(all(value is None for value in snapshot["history"]))

    def test_reachable_but_refusing_is_not_a_latency(self):
        broker = self._Broker(reject=True)
        probe = BrokerLatencyProbe(broker, "NSE", "26000", ttl_sec=0.0)

        snapshot = probe.snapshot()

        self.assertEqual(snapshot["error"], "rejected")
        self.assertIsNone(snapshot["latency_ms"])

    def test_recovery_reports_again(self):
        broker = self._Broker(fail=True)
        probe = BrokerLatencyProbe(broker, "NSE", "26000", ttl_sec=0.0)
        probe.snapshot()
        broker.fail = False

        snapshot = probe.snapshot()

        self.assertTrue(snapshot["ok"])
        self.assertIsInstance(snapshot["latency_ms"], float)
        self.assertIn(None, snapshot["history"], "the outage was erased")

    def test_no_session_reports_unavailable_without_calling(self):
        probe = BrokerLatencyProbe(None, "NSE", None)

        snapshot = probe.snapshot()

        self.assertFalse(snapshot["available"])
        self.assertNotIn("latency_ms", snapshot)


if __name__ == "__main__":
    unittest.main()
