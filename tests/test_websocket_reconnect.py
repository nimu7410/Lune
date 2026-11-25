"""The websocket feed must come back, however long the broker is away.

A live session log showed the bot losing its Shoonya feed to a broker-side
outage and answering it with 138 identical `502 Bad Gateway` handshake dumps,
24 `Max websocket reconnect attempts reached` records, and a retry that never
backed off past 15 seconds because every "give up" restarted the delay at 2.

The give-up was the real defect. `_reconnect_loop_async` ended after
`max_retries` attempts and cleared `_reconnecting`; reconnection continued only
because the last doomed attempt's `on_close`/`on_error` fired afterwards and
armed a fresh loop. Any attempt that produced no callback - a socket thread
still winding down, a false `login_status`, a token lookup that raised - broke
that chain, and the bot then ran the rest of the session with a dead feed and
nothing scheduled to reopen it. `get_fresh_price` would return None for every
entry check and the day would look, in the log, exactly like a day the strategy
chose not to trade.

These tests pin the contract that replaced it: the supervisor retries until the
feed is open or the bot is shutting down, the delay grows across the whole
outage instead of per loop, one supervisor runs at a time, the flag is always
released, and a long outage costs a bounded number of log lines.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
import threading
import time
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import WebsocketSettings  # noqa: E402
from live_websocket import LiveWebSocket  # noqa: E402
from log_throttle import LogThrottle  # noqa: E402


@contextlib.contextmanager
def quiet():
    """Silence expected failure logging so a check run's output stays readable."""
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(logging.NOTSET)


def make_socket(**overrides) -> LiveWebSocket:
    """A LiveWebSocket with only the reconnect machinery wired up.

    Delays are microscopic so a test can drive dozens of attempts without
    sleeping; the arithmetic behind them is checked separately against the real
    configured values in BackoffTests.
    """
    ws = LiveWebSocket.__new__(LiveWebSocket)
    ws.feed_opened = False
    ws.latest_price = None
    ws.latest_price_ts = None
    ws._prices = {}
    ws._price_stamps = {}
    ws.max_retries = 5
    ws._backoff_initial = 0.001
    ws._backoff_cap = 0.002
    ws._open_timeout = 0.05
    ws._reconnecting = False
    ws._consecutive_failures = 0
    ws._failure_throttle = LogThrottle(1000.0)
    ws._lock = threading.Lock()
    ws._ws_lock = threading.Lock()
    ws._ws = None
    ws._ws_thread = None
    ws._closing = False
    ws._shutting_down = False
    ws._reconnect_task = None
    ws._loop = None
    ws._order_handlers = []
    ws._price_handlers = []
    ws.resubscribe_symbols = None
    ws._primary_key = None
    ws._rest_prices = {}
    ws._rest_stamps = {}
    ws._rest_next = {}
    ws._rest_failures = {}
    ws._rest_serving = set()
    ws.api = None
    ws.trade_manager = None
    for key, value in overrides.items():
        setattr(ws, key, value)
    return ws


class _Broker:
    """Stands in for `_start_websocket_async`.

    `outage` is how many attempts fail before the feed comes up. `started`
    decides whether a failing attempt even managed to create a socket, which is
    the distinction the old code could not make.
    """

    def __init__(self, outage: int, *, started: bool = True):
        self.outage = outage
        self.started = started
        self.attempts = 0

    def bind(self, ws: LiveWebSocket):
        async def _start() -> bool:
            self.attempts += 1
            if self.attempts > self.outage:
                with ws._lock:
                    ws.feed_opened = True
                return True
            return self.started

        ws._start_websocket_async = _start
        return self


class ReconnectPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_recovers_from_outage_far_longer_than_max_retries(self):
        ws = make_socket()
        broker = _Broker(outage=40).bind(ws)          # 8x the old retry budget
        ws._reconnecting = True

        with quiet():
            await asyncio.wait_for(ws._reconnect_loop_async(), timeout=30)

        self.assertTrue(ws.feed_opened)
        self.assertEqual(broker.attempts, 41)
        self.assertFalse(ws._reconnecting)

    async def test_attempts_that_never_create_a_socket_still_retry(self):
        """The exact break in the old accidental retry chain.

        A failure that produces no socket produces no `on_close`/`on_error`, so
        under the old design nothing rearmed the loop once its budget ran out.
        """
        ws = make_socket()
        broker = _Broker(outage=12, started=False).bind(ws)
        ws._reconnecting = True

        with quiet():
            await asyncio.wait_for(ws._reconnect_loop_async(), timeout=30)

        self.assertTrue(ws.feed_opened)
        self.assertEqual(broker.attempts, 13)

    async def test_socket_that_starts_but_never_authenticates_is_a_failure(self):
        """A handshake that opens a TCP socket and then dies must not count.

        `_start_websocket_async` returning True only means an attempt is in
        flight. Treating that as success would end the supervisor with the feed
        still down - the same silent dead feed by another route.
        """
        ws = make_socket()
        attempts = {"n": 0}

        async def _start() -> bool:
            attempts["n"] += 1
            if attempts["n"] >= 4:
                with ws._lock:
                    ws.feed_opened = True
            return True                      # "started" every time

        ws._start_websocket_async = _start
        ws._reconnecting = True

        with quiet():
            await asyncio.wait_for(ws._reconnect_loop_async(), timeout=30)

        self.assertTrue(ws.feed_opened)
        self.assertEqual(attempts["n"], 4)

    async def test_flag_is_released_even_when_the_loop_raises(self):
        """A stuck flag would make every later `_schedule_reconnect` a no-op."""
        ws = make_socket()

        async def _boom() -> bool:
            raise RuntimeError("token lookup exploded")

        ws._start_websocket_async = _boom
        ws._reconnecting = True

        with self.assertRaises(RuntimeError):
            await ws._reconnect_loop_async()
        self.assertFalse(ws._reconnecting)

    async def test_flag_is_released_when_the_loop_is_cancelled(self):
        ws = make_socket()
        _Broker(outage=10_000).bind(ws)
        ws._reconnecting = True

        task = asyncio.create_task(ws._reconnect_loop_async())
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(ws._reconnecting)

    async def test_shutdown_stops_the_supervisor(self):
        ws = make_socket()
        _Broker(outage=10_000).bind(ws)
        ws._reconnecting = True

        async def _stop():
            await asyncio.sleep(0.05)
            with ws._lock:
                ws._shutting_down = True

        await asyncio.wait_for(
            asyncio.gather(ws._reconnect_loop_async(), _stop()), timeout=30
        )
        self.assertFalse(ws._reconnecting)
        self.assertFalse(ws.feed_opened)

    async def test_stop_cancels_supervisor_before_loop_closes(self):
        ws = make_socket()
        ws._reconnecting = True
        ws._reconnect_task = asyncio.create_task(ws._reconnect_loop_async())
        await asyncio.sleep(0)
        await ws.stop()
        self.assertTrue(ws._reconnect_task.done())
        self.assertFalse(ws._reconnecting)
        self.assertTrue(ws._shutting_down)

    async def test_returns_immediately_when_the_feed_is_already_back(self):
        ws = make_socket(feed_opened=True)
        broker = _Broker(outage=0).bind(ws)
        ws._reconnecting = True

        await asyncio.wait_for(ws._reconnect_loop_async(), timeout=5)

        self.assertEqual(broker.attempts, 0)
        self.assertFalse(ws._reconnecting)

    async def test_first_connect_failure_hands_off_to_the_supervisor(self):
        """`start()` used to be fire-and-forget.

        If the very first connect failed before a socket existed, no callback
        fired and nothing retried: the bot traded the session on a feed that had
        never opened once.
        """
        ws = make_socket()
        _Broker(outage=6, started=False).bind(ws)
        scheduled = {"n": 0}

        def _schedule():
            scheduled["n"] += 1

        ws._schedule_reconnect = _schedule
        with quiet():
            await asyncio.wait_for(ws._start_and_supervise(), timeout=10)
        self.assertEqual(scheduled["n"], 1)

    async def test_first_connect_success_does_not_arm_the_supervisor(self):
        ws = make_socket()
        _Broker(outage=0).bind(ws)
        scheduled = {"n": 0}
        ws._schedule_reconnect = lambda: scheduled.__setitem__("n", scheduled["n"] + 1)

        await asyncio.wait_for(ws._start_and_supervise(), timeout=10)

        self.assertTrue(ws.feed_opened)
        self.assertEqual(scheduled["n"], 0)


class SingleSupervisorTests(unittest.TestCase):
    """One outage, one supervisor.

    Every failed attempt fires both `on_error` and `on_close`, and both call
    `_schedule_reconnect`. Two concurrent loops would double the retry rate
    against a gateway that is already refusing connections.
    """

    def _armed(self) -> tuple[LiveWebSocket, list]:
        ws = make_socket()
        started: list = []
        ws._loop = None
        with patch.object(threading, "Thread") as thread_cls:
            thread_cls.side_effect = lambda **kw: started.append(kw) or _NoopThread()
            ws._schedule_reconnect()
            ws._schedule_reconnect()
            ws._schedule_reconnect()
        return ws, started

    def test_concurrent_callbacks_arm_only_one_loop(self):
        ws, started = self._armed()
        self.assertEqual(len(started), 1)
        self.assertTrue(ws._reconnecting)

    def test_shutdown_refuses_to_arm(self):
        ws = make_socket()
        ws._shutting_down = True
        started: list = []
        with patch.object(threading, "Thread") as thread_cls:
            thread_cls.side_effect = lambda **kw: started.append(kw) or _NoopThread()
            ws._schedule_reconnect()
        self.assertEqual(started, [])
        self.assertFalse(ws._reconnecting)

    def test_flag_is_released_if_the_loop_cannot_be_launched(self):
        """Otherwise the feed is dead and every later drop is ignored."""
        ws = make_socket()
        with patch.object(threading, "Thread", side_effect=RuntimeError("no threads")):
            with self.assertRaises(RuntimeError):
                ws._schedule_reconnect()
        self.assertFalse(ws._reconnecting)
        # ...and a later drop can still arm one.
        started: list = []
        with patch.object(threading, "Thread") as thread_cls:
            thread_cls.side_effect = lambda **kw: started.append(kw) or _NoopThread()
            ws._schedule_reconnect()
        self.assertEqual(len(started), 1)


class _NoopThread:
    def start(self):
        pass

    def is_alive(self):
        return False

    def join(self, *_):
        pass


class BackoffTests(unittest.TestCase):
    """The delay must grow with the outage, not with the loop.

    The old loop restarted at 2s every time it gave up, so a four-minute outage
    was met with a handshake roughly every 15 seconds for four minutes. Growth
    is measured here against the real configured values.
    """

    def setUp(self):
        cfg = WebsocketSettings()
        self.ws = make_socket(
            _backoff_initial=cfg.backoff_initial,
            _backoff_cap=cfg.backoff_cap,
            max_retries=cfg.max_retries,
        )
        self.cfg = cfg

    def test_first_delay_is_short(self):
        with patch("live_websocket.random.random", return_value=1.0):
            self.assertAlmostEqual(self.ws._next_backoff(1), self.cfg.backoff_initial)

    def test_delay_doubles_with_consecutive_failures(self):
        with patch("live_websocket.random.random", return_value=1.0):
            delays = [self.ws._next_backoff(n) for n in range(1, 6)]
        self.assertEqual(delays, sorted(delays))
        for earlier, later in zip(delays, delays[1:]):
            self.assertAlmostEqual(later, min(earlier * 2, self.cfg.backoff_cap))

    def test_delay_is_capped(self):
        with patch("live_websocket.random.random", return_value=1.0):
            for n in (20, 200, 2000):
                self.assertLessEqual(self.ws._next_backoff(n), self.cfg.backoff_cap)

    def test_cap_is_long_enough_to_ride_out_a_real_outage(self):
        """The observed outages ran minutes; a 15s ceiling meant ~16 doomed
        handshakes each. Anything under a minute reproduces that."""
        self.assertGreaterEqual(self.cfg.backoff_cap, 60.0)

    def test_jitter_stays_within_half_the_capped_delay(self):
        """Jittered, but never so short it becomes a hammer, nor 2x the cap."""
        for n in (1, 3, 9, 50):
            for _ in range(200):
                delay = self.ws._next_backoff(n)
                nominal = min(
                    self.cfg.backoff_initial * (2 ** (n - 1)), self.cfg.backoff_cap
                )
                self.assertGreaterEqual(delay, nominal * 0.5)
                self.assertLessEqual(delay, nominal)

    def test_jitter_actually_varies(self):
        """Identical schedules across a fleet re-create the thundering herd."""
        delays = {self.ws._next_backoff(6) for _ in range(50)}
        self.assertGreater(len(delays), 1)

    def test_a_long_outage_costs_far_fewer_attempts_than_before(self):
        """Compare the retry budget spent on a four-minute outage."""
        def attempts(next_delay) -> int:
            elapsed, n = 0.0, 0
            while elapsed < 240.0:
                n += 1
                elapsed += next_delay(n)
            return n

        with patch("live_websocket.random.random", return_value=1.0):
            fixed = attempts(lambda n: self.ws._next_backoff(n))
        # Old behaviour: 2,4,8,15,15 then start over at 2 forever.
        old_cycle = [2, 4, 8, 15, 15]
        old = attempts(lambda n: old_cycle[(n - 1) % len(old_cycle)])
        self.assertLess(fixed, old / 2)


class OutageCountersTests(unittest.TestCase):
    def test_authenticating_resets_the_outage(self):
        """A blip an hour later must start from the short delay, not the cap."""
        ws = make_socket()
        ws._consecutive_failures = 37
        ws.resubscribe_symbols = None
        ws.subscribe_orders = lambda: None

        ws.open_callback()

        self.assertEqual(ws._consecutive_failures, 0)
        self.assertTrue(ws.feed_opened)

    def test_open_callback_survives_a_failing_order_subscription(self):
        ws = make_socket()
        ws._consecutive_failures = 3

        def _boom():
            raise RuntimeError("no order feed")

        ws.subscribe_orders = _boom
        with quiet():
            ws.open_callback()
        self.assertTrue(ws.feed_opened)
        self.assertEqual(ws._consecutive_failures, 0)


class ErrorLogVolumeTests(unittest.TestCase):
    """One incident, a handful of lines.

    Each handshake failure logged the whole nginx 502 error page. 138 of them
    for one outage buried every other error in the session log, which is how a
    real problem goes unnoticed.
    """

    def _socket(self):
        ws = make_socket(_failure_throttle=LogThrottle(60.0))
        ws._schedule_reconnect = lambda: None
        ws._hard_reset = lambda: None
        return ws

    def test_repeated_identical_errors_are_folded(self):
        ws = self._socket()
        error = "handshake status 502 bad gateway " + "x" * 400
        with self.assertLogs("root", level="ERROR") as captured:
            for _ in range(200):
                ws.error_callback(error)
        self.assertEqual(len(captured.output), 1)

    def test_the_first_error_of_an_incident_is_never_suppressed(self):
        ws = self._socket()
        with self.assertLogs("root", level="ERROR") as captured:
            ws.error_callback("handshake status 502 bad gateway")
        self.assertEqual(len(captured.output), 1)

    def test_websocket_error_does_not_log_access_token(self):
        ws = self._socket()
        token = "example-access-token-12345"
        ws.api = SimpleNamespace(access_token=token)
        with self.assertLogs("root", level="ERROR") as captured:
            ws.error_callback(f"handshake failed for wss://api.shoonya.com/NorenWSAPI/{token}")
        self.assertNotIn(token, captured.output[0])

    def test_a_new_incident_after_recovery_logs_immediately(self):
        """Throttling must not hide the second outage of the day."""
        ws = self._socket()
        ws.resubscribe_symbols = None
        ws.subscribe_orders = lambda: None

        with self.assertLogs("root", level="ERROR"):
            ws.error_callback("handshake status 502 bad gateway")
        ws.open_callback()                      # feed comes back
        with self.assertLogs("root", level="ERROR") as second:
            ws.error_callback("handshake status 502 bad gateway")
        self.assertEqual(len(second.output), 1)

    def test_duplicate_socket_still_forces_a_reset(self):
        """Throttling the log must not swallow the branch that acts on it."""
        ws = self._socket()
        reset = {"n": 0}
        ws._hard_reset = lambda: reset.__setitem__("n", reset["n"] + 1)
        ws._schedule_reconnect = lambda: self.fail("must hard reset, not reconnect")
        with quiet():
            ws.error_callback("socket is already opened")
        self.assertEqual(reset["n"], 1)

    def test_an_error_marks_the_feed_stale(self):
        """A price from the dead connection must not price an entry."""
        ws = self._socket()
        ws.feed_opened = True
        ws.latest_price = 24350.0
        ws.latest_price_ts = 1.0
        with quiet():
            ws.error_callback("handshake status 502 bad gateway")
        self.assertFalse(ws.feed_opened)
        self.assertIsNone(ws.get_fresh_price())

    def test_reconnect_never_revives_a_previous_index_tick(self):
        for callback in ("close_callback", "error_callback"):
            with self.subTest(callback=callback):
                ws = self._socket()
                ws.feed_opened = True
                ws._prices["NSE|26000"] = 24350.0
                ws._price_stamps["NSE|26000"] = 100.0
                ws._schedule_reconnect = lambda: None
                with patch("live_websocket.time.monotonic", return_value=101.0), quiet():
                    self.assertEqual(ws.get_fresh_price_for("NSE|26000"), 24350.0)
                    if callback == "close_callback":
                        ws.close_callback()
                    else:
                        ws.error_callback("connection lost")
                    ws.feed_opened = True
                    self.assertIsNone(ws.get_fresh_price_for("NSE|26000"))

    def test_retired_socket_callbacks_cannot_touch_the_new_feed(self):
        ws = self._socket()
        old, current = object(), object()
        ws._ws = current
        ws.feed_opened = True
        ws._prices["NSE|26000"] = 24350.0
        ws._price_stamps["NSE|26000"] = 100.0
        handled = []
        ws._send_json = lambda payload: handled.append(("auth", payload))
        ws.event_handler_feed_update = lambda payload: handled.append(("tick", payload))
        ws.event_handler_order_update = lambda payload: handled.append(("order", payload))
        ws._schedule_reconnect = lambda: handled.append(("reconnect", None))

        ws._on_open(old)
        ws._on_message(old, '{"t":"tf","tk":"26000","lp":"99999"}')
        ws._on_message(old, '{"t":"om","norenordno":"wrong"}')
        ws._on_error(old, "retired connection")
        ws._on_close(old, 1000, None)

        self.assertEqual(handled, [])
        self.assertTrue(ws.feed_opened)
        self.assertEqual(ws._price_stamps["NSE|26000"], 100.0)


if __name__ == "__main__":
    unittest.main()


class StartAttemptTests(unittest.IsolatedAsyncioTestCase):
    """`_start_websocket_async` must report whether it actually tried.

    Every early return in the old version was silent. The caller assumed a
    socket was in flight and left the next retry to that socket's callbacks, so
    a return that created no socket produced no callbacks and no retry.
    """

    def _socket(self, **kw):
        ws = make_socket(**kw)
        ws.api = _FakeApi()
        return ws

    async def test_reports_success_when_a_socket_thread_starts(self):
        ws = self._socket()
        with patch("live_websocket.websocket.WebSocketApp"), patch.object(
            threading, "Thread", return_value=_NoopThread()
        ):
            self.assertTrue(await ws._start_websocket_async())

    async def test_reports_failure_when_the_token_lookup_raises(self):
        ws = self._socket()
        ws.api = _FakeApi(token=None)
        with quiet():
            self.assertFalse(await ws._start_websocket_async())

    async def test_a_logged_out_api_refreshes_the_session_and_retries(self):
        """Not a reason to stop: the token may be reissued here or published
        by another process. The old code returned without scheduling anything,
        which stranded the feed."""
        ws = self._socket()
        ws.api = _FakeApi(logged_in=False)
        refreshed = {"n": 0}
        ws._refresh_session_after_auth_failure = lambda: refreshed.__setitem__(
            "n", refreshed["n"] + 1
        )
        with quiet():
            result = await ws._start_websocket_async()
        self.assertFalse(result)
        self.assertEqual(refreshed["n"], 1)

    async def test_a_refreshed_session_lets_the_attempt_proceed(self):
        ws = self._socket()
        api = _FakeApi(logged_in=False)
        ws.api = api

        def _refresh():
            api.login_status = True

        ws._refresh_session_after_auth_failure = _refresh
        with quiet(), patch("live_websocket.websocket.WebSocketApp"), patch.object(
            threading, "Thread", return_value=_NoopThread()
        ):
            result = await ws._start_websocket_async()
        self.assertTrue(result)

    async def test_an_open_feed_is_reported_as_success_without_a_new_socket(self):
        ws = self._socket(feed_opened=True)
        with patch("live_websocket.websocket.WebSocketApp") as app:
            self.assertTrue(await ws._start_websocket_async())
            app.assert_not_called()

    async def test_a_stale_thread_is_waited_for_then_reported_as_failure(self):
        """Reported, not skipped silently - the supervisor backs off and tries
        again rather than assuming an attempt is already in flight."""
        ws = self._socket()
        stale = _StuckThread()
        ws._ws_thread = stale
        with patch("live_websocket.websocket.WebSocketApp") as app:
            result = await ws._start_websocket_async()
        self.assertFalse(result)
        self.assertGreater(stale.polls, 1)
        app.assert_not_called()

    async def test_a_stale_thread_that_finishes_does_not_block_the_attempt(self):
        ws = self._socket()
        ws._ws_thread = _DyingThread()
        with patch("live_websocket.websocket.WebSocketApp"), patch.object(
            threading, "Thread", return_value=_NoopThread()
        ):
            self.assertTrue(await ws._start_websocket_async())

    async def test_the_thread_runs_the_socket_it_was_created_for(self):
        ws = self._socket()
        seen = {}

        class _App:
            def run_forever(self_inner, **kwargs):
                seen["ran"] = self_inner

        app = _App()

        def _thread(target, args, daemon):
            class _RecordingThread(_NoopThread):
                def start(self_inner):
                    seen["published"] = ws._ws
                    ws._ws = object()  # replacement arrives before this thread runs
                    target(*args)
            return _RecordingThread()

        with patch("live_websocket.websocket.WebSocketApp", return_value=app), \
                patch.object(threading, "Thread", side_effect=_thread):
            await ws._start_websocket_async()
        self.assertIs(seen["published"], app)
        self.assertIs(seen["ran"], app)


class SilentSocketTests(unittest.IsolatedAsyncioTestCase):
    """A socket that is open but dead must be closed, or nothing reopens it."""

    class _App:
        closed = 0

        @property
        def sock(self):
            return self

        def abort(self):
            self.closed += 1

    def _socket(self, age):
        app = self._App()
        ws = make_socket(feed_opened=True, _ws=app)
        ws._last_rx = time.monotonic() - age
        armed = []
        ws._schedule_reconnect = lambda: armed.append(1)
        return ws, app, armed

    async def test_a_silent_open_socket_is_closed_and_reconnected(self):
        ws, app, armed = self._socket(LiveWebSocket.SILENT_AFTER + 1)
        with quiet():
            ws._close_if_silent()
        self.assertEqual((app.closed, len(armed), ws.feed_opened), (1, 1, False))

    async def test_a_pong_counts_as_life(self):
        ws, app, armed = self._socket(LiveWebSocket.SILENT_AFTER + 1)
        ws._on_pong(app, b"")
        ws._close_if_silent()
        self.assertEqual((app.closed, len(armed), ws.feed_opened), (0, 0, True))

    async def test_a_stale_unauthenticated_socket_is_closed_before_retrying(self):
        ws, app, _ = self._socket(0)
        ws.feed_opened = False
        ws.api = _FakeApi()
        ws._ws_thread = _StuckThread()
        self.assertFalse(await ws._start_websocket_async())
        self.assertEqual(app.closed, 1)


class _FakeApi:
    def __init__(self, token="tok", logged_in=True):
        self._token = token
        self.login_status = logged_in
        self.userid = "USER1"

    @property
    def access_token(self):
        return self._token


class _StuckThread:
    def __init__(self):
        self.polls = 0

    def is_alive(self):
        self.polls += 1
        return True

    def join(self, *_):
        pass


class _DyingThread:
    def __init__(self):
        self._alive = True

    def is_alive(self):
        alive, self._alive = self._alive, False
        return alive

    def join(self, *_):
        self._alive = False


class EndToEndReconnectTests(unittest.IsolatedAsyncioTestCase):
    """The whole chain against a broker that 502s, using the real code paths.

    Drives `error_callback` -> `_schedule_reconnect` -> `_reconnect_loop_async`
    -> `_start_websocket_async` with a fake `WebSocketApp` that reproduces the
    logged incident: a run of handshake failures, then a connection that
    authenticates.
    """

    async def test_feed_returns_after_a_run_of_502s(self):
        outage = 30                      # six times the old retry budget
        state = {"attempts": 0}
        ws = make_socket()
        ws.api = _FakeApi()
        ws._loop = asyncio.get_running_loop()
        ws.resubscribe_symbols = None
        ws.subscribe_orders = lambda: None
        ws._failure_throttle = LogThrottle(1000.0)

        error_page = (
            "Handshake status 502 Bad Gateway -+-+- {'server': 'nginx/1.28.0'} "
            "-+-+- b'<html><title>502 Bad Gateway</title></html>'"
        )

        def _app(url, **handlers):
            state["attempts"] += 1
            attempt = state["attempts"]

            class _App:
                def run_forever(self_inner, **kwargs):
                    if attempt <= outage:
                        handlers["on_error"](self_inner, error_page)
                        handlers["on_close"](self_inner, 1006, None)
                    else:
                        ws.open_callback()

            return _App()

        def _thread(target, args, daemon=None):
            class _T:
                def start(self_inner):
                    target(*args)

                def is_alive(self_inner):
                    return False

                def join(self_inner, *_):
                    pass

            return _T()

        with quiet(), \
                patch("live_websocket.websocket.WebSocketApp", side_effect=_app), \
                patch.object(threading, "Thread", side_effect=_thread):
            ws._reconnecting = True
            await asyncio.wait_for(ws._reconnect_loop_async(), timeout=60)

        self.assertTrue(ws.feed_opened)
        self.assertGreater(state["attempts"], outage)
        self.assertFalse(ws._reconnecting)
        self.assertEqual(ws._consecutive_failures, 0)


class RestPriceFallbackTests(unittest.TestCase):
    """A dropped or silent socket must not leave the bot without a price.

    On 2026-10-05 the feed dropped at 11:24 and every index logged "option legs
    cannot be built, so every entry on this index will be skipped". A REST quote
    is the exchange's current LTP, so it stands in for the missing ticks.
    """

    NIFTY, BANK = "NSE|26000", "NSE|26009"
    IN_SESSION = datetime(2026, 10, 5, 12, 0)

    def _socket(self, quotes):
        calls = []

        def get_quotes(exch, token, timeout=30):
            calls.append(f"{exch}|{token}")
            value = quotes.get(f"{exch}|{token}")
            if isinstance(value, Exception):
                raise value
            return value

        ws = make_socket(api=SimpleNamespace(get_quotes=get_quotes),
                         resubscribe_symbols=[self.NIFTY, self.BANK], _primary_key=self.NIFTY)
        return ws, calls

    def _poll(self, ws, *, at=100.0, clock=None):
        with patch("live_websocket.time.monotonic", return_value=at), \
                patch("live_websocket.ist.now_naive", return_value=clock or self.IN_SESSION), quiet():
            ws._rest_quote_once()

    def test_a_dead_socket_is_priced_from_rest(self):
        ws, calls = self._socket({self.NIFTY: {"stat": "Ok", "lp": "22473.30"},
                                  self.BANK: {"stat": "Ok", "lp": "54705.55"}})
        self._poll(ws)
        self.assertEqual(sorted(calls), sorted([self.NIFTY, self.BANK]))
        with patch("live_websocket.time.monotonic", return_value=101.0):
            self.assertEqual(ws.get_fresh_price_for(self.NIFTY), 22473.30)
            self.assertEqual(ws.get_fresh_price_for(self.BANK), 54705.55)
            self.assertEqual(ws.get_fresh_price(), 22473.30)
        # Held to the same freshness rule as a tick.
        with patch("live_websocket.time.monotonic", return_value=130.0):
            self.assertIsNone(ws.get_fresh_price_for(self.NIFTY))
            self.assertIsNone(ws.get_fresh_price())

    def test_a_live_tick_wins_and_is_not_requoted(self):
        ws, calls = self._socket({self.BANK: {"stat": "Ok", "lp": "54705.55"}})
        ws.feed_opened = True
        ws._prices[self.NIFTY] = 22480.0
        ws._price_stamps[self.NIFTY] = 99.0
        self._poll(ws)
        self.assertEqual(calls, [self.BANK])  # only the silent subscription
        with patch("live_websocket.time.monotonic", return_value=100.0):
            self.assertEqual(ws.get_fresh_price_for(self.NIFTY), 22480.0)
            self.assertEqual(ws.get_fresh_price_for(self.BANK), 54705.55)

    def test_never_borrows_another_index_price(self):
        ws, _ = self._socket({self.NIFTY: {"stat": "Ok", "lp": "22473.30"}, self.BANK: None})
        self._poll(ws)
        with patch("live_websocket.time.monotonic", return_value=101.0):
            self.assertIsNone(ws.get_fresh_price_for(self.BANK))

    def test_quotes_are_paced_and_back_off_on_failure(self):
        ws, calls = self._socket({self.NIFTY: {"stat": "Ok", "lp": "22473.30"},
                                  self.BANK: RuntimeError("502")})
        self._poll(ws, at=100.0)
        self._poll(ws, at=101.0)   # inside REST_INTERVAL: nothing re-quoted
        self.assertEqual(len(calls), 2)
        self._poll(ws, at=102.0)   # NIFTY due again; BANK backed off to 104
        self.assertEqual(calls[2:], [self.NIFTY])
        self._poll(ws, at=104.0)
        self.assertIn(self.BANK, calls[3:])
        self.assertEqual(ws._rest_next[self.BANK], 104.0 + 8.0)

    def test_a_current_lut_is_accepted(self):
        import time as _time
        ws, _ = self._socket({self.NIFTY: {"stat": "Ok", "lp": "22473.30",
                                           "lut": str(int(_time.time()) - 2)}})
        self._poll(ws)
        with patch("live_websocket.time.monotonic", return_value=101.0):
            self.assertEqual(ws.get_fresh_price_for(self.NIFTY), 22473.30)

    def test_outside_the_session_nothing_is_quoted(self):
        ws, calls = self._socket({self.NIFTY: {"stat": "Ok", "lp": "22473.30"}})
        for clock in (datetime(2026, 10, 5, 9, 14), datetime(2026, 10, 5, 15, 20)):
            self._poll(ws, clock=clock)
        self.assertEqual(calls, [])

    def test_a_bad_quote_is_not_a_price(self):
        frozen = {"stat": "Ok", "lp": "22473.30", "lut": "1700000000"}
        for quote in (None, {"stat": "Ok"}, {"stat": "Ok", "lp": "0"}, {"stat": "Ok", "lp": "nan"},
                      frozen):
            with self.subTest(quote=quote):
                ws, _ = self._socket({self.NIFTY: quote})
                self._poll(ws)
                with patch("live_websocket.time.monotonic", return_value=101.0):
                    self.assertIsNone(ws.get_fresh_price_for(self.NIFTY))
