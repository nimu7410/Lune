# live_websocket.py
"""The Shoonya WebSocket: price ticks and order updates, kept connected."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import signal
import ssl
import sys
import threading
import time
from datetime import time as dt_time
from typing import Any, Callable, Dict, List, Optional, Union

import websocket

import ist
from config import WEBSOCKET_SETTINGS, WebsocketSettings
from log_throttle import LogThrottle, suffix
from shoonya_login import redact


def _build_ssl_context() -> ssl.SSLContext:
    """
    Verify Shoonya's certificate.

    This socket carries the access token (in the URL) and the live order feed,
    so disabling verification invites a MITM.  api.shoonya.com presents a valid
    publicly-trusted chain, so verification costs nothing.
    """
    context = ssl.create_default_context()
    # On some Windows hosts the OS trust store (what create_default_context()
    # loads by default) fails to build a chain for api.shoonya.com and reports
    # it as self-signed, even though the same chain verifies cleanly against
    # certifi's CA bundle - the bundle `requests` already trusts for the REST
    # login on this same host. Layer certifi's roots on top of the OS store
    # (additive, not a replacement) so verification is robust to a stale/
    # incomplete Windows cert store without weakening it anywhere else.
    try:
        import certifi

        context.load_verify_locations(cafile=certifi.where())
    except Exception as exc:
        logging.warning("Could not load certifi CA bundle for WebSocket TLS: %s", exc)
    return context


_SSL_CTX = _build_ssl_context()


class LiveWebSocket:
    """
    Shoonya websocket manager using the OAuth websocket flow.
    Keeps the existing tick/order callbacks, reconnect behavior, and public API.

    Prices do not depend on the socket alone. A subscription whose ticks stop -
    the socket dropped, is reconnecting, or is up but silent for that token -
    is quoted over REST (`GetQuotes`) until ticks resume. A REST quote is the
    exchange's current LTP, not a remembered one, so it carries the same
    freshness rule as a tick. Without it every outage, however short, read as
    "no live price" and every entry inside it was skipped.
    """

    #: A subscription whose newest tick is older than this is quoted over REST.
    REST_STALE_AFTER = 5.0
    #: Seconds between REST quotes for one subscription while its ticks are out.
    REST_INTERVAL = 2.0
    #: A quote whose `lut` (exchange update time, epoch seconds) is older than
    #: this is a frozen price, not a current one.
    REST_MAX_LAG = 60.0
    #: Longest wait between REST quotes while the broker cannot quote either.
    REST_BACKOFF_CAP = 30.0
    #: Short, so one hung quote cannot hold up the other subscriptions for long.
    REST_TIMEOUT = 5.0
    #: Indices print `lp` only inside the session; outside it a REST quote
    #: would pass the last close off as current.
    REST_WINDOW = (dt_time(9, 15, 5), dt_time(15, 15))
    #: How old a price may be and still count as live; `get_fresh_price_for`'s.
    FRESH_FOR = 15.0
    #: A connected socket that has delivered nothing - no tick, not even a pong
    #: to the 3s heartbeat - for this long is dead without the kernel knowing.
    #: Left alone it reads as "connected" until TCP gives up (~15 min on
    #: Linux), and every order update in that window is lost.
    SILENT_AFTER = 20.0

    # Feed counters for the dashboard (`health`). Class defaults, so a socket
    # built without `__init__` still reports rather than raising.
    _opens = 0                          # authenticated connections this process
    _opened_at: Optional[float] = None  # monotonic time of the current one
    _ticks = 0                          # priced ticks received
    _last_rx: Optional[float] = None    # monotonic time of the last frame received
    #: The subscriptions the REST fallback quotes; None quotes every one. Only
    #: keys something reads a fresh price for belong here: a quote nobody reads
    #: is a broker call, a log line and a blocking wait for nothing.
    _rest_keys: Optional[frozenset] = None

    def __init__(self, api, *, ws_settings: WebsocketSettings | None = None):
        cfg = ws_settings or WEBSOCKET_SETTINGS
        self.api = api

        self.feed_opened: bool = False
        self.latest_price: Optional[float] = None
        # Monotonic stamp of the last tick, so consumers can tell a live price
        # from the last one seen before the socket dropped.
        self.latest_price_ts: Optional[float] = None

        # One price per subscribed instrument, keyed `EXCH|TOKEN`.
        #
        # The feed carries five index subscriptions now, and every tick names
        # the token it belongs to. Folding them all into `latest_price` would
        # quote whichever index ticked last - so a BANKNIFTY tick would set the
        # price a NIFTY entry measures its stop from. `latest_price` is kept as
        # the *primary* instrument's price (the first symbol passed to `start`),
        # which is exactly what it was before there was more than one, and
        # every other consumer asks for the key it means.
        self._prices: Dict[str, float] = {}
        self._price_stamps: Dict[str, float] = {}
        self._primary_key: Optional[str] = None
        # token -> subscribed `EXCH|TOKEN`. Shoonya's `tf` deltas carry `tk` but
        # not always `e`, so a tick is attributed by token against what was
        # actually subscribed rather than by trusting both fields to be present.
        self._key_by_token: Dict[str, Optional[str]] = {}

        # REST quotes for subscriptions whose ticks have stopped. Kept apart
        # from the tick prices: a dropped socket retires its ticks (see
        # `close_callback`), but not a quote that never came from it.
        self._rest_prices: Dict[str, float] = {}
        self._rest_stamps: Dict[str, float] = {}
        self._rest_next: Dict[str, float] = {}   # key -> monotonic time of next quote
        self._rest_failures: Dict[str, int] = {}
        self._rest_serving: set[str] = set()
        self._rest_thread: threading.Thread | None = None

        self._order_handlers: List[Callable[[Dict[str, Any]], None]] = []
        self._price_handlers: List[Callable[[Dict[str, Any]], None]] = []

        self.resubscribe_symbols: List[str] | None = None
        self.max_retries = cfg.max_retries
        self._backoff_initial = float(cfg.backoff_initial)
        self._backoff_cap = float(cfg.backoff_cap)
        self._open_timeout = float(cfg.open_timeout)
        self._reconnecting = False
        # Counts consecutive failed reconnect attempts in the current outage.
        # Zeroed the moment the feed authenticates, so the next drop starts
        # from the short delay again instead of inheriting an old outage's
        # ceiling.
        self._consecutive_failures = 0
        self._failure_throttle = LogThrottle(float(cfg.failure_log_interval))

        self._lock = threading.Lock()
        self._ws_lock = threading.Lock()
        self._ws: websocket.WebSocketApp | None = None
        self._ws_thread: threading.Thread | None = None
        self._closing = False
        # `_closing` is also raised for the brief window of a hard reset, so it
        # cannot double as "the bot is going away". Only this flag stops the
        # reconnect supervisor for good.
        self._shutting_down = False
        self._reconnect_task: asyncio.Task | None = None

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None

        try:
            signal.signal(signal.SIGINT, self._shutdown)
        except Exception:
            pass

    def _access_token(self) -> str:
        token = getattr(self.api, "access_token", None)
        if token:
            return str(token).strip()
        if hasattr(self.api, "ensure_websocket_session"):
            ret = self.api.ensure_websocket_session()
            if isinstance(ret, dict) and ret.get("stat") == "Ok":
                token = getattr(self.api, "access_token", None)
                if token:
                    return str(token).strip()
        raise RuntimeError("Shoonya access token is not available for websocket")

    def _user_id(self) -> str:
        user_id = getattr(self.api, "userid", None) or getattr(self.api, "user", None)
        if not user_id:
            raise RuntimeError("Shoonya user id is not available for websocket")
        return str(user_id).strip()

    def _send_json(self, payload: Dict[str, Any]) -> None:
        with self._ws_lock:
            if not self._ws:
                raise RuntimeError("WebSocket is not initialized")
            self._ws.send(json.dumps(payload, separators=(",", ":")))

    def _on_open(self, ws):
        if ws is not self._ws:
            return
        logging.info("Shoonya WebSocket TCP connection opened.")
        user_id = self._user_id()
        self._send_json(
            {
                "t": "a",
                "uid": user_id,
                "actid": user_id,
                "accesstoken": self._access_token(),
                "source": "API",
            }
        )
        logging.info("Shoonya WebSocket auth sent.")

    def _on_message(self, ws, message):
        if ws is not self._ws:
            return
        self._last_rx = time.monotonic()
        data = self._to_dict(message)
        msg_type = data.get("t")

        if msg_type in {"ak", "ck"}:
            if data.get("s") == "OK":
                self.open_callback()
            else:
                logging.error("Shoonya WebSocket auth failed: %s", redact(data.get("emsg", "rejected")))
                # Reconnecting with the same dead token would loop forever.
                self._refresh_session_after_auth_failure()
                self.error_callback(data.get("emsg") or "authentication rejected")
            return

        if msg_type in {"tk", "tf", "dk", "df"}:
            self.event_handler_feed_update(data)
            return

        if msg_type == "om":
            self.event_handler_order_update(data)
            return

        logging.debug("Unhandled Shoonya WebSocket message type: %s", msg_type)

    def _refresh_session_after_auth_failure(self) -> None:
        """
        Drop the rejected token and pick up a valid one before reconnecting.

        Prefers a session another process may have already published; only
        mints a new one if there is none.
        """
        try:
            token = getattr(self.api, "access_token", None)
            if hasattr(self.api, "_session_manager"):
                self.api._session_manager.invalidate(token)
            if hasattr(self.api, "refresh_session_after_expiry"):
                self.api.refresh_session_after_expiry()
                logging.info("Shoonya session refreshed after WebSocket auth rejection.")
        except Exception as exc:
            logging.error("Could not refresh Shoonya session after WS auth failure: %s", exc)

    def _on_pong(self, ws, _data):
        if ws is self._ws:
            self._last_rx = time.monotonic()

    def _close_if_silent(self) -> None:
        """Drop a socket that is open but has gone silent, so it is reopened."""
        last = self._last_rx
        if not self.feed_opened or last is None or time.monotonic() - last <= self.SILENT_AFTER:
            return
        ws = self._ws
        self.error_callback(
            f"no data or heartbeat reply from the broker in {self.SILENT_AFTER:.0f}s; reconnecting")
        if ws is not None:
            self._close_quietly(ws)

    @staticmethod
    def _close_quietly(ws) -> None:
        """Hang up at once; the socket's own thread then errors out and unwinds.

        Not `ws.close()`: that waits 3s for the peer's close frame, and a second
        caller closes the descriptor under the reader's `select`, which then
        sleeps out its 10s timeout instead of waking.
        """
        try:
            sock = getattr(ws, "sock", None)
            if sock is not None:
                sock.abort()
        except Exception as exc:
            logging.debug("WebSocket close failed (ignored): %s", exc)

    def _on_error(self, ws, error):
        if ws is not self._ws:
            return
        self.error_callback(error)

    def _on_close(self, ws, close_status_code, close_msg):
        if ws is not self._ws:
            return
        self.close_callback()

    def _run_websocket(self, ws: websocket.WebSocketApp) -> None:
        ws.run_forever(
            ping_interval=3,
            ping_payload='{"t":"h"}',
            sslopt={"context": _SSL_CTX},
        )

    def event_handler_feed_update(self, tick_data: dict):
        # Shoonya ``tf`` messages are deltas: many valid updates omit ``lp``.
        # Missing/invalid price fields must not erase the last valid LTP. Apart
        # from making consumers flicker between a price and zero, clearing it
        # made ForecastLogger alternate between real preview legs and NULL legs
        # on virtually every tick.
        raw_price = tick_data.get("lp")
        try:
            parsed_price = float(raw_price) if raw_price is not None else None
        except (TypeError, ValueError):
            parsed_price = None
        if parsed_price is not None and math.isfinite(parsed_price) and parsed_price > 0:
            stamp = time.monotonic()
            self._ticks += 1
            key = self._tick_key(tick_data)
            # `getattr`: a tick can reach this before `start` has run, and the
            # feed thread must not die on a half-built socket.
            prices = getattr(self, "_prices", None)
            if key is not None and prices is not None:
                with self._lock:
                    prices[key] = parsed_price
                    self._price_stamps[key] = stamp
            # `latest_price` follows the primary instrument. With a single
            # subscription every tick is that instrument by definition, so it is
            # taken as-is - unchanged from the single-feed build, and it keeps
            # working if `start` was never called. With several, an
            # unattributable tick must not be allowed to answer "what is the
            # primary index trading at", so it updates nothing.
            single_feed = len(getattr(self, "resubscribe_symbols", None) or ()) <= 1
            if single_feed or (key is not None and key == self._primary_key):
                self.latest_price = parsed_price
                self.latest_price_ts = stamp

        for handler in list(self._price_handlers):
            try:
                handler(tick_data)
            except Exception as exc:
                logging.error("Price handler error: %s", exc)

    def _to_dict(self, message: Any) -> Dict[str, Any]:
        if isinstance(message, dict):
            return message
        if isinstance(message, (bytes, bytearray)):
            try:
                return json.loads(message.decode("utf-8"))
            except Exception:
                return {"raw": message.decode("utf-8", errors="ignore")}
        if isinstance(message, str):
            try:
                return json.loads(message)
            except Exception:
                return {"raw": message}
        return {"raw": message}

    def event_handler_order_update(self, message: Any):
        msg = self._to_dict(message)
        for handler in list(self._order_handlers):
            try:
                handler(msg)
            except Exception as exc:
                logging.error("Order update handler error: %s", exc)

    def open_callback(self):
        already_open = False
        with self._lock:
            already_open = self.feed_opened
            self.feed_opened = True
            self._reconnecting = False
        self._last_rx = time.monotonic()
        self._consecutive_failures = 0
        self._failure_throttle.reset("reconnect")
        self._failure_throttle.reset("ws_error")
        self._failure_throttle.reset("logged_out")
        if not already_open:
            self._opens += 1
            self._opened_at = time.monotonic()
            logging.info("Shoonya WebSocket authenticated.")

        if self.resubscribe_symbols:
            self._safe_subscribe(self.resubscribe_symbols)

        try:
            self.subscribe_orders()
            logging.info("Subscribed to order updates.")
        except Exception as exc:
            logging.warning("Order subscription failed (continuing): %s", exc)

    def close_callback(self):
        with self._lock:
            self.feed_opened = False
            # Retire the last tick's freshness. Reconnecting flips `feed_opened`
            # back on before any new tick arrives, and without this a price from
            # the previous connection would still pass the freshness check and
            # could become an entry price - and the stop level measured from it.
            self.latest_price_ts = None
            self._price_stamps.clear()
        if self._shutting_down:
            return
        logging.warning("Shoonya WebSocket closed by server.")
        if not self._closing:
            self._schedule_reconnect()

    def error_callback(self, error: Exception | str | dict):
        if self._shutting_down:
            return
        with self._lock:
            self.feed_opened = False
            self.latest_price_ts = None
            self._price_stamps.clear()
        detail = redact(error, token=getattr(getattr(self, "api", None), "access_token", None))
        msg = detail.lower()
        # A broker outage produces one of these per attempt, each carrying the
        # full nginx error page. Unthrottled that was hundreds of near-identical
        # multi-line ERROR records for a single incident, which buries every
        # other error in the session log. The first is always emitted; the rest
        # are folded into a periodic line that reports how many were suppressed.
        emit, folded = self._failure_throttle.should_log("ws_error")
        if emit:
            brief = "broker gateway returned 502" if "502 bad gateway" in msg else detail
            logging.error("WebSocket error: %s%s", brief, suffix(folded))
        if "socket is already opened" in msg:
            logging.warning("Detected duplicate socket condition; forcing reset.")
            self._hard_reset()
            return
        self._schedule_reconnect()

    def _tick_key(self, tick_data: Dict[str, Any]) -> Optional[str]:
        """The subscribed `EXCH|TOKEN` a tick belongs to, or None.

        `tf` deltas name the token and often omit the exchange. A token shared
        by two exchanges is ambiguous without `e`, so its delta cannot update
        either price.
        """
        token = tick_data.get("tk")
        if not token:
            return None
        token = str(token).strip()
        known = getattr(self, "_key_by_token", {})
        exch = tick_data.get("e")
        if exch:
            key = f"{str(exch).strip()}|{token}"
            symbols = getattr(self, "resubscribe_symbols", None)
            return key if not symbols or key in symbols else None
        return known.get(token)

    def get_fresh_price_for(self, key: str, max_age_seconds: float = 15.0) -> Optional[float]:
        """The last tick for one `EXCH|TOKEN`, but only while it is genuinely current.

        Same rule as `get_fresh_price`, per instrument: an entry measures its
        stop from the fill, so a stale tick would put the stop at a level that
        never traded. Never falls back to another instrument's price: a missing
        feed must read as missing, not as whatever else happens to be ticking.
        Falls back to this key's own REST quote, held to the same age.
        """
        if self.feed_opened:
            with self._lock:
                price = self._prices.get(key)
                stamp = self._price_stamps.get(key)
            if price and stamp is not None and (time.monotonic() - stamp) <= max_age_seconds:
                return float(price)
        return self._rest_price(key, max_age_seconds)

    def health(self, names: Dict[str, str]) -> Dict[str, Any]:
        """The socket's state and where each index's price comes from, for the dashboard.

        `names` maps an index to its `EXCH|TOKEN`. A source is "ws" while the
        index ticks, "rest" while its REST quote stands in, "stale" when
        neither is fresh, and "none" before it has ever priced.
        """
        now = time.monotonic()
        start, end = self.REST_WINDOW
        with self._lock:
            connected = bool(self.feed_opened)
            ticks = dict(getattr(self, "_price_stamps", {}))
            quotes = dict(getattr(self, "_rest_stamps", {}))
        feeds = []
        for name, key in names.items():
            tick_age = None if ticks.get(key) is None else now - ticks[key]
            quote_age = None if quotes.get(key) is None else now - quotes[key]
            if tick_age is not None and tick_age <= self.REST_STALE_AFTER:
                source = "ws"
            elif quote_age is not None and quote_age <= self.FRESH_FOR:
                source = "rest"
            elif tick_age is not None and tick_age <= self.FRESH_FOR:
                source = "ws"           # quiet for a moment, still fresh enough to trade on
            else:
                source = "stale" if tick_age is not None or quote_age is not None else "none"
            feeds.append({"index": name, "source": source,
                          "tick_age_s": None if tick_age is None else round(tick_age, 1)})
        return {
            "connected": connected,
            "reconnecting": bool(getattr(self, "_reconnecting", False)),
            "failures": int(getattr(self, "_consecutive_failures", 0)),
            "uptime_s": round(now - self._opened_at) if connected and self._opened_at else None,
            "reconnects": max(0, self._opens - 1),
            "ticks": self._ticks,
            # Indices print only inside this window; a silent feed outside it
            # is the market closed, not a fault.
            "in_session": start <= ist.now_naive().time() < end,
            "feeds": feeds,
            "as_of_ist": ist.isoformat(),
        }

    def _rest_price(self, key: Optional[str], max_age_seconds: float) -> Optional[float]:
        """`key`'s REST quote while it is no older than `max_age_seconds`."""
        prices = getattr(self, "_rest_prices", None)
        if key is None or prices is None:
            return None
        with self._lock:
            price = prices.get(key)
            stamp = self._rest_stamps.get(key)
        if not price or stamp is None or (time.monotonic() - stamp) > max_age_seconds:
            return None
        return float(price)

    def _rest_quote_loop(self) -> None:
        while not self._shutting_down:
            try:
                self._close_if_silent()
                self._rest_quote_once()
            except Exception as exc:  # noqa: BLE001 - the loop must outlive any one fault
                emit, folded = self._failure_throttle.should_log("rest_loop")
                if emit:
                    logging.error("REST price fallback failed: %s%s", exc, suffix(folded))
            time.sleep(1.0)

    def _rest_quote_once(self) -> None:
        """Quote every subscription whose ticks have gone quiet, once."""
        start, end = self.REST_WINDOW
        if not start <= ist.now_naive().time() < end:
            return
        wanted = self._rest_keys
        for key in list(self.resubscribe_symbols or ()):
            if "|" not in key or (wanted is not None and key not in wanted):
                continue
            now = time.monotonic()
            with self._lock:
                stamp = self._price_stamps.get(key) if self.feed_opened else None
            if stamp is not None and now - stamp <= self.REST_STALE_AFTER:
                if key in self._rest_serving:
                    self._rest_serving.discard(key)
                    logging.info("Ticks resumed for %s; REST price fallback off.", key)
                continue
            if now < self._rest_next.get(key, float("-inf")):
                continue
            # A quote can take up to REST_TIMEOUT; the dead-socket check must
            # not wait behind a whole round of them.
            self._close_if_silent()
            price = self._fetch_rest_price(key)
            if price is None:
                # Back off while the broker cannot quote either, so an outage
                # costs a bounded number of requests and log lines.
                fails = self._rest_failures[key] = self._rest_failures.get(key, 0) + 1
                self._rest_next[key] = now + min(self.REST_BACKOFF_CAP,
                                                 self.REST_INTERVAL * 2 ** fails)
                continue
            self._rest_failures[key] = 0
            self._rest_next[key] = now + self.REST_INTERVAL
            with self._lock:
                self._rest_prices[key] = price
                self._rest_stamps[key] = time.monotonic()
            if key not in self._rest_serving:
                self._rest_serving.add(key)
                emit, folded = self._failure_throttle.should_log(f"rest:{key}")
                if emit:
                    logging.warning(
                        "No WebSocket tick for %s in %.0fs; pricing it from REST "
                        "quotes until ticks resume.%s", key, self.REST_STALE_AFTER, suffix(folded),
                    )

    def _fetch_rest_price(self, key: str) -> Optional[float]:
        exch, token = key.split("|", 1)
        try:
            quote = self.api.get_quotes(exch, token, timeout=self.REST_TIMEOUT)
            if not isinstance(quote, dict) or quote.get("lp") in (None, ""):
                return None     # `get_quotes` has already logged a broker refusal
            price = float(quote["lp"])
            lut = quote.get("lut")
            if lut not in (None, "") and time.time() - float(lut) > self.REST_MAX_LAG:
                return None
        except Exception as exc:  # noqa: BLE001 - network faults are expected here
            emit, folded = self._failure_throttle.should_log(f"rest_fail:{key}")
            if emit:
                logging.error("REST quote for %s failed: %s%s", key, exc, suffix(folded))
            return None
        return price if math.isfinite(price) and price > 0 else None

    def get_fresh_price(self, max_age_seconds: float = 15.0) -> Optional[float]:
        """The primary instrument's last tick, but only while it is genuinely current.

        `latest_price` keeps the last value it ever saw, even after the socket
        drops - fine for an exit, where any reasonable number beats not getting
        out, but wrong for an entry. The strategy measures its stop from the
        entry price, so entering off a stale tick puts the stop at a level that
        never existed. Returns None when the feed is down or the last tick has
        aged out, and the caller retries on its next poll.
        """
        if (self.feed_opened and self.latest_price is not None
                and self.latest_price_ts is not None
                and (time.monotonic() - self.latest_price_ts) <= max_age_seconds):
            return self.latest_price
        return self._rest_price(getattr(self, "_primary_key", None), max_age_seconds)

    def add_price_listener(self, handler: Callable[[Dict[str, Any]], None]) -> None:
        if handler:
            self._price_handlers.append(handler)

    def add_order_update_listener(self, handler: Callable[[Dict[str, Any]], None]) -> None:
        if handler:
            self._order_handlers.append(handler)

    def start(
        self,
        symbols: Union[str, List[str]],
        *,
        order_update_handler: Optional[Callable[[Dict[str, Any]], None]] = None,
        rest_keys: Optional[List[str]] = None,
    ):
        """Subscribe `symbols`. `rest_keys`: the subscriptions the REST fallback
        may quote while their ticks are out (default: every one)."""
        if order_update_handler:
            self.add_order_update_listener(order_update_handler)
        self._rest_keys = None if rest_keys is None else frozenset(rest_keys)
        self.resubscribe_symbols = [symbols] if isinstance(symbols, str) else symbols
        # The first subscription is the primary one: it is what `latest_price`
        # and `get_fresh_price` answer for, so their meaning is unchanged from
        # the single-instrument build.
        self._primary_key = self.resubscribe_symbols[0] if self.resubscribe_symbols else None
        # ponytail: drop exchange-less ticks for colliding tokens; use separate
        # sockets per exchange if a live instrument pair needs those deltas.
        self._key_by_token = {}
        for key in self.resubscribe_symbols or []:
            if "|" in key:
                token = key.split("|", 1)[1]
                if token in self._key_by_token and self._key_by_token[token] != key:
                    self._key_by_token[token] = None
                else:
                    self._key_by_token[token] = key
        loop = self._loop or asyncio.get_event_loop()
        loop.create_task(self._start_and_supervise())
        if self._rest_thread is None:
            self._rest_thread = threading.Thread(
                target=self._rest_quote_loop, name="rest-price-fallback", daemon=True)
            self._rest_thread.start()

    async def _start_and_supervise(self) -> None:
        """Open the feed, handing off to the supervisor if it does not come up.

        The very first connect used to be fire-and-forget: if it failed before a
        socket existed there were no callbacks, so nothing ever retried and the
        bot ran the whole session with a feed that had never opened.
        """
        if await self._start_websocket_async():
            if await self._await_feed_open(self._open_timeout):
                return
        self._schedule_reconnect()

    async def _start_websocket_async(self) -> bool:
        """Launch one connection attempt.

        Returns True only when a socket thread was actually started, so the
        reconnect supervisor can tell "connecting" from "could not even try"
        and back off accordingly. Every early return here used to be silent:
        the caller assumed an attempt was in flight and relied on that socket's
        `on_error`/`on_close` to drive the next retry, so a path that never
        created a socket produced no callbacks and no retry at all.
        """
        with self._lock:
            if self.feed_opened:
                logging.debug("WebSocket already authenticated; start skipped.")
                return True
            stale = self._ws_thread if (self._ws_thread and self._ws_thread.is_alive()) else None
            self._closing = False

        if stale is not None:
            # A previous attempt's `run_forever` is still unwinding. Give it a
            # moment rather than skipping the attempt outright - skipping is
            # what silently stalled reconnection once the supervisor's retry
            # budget ran out. The feed is down, so that socket serves nothing:
            # close it, or one that connected but never authenticated lives on
            # its pings forever and fails every later attempt right here.
            self._close_quietly(self._ws)
            await self._wait_for_thread(stale, 2.0)
            with self._lock:
                if self.feed_opened:
                    return True
                if self._ws_thread is stale and stale.is_alive():
                    logging.debug("Previous WebSocket thread still running; will retry.")
                    return False

        if not getattr(self.api, "login_status", True):
            # Not fatal and not a reason to stop reconnecting: the token can be
            # reissued by this process or published by another one. Try to pick
            # a valid session up, then report failure so the caller retries.
            emit, folded = self._failure_throttle.should_log("logged_out")
            if emit:
                logging.warning(
                    "Shoonya API not logged in; refreshing session before WebSocket.%s",
                    suffix(folded),
                )
            self._refresh_session_after_auth_failure()
            if not getattr(self.api, "login_status", True):
                return False

        try:
            token = self._access_token()
            url = f"wss://api.shoonya.com/NorenWSAPI/{token}"
            logging.info("Opening Shoonya WebSocket.")
            ws = websocket.WebSocketApp(
                url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_pong=self._on_pong,
            )
            thread = threading.Thread(target=self._run_websocket, args=(ws,), daemon=True)
            with self._lock:
                self._ws = ws
                self._ws_thread = thread
            thread.start()
            return True
        except Exception as exc:
            logging.error("WebSocket start failed: %s", redact(exc, token=token if "token" in locals() else None))
            return False

    @staticmethod
    async def _wait_for_thread(thread, timeout: float) -> None:
        """Poll a winding-down socket thread instead of blocking on join().

        `asyncio.to_thread` would park this coroutine on an executor thread for
        the whole timeout; the reconnect supervisor runs on the trading loop, so
        that is time the loop is not servicing anything else.
        """
        deadline = time.monotonic() + timeout
        while thread.is_alive() and time.monotonic() < deadline:
            await asyncio.sleep(0.05)

    def _hard_reset(self):
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(
                lambda: asyncio.create_task(self._hard_reset_async())
            )
        else:
            asyncio.run(self._hard_reset_async())

    async def _hard_reset_async(self):
        with self._lock:
            self.feed_opened = False
            self.latest_price_ts = None
            self._price_stamps.clear()
        try:
            if self._ws:
                self._closing = True
                self._ws.close()
                logging.info("Previous WebSocket closed.")
            if self._ws_thread and self._ws_thread.is_alive():
                await self._wait_for_thread(self._ws_thread, 2.0)
        except Exception as exc:
            logging.debug("WebSocket close failed (ignored): %s", exc)
        self._closing = False
        await asyncio.sleep(1)
        if not await self._start_websocket_async():
            self._schedule_reconnect()

    def _schedule_reconnect(self):
        """Arm the reconnect supervisor, unless one is already running."""
        with self._lock:
            if self._shutting_down or self._reconnecting:
                return
            self._reconnecting = True
        try:
            if self._loop and self._loop.is_running():
                self._loop.call_soon_threadsafe(self._start_reconnect_task)
            else:
                threading.Thread(
                    target=lambda: asyncio.run(self._reconnect_loop_async()),
                    daemon=True,
                ).start()
        except Exception:
            # Never leave the flag raised with nothing running behind it: that
            # would make every later `_schedule_reconnect` a no-op and the feed
            # would stay down for the rest of the session.
            with self._lock:
                self._reconnecting = False
            raise

    def _start_reconnect_task(self) -> None:
        if self._shutting_down:
            with self._lock:
                self._reconnecting = False
            return
        self._reconnect_task = asyncio.create_task(self._reconnect_loop_async())

    async def stop(self) -> None:
        """Stop callbacks and finish the supervisor before its loop disappears."""
        with self._lock:
            self._shutting_down = True
            self._closing = True
            task = self._reconnect_task
            ws = self._ws
            thread = self._ws_thread
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if ws:
            ws.close()
        if thread and thread.is_alive():
            await self._wait_for_thread(thread, 2.0)

    def _next_backoff(self, failures: int) -> float:
        """Exponential backoff with jitter, capped, based on the outage length.

        Jitter matters because the drop that starts an outage is usually
        broker-side and hits every client at once; without it every bot in the
        fleet retries on the same schedule and re-creates the thundering herd
        that the gateway was already failing under.
        """
        # Clamp the exponent, not just the result: a long outage keeps counting
        # failures, and 2 ** (a few thousand) raises OverflowError before the
        # cap can ever apply - which would kill the supervisor thread and take
        # the feed down for good.
        steps = min(max(0, failures - 1), 32)
        capped = min(self._backoff_initial * (2 ** steps), self._backoff_cap)
        return capped * (0.5 + random.random() * 0.5)

    async def _await_feed_open(self, timeout: float) -> bool:
        """Wait for the freshly-opened socket to authenticate."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if self.feed_opened:
                    return True
                if self._shutting_down:
                    return False
            await asyncio.sleep(0.2)
        with self._lock:
            return self.feed_opened

    async def _reconnect_loop_async(self):
        """Reopen the feed, and keep trying until it is open or the bot exits.

        This loop is the only thing that reopens the socket, so it must not
        return while the feed is down and the bot is still trading. The previous
        version stopped after `max_retries` attempts and logged "Max websocket
        reconnect attempts reached"; reconnection carried on afterwards only by
        accident, because the last doomed attempt's `on_close`/`on_error` fired
        after the flag was cleared and armed a brand new loop. Two consequences,
        both seen in a live session log:

        * Any attempt that produced no callback - a socket thread still winding
          down, a false `login_status`, a token lookup that raised - broke that
          accidental chain and left the feed dead with nothing scheduled to
          reopen it, while `get_fresh_price` quietly returned None for the rest
          of the day.
        * Each accidental restart began at the initial delay again, so the
          backoff never actually grew. A multi-minute broker outage was met with
          a doomed handshake every few seconds and hundreds of identical 502
          error records.

        The delay now grows across the whole outage rather than per loop, and
        the failure logging is throttled so an outage costs a bounded number of
        lines however long it lasts.
        """
        try:
            while True:
                with self._lock:
                    if self.feed_opened:
                        return
                    if self._shutting_down:
                        return

                self._consecutive_failures += 1
                failures = self._consecutive_failures
                delay = self._next_backoff(failures)

                if failures <= self.max_retries:
                    logging.info(
                        "Reconnect attempt %s in %.1fs.", failures, delay
                    )
                else:
                    emit, folded = self._failure_throttle.should_log("reconnect")
                    if emit:
                        logging.error(
                            "Shoonya WebSocket feed still down after %s attempts; "
                            "retrying in %.1fs.%s",
                            failures,
                            delay,
                            suffix(folded),
                        )

                await asyncio.sleep(delay)

                with self._lock:
                    if self._shutting_down:
                        return

                if await self._start_websocket_async():
                    if await self._await_feed_open(self._open_timeout):
                        logging.info(
                            "Shoonya WebSocket feed restored after %s attempt(s).",
                            failures,
                        )
                        return
        finally:
            # Always released, including on cancellation, so a later drop can
            # arm a fresh supervisor.
            with self._lock:
                self._reconnecting = False

    def _safe_subscribe(self, symbols: List[str]):
        try:
            self.subscribe(symbols)
            logging.info("Subscribed to %s", symbols)
        except Exception as exc:
            logging.error("subscribe(%s) failed: %s", symbols, exc)

    def subscribe(self, symbols: Union[str, List[str]]):
        keys = "#".join(symbols) if isinstance(symbols, list) else symbols
        self._send_json({"t": "t", "k": keys})

    def unsubscribe(self, symbols: Union[str, List[str]]):
        keys = "#".join(symbols) if isinstance(symbols, list) else symbols
        self._send_json({"t": "u", "k": keys})

    def subscribe_orders(self):
        self._send_json({"t": "o", "actid": self._user_id()})

    def _shutdown(self, *_):
        logging.info("Received SIGINT; shutting down WebSocket.")
        with self._lock:
            self._shutting_down = True
        try:
            self.unsubscribe("*")
        except Exception:
            pass
        try:
            if self._ws:
                self._closing = True
                self._ws.close()
        except Exception:
            pass
        sys.exit(0)
