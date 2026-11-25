# data_handler.py
"""Today's 5-minute candles for one index, refreshed at each bar boundary."""

from __future__ import annotations

import asyncio
import datetime as _dt
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

import ist
from config import DATA_SETTINGS


def _to_iso(dt_obj: _dt.datetime) -> str:
    """Publish a bar time in the feed's IST wire format.

    Earlier builds emitted these digits with a `Z` suffix - IST wall clock
    labelled UTC - which every consumer then had to un-lie about. The offset is
    now stated honestly, so an external reader of `/data` gets the instant the
    bar actually belongs to instead of one 5h30m late.
    """
    return ist.to_ist(dt_obj).strftime(ist.FEED_TIME_FORMAT)


def _as_ist_series(values: pd.Series) -> pd.Series:
    """Parse a column of broker candle times into IST-aware timestamps.

    Shoonya sends `%d-%m-%Y %H:%M:%S` IST wall clock with no offset, so the
    digits are *labelled* IST rather than shifted into it - reading them as UTC
    and converting would move every bar 5h30m and make the 09:15 session open
    land at 14:45, which turns the strategy's session guard entry-inert for the
    whole day. The looser paths below only exist for a broker format change and
    keep the same rule: an explicit offset is honoured, a bare wall clock is IST.
    """
    parsed = pd.to_datetime(
        values, format="%d-%m-%Y %H:%M:%S", errors="coerce", dayfirst=True
    )
    if parsed.isna().all():
        try:
            parsed = pd.to_datetime(values, errors="coerce")
        except (ValueError, TypeError):
            # Rows carrying different explicit offsets have no single naive
            # dtype; normalising through UTC is lossless for labelled input.
            parsed = pd.to_datetime(values, utc=True, errors="coerce")
    if not pd.api.types.is_datetime64_any_dtype(parsed):
        parsed = pd.to_datetime(parsed, utc=True, errors="coerce")
    if parsed.dt.tz is None:
        return parsed.dt.tz_localize(ist.IST_NAME)
    return parsed.dt.tz_convert(ist.IST_NAME)


def shoonya_candles(
    api, exch: str, tok: str, start: _dt.datetime, end: Optional[_dt.datetime] = None,
    interval: int = 5, *, warn_on_error: bool = False,
) -> pd.DataFrame:
    """Shoonya's candles of one instrument from `start` (to `end`), oldest first.

    Columns time (IST-aware, the candle's start), open, high, low, close and
    volume; empty when the broker has none. `start`/`end` are IST moments.
    """
    ret = api.get_time_price_series(
        exchange=exch,
        token=tok,
        starttime=ist.to_ist(start).timestamp(),
        endtime=None if end is None else ist.to_ist(end).timestamp(),
        interval=interval,
        warn_on_error=warn_on_error,
    )
    df = pd.DataFrame(ret or []).rename(columns={"timestamp": "time"})
    if df.empty or "time" not in df.columns:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    df = df.sort_values(by="time")
    df["time"] = _as_ist_series(df["time"])
    df["open"] = pd.to_numeric(df.get("into"), errors="coerce")
    df["high"] = pd.to_numeric(df.get("inth"), errors="coerce")
    df["low"] = pd.to_numeric(df.get("intl"), errors="coerce")
    df["close"] = pd.to_numeric(df.get("intc"), errors="coerce")
    df["volume"] = pd.to_numeric(df.get("intv"), errors="coerce").fillna(0) if "intv" in df else 0
    df = df.dropna(subset=["time", "open", "high", "low", "close"])
    return df.sort_values("time").reset_index(drop=True)


class DataHandler:
    """
    REST-driven candle fetcher:
      - Uses Shoonya get_time_price_series with 5m interval from session start.
      - Refreshes around each 5m boundary with short retries for delayed bars.
      - Thread-safe snapshots for Flask/pivot consumers.
    """

    def __init__(
        self,
        api,
        exch: str,
        tok: str,
        *,
        now_fn: Callable[[], _dt.datetime] | None = None,
    ) -> None:
        cfg = DATA_SETTINGS
        self.api = api
        self.exch = exch
        self.tok = tok
        self.refresh_interval = max(5, cfg.refresh_interval)
        self.boundary_grace_seconds = max(0, cfg.boundary_grace_seconds)
        self.boundary_retry_interval = max(0.2, cfg.boundary_retry_interval)
        self.boundary_retry_attempts = max(1, cfg.boundary_retry_attempts)
        self.interval_minutes = max(1, int(cfg.candle_interval_minutes))
        self.session_open = cfg.session_open
        self.session_close = cfg.session_close
        self._now = now_fn or ist.now
        # So "the market is shut" is stated once per closed stretch rather than
        # every five minutes all night.
        self._reported_closed = False
        self._empty_warning_at = 0.0

        self._lock = threading.Lock()
        self._refresh_lock = asyncio.Lock()
        self._five_min: List[Dict[str, Any]] = []
        self._day_start = self._today_midnight()

        self._last_refresh: Optional[_dt.datetime] = None
        self._history_ready: bool = False
        self._last_bar_time: Optional[_dt.datetime] = None

        # Bumped only when the published bars actually differ. Consumers cache
        # derived work against this and rebuild exactly when it moves. Never
        # reset for the lifetime of the handler, including across day rollover.
        self._data_revision: int = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def _today_midnight(self) -> _dt.datetime:
        """Start of the current *IST* trading day.

        This is the `starttime` every history fetch is anchored to, so the zone
        it is computed in decides which day's candles the chart and the strategy
        see. A UTC midnight (the previous behaviour) is 05:30 IST, which happens
        to sit before the 09:15 open and so looked correct during market hours
        while silently resolving to the *previous* IST day for any call made
        between 00:00 and 05:29 IST.
        """
        return ist.midnight(self._now())

    def start_live_updates(self) -> List[asyncio.Task]:
        """
        Starts the periodic refresh loop in the current event loop.
        Returns created tasks for optional tracking.
        """
        loop = asyncio.get_running_loop()
        tasks = [
            loop.create_task(self._bootstrap_history()),
            loop.create_task(self._boundary_loop()),
        ]
        return tasks

    async def _bootstrap_history(self) -> None:
        if not self._market_can_have_candles(self._now()):
            logging.info(
                "Started outside %s-%s; the first candle fetch waits for the "
                "session to open.",
                self.session_open.strftime("%H:%M"),
                self.session_close.strftime("%H:%M"),
            )
            return
        await self._refresh_once()

    def _market_can_have_candles(self, now: _dt.datetime) -> bool:
        """True while the exchange could have produced a candle by `now`.

        Weekends and holidays are not modelled: on those the broker simply
        returns nothing, which this loop already handles quietly. The point of
        the check is the 18 hours a day when polling cannot possibly succeed.
        """
        return self.session_open <= now.time() <= self.session_close

    async def _boundary_loop(self) -> None:
        """
        Sleeps until each 5m boundary, then polls every second (bounded attempts)
        until a new bar appears. Adds a small grace to allow server-side close.
        Falls back to the next boundary if the bar never appears.

        Outside market hours the poll is skipped entirely: the broker has no
        candle to return, so every attempt spent an API call to be told "no
        data" and wrote two warning lines for it - 24 of them before the 2026-08-17
        session had even opened, and every five minutes all night on a process
        that stays up.
        """
        while True:
            now = self._now()
            next_boundary = self._next_boundary(now)
            sleep_s = max(1.0, (next_boundary - now).total_seconds())
            await asyncio.sleep(sleep_s)

            await asyncio.sleep(self.boundary_grace_seconds)

            if not self._market_can_have_candles(self._now()):
                if not self._reported_closed:
                    self._reported_closed = True
                    logging.info(
                        "Outside %s-%s; pausing candle refresh until the next session.",
                        self.session_open.strftime("%H:%M"),
                        self.session_close.strftime("%H:%M"),
                    )
                continue
            self._reported_closed = False

            try:
                await self._fetch_until_new_bar(self._last_bar_time)
            except Exception as exc:  # noqa: BLE001
                logging.error("Boundary refresh failed: %s", exc)
                # If a boundary fetch fails, wait a bit before continuing
                await asyncio.sleep(self.refresh_interval)

    # ------------------------------------------------------------------
    # Core refresh logic
    # ------------------------------------------------------------------
    def _fetch_5m_df(self, day_start: _dt.datetime) -> pd.DataFrame:
        """
        Fetch full-day series from Shoonya starting at day_start using the configured candle interval.
        """
        df = shoonya_candles(self.api, self.exch, self.tok, day_start, interval=self.interval_minutes)
        return pd.DataFrame() if df.empty else df

    async def _refresh_once(self) -> None:
        # Serialise fetch+publish. _bootstrap_history and _boundary_loop both
        # call this, and their network fetches run concurrently on threads: a
        # slow early fetch could otherwise land after a newer one and republish
        # older bars. The revision would still advance while the data went
        # backwards, which is exactly the lie the counter must never tell.
        async with self._refresh_lock:
            await self._refresh_once_locked()

    async def _refresh_once_locked(self) -> None:
        day_start = self._today_midnight()
        df = await asyncio.to_thread(self._fetch_5m_df, day_start)

        if df.empty:
            # Before the first candle of the day this is the expected answer,
            # not a fault; only an empty response once the session is under way
            # says something is wrong with the feed.
            if self._five_min and time.monotonic() - self._empty_warning_at >= 60:
                logging.warning("5m refresh returned no rows.")
                self._empty_warning_at = time.monotonic()
            else:
                logging.debug("5m refresh returned no rows.")
            # A transient empty response must not erase the day's valid bars.
            if self._five_min and self._day_start == day_start:
                refresh_time = self._now()
                with self._lock:
                    self._last_refresh = refresh_time
                return
            bars: List[Dict[str, Any]] = []
        else:
            bars = [
                {
                    "time": row.time.to_pydatetime(),
                    "open": float(row.open),
                    "high": float(row.high),
                    "low": float(row.low),
                    "close": float(row.close),
                }
                for row in df.itertuples()
            ]

        # _now is caller-injectable; resolve it before taking the lock so an
        # exotic clock can never re-enter and deadlock on _lock.
        refresh_time = self._now()

        with self._lock:
            # Compare the whole normalized series, not just length/last time:
            # we refetch the full day, so the broker can revise any bar in
            # place. The boundary loop also refetches identical data on retry,
            # which must not count as a change.
            if bars != self._five_min:
                self._five_min = bars
                self._data_revision += 1
            self._last_bar_time = bars[-1]["time"] if bars else None
            self._day_start = day_start
            self._last_refresh = refresh_time
            self._history_ready = True

    async def _fetch_until_new_bar(self, last_seen: Optional[_dt.datetime]) -> None:
        """
        Refresh until the next timestamp proves the closing bar is final, or
        the strategy's 90-second clock fallback can use the latest revision.
        """
        now = ist.to_ist(self._now())
        boundary = now.replace(
            minute=(now.minute // self.interval_minutes) * self.interval_minutes,
            second=0, microsecond=0,
        )
        expected = boundary - _dt.timedelta(minutes=self.interval_minutes)
        attempts = 0
        current_last: Optional[_dt.datetime] = None
        while attempts < self.boundary_retry_attempts:
            attempts += 1
            await self._refresh_once()
            with self._lock:
                current_last = self._last_bar_time
            if current_last and current_last >= boundary:
                return
            if attempts < self.boundary_retry_attempts:
                await asyncio.sleep(self.boundary_retry_interval)
        if current_last is None or current_last < expected:
            logging.warning(
                "5m bar %s missing after %d attempts (last_seen=%s, current=%s)",
                expected, attempts, last_seen, current_last,
            )

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------
    @staticmethod
    def _format_bars(bars: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            {
                "time": _to_iso(bar["time"]),
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
            }
            for bar in bars
        ]

    def get_latest_data(self) -> List[Dict[str, Any]]:
        """Return full-day 5m candles as JSON-serializable dicts."""
        with self._lock:
            bars = list(self._five_min)
        return self._format_bars(bars)

    @property
    def data_revision(self) -> int:
        """Monotonic counter, bumped whenever the published bars change."""
        with self._lock:
            return self._data_revision

    def get_latest_data_if_changed(
        self, known_revision: Optional[int]
    ) -> tuple[int, Optional[List[Dict[str, Any]]]]:
        """
        Return (revision, candles) when the data differs from known_revision,
        else (revision, None).

        Revision and bars are read in one critical section on purpose. Reading
        the revision and the candles separately lets a refresh land between the
        two calls, which would file stale candles under a fresh revision and
        pin that wrong result until the next change.
        """
        with self._lock:
            revision = self._data_revision
            if known_revision == revision:
                return revision, None
            bars = list(self._five_min)
        # Formatting allocates; no reason to hold the data lock for it.
        return revision, self._format_bars(bars)

    def get_health(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "history_ready": self._history_ready,
                "last_refresh": _to_iso(self._last_refresh) if self._last_refresh else None,
                "five_min_bars": len(self._five_min),
                "last_bar_time": _to_iso(self._last_bar_time) if self._last_bar_time else None,
            }

    def _next_boundary(self, now: _dt.datetime) -> _dt.datetime:
        interval = max(1, self.interval_minutes)
        minute_block = (now.minute // interval) * interval
        boundary = now.replace(minute=minute_block, second=0, microsecond=0)
        if boundary <= now:
            boundary = boundary + _dt.timedelta(minutes=interval)
        return boundary
