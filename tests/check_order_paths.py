#!/usr/bin/env python
r"""Prove that a met entry condition actually reaches an order - TEST and LIVE.

    python tests\check_order_paths.py                 # everything
    python tests\check_order_paths.py --stage 3 -v    # one stage, with logs

The failure this exists for has no symptom. Signals print, the conditions are
met, and no trade appears - because something between the signal and the broker
declined the entry without saying so. A silent skip and a genuine no-signal day
look identical in the log and identical in `botdata.trade_logs`.

So every check here asks two questions of a refusal, not one: did the order
path do the right thing, and *did it say so* at a level `main.py`'s WARNING
handler would keep. A refusal logged at DEBUG is reported as a failure even
when the refusal itself was correct.

Five stages, each independently reportable:

  1. STRATEGY         - v3's recorded sessions through the real `StrategyEngine`
                        and v3's own data path, candle by candle: every entry
                        the live replay offers is the carry v3's backtest takes
                        on its B slots (A is retired), at the 15:11 night fill,
                        and nothing else.
  2. TEST ORDER PATH  - the real `TradeManagerTest` and `StrategyDecider`
                        against an in-memory database.
  3. LIVE ORDER PATH  - the real `TradeManagerLive` against a fake broker and
                        a fake WebSocket, including the failure modes that can
                        swallow an entry: legs missing, a stale leg bias, a
                        rejected leg, a silent order feed, an orphan position,
                        a partial fill, a lost acknowledgement - and the
                        synthetic long itself: the call bought first, then
                        the put sold, at the configured lots.
  4. FULL STACK       - those sessions driving the real decider into both
                        managers, end to end: the night entry at 15:11, the
                        next-open exit, and no trade at all on a shock session.
  5. SILENT SKIP      - every refusal observed above had to name itself.

Nothing here touches the production database, the broker, or the running bot:
the managers are constructed against `tests/order_path_fakes.py`. It is safe to
run while the bot is trading.

Exit code is 0 only when every check passes.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import tempfile
import time
import traceback
from dataclasses import replace
from datetime import date, datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Any, Callable, Optional

TESTS_DIR = Path(__file__).resolve().parent
BASE_DIR = TESTS_DIR.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import instruments
import ist
import log_throttle
import trade_manager as tm
import strategy_logic
from config import TRADE_SETTINGS
from strategy_logic import (
    CARRY_BOOK,
    FULL_SESSION_BARS,
    MOMENTUM_BOOK,
    StrategyDecider,
    StrategyEngine,
    bar_time,
)
from trade_manager import TradeManagerLive, TradeManagerTest

from tests.order_path_fakes import (
    SESSIONS,
    FakeBroker,
    FakeDB,
    FakeSymbols,
    LogCapture,
    ScriptedService,
    candle_close,
    day_index,
    v3_market,
    write_candles,
    write_live_root,
)

#: The day the order-path stages (2, 3, 5) run on. Any 2026 session: the LIVE
#: manager refuses entries in a year with no holiday calendar configured.
ORDER_DAY = date(2026, 8, 14)
BAR_MINUTES = strategy_logic.BAR_INTERVAL_MINUTES


# --------------------------------------------------------------------------- #
#  Reporting
# --------------------------------------------------------------------------- #
class Report:
    def __init__(self) -> None:
        self.rows: list[tuple[str, Optional[bool], str, str]] = []
        self._section: Optional[str] = None

    def section(self, name: str) -> None:
        self._section = name
        self.rows.append((name, None, "", "__section__"))

    def add(self, name: str, ok: Optional[bool], detail: str = "") -> None:
        # Coerced, because a check that hands back a list or a row dict would
        # otherwise count as neither a pass nor a failure and vanish silently -
        # which is the exact class of bug this script exists to catch.
        self.rows.append((name, None if ok is None else bool(ok), detail, self._section or ""))

    def note(self, text: str) -> None:
        self.rows.append((text, None, "", "__note__"))

    @property
    def failures(self) -> list[tuple[str, str]]:
        return [(n, d) for n, ok, d, kind in self.rows
                if ok is False and kind not in {"__section__", "__note__"}]

    @property
    def passed(self) -> int:
        return sum(1 for _n, ok, _d, kind in self.rows
                   if ok is True and kind not in {"__section__", "__note__"})

    def show(self) -> None:
        for name, ok, detail, kind in self.rows:
            if kind == "__section__":
                print(f"\n{name}")
                print("-" * 74)
            elif kind == "__note__":
                print(f"       {name}")
            else:
                mark = "SKIP" if ok is None else ("PASS" if ok else "FAIL")
                print(f"  {mark:<4} {name}{('  -  ' + detail) if detail else ''}")


class Clock:
    """The simulated session clock both the managers and the broker read."""

    def __init__(self, moment: datetime) -> None:
        self.moment = moment
        self._tick = 0

    def set(self, moment: datetime) -> None:
        self.moment = moment

    def naive(self) -> datetime:
        """IST-naive, as `strategy_logic.now_ist()` returns; compared to bar times."""
        return self.moment

    def aware(self) -> datetime:
        """Aware IST for database writes, strictly increasing per call.

        `_wait_leg_status` accepts an order update only if its timestamp is
        *after* the stamp taken before the order was placed, so a clock that
        returned the same instant twice would make every leg time out.
        """
        self._tick += 1
        return (self.moment + timedelta(microseconds=self._tick)).replace(tzinfo=ist.IST)


class ClockPatch:
    """Point every module's clock at one simulated moment, then restore."""

    MODULES = (strategy_logic, tm)

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self._saved: list[tuple[Any, str, Any]] = []

    def __enter__(self) -> Clock:
        for module in self.MODULES:
            for name, fn in (("now_ist", self.clock.naive), ("_now_ist", self.clock.aware)):
                if hasattr(module, name):
                    self._saved.append((module, name, getattr(module, name)))
                    setattr(module, name, fn)
        # Throttled refusal lines are once-per-interval; a shared throttle would
        # let the first check consume the only emission and make later checks
        # look silent when they are not.
        self._saved.append((tm, "_THROTTLE", tm._THROTTLE))
        tm._THROTTLE = log_throttle.LogThrottle()
        return self.clock

    def __exit__(self, *exc) -> None:
        for module, name, value in reversed(self._saved):
            setattr(module, name, value)
        self._saved.clear()
        return None


def entry_moment(day, hour: int, minute: int, second: int = 5) -> datetime:
    return datetime.combine(day, dt_time(hour, minute, second))


def pending(book: str = MOMENTUM_BOOK, position: str = "long",
            stop_points: float = 40.0, key: str = "v3:probe",
            setup: Optional[str] = None) -> dict[str, Any]:
    return {
        "key": key, "position": position, "source": book,
        "stop_loss_points": stop_points, "book": book, "setup": setup,
        "signal_time": "2026-08-14T11:45:00", "execute_time": "2026-08-14T11:50:00",
    }


# --------------------------------------------------------------------------- #
#  Stage 1 - v3's recorded sessions through the real engine
# --------------------------------------------------------------------------- #
def live_intents(day: str) -> tuple[dict[datetime, dict[str, Any]], Any]:
    """Every entry the real engine offers through `day`, keyed by the moment.

    Each candle is written the way `live_v3.build` writes it and read back
    through `live_v3.market`, so this is the production path, not a copy of it.
    The engine is asked at every candle close and inside the night fill window.
    """
    night = datetime.combine(date.fromisoformat(day), strategy_logic.NIGHT_ENTRY_TIME)
    with tempfile.TemporaryDirectory() as tmp:
        root = write_live_root(Path(tmp), day)
        engine = StrategyEngine(root=root)
        intents: dict[datetime, dict[str, Any]] = {}
        for candles in range(FULL_SESSION_BARS + 1):
            write_candles(root, day, candles)
            now = candle_close(day, candles)
            engine.refresh(now)
            moments = [now]
            if now <= night + timedelta(seconds=5) < now + timedelta(minutes=BAR_MINUTES):
                moments.append(night + timedelta(seconds=5))
            for moment in moments:
                intent = engine.entry_intent(moment)
                if intent is not None:
                    intents[moment] = intent
        return intents, engine.snapshot


def stage_strategy(report: Report, verbose: bool) -> None:
    report.section("1. STRATEGY  (v3's recorded sessions, live, candle by candle)")
    m = v3_market()
    trades = carry_trades(m)
    for day in SESSIONS:
        d = day_index(m, day)
        want = [t for t in trades.itertuples() if t.d0 == d]
        try:
            got, snapshot = live_intents(day)
        except Exception as exc:  # noqa: BLE001
            report.add(f"{day}: replayed live", False, f"{type(exc).__name__}: {exc}")
            if verbose:
                traceback.print_exc()
            continue
        night = datetime.combine(date.fromisoformat(day), strategy_logic.NIGHT_ENTRY_TIME)
        same = (sorted(got) == ([night + timedelta(seconds=5)] if want else [])
                and all((i["index"], i["setup"]) == (want[0].sym, "B") for i in got.values()))
        report.add(f"{day}: live entries are v3's backtest carries at the night fill ({len(want)})",
                   same, "" if same else f"live {[(str(k), i['index']) for k, i in got.items()]} "
                                         f"backtest {[t.sym for t in want]}")
        for at_, intent in sorted(got.items()):
            book_ok = intent["book"] == CARRY_BOOK and intent["stop_loss_points"] == 0.0
            report.add(f"{day} {at_:%H:%M}: BUY {intent['index']} ({intent['setup']}, "
                       f"{intent['book']}, stop {intent['stop_loss_points']:.1f} pts)", book_ok,
                       "" if book_ok else "not the night carry")
        exits = sorted((e["index"], e["reason"]) for e in snapshot.exits)
        expected = sorted((t.sym, t.reason) for t in trades.itertuples() if t.d1 == d)
        report.add(f"{day}: v3's own exits ({', '.join(r for _i, r in exits) or 'none'})",
                   exits == expected, "" if exits == expected else f"backtest {expected}")
        if snapshot.shock:
            report.note(f"{day} is a shock session: no night trade")
        for name, verdict in snapshot.verdicts.items():
            report.note(f"{day} {name} last candle: "
                        + ("accepted" if verdict["accepted"]
                           else f"blocked by {'; '.join(verdict['blocked_by']) or 'nothing'}"))


# --------------------------------------------------------------------------- #
#  Stage 2 - the TEST order path
# --------------------------------------------------------------------------- #
#: Every in-memory database a TEST manager was built against, so the stage can
#: check them all for unmodelled SQL rather than only the last one it happened
#: to leave in scope.
TEST_DATABASES: list[FakeDB] = []


def build_test_manager(db: FakeDB, symbols: FakeSymbols, price: float,
                       service: Optional[Any] = None) -> TradeManagerTest:
    TEST_DATABASES.append(db)
    return TradeManagerTest(
        api=None,
        db_connector=db,
        symbol_fetcher=symbols,
        live_price_getter=lambda: price,
        fresh_price_getter=lambda: price,
        strategy_service=service or ScriptedService(),
    )


def stage_test_orders(report: Report, day, capture: LogCapture, verbose: bool) -> None:
    report.section("2. TEST ORDER PATH  (real TradeManagerTest, paper)")
    loggers = (tm.LOGGER, strategy_logic.LOGGER)

    # -- a met condition books a paper trade ------------------------------
    with ClockPatch(Clock(entry_moment(day, 10, 30))):
        db, symbols = FakeDB(), FakeSymbols(ready=True)
        manager = build_test_manager(db, symbols, 24_350.0)
        booked = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
        ok = booked and manager.in_trade and len(db.trade_logs) == 1
        report.add("met condition books a paper trade", ok,
                   "" if ok else f"booked={booked} rows={len(db.trade_logs)}")
        if db.trade_logs:
            row = db.trade_logs[0]
            report.add("paper row carries symbol, direction, stop and book",
                       row["symbol"] == FakeSymbols.SELL_LEG
                       and row["direction"] == "LONG"
                       and row["stop_price"] == 24_310.0
                       and row["book"] == MOMENTUM_BOOK,
                       f"symbol={row['symbol']} stop={row['stop_price']} book={row['book']}")

    # -- the original defect: no option legs must not delete the rehearsal --
    with ClockPatch(Clock(entry_moment(day, 10, 30))):
        db = FakeDB()
        symbols = FakeSymbols(ready=False, reason="no live price from the WebSocket")
        manager = build_test_manager(db, symbols, 24_350.0)
        capture.clear()
        with capture.attached(*loggers):
            booked = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
        ok = booked and len(db.trade_logs) == 1
        report.add("missing option legs still book a paper trade", ok,
                   "" if ok else "a leg-less paper entry was dropped")
        report.add("paper fallback symbol is used and is not tradeable",
                   db.trade_logs and db.trade_logs[0]["symbol"] == TradeManagerTest.PAPER_FALLBACK_SYMBOL
                   and "PAPER" in TradeManagerTest.PAPER_FALLBACK_SYMBOL)
        report.add("the reason reaches the log at WARNING or above",
                   capture.has("no live price from the WebSocket", logging.WARNING),
                   "" if capture.has("no live price from the WebSocket", logging.WARNING)
                   else "refusal was silent below WARNING")

    # -- stop, EOD, and the manager cutoff --------------------------------
    with ClockPatch(Clock(entry_moment(day, 11, 0))) as clock:
        db, symbols = FakeDB(), FakeSymbols(ready=True)
        service = ScriptedService()
        manager = build_test_manager(db, symbols, 24_350.0)
        manager.decider = StrategyDecider(service, tag="CHECK")
        service.runtime.intent = pending()

        asyncio.run(manager._cycle_once())
        entered = manager.in_trade and len(db.trade_logs) == 1
        report.add("decider drives a full TEST entry cycle", entered,
                   "" if entered else "the decider did not reach the manager")

        # Price through the stop: the level frozen on the trade row, breached.
        service.runtime.intent = None
        manager.live_price_getter = lambda: 24_300.0
        manager.fresh_price_getter = lambda: 24_300.0
        clock.set(entry_moment(day, 11, 5))
        capture.clear()
        with capture.attached(*loggers):
            asyncio.run(manager._cycle_once())
        closed = not manager.in_trade and db.trade_logs[0]["is_in_trade"] is False
        report.add("stop breach exits and closes the paper row", closed,
                   "" if closed else "the stop did not close the trade")
        report.add("the stop announces itself", capture.has("stop level", logging.WARNING))

    with ClockPatch(Clock(entry_moment(day, 14, 45))) as clock:
        db, symbols = FakeDB(), FakeSymbols(ready=True)
        service = ScriptedService()
        manager = build_test_manager(db, symbols, 24_350.0)
        manager.decider = StrategyDecider(service, tag="CHECK")
        service.runtime.intent = pending(key="pending:late")
        capture.clear()
        with capture.attached(*loggers):
            asyncio.run(manager._cycle_once())
        refused = not manager.in_trade and not db.trade_logs
        report.add(f"intraday entry after {TRADE_SETTINGS.no_new_trades_after:%H:%M} is refused",
                   refused, "" if refused else "a late entry was taken")
        report.add("the late-entry refusal is logged at WARNING or above",
                   capture.has("no new trades after", logging.WARNING),
                   "" if capture.has("no new trades after", logging.WARNING)
                   else "the entry vanished with no log line")

    # -- the carry leg -----------------------------------------------------
    with ClockPatch(Clock(entry_moment(day, 14, 56))) as clock:
        db, symbols = FakeDB(), FakeSymbols(ready=True, bias="HH")
        service = ScriptedService()
        manager = build_test_manager(db, symbols, 24_355.0)
        manager.decider = StrategyDecider(service, tag="CHECK")
        service.runtime.intent = {
            "key": f"v3:{day}T14:50:00", "position": "long",
            "source": CARRY_BOOK, "stop_loss_points": 0.0, "book": CARRY_BOOK,
            "signal_time": f"{day}T14:50:00", "execute_time": f"{day}T14:55:00",
        }
        asyncio.run(manager._cycle_once())
        held = manager.in_trade and db.trade_logs and db.trade_logs[0]["book"] == CARRY_BOOK
        report.add("the carry is entered at 14:55+", bool(held),
                   "" if held else "the carry entry did not book")
        report.add("the carry row carries no stop",
                   bool(db.trade_logs) and db.trade_logs[0]["stop_price"] is None,
                   "v3's carry has no stop; the row must not invent one")

        # Nothing flattens it at the end of the day: it is a carry by design.
        clock.set(entry_moment(day, 15, 29))
        service.runtime.intent = None
        asyncio.run(manager._cycle_once())
        report.add("no end-of-day flatten touches the carry",
                   manager.in_trade, "" if manager.in_trade else "the carry was closed the same day")

        # Next session's 09:15 open closes it.
        clock.set(entry_moment(day + timedelta(days=1), 9, 15, 30))
        asyncio.run(manager._cycle_once())
        exited = not manager.in_trade and db.trade_logs[0]["is_in_trade"] is False
        report.add("the carry is closed at the next 09:15 open", exited,
                   "" if exited else "the carry was held past its exit")
        report.add("the carry close records its exit day",
                   bool(exited and db.trade_logs[0]["exit_time"]
                        and db.trade_logs[0]["exit_time"].date() == day + timedelta(days=1)))

    # -- an entry with no opening quote is held, never booked ---------------
    with ClockPatch(Clock(entry_moment(day, 11, 30))):
        db, symbols = FakeDB(), FakeSymbols(ready=True)
        service = ScriptedService()
        manager = build_test_manager(db, symbols, 0.0)
        manager.decider = StrategyDecider(service, tag="CHECK")
        service.runtime.intent = pending(key="pending:nofeed")
        capture.clear()
        with capture.attached(*loggers):
            asyncio.run(manager._cycle_once())
        report.add("an entry with no live price is skipped, not booked",
                   not manager.in_trade and not db.trade_logs)
        report.add("the missing entry price is logged at WARNING or above",
                   capture.has("no live price for entry", logging.WARNING))

    # A cached quote is not the intended opening quote, for either leg.
    for label, stop_points in (("no stop", 0.0), ("a stop", 40.0)):
        with ClockPatch(Clock(entry_moment(day, 14, 56))):
            db, symbols = FakeDB(), FakeSymbols(ready=True, bias="HH")
            service = ScriptedService()
            manager = build_test_manager(db, symbols, 24_339.8, service=service)
            # The last known price is available; only the freshness check fails.
            manager.fresh_price_getter = lambda: None
            manager.decider = StrategyDecider(service, tag="CHECK")
            service.runtime.intent = {
                "key": f"v3:{day}T14:50:00", "position": "long",
                "source": CARRY_BOOK, "stop_loss_points": stop_points,
                "book": CARRY_BOOK, "signal_time": f"{day}T14:50:00",
                "execute_time": f"{day}T14:55:00",
            }
            capture.clear()
            with capture.attached(*loggers):
                asyncio.run(manager._cycle_once())
            booked = bool(db.trade_logs)
            report.add(
                f"a stale feed with {label}: entry is refused",
                not booked,
                "" if not booked else "a cached quote was used as the opening fill",
            )
            report.add(
                f"the {label} decision says which source failed",
                capture.has("fresh price getter returned", logging.WARNING)
                or capture.has("intent expired", logging.WARNING),
                "" if capture.has("returned", logging.WARNING)
                else "the log cannot tell a dead feed from a stale-flagged one",
            )

    # -- the synthetic long on paper --------------------------------------
    with ClockPatch(Clock(entry_moment(day, 10, 30))):
        db = FakeDB()
        manager = build_test_manager(db, FakeSymbols(), 24_350.0)
        capture.clear()
        with capture.attached(*loggers):
            booked = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
        report.add("paper books the synthetic long: the put on the row, the call with it",
                   booked and len(db.trade_logs) == 1
                   and db.trade_logs[0]["symbol"] == FakeSymbols.SELL_LEG
                   and capture.has(f"with {FakeSymbols.BUY_LEG}", logging.INFO),
                   f"rows={[r['symbol'] for r in db.trade_logs]}")

    # Every database this stage built, checked together: an unmodelled
    # statement means the order path changed shape and the checks above were
    # measuring something that no longer exists.
    unsupported = [q for seen in TEST_DATABASES for q in seen.unsupported]
    report.add("TEST path issued no unmodelled SQL", not unsupported,
               "" if not unsupported else unsupported[0][:70])


# --------------------------------------------------------------------------- #
#  Stage 3 - the LIVE order path
# --------------------------------------------------------------------------- #
def live_settings() -> Any:
    """Production settings with the waits shortened.

    Only polling granularity and retry backoff change. The timeouts stay long
    enough that a leg which never confirms is genuinely waited for, and short
    enough that the failure checks finish in seconds rather than minutes.
    """
    return replace(
        TRADE_SETTINGS,
        poll_interval_sec=0.02,
        buy_wait_timeout_sec=2.0,
        sell_wait_timeout_sec=2.0,
        exchordid_wait_timeout_sec=2.0,
        exit_exchordid_wait_timeout_sec=2.0,
        exit_retry_backoff_sec=0.01,
        emergency_flatten_sleep_sec=0.01,
        entry_fail_cooldown_sec=0.0,
    )


def build_live_manager(db: FakeDB, broker: FakeBroker, symbols: FakeSymbols,
                       price: float, *, lots: int = 1) -> TradeManagerLive:
    service = ScriptedService()
    service.runtime.intent = pending()
    manager = TradeManagerLive(
        api=broker,
        db_connector=db,
        symbol_fetcher=symbols,
        live_price_getter=lambda: price,
        fresh_price_getter=lambda: price,
        strategy_service=service,
        trade_settings=replace(live_settings(), lots=lots),
    )
    manager.ENTRY_POSITION_VERIFY_TIMEOUT_SEC = 0.5
    manager.ENTRY_POSITION_VERIFY_POLL_SEC = 0.02
    manager.RECONCILE_INTERVAL_SEC = 0.0
    return manager


def live_case(day, hour: int = 10, minute: int = 30, **broker_kwargs):
    """One isolated LIVE manager with its own database, broker and clock."""
    clock = Clock(entry_moment(day, hour, minute))
    db = FakeDB()
    symbols = broker_kwargs.pop("symbols", None) or FakeSymbols(ready=True)
    broker = FakeBroker(db, clock=clock.aware, **broker_kwargs)
    return clock, db, broker, symbols


def stage_live_orders(report: Report, day, capture: LogCapture, verbose: bool) -> None:
    report.section("3. LIVE ORDER PATH  (real TradeManagerLive, fake broker)")
    loggers = (tm.LOGGER, strategy_logic.LOGGER)
    lot = TRADE_SETTINGS.lot_size
    unsupported: list[str] = []

    # -- 1. the happy path -------------------------------------------------
    clock, db, broker, symbols = live_case(day)
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0)
        capture.clear()
        with capture.attached(*loggers):
            entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
    unsupported += db.unsupported
    report.add("hedged LIVE entry completes", entered,
               "" if entered else "; ".join(capture.messages(logging.ERROR))[:150] or "entry returned False")
    report.add("both legs were sent, BUY first",
               [o["symbol"] for o in broker.orders] == [FakeSymbols.BUY_LEG, FakeSymbols.SELL_LEG],
               str([(o["symbol"], o["side"]) for o in broker.orders]))
    report.add("broker net position is the intended hedge",
               broker.positions.get(FakeSymbols.BUY_LEG) == lot
               and broker.positions.get(FakeSymbols.SELL_LEG) == -lot,
               str(broker.positions))
    trade_ok = len(db.trade_logs) == 1 and db.trade_logs[0]["is_in_trade"]
    report.add("an OPEN trade row was committed", trade_ok,
               "" if trade_ok else f"{len(db.trade_logs)} trade row(s)")
    if trade_ok:
        row = db.trade_logs[0]
        report.add("the trade row carries both order numbers and both legs",
                   bool(row["buy_orderno"] and row["sell_orderno"]
                        and row["buy_symbol"] and row["sell_symbol"]),
                   f"buy#{row['buy_orderno']} sell#{row['sell_orderno']}")
    report.add("order_logs reached lifecycle OPEN",
               bool(db.order_logs) and db.order_logs[-1]["lifecycle_state"] == "OPEN",
               db.order_logs[-1]["lifecycle_state"] if db.order_logs else "no row")
    report.add("exchange order ids were captured for both legs",
               bool(db.order_logs and db.order_logs[-1]["buy_exchordid"]
                    and db.order_logs[-1]["sell_exchordid"]))

    # -- 2. the exit, from the same manager --------------------------------
    with ClockPatch(clock):
        capture.clear()
        with capture.attached(*loggers):
            asyncio.run(manager._exit_trade("stop_loss", 0, 24_300.0))
    unsupported += db.unsupported
    flat = broker.positions.get(FakeSymbols.BUY_LEG) == 0 and broker.positions.get(FakeSymbols.SELL_LEG) == 0
    report.add("LIVE exit closes both legs to flat", flat, str(broker.positions))
    closed = db.trade_logs and db.trade_logs[0]["is_in_trade"] is False \
        and db.trade_logs[0]["trade_state"] == "CLOSED"
    report.add("the trade row is closed with an exit price", bool(closed),
               "" if closed else "the trade row is still open after the exit")
    report.add("the LIVE close records its exit time",
               bool(closed and db.trade_logs[0]["exit_time"]))
    report.add("order_logs reached lifecycle CLOSED",
               db.order_logs[-1]["lifecycle_state"] == "CLOSED",
               db.order_logs[-1]["lifecycle_state"])
    report.add("the manager is flat and ready for the next signal",
               not manager.in_trade and manager.trade_row_id is None)

    # A live trade keeps its original size after a config change, and a
    # smaller freeze on the next session must be handled in exit chunks.
    clock, db, broker, symbols = live_case(day, freeze_qty=130)
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0, lots=2)
        entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
        stored_qty = db.trade_logs[0]["quantity"] if db.trade_logs else None
        broker.freeze_qty = 65
        resumed = build_live_manager(db, broker, symbols, 24_300.0)
        held_qty = resumed.order_quantity
        verified = asyncio.run(resumed._verify_resumed_trade())
        asyncio.run(resumed._exit_trade("multi_lot_check", 0, 24_300.0))
    unsupported += db.unsupported
    exits = broker.orders[2:]
    report.add("two-lot entry persists 130 units across restart",
               entered and stored_qty == 130 and held_qty == 130 and verified,
               f"stored={stored_qty} resumed={held_qty} verified={verified}")
    report.add("smaller freeze closes each leg in two confirmed chunks",
               len(exits) == 4 and all(o["qty"] == 65 for o in exits)
               and not resumed.in_trade and all(q == 0 for q in broker.positions.values()),
               str([(o["symbol"], o["qty"]) for o in exits]))

    clock, db, broker, symbols = live_case(day, freeze_qty=65)
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0, lots=2)
        entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
    unsupported += db.unsupported
    report.add("freeze rejection occurs before either entry leg is sent",
               not entered and not broker.orders and not db.trade_logs)

    clock, db, broker, symbols = live_case(day, freeze_qty=130)
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0, lots=2)
        entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
        async def unconfirmed_exit(_symbol, *, reason):
            return False
        manager._flatten_to_zero = unconfirmed_exit
        asyncio.run(manager._exit_trade("multi_lot_uncertain", 0, 24_300.0))
    unsupported += db.unsupported
    report.add("unconfirmed first-leg exit keeps the hedge and quarantines",
               entered and len(broker.orders) == 2 and manager.in_trade
               and manager._quarantine_active and db.trade_logs[0]["is_in_trade"])

    clock, db, broker, symbols = live_case(day, freeze_qty=130)
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0, lots=2)
        entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
        outcomes = iter((True, False))
        async def partly_confirmed_exit(_symbol, *, reason):
            return next(outcomes)
        manager._flatten_to_zero = partly_confirmed_exit
        asyncio.run(manager._exit_trade("multi_lot_uncertain", 0, 24_300.0))
    unsupported += db.unsupported
    report.add("unconfirmed second-leg exit cannot trigger duplicate cleanup",
               entered and len(broker.orders) == 2 and manager.in_trade
               and manager._quarantine_active and db.trade_logs[0]["is_in_trade"])

    # -- 3. option legs not ready: the original defect, LIVE side ----------
    clock, db, broker, _s = live_case(day)
    symbols = FakeSymbols(ready=False, reason="no live price from the WebSocket")
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0)
        capture.clear()
        with capture.attached(*loggers):
            entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
    report.add("LIVE refuses to trade without a real symbol", entered is False)
    report.add("no order was sent and no trade row written",
               not broker.orders and not db.trade_logs)
    report.add("the refusal names its cause at WARNING or above",
               capture.has("no live price from the WebSocket", logging.WARNING),
               "" if capture.has("no live price from the WebSocket", logging.WARNING)
               else "SILENT SKIP - this is the defect that produced a no-trade day")

    # -- 4. a stale leg bias must not be traded ----------------------------
    clock, db, broker, _s = live_case(day)
    symbols = FakeSymbols(ready=True, bias="LL")  # legs built for a short
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0)
        capture.clear()
        with capture.attached(*loggers):
            entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
    report.add("a leg/direction mismatch is refused", entered is False and not broker.orders)
    report.add("the mismatch is logged at WARNING or above",
               capture.has("leg bias mismatch", logging.WARNING))

    # -- 5. the broker rejects the hedge leg -------------------------------
    clock, db, broker, symbols = live_case(day, reject=[FakeSymbols.BUY_LEG])
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0)
        capture.clear()
        with capture.attached(*loggers):
            entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
    unsupported += db.unsupported
    report.add("a rejected BUY leg aborts the entry", entered is False)
    report.add("no trade row survives a rejected leg", not db.trade_logs)
    report.add("no second leg was sent after the rejection",
               all(o["symbol"] != FakeSymbols.SELL_LEG for o in broker.orders),
               str([o["symbol"] for o in broker.orders]))
    report.add("the rejection is reported at ERROR", capture.has("BUY leg", logging.ERROR))

    # -- 6a. the WebSocket goes silent, but order history still answers ----
    #        This is the designed resilience: a lost push must not cost a trade.
    clock, db, broker, symbols = live_case(day, ws=False)
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0)
        capture.clear()
        started = time.monotonic()
        with capture.attached(*loggers):
            entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
        elapsed = time.monotonic() - started
    unsupported += db.unsupported
    report.add("a lost order update is recovered from order history, not lost",
               entered is True and len(db.trade_logs) == 1,
               f"entered={entered} rows={len(db.trade_logs)} after {elapsed:.1f}s")

    # -- 6b. no push and no history: the outcome is unknowable -------------
    clock, db, broker, symbols = live_case(day, ws=False, history=False)
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0)
        capture.clear()
        with capture.attached(*loggers):
            entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
    unsupported += db.unsupported
    report.add("an unconfirmable order never becomes a trade",
               entered is False and not db.trade_logs)
    report.add("an unresolved live order is quarantined without a racing flatten",
               manager._quarantine_active and len(broker.orders) == 1,
               str(broker.positions))
    report.add("the stall is reported at ERROR or above",
               capture.has("did not complete", logging.ERROR)
               or capture.has("timed out", logging.ERROR))

    # -- 7. an orphan position must block, not double up -------------------
    clock, db, broker, symbols = live_case(day, positions={FakeSymbols.SELL_LEG: -lot})
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0)
        capture.clear()
        with capture.attached(*loggers):
            entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
    report.add("an existing broker position blocks a new entry",
               entered is False and not broker.orders)
    report.add("the block is reported at ERROR", capture.has("already shows quantity", logging.ERROR))
    report.add("the pair is quarantined after an orphan is seen", manager._quarantine_active)

    # -- 8. a partial fill must roll back, not be held ---------------------
    clock, db, broker, symbols = live_case(day, fill_ratio=0.5)
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0, lots=2)
        capture.clear()
        with capture.attached(*loggers):
            entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
    unsupported += db.unsupported
    report.add("a partial fill is not accepted as an entry",
               entered is False and not db.trade_logs)
    report.add("both legs are rolled back to flat after a partial fill",
               broker.positions.get(FakeSymbols.BUY_LEG) == 0
               and broker.positions.get(FakeSymbols.SELL_LEG) == 0,
               str(broker.positions))
    report.add("the quantity mismatch is reported at CRITICAL",
               capture.has("quantity mismatch", logging.CRITICAL))

    # -- 9. a lost acknowledgement is recovered, never resubmitted ---------
    clock, db, broker, symbols = live_case(day, stat_not_ok=True)
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0)
        capture.clear()
        with capture.attached(*loggers):
            entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
    unsupported += db.unsupported
    sent = [o["symbol"] for o in broker.orders]
    report.add("an unacknowledged order is recovered from the order book, not resent",
               sent.count(FakeSymbols.BUY_LEG) == 1, str(sent))
    report.add("recovery still produces exactly one entry",
               entered is True and len(db.trade_logs) == 1,
               f"entered={entered} rows={len(db.trade_logs)}")

    # -- 10. a failed entry must not disable the bot for the rest of the day
    clock, db, broker, symbols = live_case(day, reject=[FakeSymbols.BUY_LEG])
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0)
        manager.strategy.runtime.intent = pending(key="pending:first")
        asyncio.run(manager._enter_trade("LONG", 24_350.0, pending(key="pending:first")))
        broker.reject.clear()
        manager._clear_quarantine()
        manager.strategy.runtime.intent = pending(key="pending:second")
        capture.clear()
        with capture.attached(*loggers):
            recovered = asyncio.run(manager._enter_trade("LONG", 24_360.0, pending(key="pending:second")))
    unsupported += db.unsupported
    report.add("a later entry still works after a failed one", recovered is True,
               "" if recovered else "; ".join(capture.messages(logging.ERROR))[:150])

    # -- 11. the synthetic long: call bought, then put sold -----------------
    clock, db, broker, symbols = live_case(day)
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0)
        entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
    unsupported += db.unsupported
    report.add("the call is bought first, then the put of the same strike sold",
               entered and [(o["symbol"], o["side"]) for o in broker.orders]
               == [(FakeSymbols.BUY_LEG, "B"), (FakeSymbols.SELL_LEG, "S")]
               and broker.positions.get(FakeSymbols.BUY_LEG) == lot
               and broker.positions.get(FakeSymbols.SELL_LEG) == -lot,
               str([(o["symbol"], o["side"]) for o in broker.orders]))
    report.add("the trade row records both legs of the synthetic long",
               bool(db.trade_logs) and (db.trade_logs[0]["buy_symbol"], db.trade_logs[0]["sell_symbol"])
               == (FakeSymbols.BUY_LEG, FakeSymbols.SELL_LEG))

    clock, db, broker, symbols = live_case(day)
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0, lots=2)
        entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
    unsupported += db.unsupported
    report.add("the configured lots go on both legs",
               entered and [o["qty"] for o in broker.orders] == [2 * lot, 2 * lot]
               and db.trade_logs[0]["quantity"] == 2 * lot,
               str([(o["symbol"], o["qty"]) for o in broker.orders]))

    clock, db, broker, symbols = live_case(day, reject=[FakeSymbols.SELL_LEG])
    with ClockPatch(clock):
        manager = build_live_manager(db, broker, symbols, 24_350.0)
        capture.clear()
        with capture.attached(*loggers):
            entered = asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
    unsupported += db.unsupported
    report.add("a put the broker refuses (margin, say) unwinds the bought call",
               entered is False and not db.open_trades
               and broker.positions.get(FakeSymbols.BUY_LEG, 0) == 0
               and broker.positions.get(FakeSymbols.SELL_LEG, 0) == 0,
               str(broker.positions))
    report.add("the refused put is reported at ERROR",
               capture.has("SELL leg did not complete", logging.ERROR))

    # -- 12. the harness modelled the real statements ----------------------
    report.add("LIVE path issued no unmodelled SQL", not unsupported,
               "" if not unsupported else unsupported[0][:70])


# --------------------------------------------------------------------------- #
#  Stage 4 - v3's signal, the real decider, both real managers
# --------------------------------------------------------------------------- #
#: The sessions driven end to end: a carry bought at the night fill and sold
#: the next morning, and a shock session with no trade at all (A, which used to
#: trade both, is retired).
FULL_STACK_SESSIONS = ("2015-02-10", "2015-06-29")


def replay_full_stack(day: str) -> dict[str, Any]:
    """Drive a TEST and a LIVE manager through `day` and the next open on v3.

    Ticks walk each candle open -> low -> high -> close, every index at once,
    and the engine is refreshed as each candle closes - the order the live
    process sees them in. Returns each manager's trade rows and the ticks.
    """
    m = v3_market()
    d = day_index(m, day)
    session, following = m.cal[d].date(), m.cal[d + 1].date()
    clock = Clock(datetime.combine(session, dt_time(9, 14)))
    ticks: dict[str, float] = {}
    getters = {name: (lambda name=name: ticks.get(name)) for name in instruments.NAMES}
    results: dict[str, Any] = {}

    with tempfile.TemporaryDirectory() as tmp, ClockPatch(clock):
        root = write_live_root(Path(tmp), day)
        engine = StrategyEngine(root=root)
        managers = {}
        for kind in ("TEST", "LIVE"):
            db = FakeDB()
            fetchers = {name: FakeSymbols(root=instruments.get(name).option_root)
                        for name in instruments.NAMES}
            common = dict(
                db_connector=db, symbol_fetcher=fetchers["NIFTY"], symbol_fetchers=fetchers,
                index_live_price_getters=getters,
                live_price_getter=getters["NIFTY"], fresh_price_getter=getters["NIFTY"],
                strategy_service=engine,
            )
            if kind == "TEST":
                TEST_DATABASES.append(db)
                manager = TradeManagerTest(api=None, **common)
            else:
                broker = FakeBroker(db, clock=clock.aware)
                manager = TradeManagerLive(api=broker, trade_settings=live_settings(), **common)
                manager.ENTRY_POSITION_VERIFY_TIMEOUT_SEC = 0.5
                manager.ENTRY_POSITION_VERIFY_POLL_SEC = 0.02
                manager.RECONCILE_INTERVAL_SEC = 1e9  # reconciliation is stage 3's job
                # Old sessions: the holiday calendar is only kept for this year.
                manager._holiday_years = {session.year, following.year}
            managers[kind] = (manager, db)

        def at(when: datetime, prices: dict[str, float]) -> None:
            clock.set(when)
            ticks.clear()
            ticks.update(prices)
            for manager, _db in managers.values():
                asyncio.run(manager._cycle_once())

        def candle(day_index_: int, b: int, field) -> dict[str, float]:
            values = getattr(m, field)[:, day_index_, b]
            return {name: float(values[k]) for k, name in enumerate(m.names) if values[k] == values[k]}

        logging.disable(logging.CRITICAL)
        try:
            for b in range(FULL_SESSION_BARS):
                start = datetime.combine(session, bar_time(b))
                write_candles(root, day, b)
                engine.refresh(start + timedelta(seconds=1))
                at(start + timedelta(seconds=2), candle(d, b, "O"))
                at(start + timedelta(seconds=60), candle(d, b, "L"))
                at(start + timedelta(seconds=120), candle(d, b, "H"))
                at(start + timedelta(seconds=290), candle(d, b, "C"))
            at(datetime.combine(following, dt_time(9, 15, 1)), candle(d + 1, 0, "O"))
        finally:
            logging.disable(logging.NOTSET)
        for kind, (manager, db) in managers.items():
            results[kind] = (db.trade_logs, db)
    return results


def carry_trades(m):
    """v3's own backtest on the slots Lune trades: the carries only."""
    return strategy_logic.V3.backtest(m, strategy_logic.live_slots(m))


def stage_full_stack(report: Report, verbose: bool) -> None:
    report.section("4. FULL STACK  (v3 signal -> decider -> manager -> order -> exit)")
    m = v3_market()
    trades = carry_trades(m)
    night_bar = (strategy_logic.NIGHT_ENTRY_TIME.hour * 60
                 + strategy_logic.NIGHT_ENTRY_TIME.minute - 555) // BAR_MINUTES
    for day in FULL_STACK_SESSIONS:
        d = day_index(m, day)
        want = [t for t in trades.itertuples() if t.d0 == d]
        try:
            results = replay_full_stack(day)
        except Exception as exc:  # noqa: BLE001
            report.add(f"{day}: full-stack replay ran", False, f"{type(exc).__name__}: {exc}")
            if verbose:
                traceback.print_exc()
            continue
        for kind, (rows, db) in results.items():
            report.add(f"{day} {kind}: v3's {len(want)} trade(s) booked", len(rows) == len(want),
                       f"{len(rows)} row(s)")
            for row, t in zip(rows, want):
                k = m.idx[t.sym]
                # The managers cycle at each candle's open, low (+60s), high and
                # close; 15:11:00 is the night candle's low tick.
                entry = float(m.L[k, d, night_bar])
                exit_price = t.exit
                if t.reason == "stop":
                    # The resting stop fills at its level; the bot sells on the
                    # tick that breaches it, which can only be at or below.
                    exit_ok = row["exit_price"] is not None and row["exit_price"] <= exit_price + 1e-9
                else:
                    exit_ok = row["exit_price"] == exit_price
                book = (CARRY_BOOK if t.setup == "B" else
                        strategy_logic.INTRADAY_BOOK if t.reason == "intraday_exit" else MOMENTUM_BOOK)
                stop = None if t.setup == "B" else entry - strategy_logic.P["stop"] * m.atr[k, d]
                ok = (row["index_name"] == t.sym and row["book"] == book
                      and row["fill_price"] == entry and not row["is_in_trade"] and exit_ok
                      and (row["stop_price"] is None if stop is None
                           else abs(row["stop_price"] - stop) < 1e-6))
                report.add(f"{day} {kind}: {t.sym} {t.setup} @ {entry:.2f} -> {t.reason}", ok,
                           f"book={row['book']} fill={row['fill_price']} stop={row['stop_price']} "
                           f"exit={row['exit_price']}")
            report.add(f"{day} {kind}: no unmodelled SQL", not db.unsupported,
                       (db.unsupported or [""])[0][:70])


# --------------------------------------------------------------------------- #
#  Stage 5 - the silent-skip audit
# --------------------------------------------------------------------------- #
def stage_silent_skip(report: Report, day, capture: LogCapture) -> None:
    report.section("5. SILENT-SKIP AUDIT  (every refusal must be visible at WARNING)")
    report.note("main.py configures logging at WARNING. A refusal logged below that is")
    report.note("invisible: the bot declines every entry and the log looks like a quiet day.")

    lot = TRADE_SETTINGS.lot_size
    scenarios: list[tuple[str, Callable[[], list[logging.LogRecord]]]] = []

    def run_live(symbols=None, **broker_kwargs):
        def go() -> list[logging.LogRecord]:
            clock, db, broker, default = live_case(day, **broker_kwargs)
            with ClockPatch(clock):
                manager = build_live_manager(db, broker, symbols or default, 24_350.0)
                capture.clear()
                with capture.attached(tm.LOGGER, strategy_logic.LOGGER):
                    asyncio.run(manager._enter_trade("LONG", 24_350.0, pending()))
            return list(capture.records)
        return go

    def run_test(symbols=None, price=24_350.0, moment=(10, 30)):
        def go() -> list[logging.LogRecord]:
            with ClockPatch(Clock(entry_moment(day, *moment))):
                db = FakeDB()
                service = ScriptedService()
                manager = build_test_manager(db, symbols or FakeSymbols(ready=True), price,
                                             service=service)
                manager.decider = StrategyDecider(service, tag="AUDIT")
                service.runtime.intent = pending(key="pending:audit")
                capture.clear()
                with capture.attached(tm.LOGGER, strategy_logic.LOGGER):
                    asyncio.run(manager._cycle_once())
            return list(capture.records)
        return go

    scenarios = [
        ("LIVE: option legs never published",
         run_live(symbols=FakeSymbols(ready=False, reason="no live price from the WebSocket"))),
        ("LIVE: legs built for the opposite bias", run_live(symbols=FakeSymbols(ready=True, bias="LL"))),
        ("LIVE: call leg rejected by the broker", run_live(reject=[FakeSymbols.BUY_LEG])),
        ("LIVE: put leg rejected by the broker", run_live(reject=[FakeSymbols.SELL_LEG])),
        ("LIVE: order updates never arrive and history is silent",
         run_live(ws=False, history=False)),
        ("LIVE: a position already exists at the broker",
         run_live(positions={FakeSymbols.SELL_LEG: -lot})),
        ("LIVE: legs fill only partially", run_live(fill_ratio=0.5)),
        ("TEST: no live price for the entry", run_test(price=0.0)),
        ("TEST: past the no-new-trades cutoff", run_test(moment=(14, 45))),
    ]

    for name, run in scenarios:
        try:
            records = run()
        except Exception as exc:  # noqa: BLE001
            report.add(name, False, f"raised {type(exc).__name__}: {exc}")
            continue
        visible = [r for r in records if r.levelno >= logging.WARNING]
        debug_only = [r for r in records if r.levelno < logging.WARNING]
        if visible:
            loudest = max(visible, key=lambda r: r.levelno)
            report.add(name, True, f"{loudest.levelname}: {loudest.getMessage()[:64]}")
        else:
            report.add(name, False,
                       f"SILENT - {len(debug_only)} line(s) below WARNING" if debug_only
                       else "SILENT - nothing was logged at all")


# --------------------------------------------------------------------------- #
#  Runner
# --------------------------------------------------------------------------- #
def run_all(
    *,
    stages: Optional[set[int]] = None,
    verbose: bool = False,
    report: Optional[Report] = None,
) -> Report:
    """Run the requested stages and return the filled report.

    Exposed rather than buried in `main` so `run_checks.py` - the one command
    that gates a restart - can fold these verdicts into its own. An order path
    that is only verified when someone remembers to run a second script is a
    path that stops being verified.
    """
    stages = stages or {1, 2, 3, 4, 5}
    report = report if report is not None else Report()
    capture = LogCapture()
    try:
        if 1 in stages:
            stage_strategy(report, verbose)
        if 2 in stages:
            stage_test_orders(report, ORDER_DAY, capture, verbose)
        if 3 in stages:
            stage_live_orders(report, ORDER_DAY, capture, verbose)
        if 4 in stages:
            stage_full_stack(report, verbose)
        if 5 in stages:
            stage_silent_skip(report, ORDER_DAY, capture)
    except Exception as exc:  # noqa: BLE001
        report.add("checks completed", False, f"{type(exc).__name__}: {exc}")
        if verbose:
            traceback.print_exc()
    return report


# --------------------------------------------------------------------------- #
#  Entry point
# --------------------------------------------------------------------------- #
def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--stage", type=int, choices=(1, 2, 3, 4, 5), action="append",
                        help="run only these stages (repeatable)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="show manager logs and tracebacks")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO, format="        %(levelname)-8s %(message)s")
    else:
        # These checks exercise failure paths on purpose, so the managers emit a
        # wall of ERROR/CRITICAL lines that are the checks passing. Only the
        # verdicts below are printed; `LogCapture` still sees everything.
        logging.basicConfig(level=logging.CRITICAL + 1)

    print(f"Lune order-path check   {datetime.now():%Y-%m-%d %H:%M:%S}")

    report = run_all(stages=set(args.stage or (1, 2, 3, 4, 5)), verbose=args.verbose)
    report.show()

    failures = report.failures
    print("\n" + "=" * 74)
    if failures:
        print(f"  {len(failures)} CHECK(S) FAILED - the order path cannot be trusted")
        for name, detail in failures:
            print(f"    - {name}{('  -  ' + detail) if detail else ''}")
    else:
        print(f"  ALL {report.passed} CHECKS PASSED - TEST and LIVE orders both reach the broker")
    print("=" * 74)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
