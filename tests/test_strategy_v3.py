"""v3 as Lune runs it: the frozen code, unchanged, making every decision.

  ParityTests   - candle by candle, the live replay schedules exactly the
                  carries v3's own backtest takes on its B slots (A is retired),
                  moved to the night fill, and reports its exits
  SpliceTests   - the production data path (`live_v3.market` on files) gives
                  the same replay as the in-memory view the parity runs on
  EngineTests   - when the night entry may be filled, when the waterfall cannot
                  be ranked yet, the HH/LL desk holding it back, when the
                  background refresh runs, and that a torn cache heals itself
  ConsoleTests  - gate verdicts, chart markers and the live forecast
  DeciderTests  - v3's exits: the next open (and a legacy A row's 14:55 shock
                  exit and stop)
"""

from __future__ import annotations

import dataclasses
import logging
import tempfile
import threading
import time
import unittest
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pyarrow

import manual_desk
import strategy_logic as S
from tests.log_capture import capture_logs
from tests.order_path_fakes import (
    SESSIONS,
    candle_close,
    day_index,
    live_view,
    v3_market,
    v3_trades,
    write_candles,
    write_live_root,
)


def setUpModule() -> None:
    # `live_v3.market` reads its day cache through a bare `open()`; that is v3's
    # frozen code, not ours to change, and the handle is closed when collected.
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed file.*history_")


def book_for(setup: str, shock: bool) -> str:
    return S.CARRY_BOOK if setup == "B" else S.INTRADAY_BOOK if shock else S.MOMENTUM_BOOK


def carry_trades(m):
    """v3's own backtest on the slots Lune trades: the carries only."""
    return S.V3.backtest(m, S.live_slots(m))


# --------------------------------------------------------------------------- #
#  The live replay is v3's backtest                                           #
# --------------------------------------------------------------------------- #
class ParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.m = v3_market()
        cls.trades = carry_trades(cls.m)
        cls.shocks = S.V3.shock_sessions(cls.m)

    def test_only_the_carry_is_traded(self) -> None:
        self.assertEqual(S.LIVE_SETUPS, ("B",))
        slots = S.live_slots(self.m)
        self.assertTrue(slots)
        self.assertEqual({setup for setup, *_ in slots}, {"B"})
        self.assertEqual({hold for _, _, hold, _ in slots}, {S.V3.CARRY})
        self.assertTrue(any(t.setup == "A" for t in v3_trades().itertuples()),
                        "the frozen backtest still takes A here, so dropping it is tested")

    def test_every_live_entry_is_a_backtest_carry_at_the_night_fill(self) -> None:
        seen = 0
        for day in SESSIONS:
            d = day_index(self.m, day)
            want = [t for t in self.trades.itertuples() if t.d0 == d]
            with self.subTest(day=day):
                self.assertLessEqual(len(want), 1, "one slot")
                for candles in range(1, S.FULL_SESSION_BARS + 1):
                    entry = S.replay(live_view(self.m, d, candles), candle_close(day, candles)).entry
                    if not want or candles < int(want[0].b0):
                        # Before the signal candle has closed there is no carry.
                        self.assertIsNone(entry, candles)
                        continue
                    t = want[0]
                    seen += 1
                    self.assertEqual((entry["index"], entry["setup"], entry["book"]),
                                     (t.sym, "B", S.CARRY_BOOK))
                    self.assertEqual(entry["signal_time"], datetime.combine(
                        date.fromisoformat(day), S.bar_time(int(t.b0) - 1)))
                    self.assertEqual(entry["execute_time"], datetime.combine(
                        date.fromisoformat(day), S.NIGHT_ENTRY_TIME))
                    self.assertEqual(entry["stop_loss_points"], 0.0)
                    self.assertEqual(entry["unsettled"], ())
                    self.assertIn(t.sym, entry["candidates"])
        self.assertGreater(seen, 0, "some session here holds a carry")

    def test_the_replay_reports_v3s_own_carry_exits(self) -> None:
        for day in SESSIONS:
            d = day_index(self.m, day)
            snapshot = S.replay(live_view(self.m, d, 75), candle_close(day, 75))
            want = sorted((t.sym, t.reason, float(t.exit))
                          for t in self.trades.itertuples() if t.d1 == d)
            with self.subTest(day=day):
                self.assertEqual(
                    sorted((e["index"], e["reason"], e["price"]) for e in snapshot.exits), want)

    def test_shock_sessions_are_known_at_the_open(self) -> None:
        for day in SESSIONS:
            d = day_index(self.m, day)
            with self.subTest(day=day):
                self.assertEqual(
                    S.replay(live_view(self.m, d, 1), candle_close(day, 1)).shock,
                    bool(self.shocks[d]))


class SpliceTests(unittest.TestCase):
    """Files written the way `live_v3.build` writes them and read back through
    `live_v3.market` - what the bot actually runs - give the same replay."""

    def test_files_and_memory_give_the_same_replay(self) -> None:
        m, day = v3_market(), "2015-02-10"
        d = day_index(m, day)
        with tempfile.TemporaryDirectory() as tmp:
            root = write_live_root(Path(tmp), day)
            engine = S.StrategyEngine(root=root)
            before = engine.refresh(candle_close(day, 0))
            self.assertFalse(before.live, "no candle yet: yesterday's book")
            self.assertIsNone(before.entry)
            for candles in (1, 25, 26, 58, 59, 68, 69, 75):
                write_candles(root, day, candles)
                files = engine.refresh(candle_close(day, candles))
                memory = S.replay(live_view(m, d, candles), candle_close(day, candles))
                with self.subTest(candles=candles):
                    for field in dataclasses.fields(S.Snapshot):
                        self.assertEqual(getattr(files, field.name),
                                         getattr(memory, field.name), field.name)


# --------------------------------------------------------------------------- #
#  The engine                                                                 #
# --------------------------------------------------------------------------- #
DAY = date(2026, 10, 5)
FILL = datetime(2026, 10, 5, 15, 11)
ENTRY = {
    "index": "BANKNIFTY", "setup": "B", "book": S.CARRY_BOOK, "stop_loss_points": 0.0,
    "signal_time": datetime(2026, 10, 5, 14, 45), "execute_time": FILL,
    "candidates": ("BANKNIFTY", "NIFTY"), "unsettled": (),
}


def snapshot(**changes) -> S.Snapshot:
    base = S.Snapshot(
        as_of=FILL + timedelta(seconds=2), day=DAY, live=True, candles=71, entry=None,
        shock=False, edge_alerts=(), behind=(), open_prices={"NIFTY": 24_000.0},
        markers={}, verdicts={}, forecast={}, position=None, exits=(),
    )
    return dataclasses.replace(base, **changes)


def engine(**changes) -> S.StrategyEngine:
    out = S.StrategyEngine(root=Path(tempfile.gettempdir()))
    out.snapshot = snapshot(**changes)
    return out


class EngineTests(unittest.TestCase):
    def test_the_night_fill_is_at_15_11(self) -> None:
        self.assertEqual(S.NIGHT_ENTRY_TIME, datetime(2026, 1, 1, 15, 11).time())
        self.assertEqual(S.LAST_ENTRY_TIME, S.NIGHT_ENTRY_TIME)
        self.assertGreater(S.NIGHT_ENTRY_TIME, manual_desk.MANUAL_SETTINGS.square_off_at,
                           "after the HH/LL desk has squared its day off")
        self.assertLess(S.LEGS_CLOSE_TIME, manual_desk.MANUAL_SETTINGS.market_close)

    def test_the_entry_is_offered_only_inside_its_fill_window(self) -> None:
        e = engine(entry=ENTRY)
        self.assertIsNone(e.entry_intent(FILL - timedelta(seconds=1)))
        self.assertIsNone(e.entry_intent(datetime(2026, 10, 5, 14, 55)), "not at v3's own fill")
        intent = e.entry_intent(FILL)
        self.assertEqual(intent["key"], "v3:BANKNIFTY:2026-10-05T15:11:00")
        self.assertEqual((intent["index"], intent["setup"], intent["book"], intent["position"]),
                         ("BANKNIFTY", "B", S.CARRY_BOOK, "long"))
        self.assertEqual(intent["stop_loss_points"], 0.0)
        self.assertEqual(intent["priority_candidates"], ["BANKNIFTY", "NIFTY"])
        self.assertEqual(intent["signal_time"], "2026-10-05T14:45:00")
        self.assertIsNotNone(e.entry_intent(FILL + timedelta(seconds=S.ENTRY_WINDOW_SECONDS - 1)))
        self.assertIsNone(e.entry_intent(FILL + timedelta(seconds=S.ENTRY_WINDOW_SECONDS)))

    def test_the_night_entry_waits_for_the_desk_to_be_flat(self) -> None:
        e = engine(entry=ENTRY)
        desk = {"busy": "the HH/LL desk is holding FINNIFTY"}
        e.set_entry_blocker(lambda: desk["busy"])
        with capture_logs(S.LOGGER) as captured:
            self.assertIsNone(e.entry_intent(FILL))
            self.assertIsNone(e.entry_intent(FILL + timedelta(seconds=5)))
            self.assertEqual(len(captured.at_least(logging.WARNING)), 1, "said once")
            self.assertTrue(captured.has("held: the HH/LL desk is holding FINNIFTY", logging.WARNING))
            desk["busy"] = None
            self.assertEqual(e.entry_intent(FILL + timedelta(seconds=10))["index"], "BANKNIFTY")
        self.assertFalse(captured.at_least(logging.ERROR))

    def test_a_desk_never_flat_in_the_window_skips_the_night_entry_out_loud(self) -> None:
        e = engine(entry=ENTRY)
        e.set_entry_blocker(lambda: "the HH/LL desk is holding FINNIFTY")
        with capture_logs(S.LOGGER) as captured:
            self.assertIsNone(e.entry_intent(FILL))
            end = FILL + timedelta(seconds=S.ENTRY_WINDOW_SECONDS)
            self.assertIsNone(e.entry_intent(end))
            self.assertIsNone(e.entry_intent(end + timedelta(seconds=5)))
        self.assertTrue(captured.has("SKIPPED", logging.ERROR))
        self.assertEqual(len(captured.at_least(logging.ERROR)), 1, "said once")

    def test_a_failing_desk_check_holds_the_entry(self) -> None:
        e = engine(entry=ENTRY)

        def broken():
            raise RuntimeError("desk gone")

        e.set_entry_blocker(broken)
        with capture_logs(S.LOGGER) as captured:
            self.assertIsNone(e.entry_intent(FILL))
        self.assertTrue(captured.has("v3 entry check failed (desk gone)", logging.ERROR))

    def test_nothing_scheduled_is_no_intent(self) -> None:
        self.assertIsNone(engine().entry_intent(FILL))
        empty = engine()
        empty.snapshot = None
        self.assertIsNone(empty.entry_intent(FILL))

    def test_an_unranked_waterfall_waits_then_refuses_out_loud(self) -> None:
        e = engine(entry={**ENTRY, "unsettled": ("MIDCPNIFTY",)})
        with capture_logs(S.LOGGER) as captured:
            self.assertIsNone(e.entry_intent(FILL + timedelta(seconds=10)))
            self.assertFalse(captured.at_least(logging.WARNING), "still waiting is not news")
            self.assertIsNone(e.entry_intent(FILL + timedelta(seconds=S.SETTLE_SECONDS)))
            self.assertIsNone(e.entry_intent(FILL + timedelta(seconds=S.SETTLE_SECONDS + 5)))
        self.assertTrue(captured.has("MIDCPNIFTY had no 14:45 candle", logging.WARNING))
        self.assertEqual(len(captured.at_least(logging.WARNING)), 1, "said once")

    def test_legs_are_kept_ready_through_the_entry_windows(self) -> None:
        e = engine()
        # From the first B candle until the night fill window closes (15:11 +
        # 3 min); no entry can fill outside it, so no legs are asked for.
        for (hour, minute), want in (((11, 15), False), ((14, 44), False), ((14, 45), True),
                                     ((15, 11), True), ((15, 14), True), ((15, 15), False),
                                     ((15, 25), False)):
            with self.subTest(at=f"{hour}:{minute}"):
                now = datetime.combine(DAY, datetime.min.time()).replace(hour=hour, minute=minute)
                self.assertEqual(e.wants_long_legs("NIFTYNXT50", now), want)
        evening = datetime(2026, 10, 5, 15, 0)
        self.assertFalse(e.wants_long_legs("UNKNOWN", evening))
        self.assertFalse(engine(as_of=datetime(2026, 10, 2, 15, 30)).wants_long_legs("NIFTY", evening),
                         "not before today's data has been read")

    def test_the_session_open_is_served_once_read(self) -> None:
        e = engine()
        self.assertEqual(e.session_open_price("NIFTY", DAY), 24_000.0)
        self.assertIsNone(e.session_open_price("NIFTY", DAY + timedelta(days=1)))
        self.assertIsNone(e.session_open_price("SENSEX", DAY))
        self.assertIsNone(engine(live=False).session_open_price("NIFTY", DAY))

    def test_candles_closed(self) -> None:
        for (hour, minute, second), want in (((9, 14, 0), 0), ((9, 19, 59), 0), ((9, 20, 0), 1),
                                             ((11, 20, 2), 25), ((15, 30, 0), 75), ((16, 0, 0), 75)):
            with self.subTest(at=(hour, minute, second)):
                self.assertEqual(S.candles_closed(datetime(2026, 10, 5, hour, minute, second)), want)

    def test_a_refresh_is_due_until_the_newest_candle_is_in(self) -> None:
        now = FILL + timedelta(seconds=3)
        self.assertFalse(engine()._due(now), "caught up")
        for changes in ({"candles": 70}, {"behind": ("SENSEX",)},
                        {"as_of": datetime(2026, 10, 2, 15, 30)}, {"live": False, "candles": 75}):
            with self.subTest(**{k: str(v) for k, v in changes.items()}):
                self.assertTrue(engine(**changes)._due(now))
        missing = engine()
        missing.snapshot = None
        self.assertTrue(missing._due(now))
        early = engine(as_of=datetime(2026, 10, 5, 9, 0), live=False, candles=75)
        self.assertFalse(early._due(datetime(2026, 10, 5, 9, 10)), "history built; no candle yet")

    def test_a_failure_backs_off_until_the_next_candle(self) -> None:
        e = engine(candles=70)
        now = FILL + timedelta(seconds=3)
        e._target = S.candles_closed(now)
        e._next_try = time.monotonic() + 30
        self.assertFalse(e._due(now))
        self.assertTrue(e._due(now + timedelta(minutes=5)), "a new candle is tried at once")

    def test_a_torn_history_cache_is_rebuilt(self) -> None:
        day = "2015-02-10"
        with tempfile.TemporaryDirectory() as tmp:
            root = write_live_root(Path(tmp), day)
            write_candles(root, day, 30)
            (root / "history_20150210.pkl").write_bytes(b"\x80\x05torn")
            e = S.StrategyEngine(root=root)
            with capture_logs(S.LOGGER) as captured:
                snap = e.refresh(candle_close(day, 30))
        self.assertTrue(snap.live)
        self.assertEqual(snap.candles, 30)
        self.assertTrue(captured.has("history cache unreadable", logging.ERROR))

    def test_torn_live_data_is_rebuilt_from_scratch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            live = Path(tmp) / "data_live"
            live.mkdir()
            (live / "stale").write_text("x")
            calls = []

            def build(source, moment, root, history, refetch_last=False):
                calls.append((root.exists(), refetch_last))
                if len(calls) == 1:
                    raise pyarrow.ArrowInvalid("Parquet magic bytes not found")
                return False

            with mock.patch.object(S, "build_live", build), capture_logs(S.LOGGER) as captured:
                S.StrategyEngine(root=live, source=object())._build(pd.Timestamp("2026-10-05 09:00"))
        self.assertEqual(calls, [(True, True), (False, False)], "rebuilt into an empty folder")
        self.assertTrue(captured.has("rebuilding", logging.ERROR))

    def test_the_refresh_thread_reports_failures_and_stops(self) -> None:
        e = S.StrategyEngine(root=Path(tempfile.gettempdir()),
                             now_fn=lambda: FILL)
        called = threading.Event()

        def refresh(now=None):
            called.set()
            raise RuntimeError("Shoonya unreachable")

        with mock.patch.object(e, "refresh", refresh), capture_logs(S.LOGGER) as captured, \
                mock.patch.object(S, "_THROTTLE", S.LogThrottle()):
            e.start()
            self.assertTrue(called.wait(5))
            self.assertTrue(e.stop(timeout=5))
        self.assertTrue(captured.has("v3 refresh failed: Shoonya unreachable", logging.ERROR))
        self.assertGreater(e._next_try, time.monotonic(), "it backs off")


# --------------------------------------------------------------------------- #
#  What the console shows                                                     #
# --------------------------------------------------------------------------- #
CONSTANTS = {"atr": 200.0, "sma": 24_000.0, "pC": 24_300.0, "dO": 24_300.0,
             "cost": 10.0, "day": DAY}


def gates(bar=30, close=24_400.0, mean=24_350.0, shock=False, **changes):
    return S.score_gates(bar, close, mean, {**CONSTANTS, **changes}, shock=shock, accepted=False)


class ConsoleTests(unittest.TestCase):
    def test_a_clean_candle_passes_every_gate(self) -> None:
        verdict = gates()
        self.assertEqual((verdict["passed"], verdict["total"], verdict["blocked_by"]), (6, 6, []))
        self.assertEqual([g["name"] for g in verdict["gates"]], list(S.CARRY_GATES))
        self.assertEqual(verdict["confirmed_time"], "2026-10-05T11:45:00+05:30")
        self.assertEqual((verdict["bar"], verdict["tod"], verdict["daily_atr"]), (30, 705, 200.0))
        self.assertFalse(verdict["carry"]["window"])

    def test_each_gate_fails_on_its_own(self) -> None:
        for changes, gate in (({"sma": 24_500.0}, "Regime"), ({"atr": 150.0}, "ATR vs cost"),
                              ({"pC": 24_700.0}, "Drop gate"), ({"dO": 24_450.0}, "Positive session"),
                              ({"mean": 24_450.0}, "Running average"),
                              ({"shock": True}, "No overnight shock")):
            with self.subTest(gate=gate):
                self.assertEqual(gates(**changes)["blocked_by"], [gate])

    def test_no_momentum_gate_is_scored(self) -> None:
        for bar in (23, 24, 60, 61):
            with self.subTest(bar=bar):
                names = [g["name"] for g in gates(bar=bar)["gates"]]
                self.assertEqual(names, list(S.CARRY_GATES))
                self.assertNotIn("Momentum trigger", names)

    def test_the_carry_is_read_on_the_14_45_and_14_50_candles(self) -> None:
        for bar in (66, 67):
            verdict = gates(bar=bar)
            self.assertTrue(verdict["carry"]["window"])
            self.assertEqual([g["name"] for g in verdict["gates"]], list(S.CARRY_GATES))
        self.assertFalse(gates(bar=68)["carry"]["window"])

    def test_a_shock_session_blocks_the_carry(self) -> None:
        self.assertEqual(gates(bar=67, shock=True)["blocked_by"], ["No overnight shock"])

    def test_reach_is_the_highest_price_floor(self) -> None:
        # The running mean 24,350 sits above SMA, the drop limit and the open.
        self.assertEqual(gates()["reach"], {"leg": "carry", "open": True, "price": 24_350.0,
                                            "points": -50.0, "atr": -0.25})
        self.assertEqual(gates(close=24_200.0, mean=24_250.0)["reach"]["points"], 100.0)
        self.assertEqual(gates(mean=24_500.0)["reach"]["price"], 24_500.0)
        self.assertEqual(gates(close=23_900.0, sma=24_600.0)["reach"]["points"], 700.0)
        carry = gates(bar=66)["reach"]
        self.assertEqual((carry["leg"], carry["price"], carry["open"]), ("carry", 24_350.0, True))
        for kw in ({"bar": 66, "shock": True}, {"shock": True}, {"bar": 68}, {"atr": 150.0}):
            with self.subTest(**kw):
                self.assertFalse(gates(**kw)["reach"]["open"])
        self.assertIsNone(gates(atr=float("nan"))["reach"])

    def test_live_reach_is_exact_for_the_forming_bar(self) -> None:
        view = self._view("2015-02-10", 24)
        at = candle_close("2015-02-10", 24) + timedelta(seconds=30)
        level = view.live_verdict(18_900.0, at)["reach"]["price"]
        for price, clear in ((level + 0.05, True), (level - 0.05, False)):
            live = view.live_verdict(price, at)
            price_gates = [g["ok"] for g in live["gates"] if g["name"] != "ATR vs cost"]
            self.assertEqual(all(price_gates), clear, price)

    def _view(self, day: str, candles: int, index: str = "BANKNIFTY") -> S.IndexView:
        m = v3_market()
        e = S.StrategyEngine(root=Path(tempfile.gettempdir()))
        e.snapshot = S.replay(live_view(m, day_index(m, day), candles), candle_close(day, candles))
        return e.view(index)

    def test_markers_and_verdicts_follow_v3s_signals(self) -> None:
        # 2015-02-10: BANKNIFTY's A fired on the 11:15 candle (retired: no
        # marker, no acceptance), its B on 14:50.
        view = self._view("2015-02-10", 25)
        markers = view.pivot_points()
        self.assertEqual(markers, {"hh_points": [], "ll_points": []})
        (key, verdict), = view.gate_verdicts().items()
        self.assertEqual(key, "2015-02-10T11:15:00+05:30")
        self.assertFalse(verdict["accepted"])

        view = self._view("2015-02-10", 68)
        self.assertEqual([(p["time"], p["leg"], p["type"], p["bias"])
                          for p in view.pivot_points()["ll_points"]],
                         [("2015-02-10T14:50:00+05:30", "carry", "LL", "HH")])
        (verdict,) = view.gate_verdicts().values()
        self.assertTrue(verdict["accepted"])
        self.assertEqual(verdict["passed"], verdict["total"])
        self.assertTrue(verdict["carry"]["window"] and verdict["carry"]["accepted"])

    def test_the_live_forecast_matches_the_close_it_forecasts(self) -> None:
        m = v3_market()
        d = day_index(m, "2015-02-10")
        close = float(m.C[m.idx["BANKNIFTY"], d, 24])
        live = self._view("2015-02-10", 24).live_verdict(
            close, candle_close("2015-02-10", 24) + timedelta(seconds=30))
        (settled,) = self._view("2015-02-10", 25).gate_verdicts().values()
        self.assertTrue(live["live"])
        self.assertEqual(live["price"], close)
        self.assertEqual(live["gates"][:3], settled["gates"][:3])
        for field in ("bar", "tod", "daily_atr", "confirmed_time"):
            self.assertEqual(live[field], settled[field], field)
        self.assertEqual(live["accepted"], live["passed"] == live["total"])
        self.assertFalse(live["carry"]["accepted"], "a forecast is never a fill")

    def test_the_forecast_refuses_rather_than_invents(self) -> None:
        view = self._view("2015-02-10", 24)
        at = candle_close("2015-02-10", 24) + timedelta(seconds=30)
        for price in (None, 0.0, -1.0):
            with self.subTest(price=price):
                self.assertIsNone(view.live_verdict(price, at))
        self.assertIsNone(view.live_verdict(18_900.0, at + timedelta(minutes=11)), "stalled data")
        self.assertIsNone(view.live_verdict(18_900.0, at - timedelta(minutes=1)), "clock behind")
        self.assertIsNone(self._view("2015-02-10", 75).live_verdict(
            18_900.0, candle_close("2015-02-10", 75)), "the session is over")
        self.assertIsNone(self._view("2015-02-10", 24, "SENSEX").live_verdict(18_900.0, at),
                          "no data for that index")
        empty = S.StrategyEngine(root=Path(tempfile.gettempdir())).view("NIFTY")
        self.assertIsNone(empty.live_verdict(18_900.0, at))
        self.assertEqual(empty.pivot_points(), {"hh_points": [], "ll_points": []})
        self.assertEqual(empty.gate_verdicts(), {})


# --------------------------------------------------------------------------- #
#  v3's exits, as the decider carries them out                               #
# --------------------------------------------------------------------------- #
class DeciderTests(unittest.IsolatedAsyncioTestCase):
    async def run_step(self, now: datetime, service=None, price=101.0, **kwargs):
        exits: list[tuple[str, float]] = []
        entries: list[dict] = []

        async def exit_(reason, value):
            exits.append((reason, value))

        async def enter(direction, value, intent):
            entries.append(intent)
            return True

        async def price_getter(_context):
            return price

        decider = kwargs.pop("decider", None) or S.StrategyDecider(
            service or SimpleNamespace(entry_intent=lambda now: None), tag="T")
        with mock.patch.object(S, "now_ist", return_value=now):
            await decider.step(price_getter=price_getter, enter=enter, exit_=exit_,
                               direction="LONG", **kwargs)
        return exits, entries

    async def test_a_shock_session_a_trade_is_sold_at_14_55(self) -> None:
        held = dict(in_trade=True, book=S.INTRADAY_BOOK, entry_day=DAY, stop_price=90.0)
        exits, _ = await self.run_step(datetime(2026, 10, 5, 14, 54, 59), **held)
        self.assertEqual(exits, [])
        exits, _ = await self.run_step(datetime(2026, 10, 5, 14, 55), **held)
        self.assertEqual(exits, [("intraday_exit", 101.0)])

    async def test_an_overnight_a_trade_is_held_past_14_55(self) -> None:
        held = dict(in_trade=True, book=S.MOMENTUM_BOOK, entry_day=DAY, stop_price=90.0)
        exits, _ = await self.run_step(datetime(2026, 10, 5, 15, 20), **held)
        self.assertEqual(exits, [])
        exits, _ = await self.run_step(datetime(2026, 10, 6, 9, 15, 1), **held)
        self.assertEqual(exits, [("carry_open", 101.0)])

    async def test_the_stop_guards_both_a_holds_but_never_the_carry(self) -> None:
        for book in (S.MOMENTUM_BOOK, S.INTRADAY_BOOK):
            with self.subTest(book=book):
                exits, _ = await self.run_step(datetime(2026, 10, 5, 12, 0), price=89.0,
                                               in_trade=True, book=book, entry_day=DAY,
                                               stop_price=90.0)
                self.assertEqual(exits, [("stop_loss", 89.0)])
        exits, _ = await self.run_step(datetime(2026, 10, 5, 15, 0), price=1.0, in_trade=True,
                                       book=S.CARRY_BOOK, entry_day=DAY, stop_price=None)
        self.assertEqual(exits, [])

    async def test_the_next_open_reads_the_09_15_candle_once_in(self) -> None:
        service = SimpleNamespace(entry_intent=lambda now: None,
                                  session_open_price=lambda index, day: 24_010.0)
        exits, _ = await self.run_step(datetime(2026, 10, 6, 9, 40), service=service,
                                       in_trade=True, book=S.CARRY_BOOK, entry_day=DAY,
                                       index="BANKNIFTY")
        self.assertEqual(exits, [("carry_open", 24_010.0)])

    async def test_a_night_trade_is_held_overnight_and_sold_at_the_next_open(self) -> None:
        held = dict(in_trade=True, book=S.CARRY_BOOK, entry_day=DAY, stop_price=None)
        for at in (datetime(2026, 10, 5, 15, 12), datetime(2026, 10, 5, 15, 29),
                   datetime(2026, 10, 6, 9, 14, 59)):
            with self.subTest(at=at):
                exits, _ = await self.run_step(at, price=1.0, **held)
                self.assertEqual(exits, [])
        exits, _ = await self.run_step(datetime(2026, 10, 6, 9, 15), **held)
        self.assertEqual(exits, [("carry_open", 101.0)])

    async def test_an_undated_row_is_a_later_session_only_before_the_night_fill(self) -> None:
        self.assertTrue(S.StrategyDecider.exit_due(datetime(2026, 10, 6, 9, 15), None))
        self.assertTrue(S.StrategyDecider.exit_due(datetime(2026, 10, 6, 15, 10), None))
        self.assertFalse(S.StrategyDecider.exit_due(datetime(2026, 10, 5, 15, 12), None))

    async def test_an_entry_is_taken_once(self) -> None:
        intent = {"key": "v3:NIFTY:2026-10-05T15:11:00", "index": "NIFTY", "book": S.CARRY_BOOK,
                  "stop_loss_points": 0.0}
        decider = S.StrategyDecider(SimpleNamespace(entry_intent=lambda now: dict(intent)), tag="T")
        taken = []
        for _ in range(2):
            _exits, entries = await self.run_step(FILL, decider=decider, in_trade=False,
                                                  live_price_getter=lambda: 24_000.0)
            taken += entries
        self.assertEqual([entry["key"] for entry in taken], [intent["key"]])



class NightTradeManagerTests(unittest.TestCase):
    """What the HH/LL desk reads of the v3 manager before it opens a day trade."""

    def test_the_v3_manager_says_when_it_is_not_flat(self) -> None:
        from trade_manager import TradeManagerTest
        from tests.order_path_fakes import FakeDB, FakeSymbols

        manager = TradeManagerTest(api=None, db_connector=FakeDB(), symbol_fetcher=FakeSymbols(ready=True),
                                   live_price_getter=lambda: 24_000.0, fresh_price_getter=lambda: 24_000.0,
                                   strategy_service=engine())
        self.assertIsNone(manager.busy_reason())
        manager.in_trade, manager._index = True, "BANKNIFTY"
        self.assertEqual(manager.busy_reason(), "holding BANKNIFTY")
        manager._clear_position()
        self.assertIsNone(manager.busy_reason())
        manager.entering = True
        self.assertEqual(manager.busy_reason(), "entering a position")


if __name__ == "__main__":
    unittest.main()
