"""The ways a day ends with the bot running and no trade in the book.

They share one shape: the bot keeps running, the chart keeps drawing, and the
absence of a trade is indistinguishable from a strategy that simply declined.

  EntryPriceTests     - a missing opening quote never becomes a cached fill;
                        the entry is held and retried, and the log says
                        which price source failed
  SnapshotSignalTests - the console still explains a declined candle, and a
                        broken strategy read never takes the chart down

v3's own data validation (gap-free candle prefixes, invalid days dropped and
never repaired) lives in `strategy/v3` and is part of the frozen release; v3's
signals as Lune runs them are covered by `test_strategy_v3.py`.
"""

from __future__ import annotations

import asyncio
import logging
import unittest
from types import SimpleNamespace

import strategy_logic
from tests.log_capture import capture_logs
from strategy_logic import StrategyDecider
from live_server import PivotSnapshotService


class EntryPriceTests(unittest.TestCase):
    """v3 never substitutes a cached price for the intended opening quote."""

    def _decider(self) -> StrategyDecider:
        return StrategyDecider(object(), tag="TEST")

    @staticmethod
    def _fallback(price):
        async def getter(_context: str):
            return price
        return getter

    def _price(self, fallback, fresh, intent):
        with capture_logs(strategy_logic.LOGGER) as captured:
            price = asyncio.run(
                self._decider()._entry_price(self._fallback(fallback), fresh)
            )
        return price, captured

    def test_a_fresh_tick_is_always_preferred(self) -> None:
        price, captured = self._price(24_000.0, lambda: 24_350.0, {"stop_loss_points": 40.0})
        self.assertEqual(price, 24_350.0)
        self.assertEqual(captured.at_least(logging.WARNING), [])

    def test_an_entry_with_a_stop_still_insists_on_a_fresh_tick(self) -> None:
        price, captured = self._price(24_340.0, lambda: None, {"stop_loss_points": 40.0})
        self.assertIsNone(price, "a stop measured from a stale fill is the hazard")
        self.assertTrue(captured.at_least(logging.WARNING))

    def test_a_stopless_entry_does_not_use_a_stale_fallback(self) -> None:
        price, captured = self._price(24_339.8, lambda: None, {"stop_loss_points": 0.0})
        self.assertIsNone(price)
        self.assertTrue(captured.has("entry held", logging.WARNING))

    def test_a_stopless_entry_with_no_price_anywhere_is_still_refused(self) -> None:
        price, captured = self._price(None, lambda: None, {"stop_loss_points": 0.0})
        self.assertIsNone(price)
        self.assertTrue(captured.at_least(logging.WARNING))

    def test_the_refusal_reports_what_each_source_returned(self) -> None:
        """The 08/17 log could not distinguish a dead feed from a stale flag."""
        _price, captured = self._price(24_339.8, lambda: None, {"stop_loss_points": 40.0})
        message = captured.text(logging.WARNING)
        self.assertIn("returned None", message)
        self.assertIn("24339.80", message)

    def test_a_missing_intent_is_treated_as_carrying_a_stop(self) -> None:
        price, captured = self._price(24_339.8, lambda: None, None)
        self.assertIsNone(price, "an unknown entry must get the conservative rule")
        self.assertTrue(captured.at_least(logging.WARNING))

    def test_a_raising_price_getter_does_not_take_the_loop_down(self) -> None:
        def explode():
            raise RuntimeError("socket gone")

        price, captured = self._price(24_339.8, explode, {"stop_loss_points": 0.0})
        self.assertIsNone(price)
        self.assertTrue(captured.has("live price lookup failed", logging.ERROR))


class FeedGapAtTheFillTests(unittest.TestCase):
    """A WebSocket reconnect at the fill moment delays the entry, never deletes it.

    The recorded sessions reconnect for ~20s a few times a week; one landing on
    14:50:00 used to expire the day's carry on its first tick-less poll.
    """

    INTENT = {"key": "v3:NIFTY:2026-10-05T14:50:00", "index": "NIFTY", "stop_loss_points": 0.0}

    def _run(self, ticks, intents):
        decider = StrategyDecider(SimpleNamespace(entry_intent=lambda _now: next(intents)), tag="TEST")
        entered = []

        async def enter(direction, price, intent):
            entered.append((direction, price, intent["key"]))
            return True

        async def cached(*_args):
            return 24_300.0          # a last known price: never a fill

        async def run():
            for _ in range(len(ticks)):
                await decider.step(in_trade=False, direction=None, price_getter=cached,
                                   enter=enter, exit_=None, index_price_getter=cached,
                                   index_live_price_getter=lambda _index: next(iter_ticks))

        iter_ticks = iter(ticks)
        with capture_logs(strategy_logic.LOGGER) as captured:
            asyncio.run(run())
        return entered, captured

    def test_the_entry_waits_out_the_gap_at_a_fresh_price(self):
        entered, captured = self._run([None, None, 24_350.0],
                                      iter([dict(self.INTENT)] * 4))
        self.assertEqual(entered, [("LONG", 24_350.0, self.INTENT["key"])])
        self.assertEqual(len(captured.at_least(logging.WARNING)), 1, "one warning per gap")

    def test_the_fill_window_still_closes_on_it(self):
        entered, _ = self._run([None, 24_350.0], iter([dict(self.INTENT), None]))
        self.assertEqual(entered, [])


class SnapshotSignalTests(unittest.TestCase):
    """What the console actually receives, through the real snapshot service."""

    MARKER = {
        "time": "2026-08-19T11:45:00+05:30", "price": 24_500.0, "type": "LL",
        "confirmed_time": "2026-08-19T11:45:00+05:30", "low": 24_500.0,
        "leg": "momentum",
    }
    CANDLES = [
        {"time": "2026-08-19T11:45:00+05:30", "open": 24_463.05,
         "high": 24_572.85, "low": 24_403.0, "close": 24_500.0},
        {"time": "2026-08-19T11:50:00+05:30", "open": 24_498.25,
         "high": 24_599.0, "low": 24_471.9, "close": 24_573.85},
    ]

    def _service(self, verdicts):
        handler = SimpleNamespace(get_latest_data=lambda: list(self.CANDLES))
        strategy = SimpleNamespace(
            pivot_points=lambda: {"hh_points": [], "ll_points": [dict(self.MARKER)]},
            gate_verdicts=lambda: verdicts,
        )
        return PivotSnapshotService(handler, strategy_service=strategy)

    def test_the_newest_verdict_is_served(self):
        """v3 scores every bar, so the newest one is the interesting reading."""
        older = {"accepted": False, "blocked_by": ["Regime"], "gates": []}
        newest = {"accepted": True, "passed": 4, "total": 4, "blocked_by": [], "gates": []}
        snapshot = self._service(
            {"2026-08-19T11:40:00+05:30": older, self.MARKER["confirmed_time"]: newest}
        ).latest_snapshot()
        self.assertEqual(snapshot["signal"], newest)

    def test_a_verdict_is_served_even_when_no_marker_is_shown(self):
        """The ~99% of bars that decline are exactly what needs explaining."""
        handler = SimpleNamespace(get_latest_data=lambda: list(self.CANDLES))
        verdict = {"accepted": False, "blocked_by": ["ATR vs cost"], "gates": []}
        strategy = SimpleNamespace(
            pivot_points=lambda: {"hh_points": [], "ll_points": []},
            gate_verdicts=lambda: {"2026-08-19T11:45:00+05:30": verdict},
        )
        snapshot = PivotSnapshotService(handler, strategy_service=strategy).latest_snapshot()
        self.assertEqual(snapshot["signal"], verdict)
        self.assertEqual(snapshot["pivots"], {"hh_points": [], "ll_points": []})

    def test_no_strategy_service_serves_no_signal(self):
        handler = SimpleNamespace(get_latest_data=lambda: list(self.CANDLES))
        snapshot = PivotSnapshotService(handler).latest_snapshot()
        self.assertIsNone(snapshot["signal"])
        self.assertEqual(snapshot["pivots"], {"hh_points": [], "ll_points": []})

    def test_the_engine_payload_carries_the_live_score_and_the_leg(self):
        """What `/entry_gates` actually hands the bay, wiring included."""
        service = self._service({self.MARKER["confirmed_time"]: {"accepted": False}})
        service.strategy_service.live_verdict = lambda price: {"live": True, "price": price}
        service.attach_price_getter(lambda: 24_500.0)

        engine = service.latest_engine()
        self.assertEqual(engine["live"], {"live": True, "price": 24_500.0})
        # The leg rides along: a carry that was taken is a fact the gates on the
        # bar being scored have already moved past.
        self.assertEqual(engine["last"]["leg"], self.MARKER["leg"])

    def test_a_stale_tick_is_no_live_score_at_all(self):
        """The fresh getter answers None, and the bay falls back to the close."""
        service = self._service({self.MARKER["confirmed_time"]: {"accepted": False}})
        service.strategy_service.live_verdict = lambda price: {"live": True, "price": price}
        for getter in (lambda: None, None):
            with self.subTest(getter=getter):
                service.attach_price_getter(getter)
                engine = service.latest_engine()
                self.assertIsNone(
                    engine["live"] if getter is None else engine["live"]["price"]
                )
                self.assertIsNotNone(engine["signal"])

    def test_a_raising_live_score_does_not_take_the_chart_down(self):
        def explode(_price):
            raise RuntimeError("runtime mid-update")

        service = self._service({self.MARKER["confirmed_time"]: {"accepted": False}})
        service.strategy_service.live_verdict = explode
        service.attach_price_getter(lambda: 24_500.0)
        self.assertIsNone(service.latest_engine()["live"])

    def test_a_raising_strategy_does_not_take_the_chart_down(self):
        def explode():
            raise RuntimeError("runtime mid-update")

        handler = SimpleNamespace(get_latest_data=lambda: list(self.CANDLES))
        strategy = SimpleNamespace(
            pivot_points=lambda: {"hh_points": [], "ll_points": [dict(self.MARKER)]},
            gate_verdicts=explode,
        )
        snapshot = PivotSnapshotService(handler, strategy_service=strategy).latest_snapshot()
        self.assertIsNone(snapshot["signal"])
        self.assertEqual(len(snapshot["candles"]), 2)


if __name__ == "__main__":
    unittest.main()
