# tests/test_entry_not_silently_skipped.py
"""Regression tests for the silent entry-skip defect.

The bug: `_enter_trade` refused every entry when `GetSymbol` had not published
option legs, and said so at DEBUG. `main.py` configures logging at WARNING, so the
line was never written - a bot that declined to trade all day looked exactly
like a strategy that had no signal. TEST mode was affected too, so paper
trading could not be used to detect it either.

These lock in the three properties that fix required:
  1. TEST records the trade anyway, against a marked synthetic symbol.
  2. LIVE still refuses (it cannot invent a symbol) but never below WARNING.
  3. Whatever `GetSymbol` reports as the cause reaches the log line.

Written against `unittest`, not pytest. `tests/run_checks.py` - the one command
that gates a restart - discovers with `unittest`, which imports a pytest module
happily and then collects nothing from it: the checks would go green while
these eight tests never ran, which is a worse failure than the red they replace.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import ist
import strategy_logic
import log_throttle
import trade_manager
from trade_manager import TradeManagerTest
from tests.log_capture import capture_logs

ENTRY_MOMENT = datetime(2026, 8, 17, 14, 56, 10, tzinfo=ist.IST)


class _DB:
    def __init__(self) -> None:
        self.rows: list[tuple] = []

    def ensure_schema(self) -> None:
        pass

    def execute(self, *a, **k):
        return []

    async def execute_async(self, sql, params=None, fetch=False):
        if "INSERT INTO botdata.trade_logs" in sql:
            self.rows.append(params)
            return [{"id": len(self.rows)}]
        return []


class _Symbols:
    """Stands in for GetSymbol; `ready=False` reproduces an unpublished leg set."""

    def __init__(self, ready: bool, reason: str | None = None) -> None:
        self.final_symbols = (
            {"buy": "NIFTY_B", "sell": "NIFTY_S", "sell_side": "S", "bias": "HH"}
            if ready
            else None
        )
        self.final_symbol = "NIFTY_S" if ready else None
        self.legs_blocked_reason = reason


def _pending(book: str = strategy_logic.CARRY_BOOK) -> dict:
    return {
        "key": "v3:NIFTY:2026-08-17T14:55:00",
        "position": "long",
        "source": book,
        "stop_loss_points": 0.0,
        "book": book,
        "signal_time": "2026-08-17T14:50:00",
        "execute_time": "2026-08-17T14:55:00",
    }


def _manager(symbols: _Symbols, db: _DB) -> TradeManagerTest:
    return TradeManagerTest(
        api=None,
        db_connector=db,
        symbol_fetcher=symbols,
        live_price_getter=lambda: 24380.0,
        fresh_price_getter=lambda: 24380.0,
        strategy_service=types.SimpleNamespace(
            runtime=types.SimpleNamespace(entry_intent=lambda now=None: None)
        ),
    )


class EntryNotSilentlySkippedTests(unittest.TestCase):
    def setUp(self) -> None:
        frozen = lambda: ENTRY_MOMENT  # noqa: E731 - a clock, not a helper
        for module, name in (
            (trade_manager, "now_ist"), (trade_manager, "_now_ist"), (strategy_logic, "now_ist"),
        ):
            patcher = mock.patch.object(module, name, frozen)
            patcher.start()
            self.addCleanup(patcher.stop)
        # Each test gets a clean throttle, or the first test eats the only emit.
        throttle = mock.patch.object(trade_manager, "_THROTTLE", log_throttle.LogThrottle())
        throttle.start()
        self.addCleanup(throttle.stop)

    def test_paper_trade_is_recorded_when_legs_are_ready(self) -> None:
        db = _DB()
        tm = _manager(_Symbols(ready=True), db)
        self.assertIs(asyncio.run(tm._enter_trade("LONG", 24380.0, _pending())), True)
        self.assertIs(tm.in_trade, True)
        self.assertEqual(len(db.rows), 1)
        self.assertEqual(tm.symbol, "NIFTY_S")
        # Both legs reach the ledger, or the console's held row shows BUY "—".
        self.assertEqual(db.rows[0][-2:], ("NIFTY_S", "NIFTY_B"))

    def test_paper_trade_is_still_recorded_when_legs_are_missing(self) -> None:
        """The heart of it: TEST must rehearse the strategy without the plumbing."""
        db = _DB()
        tm = _manager(_Symbols(ready=False), db)
        self.assertIs(asyncio.run(tm._enter_trade("LONG", 24380.0, _pending())), True)
        self.assertIs(tm.in_trade, True)
        self.assertEqual(len(db.rows), 1, "a leg-less paper entry must not be dropped")
        self.assertEqual(tm.symbol, TradeManagerTest.PAPER_FALLBACK_SYMBOL)

    def test_missing_legs_are_reported_at_warning_not_debug(self) -> None:
        db = _DB()
        tm = _manager(_Symbols(ready=False, reason="no live price from the WebSocket"), db)
        with capture_logs(trade_manager.LOGGER) as captured:
            asyncio.run(tm._enter_trade("LONG", 24380.0, _pending()))
        warnings = captured.at_least(logging.WARNING)
        self.assertTrue(warnings, "a skipped/degraded entry must be visible at INFO or above")
        self.assertIn("no live price from the WebSocket", warnings[0].getMessage())

    def test_fallback_symbol_is_not_a_real_tradeable_symbol(self) -> None:
        """Guards against the placeholder ever being mistaken for a live leg."""
        self.assertIn("PAPER", TradeManagerTest.PAPER_FALLBACK_SYMBOL)
        self.assertFalse(TradeManagerTest.PAPER_FALLBACK_SYMBOL.startswith("NIFTY"))

    def test_live_refuses_but_logs_at_warning(self) -> None:
        """LIVE cannot invent a symbol - but the refusal must never be DEBUG."""
        manager = trade_manager.TradeManagerLive.__new__(trade_manager.TradeManagerLive)
        manager.entering = False
        manager.symbol_fetcher = _Symbols(
            ready=False, reason="no live price from the WebSocket"
        )
        manager._legs_missing_since = None
        manager._watch_symbols = set()

        with capture_logs(trade_manager.LOGGER) as captured:
            result = asyncio.run(manager._enter_trade("LONG", 24380.0, _pending()))

        self.assertIs(result, False, "LIVE must not trade without a real symbol")
        warnings = captured.at_least(logging.WARNING)
        self.assertTrue(warnings, "LIVE's refusal must be visible at INFO or above")
        self.assertIn("no live price from the WebSocket", warnings[0].getMessage())

    def test_log_throttle_emits_once_then_counts_suppressed(self) -> None:
        throttle = log_throttle.LogThrottle(interval_sec=3600.0)
        self.assertEqual(throttle.should_log("k"), (True, 0))
        for _ in range(4):
            self.assertIs(throttle.should_log("k")[0], False)
        throttle.reset("k")
        self.assertEqual(throttle.should_log("k"), (True, 0), "reset re-arms immediately")

    def test_getsymbol_reports_why_legs_were_not_built(self) -> None:
        """A zero price must leave a machine-readable reason for the managers."""
        from getdata import GetSymbol

        fetcher = GetSymbol.__new__(GetSymbol)
        fetcher.tok = "26000"
        fetcher.legs_blocked_reason = None
        fetcher.final_symbols = {"buy": "OLD_BUY", "sell": "OLD_SELL"}
        fetcher.final_symbol = "OLD_SELL"
        fetcher.get_live_price = lambda: None  # fresh getter after disconnect
        # Legs are wanted: a price is only demanded while an entry can fill.
        fetcher.set_pivot_type_getter(lambda: "HH")

        self.assertIsNone(GetSymbol.update_symbol(fetcher))
        self.assertIsNone(fetcher.final_symbols)
        self.assertIsNone(fetcher.final_symbol)
        self.assertIsNotNone(fetcher.legs_blocked_reason)
        self.assertIn("no live price", fetcher.legs_blocked_reason)


if __name__ == "__main__":
    unittest.main()
