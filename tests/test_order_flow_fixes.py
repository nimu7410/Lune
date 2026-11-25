"""Regression tests for the LIVE order-flow fixes of 2026-11-15.

  1. Rollback and cleanup close the short leg first, and keep the long as its
     hedge whenever the short cannot be confirmed flat.
  2. An exit never sends a new order while an earlier one is unresolved.
  3. A live entry and exit are logged at WARNING (main.py keeps nothing lower),
     and the close records the rupee P&L of the actual fills.
  4. Entries are refused on a one-sided or too-wide book; exits never are.
"""

from __future__ import annotations

import logging
import unittest
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import trade_manager as tm
from config import TRADE_SETTINGS
from strategy_logic import MOMENTUM_BOOK
from tests.log_capture import capture_logs
from tests.order_path_fakes import FakeBroker, FakeDB, FakeSymbols, ScriptedService
from trade_manager import OrderPreflightError, TradeManagerLive

BUY, SELL = FakeSymbols.BUY_LEG, FakeSymbols.SELL_LEG
LOT = 65


class PricedBroker(FakeBroker):
    """Fills each symbol at its own premium (the base fake fills all at one price)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prices: dict[str, float] = {}
        self.quotes: dict[str, dict] = {}

    def _push(self, orderno, tsym, status, fill, avg):
        super()._push(orderno, tsym, status, fill, self.prices.get(tsym, avg))

    def place_order(self, **kwargs):
        ret = super().place_order(**kwargs)
        # An order's history keeps the price it filled at, as the broker's does.
        self.orders[-1]["avg"] = self.prices.get(kwargs["tradingsymbol"], self.last_price)
        return ret

    def single_order_history(self, orderno):
        rows = super().single_order_history(orderno)
        record = next((o for o in self.orders if o["orderno"] == orderno), None)
        for row in rows:
            if record is not None:
                row["avgprc"] = record.get("avg", row["avgprc"])
        return rows

    def get_quotes(self, exchange, token):
        quote = super().get_quotes(exchange, token)
        quote.update(self.quotes.get(token, {}))
        return quote


def live_manager(broker_type=PricedBroker):
    db = FakeDB()
    broker = broker_type(db)
    service = ScriptedService()
    intent = {"key": "v3:probe", "book": MOMENTUM_BOOK, "stop_loss_points": 40.0,
              "source": MOMENTUM_BOOK, "position": "long"}
    service.runtime.intent = intent
    settings = replace(
        TRADE_SETTINGS, poll_interval_sec=0.01, buy_wait_timeout_sec=1.0,
        sell_wait_timeout_sec=1.0, exchordid_wait_timeout_sec=1.0,
        exit_exchordid_wait_timeout_sec=1.0, exit_retry_backoff_sec=0.01,
        emergency_flatten_sleep_sec=0.01, entry_fail_cooldown_sec=0.0,
    )
    manager = TradeManagerLive(
        api=broker, db_connector=db, symbol_fetcher=FakeSymbols(),
        live_price_getter=lambda: 24_350.0, fresh_price_getter=lambda: 24_350.0,
        strategy_service=service, trade_settings=settings,
    )
    manager.ENTRY_POSITION_VERIFY_TIMEOUT_SEC = 2.0
    manager.ENTRY_POSITION_VERIFY_POLL_SEC = 0.01
    return manager, db, broker, intent


# --------------------------------------------------------------------------- #
#  1. Short first; the long is the hedge until the short is gone
# --------------------------------------------------------------------------- #
class ShortFirstTests(unittest.IsolatedAsyncioTestCase):
    def bare(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager._flatten_to_zero = AsyncMock(return_value=True)
        return manager

    async def test_rollback_flattens_short_then_long(self):
        manager = self.bare()
        self.assertEqual(await manager._rollback_entry_symbols(BUY, SELL, reason="t"), (True, True))
        self.assertEqual([c.args[0] for c in manager._flatten_to_zero.await_args_list], [SELL, BUY])

    async def test_rollback_keeps_long_when_short_will_not_close(self):
        manager = self.bare()
        manager._flatten_to_zero = AsyncMock(return_value=False)
        self.assertEqual(await manager._rollback_entry_symbols(BUY, SELL, reason="t"), (False, False))
        manager._flatten_to_zero.assert_awaited_once_with(SELL, reason="t")

    async def test_failed_put_with_unflattenable_short_keeps_the_call(self):
        manager = self.bare()
        manager._flatten_to_zero = AsyncMock(return_value=False)
        manager._submit_order_strict = AsyncMock(return_value="O2")
        manager._set_leg_orderno = AsyncMock()
        manager._wait_leg_status = AsyncMock(side_effect=RuntimeError("SELL leg REJECTED"))
        manager._cancel_order_if_possible = AsyncMock()
        manager._wait_leg_terminal = AsyncMock(return_value="REJECTED")
        manager._held_quantity, manager._index, manager.POLL_INTERVAL_SEC = LOT, "NIFTY", 0.01
        manager.NOT_OK_STATES, manager.EXCHORDID_WAIT_TIMEOUT_SEC = ("REJECTED",), 0.1
        with self.assertRaisesRegex(RuntimeError, "Aborting hedge"):
            await manager._enter_leg(1, "sell", "S", SELL, 0.1, label="SELL", also_flatten=(BUY,))
        manager._flatten_to_zero.assert_awaited_once_with(SELL, reason="entry_sell_failed")

    async def test_reconcile_flattens_shorts_first_and_holds_longs_behind_a_stuck_short(self):
        for short_ok, expected in ((True, [SELL, BUY]), (False, [SELL])):
            with self.subTest(short_ok=short_ok):
                manager = self.bare()
                manager._flatten_to_zero = AsyncMock(side_effect=lambda s, reason: short_ok or s != SELL)
                manager._last_reconcile_ts, manager.RECONCILE_INTERVAL_SEC = 0.0, 0.0
                manager._reconcile_unresolved_attempts = AsyncMock(return_value=False)
                manager.entering = manager.exiting = manager.in_trade = False
                manager.symbol_fetcher = None
                manager._watch_symbols = {BUY, SELL}
                manager._quarantine_symbols = None
                manager._position_book = AsyncMock(return_value={BUY: LOT, SELL: -LOT})
                manager._market_open = Mock(return_value=True)
                await manager._reconcile_positions()
                self.assertEqual([c.args[0] for c in manager._flatten_to_zero.await_args_list],
                                 expected)


# --------------------------------------------------------------------------- #
#  2. No exit order while an earlier one is unresolved
# --------------------------------------------------------------------------- #
class ExitGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_unresolved_attempt_holds_the_exit_and_frees_the_lock(self):
        manager, db, broker, intent = live_manager()
        self.assertTrue(await manager._enter_trade("LONG", 24_350.0, dict(intent)))
        sent = len(broker.orders)
        manager._reconcile_unresolved_attempts = AsyncMock(return_value=True)
        await manager._exit_trade("stop_loss", 0, 24_300.0)
        self.assertEqual(len(broker.orders), sent)
        self.assertTrue(manager.in_trade)
        self.assertFalse(manager.exiting)
        self.assertIsNone(manager._exit_lock_conn)
        # Once the broker has answered, the very next exit goes through.
        manager._reconcile_unresolved_attempts = AsyncMock(return_value=False)
        await manager._exit_trade("stop_loss", 0, 24_300.0)
        self.assertFalse(manager.in_trade)
        self.assertTrue(all(q == 0 for q in broker.positions.values()), broker.positions)


# --------------------------------------------------------------------------- #
#  3. Lifecycle at WARNING; P&L from the fills
# --------------------------------------------------------------------------- #
class LifecycleAndPnlTests(unittest.IsolatedAsyncioTestCase):
    async def test_round_trip_logs_at_warning_and_books_fill_pnl(self):
        manager, db, broker, intent = live_manager()
        broker.prices = {BUY: 300.0, SELL: 100.0}
        # main.py's handlers keep WARNING and up only: that is the bar.
        with capture_logs(tm.LOGGER) as logs:
            self.assertTrue(await manager._enter_trade("LONG", 24_350.0, dict(intent)))
            broker.prices = {BUY: 350.0, SELL: 70.0}
            # The index closed below the entry; the fills still made money.
            await manager._exit_trade("carry_open", 0, 24_300.0)
        text = logs.text(logging.WARNING)
        self.assertIn("[LIVE] ENTER", text)
        self.assertIn("[LIVE] EXIT (flat)", text)
        row = db.trade_logs[0]
        self.assertEqual(row["realized_pnl"], (350 - 300 + 100 - 70) * LOT)
        self.assertEqual(row["win"], 1)
        self.assertEqual(row["exit_price"], 24_300.0)

    def pnl_manager(self, row, attempts, history):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager._held_quantity, manager._index, manager._sell_entry_side = LOT, "NIFTY", "S"
        manager._fetch_order_log_row = AsyncMock(return_value=row)
        manager.db = SimpleNamespace(execute_async=AsyncMock(return_value=attempts))
        manager._broker_terminal_record = AsyncMock(side_effect=lambda o: history.get(o))
        return manager

    ENTRY = {"buy_fill_qty": LOT, "buy_avg_price": 300.0, "sell_fill_qty": LOT, "sell_avg_price": 100.0}

    async def test_partial_exit_and_its_residual_are_both_counted(self):
        attempts = [
            {"orderno": "X1", "symbol": SELL, "side": "B", "requested_qty": LOT},
            {"orderno": "X2", "symbol": SELL, "side": "B", "requested_qty": 40},
            {"orderno": "X3", "symbol": BUY, "side": "S", "requested_qty": LOT},
        ]
        history = {"X1": {"status": "CANCELED", "fillshares": "25", "avgprc": "70"},
                   "X2": {"status": "COMPLETE", "fillshares": "40", "avgprc": "80"},
                   "X3": {"status": "COMPLETE", "avgprc": "350"}}   # fillshares omitted
        manager = self.pnl_manager(self.ENTRY, attempts, history)
        expected = -300 * LOT + 100 * LOT - 70 * 25 - 80 * 40 + 350 * LOT
        self.assertEqual(await manager._realized_pnl(1, BUY, SELL), expected)

    async def test_incomplete_records_claim_nothing(self):
        full = [{"orderno": "X1", "symbol": SELL, "side": "B", "requested_qty": LOT},
                {"orderno": "X2", "symbol": BUY, "side": "S", "requested_qty": LOT}]
        done = {"X1": {"status": "COMPLETE", "fillshares": str(LOT), "avgprc": "70"},
                "X2": {"status": "COMPLETE", "fillshares": str(LOT), "avgprc": "350"}}
        cases = {
            "entry average missing": ({**self.ENTRY, "buy_avg_price": None}, full, done),
            "entry fill short": ({**self.ENTRY, "sell_fill_qty": 30}, full, done),
            "exit history unreadable": (self.ENTRY, full, {"X1": done["X1"]}),
            "legs do not net to zero": (self.ENTRY, full[:1], done),
            "exit price missing": (self.ENTRY, full, {**done, "X2": {"status": "COMPLETE",
                                                                    "fillshares": str(LOT)}}),
        }
        for name, (row, attempts, history) in cases.items():
            with self.subTest(name):
                manager = self.pnl_manager(row, attempts, history)
                self.assertIsNone(await manager._realized_pnl(1, BUY, SELL))


# --------------------------------------------------------------------------- #
#  4. Spread guard
# --------------------------------------------------------------------------- #
class SpreadGuardTests(unittest.TestCase):
    def manager(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.MAX_ENTRY_SPREAD_PCT = 0.03
        return manager

    def test_spread_rules(self):
        tick = Decimal("0.05")
        cases = {
            "tight": ({"bp1": "99.95", "sp1": "100.05"}, True),
            "at the limit": ({"bp1": "98.5", "sp1": "101.5"}, True),       # 3.0 on mid 100
            "too wide": ({"bp1": "98", "sp1": "102"}, False),
            "one tick on a cheap option": ({"bp1": "1.00", "sp1": "1.05"}, True),
            "no bid": ({"bp1": "0", "sp1": "100"}, False),
            "bid missing": ({"sp1": "100"}, False),
            "crossed": ({"bp1": "101", "sp1": "100"}, False),
            "garbage": ({"bp1": "n/a", "sp1": "100"}, False),
        }
        for name, (quote, ok) in cases.items():
            with self.subTest(name):
                if ok:
                    self.manager()._check_entry_spread("LEG", quote, tick)
                else:
                    with self.assertRaisesRegex(OrderPreflightError, "entry refused"):
                        self.manager()._check_entry_spread("LEG", quote, tick)

    def test_exit_order_is_never_refused_for_spread(self):
        manager = self.manager()
        manager._contract_details = Mock(return_value={
            "token": "T", "lot": LOT, "max_qty": 650, "tick": Decimal("0.05"), "uc": None, "lc": None})
        manager._exchange_for = Mock(return_value="NFO")
        manager.api = Mock(get_quotes=Mock(return_value={"stat": "Ok", "bp1": "50", "sp1": "100"}),
                           place_order=Mock(return_value={"stat": "Ok", "norenordno": "1"}))
        with self.assertRaisesRegex(OrderPreflightError, "spread"):
            manager._place_broker_order("B", "LEG", qty=LOT)
        manager.api.place_order.assert_not_called()
        manager._place_broker_order("B", "LEG", qty=LOT, reduce_only=True)
        manager.api.place_order.assert_called_once()


class SpreadGuardEntryTests(unittest.IsolatedAsyncioTestCase):
    async def test_wide_put_is_found_before_the_call_is_bought(self):
        manager, db, broker, intent = live_manager()
        broker.quotes = {SELL: {"bp1": "60", "sp1": "100"}}
        self.assertFalse(await manager._enter_trade("LONG", 24_350.0, dict(intent)))
        self.assertEqual(broker.orders, [])
        self.assertFalse(db.trade_logs)
        self.assertFalse(manager.in_trade)


if __name__ == "__main__":
    unittest.main()
