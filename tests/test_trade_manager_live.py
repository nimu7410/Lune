import unittest
import time
from dataclasses import replace
from unittest.mock import AsyncMock, Mock, patch
from types import SimpleNamespace
from datetime import datetime, timedelta
import instruments
import ist
import trade_manager as tm
from config import TRADE_SETTINGS
from strategy_logic import MOMENTUM_BOOK, StrategyDecider
from tests.order_path_fakes import FakeBroker, FakeDB, FakeSymbols, ScriptedService

from trade_manager import (
    TradeManagerLive,
    TradeManagerTest,
    UncertainOrderError,
    UncertainTradeCommitError,
)


class LiveRiskGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_price_uses_fresh_feed_without_database_history(self):
        for manager_type in (TradeManagerLive, TradeManagerTest):
            with self.subTest(manager=manager_type.__name__):
                manager = manager_type.__new__(manager_type)
                manager.db = SimpleNamespace(execute_async=AsyncMock())
                manager.fresh_price_getter = Mock(return_value="24100.0")
                self.assertEqual(await manager._get_price_with_fallback("carry exit"), 24100.0)
                for invalid in (None, 0, -1, float("nan"), float("inf"), "bad"):
                    manager.fresh_price_getter.return_value = invalid
                    self.assertIsNone(await manager._get_price_with_fallback("carry exit"))
                manager.fresh_price_getter.side_effect = RuntimeError("feed unavailable")
                self.assertIsNone(await manager._get_price_with_fallback("carry exit"))
                manager.db.execute_async.assert_not_awaited()

    async def test_old_cached_price_cannot_be_used_after_feed_loss(self):
        for manager_type in (TradeManagerLive, TradeManagerTest):
            with self.subTest(manager=manager_type.__name__):
                manager = manager_type.__new__(manager_type)
                manager.fresh_price_getter = Mock(return_value=None)
                manager.live_price_getter = Mock(return_value=24_000.0)
                manager._last_valid_price = 24_000.0
                manager.db = SimpleNamespace(execute_async=AsyncMock())
                self.assertIsNone(await manager._get_price_with_fallback("carry exit"))
                manager.db.execute_async.assert_not_awaited()

    async def test_paper_stop_never_uses_stale_price(self):
        manager = TradeManagerTest.__new__(TradeManagerTest)
        manager._index = None
        manager.index_live_price_getters = {}
        manager.fresh_price_getter = Mock(return_value=None)
        manager.live_price_getter = Mock(return_value=23_000.0)
        manager.db = SimpleNamespace(execute_async=AsyncMock())
        self.assertIsNone(await manager._index_price_with_fallback("NIFTY", "stop check"))
        manager.db.execute_async.assert_not_awaited()
        for invalid in (float("nan"), float("inf"), "bad"):
            manager.fresh_price_getter.return_value = invalid
            self.assertIsNone(manager._live_price_for("NIFTY"))

    async def test_stale_legs_cannot_reach_order_path(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.entering = False
        manager.symbol_fetchers = {}
        manager.symbol_fetcher = SimpleNamespace(final_symbols={
            "buy": "HEDGE", "sell": "SHORT", "sell_side": "S", "bias": "HH",
            "built_at": time.monotonic() - 60,
        })
        manager._try_acquire_entry_lock = AsyncMock()
        self.assertFalse(await manager._enter_trade("LONG", 24000.0, {}))
        manager._try_acquire_entry_lock.assert_not_awaited()

    async def test_entry_preflight_rechecks_intent_and_fresh_spot(self):
        pending = {"key": "v3:NIFTY:signal"}
        for active, price in ((None, 24000.0), (pending, None)):
            with self.subTest(active=active is not None, fresh_price=price is not None):
                manager = TradeManagerLive.__new__(TradeManagerLive)
                manager._index = "NIFTY"
                manager.lot_size = 65
                manager.lots = 1
                manager._has_nonterminal_orders = AsyncMock(return_value=False)
                manager._contract_details = Mock(return_value={"lot": 65, "max_qty": 65})
                manager._check_entry_book = Mock()
                manager.strategy = SimpleNamespace(entry_intent=Mock(return_value=active))
                manager._live_price_for = Mock(return_value=price)
                manager._ensure_order_log_row = AsyncMock()
                manager._submit_order_strict = AsyncMock()

                placed = await manager._place_hedge_seq(
                    {"buy": "HEDGE", "sell": "SHORT"}, pending
                )

                self.assertIsNone(placed)
                manager._ensure_order_log_row.assert_not_awaited()
                manager._submit_order_strict.assert_not_awaited()

    async def test_unknown_holiday_year_blocks_new_entries(self):
        manager = self.manager()
        manager.entering = False
        manager._holiday_years = {2026}
        manager._entry_cooldown_active = Mock(return_value=False)
        manager._enter_trade = AsyncMock()
        with patch("trade_manager.now_ist", return_value=datetime(2027, 1, 5, tzinfo=ist.IST)):
            self.assertFalse(await manager._enter_for_strategy("LONG", 100.0, {}))
        manager._enter_trade.assert_not_awaited()

    async def test_latched_stop_retries_exit_during_feed_outage(self):
        decider = StrategyDecider(SimpleNamespace(), tag="TEST")
        getter = AsyncMock(side_effect=[90.0, None])
        exit_ = AsyncMock()
        now = ist.now()
        for _ in range(2):
            await decider._watch_stop(now, "LONG", MOMENTUM_BOOK, now.date(),
                                      100.0, getter, exit_)
        self.assertEqual(exit_.await_count, 2)
        self.assertEqual(exit_.await_args.args, ("stop_loss", 90.0))

    def manager(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.api = Mock()
        manager._last_valid_price = 24_000.0
        manager.live_price_getter = Mock(return_value=0.0)
        manager.fresh_price_getter = Mock(return_value=None)
        manager.db = SimpleNamespace(execute_async=AsyncMock())
        return manager

    async def test_every_entry_is_the_configured_lots_on_both_legs(self):
        # v3 trades one fixed lot: no chooser resizes an entry.
        manager = self.manager()
        manager._index, manager._held_quantity, manager.lot_size = "SENSEX", None, 65
        for lots in (1, 2):
            manager.lots = lots
            self.assertEqual(manager.order_quantity, lots * instruments.get("SENSEX").quantity)
        self.assertFalse(hasattr(manager, "_choose_spread"))
        self.assertFalse(hasattr(manager, "_free_margin"))

    async def test_stale_tick_is_never_used_to_trigger_stop(self):
        manager = self.manager()
        price = await manager._get_price_with_fallback("stop check")
        self.assertIsNone(price)
        manager.db.execute_async.assert_not_awaited()

    async def test_chart_stop_uses_fresh_tick_only(self):
        manager = self.manager()
        manager.live_price_getter.return_value = 23_000.0
        manager._index = None
        manager.index_live_price_getters = {}
        self.assertIsNone(await manager._held_price_getter("stop check"))

    async def test_carry_exit_can_flatten_when_held_index_feed_is_down(self):
        manager = self.manager()
        manager._index = "SENSEX"
        manager.entry_price = 80_000.0
        manager.index_live_price_getters = {"SENSEX": Mock(return_value=None)}
        self.assertEqual(await manager._held_price_getter("carry exit"), 80_000.0)

    def test_fresh_price_getter_failures_and_invalid_values_fail_closed(self):
        manager = self.manager()
        for value in (None, 0, -1, float("nan"), float("inf"), "bad"):
            manager.fresh_price_getter = Mock(return_value=value)
            self.assertIsNone(manager._read_fresh_price())
        manager.fresh_price_getter = Mock(side_effect=RuntimeError("feed down"))
        self.assertIsNone(manager._read_fresh_price())
        manager.fresh_price_getter = Mock(return_value="24250.5")
        self.assertEqual(manager._read_fresh_price(), 24250.5)


class EntryQuantityValidationTests(unittest.IsolatedAsyncioTestCase):
    def manager(self, *, row, positions, flatten=(True, True)):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.lot_size = 65
        manager.ENTRY_POSITION_VERIFY_TIMEOUT_SEC = 0.0
        manager.ENTRY_POSITION_VERIFY_POLL_SEC = 0.001
        manager._fetch_order_log_row = AsyncMock(return_value=row)
        manager._position_qty = AsyncMock(side_effect=positions)
        manager._flatten_to_zero = AsyncMock(side_effect=flatten)
        return manager

    async def test_accepts_exact_short_entry_fills_and_positions(self):
        manager = self.manager(
            row={"buy_fill_qty": 65, "sell_fill_qty": 65},
            positions=[65, -65],
        )

        await manager._validate_entry_execution(
            row_id=7, buy_sym="HEDGE", sell_sym="SHORT", sell_side="S"
        )

        manager._flatten_to_zero.assert_not_awaited()

    async def test_accepts_exact_long_futures_second_leg(self):
        manager = self.manager(
            row={"buy_fill_qty": 65, "sell_fill_qty": 65},
            positions=[65, 65],
        )

        await manager._validate_entry_execution(
            row_id=8, buy_sym="HEDGE", sell_sym="FUT", sell_side="B"
        )

        manager._flatten_to_zero.assert_not_awaited()

    async def test_missing_websocket_fill_is_recovered_from_exact_orders(self):
        incomplete = {"buy_orderno": "BUY1", "sell_orderno": "SELL1",
                      "buy_fill_qty": None, "sell_fill_qty": None}
        complete = {**incomplete, "buy_fill_qty": 65, "sell_fill_qty": 65}
        manager = self.manager(row=incomplete, positions=[65, -65])
        manager._fetch_order_log_row = AsyncMock(side_effect=[incomplete, complete])
        manager._reconcile_exact_order = AsyncMock(return_value=True)
        await manager._validate_entry_execution(
            row_id=18, buy_sym="HEDGE", sell_sym="SHORT", sell_side="S"
        )
        self.assertEqual(manager._reconcile_exact_order.await_count, 2)
        manager._flatten_to_zero.assert_not_awaited()

    async def test_partial_fill_rolls_back_both_legs_even_if_positions_look_full(self):
        manager = self.manager(
            row={"buy_fill_qty": 65, "sell_fill_qty": 30},
            positions=[65, -65],
        )

        with self.assertRaisesRegex(RuntimeError, "recorded fills=65/30"):
            await manager._validate_entry_execution(
                row_id=9, buy_sym="HEDGE", sell_sym="SHORT", sell_side="S"
            )

        self.assertEqual(manager._flatten_to_zero.await_count, 2)

    async def test_position_mismatch_rolls_back_both_legs(self):
        manager = self.manager(
            row={"buy_fill_qty": 65, "sell_fill_qty": 65},
            positions=[65, -30],
        )

        with self.assertRaisesRegex(RuntimeError, "broker net=65/-30"):
            await manager._validate_entry_execution(
                row_id=10, buy_sym="HEDGE", sell_sym="SHORT", sell_side="S"
            )

        self.assertEqual(manager._flatten_to_zero.await_count, 2)

    async def test_unknown_fill_and_positions_fail_closed(self):
        manager = self.manager(
            row={"buy_fill_qty": None, "sell_fill_qty": None},
            positions=[None, None],
            flatten=(False, False),
        )

        with self.assertRaisesRegex(RuntimeError, "recorded fills=None/None"):
            await manager._validate_entry_execution(
                row_id=11, buy_sym="HEDGE", sell_sym="SHORT", sell_side="S"
            )

        # The short could not be confirmed flat, so its long is kept as the hedge.
        manager._flatten_to_zero.assert_awaited_once_with("SHORT", reason="entry_quantity_mismatch")

    async def test_raising_short_flatten_keeps_the_long_as_its_hedge(self):
        manager = self.manager(
            row={"buy_fill_qty": 65, "sell_fill_qty": 12},
            positions=[65, -12],
        )
        manager._flatten_to_zero = AsyncMock(side_effect=[RuntimeError("broker down"), True])

        with self.assertRaisesRegex(RuntimeError, "recorded fills=65/12"):
            await manager._validate_entry_execution(
                row_id=12, buy_sym="HEDGE", sell_sym="SHORT", sell_side="S"
            )

        manager._flatten_to_zero.assert_awaited_once_with("SHORT", reason="entry_quantity_mismatch")

    async def test_rollback_closes_the_short_before_the_long(self):
        manager = self.manager(row={}, positions=[])

        result = await manager._rollback_entry_symbols("HEDGE", "SHORT", reason="test")

        self.assertEqual(result, (True, True))
        self.assertEqual([c.args[0] for c in manager._flatten_to_zero.await_args_list],
                         ["SHORT", "HEDGE"])

    async def test_identical_symbols_are_flattened_only_once(self):
        manager = self.manager(row={}, positions=[])

        result = await manager._rollback_entry_symbols("SAME", "SAME", reason="test")

        self.assertEqual(result, (True, True))
        manager._flatten_to_zero.assert_awaited_once_with("SAME", reason="test")

    async def test_overfill_fails_closed(self):
        manager = self.manager(
            row={"buy_fill_qty": 130, "sell_fill_qty": 65},
            positions=[130, -65],
        )

        with self.assertRaisesRegex(RuntimeError, "recorded fills=130/65"):
            await manager._validate_entry_execution(
                row_id=13, buy_sym="HEDGE", sell_sym="SHORT", sell_side="S"
            )

        self.assertEqual(manager._flatten_to_zero.await_count, 2)

    async def test_wrong_position_sign_fails_closed(self):
        manager = self.manager(
            row={"buy_fill_qty": 65, "sell_fill_qty": 65},
            positions=[65, 65],
        )

        with self.assertRaisesRegex(RuntimeError, "broker net=65/65"):
            await manager._validate_entry_execution(
                row_id=14, buy_sym="HEDGE", sell_sym="SHORT", sell_side="S"
            )

        self.assertEqual(manager._flatten_to_zero.await_count, 2)

    async def test_delayed_position_book_convergence_is_accepted(self):
        manager = self.manager(
            row={"buy_fill_qty": 65, "sell_fill_qty": 65},
            positions=[None, None, 65, -30, 65, -65],
        )
        manager.ENTRY_POSITION_VERIFY_TIMEOUT_SEC = 0.2

        await manager._validate_entry_execution(
            row_id=15, buy_sym="HEDGE", sell_sym="SHORT", sell_side="S"
        )

        self.assertEqual(manager._position_qty.await_count, 6)
        manager._flatten_to_zero.assert_not_awaited()

    async def test_missing_order_log_row_fails_closed(self):
        manager = self.manager(row=None, positions=[65, -65])

        with self.assertRaisesRegex(RuntimeError, "recorded fills=None/None"):
            await manager._validate_entry_execution(
                row_id=16, buy_sym="HEDGE", sell_sym="SHORT", sell_side="S"
            )

        self.assertEqual(manager._flatten_to_zero.await_count, 2)

    async def test_database_read_exception_still_flattens_both_symbols(self):
        manager = self.manager(row=None, positions=[65, -65])
        manager._fetch_order_log_row = AsyncMock(side_effect=RuntimeError("database unavailable"))

        with self.assertRaisesRegex(RuntimeError, "recorded fills=None/None"):
            await manager._validate_entry_execution(
                row_id=17, buy_sym="HEDGE", sell_sym="SHORT", sell_side="S"
            )

        self.assertEqual(manager._flatten_to_zero.await_count, 2)

    async def test_fill_mismatch_does_not_wait_for_position_grace_period(self):
        manager = self.manager(
            row={"buy_fill_qty": 64, "sell_fill_qty": 65},
            positions=[64, -65],
        )
        manager.ENTRY_POSITION_VERIFY_TIMEOUT_SEC = 30.0

        with self.assertRaises(RuntimeError):
            await manager._validate_entry_execution(
                row_id=18, buy_sym="HEDGE", sell_sym="SHORT", sell_side="S"
            )

        self.assertEqual(manager._position_qty.await_count, 2)


class PositionQuantityParsingTests(unittest.IsolatedAsyncioTestCase):
    def manager(self, payload):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.api = SimpleNamespace(get_positions=lambda: payload)
        manager._position_lookup_blocked_until = 0.0
        manager.POSITION_QTY_TIMEOUT_SEC = 1.0
        manager.POSITION_QTY_BACKOFF_SEC = 60.0
        return manager

    async def test_error_or_malformed_book_never_means_flat(self):
        for payload in (
            {"stat": "Not_Ok", "emsg": "session expired"},
            {"stat": "Ok"},
            [{"stat": "Not_Ok", "emsg": "error"}],
            [{"tsym": "LEG", "qty": "65"}],
            ["bad row"],
        ):
            self.assertIsNone(await self.manager(payload)._position_qty("LEG"))

    async def test_multiple_product_rows_only_count_normal_positions(self):
        manager = self.manager([
            {"tsym": "LEG", "prd": "M", "netqty": "30"},
            {"tsym": "LEG", "prd": "M", "netqty": "-5"},
            {"tsym": "LEG", "prd": "I", "netqty": "10"},
        ])
        self.assertEqual(await manager._position_qty("LEG"), 25)

    async def test_explicit_zero_netqty_is_not_replaced_by_gross_qty(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.api = SimpleNamespace(
            get_positions=lambda: [{"tsym": "LEG", "netqty": 0, "qty": 65}]
        )
        manager._position_lookup_blocked_until = 0.0
        manager.POSITION_QTY_TIMEOUT_SEC = 1.0
        manager.POSITION_QTY_BACKOFF_SEC = 60.0

        self.assertEqual(await manager._position_qty("LEG"), 0)

    async def test_successful_empty_position_book_means_flat(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.api = SimpleNamespace(get_positions=lambda: [])
        manager._position_lookup_blocked_until = 0.0
        manager.POSITION_QTY_TIMEOUT_SEC = 1.0
        manager.POSITION_QTY_BACKOFF_SEC = 60.0
        self.assertEqual(await manager._position_qty("ABSENT"), 0)

    async def test_invalid_position_payload_remains_unknown(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.api = SimpleNamespace(get_positions=lambda: None)
        manager._position_lookup_blocked_until = 0.0
        manager.POSITION_QTY_TIMEOUT_SEC = 1.0
        manager.POSITION_QTY_BACKOFF_SEC = 60.0
        self.assertIsNone(await manager._position_qty("ABSENT"))


class EntryRollbackOrderTests(unittest.IsolatedAsyncioTestCase):
    def manager(self, quantities):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.EMERGENCY_FLATTEN_RETRIES = 2
        manager.EMERGENCY_FLATTEN_SLEEP_SEC = 0.0
        manager._position_qty = AsyncMock(side_effect=quantities)
        manager._active_orderlog_row_id = 1
        manager._submit_order_strict = AsyncMock(return_value="ORDER-1")
        manager._contract_details = Mock(return_value={"lot": 1, "max_qty": 100})
        manager._broker_terminal_record = AsyncMock(return_value={
            "status": "COMPLETE", "fillshares": str(abs(quantities[0] or 0)),
        })
        return manager

    async def test_positive_actual_quantity_is_sold_exactly(self):
        manager = self.manager([30, 30, 0, 0])

        self.assertTrue(await manager._flatten_to_zero("LEG", reason="test"))

        manager._submit_order_strict.assert_awaited_once_with(
            row_id=1, phase="FLATTEN", leg="position", attempt=1,
            side="S", symbol="LEG", qty=30, reduce_only=True,
        )

    async def test_negative_actual_quantity_is_bought_exactly(self):
        manager = self.manager([-42, -42, 0, 0])

        self.assertTrue(await manager._flatten_to_zero("LEG", reason="test"))

        manager._submit_order_strict.assert_awaited_once_with(
            row_id=1, phase="FLATTEN", leg="position", attempt=1,
            side="B", symbol="LEG", qty=42, reduce_only=True,
        )

    async def test_unknown_quantity_never_sends_blind_flatten_order(self):
        manager = self.manager([None, None, None])

        self.assertFalse(await manager._flatten_to_zero("LEG", reason="test"))

        manager._submit_order_strict.assert_not_awaited()

    async def test_uncertain_flatten_ack_is_not_retried(self):
        manager = self.manager([30, 30])
        manager._submit_order_strict = AsyncMock(
            side_effect=UncertainOrderError("acknowledgement uncertain")
        )

        self.assertFalse(await manager._flatten_to_zero("LEG", reason="test"))
        manager._submit_order_strict.assert_awaited_once()

    async def test_partial_multi_lot_exit_resizes_from_confirmed_residual(self):
        manager = self.manager([130, 130, 65, 65, 0, 0])
        manager._contract_details.return_value = {"lot": 65, "max_qty": 130}
        manager._broker_terminal_record.side_effect = [
            {"status": "CANCELED", "fillshares": "65"},
            {"status": "COMPLETE", "fillshares": "65"},
        ]
        self.assertTrue(await manager._flatten_to_zero("LEG", reason="test"))
        self.assertEqual([call.kwargs["qty"] for call in manager._submit_order_strict.await_args_list], [130, 65])

    async def test_unreadable_fill_stops_before_another_multi_lot_order(self):
        manager = self.manager([130, 130])
        manager._contract_details.return_value = {"lot": 65, "max_qty": 130}
        manager._broker_terminal_record.return_value = {"status": "CANCELED", "fillshares": "n/a"}
        self.assertFalse(await manager._flatten_to_zero("LEG", reason="test"))
        manager._submit_order_strict.assert_awaited_once()

    async def test_cancel_without_fill_field_is_a_verified_zero_fill_and_retried(self):
        # The broker omits `fillshares` on an IOC that traded nothing. That is
        # a no-fill to retry inside the budget, not an unknown that strands
        # the position - the position book still has to agree each time.
        manager = self.manager([130])
        manager._position_qty = AsyncMock(return_value=130)
        manager._contract_details.return_value = {"lot": 65, "max_qty": 130}
        manager._broker_terminal_record.return_value = {"status": "CANCELED"}
        self.assertFalse(await manager._flatten_to_zero("LEG", reason="test"))
        self.assertEqual(manager._submit_order_strict.await_count, 2 + manager.EMERGENCY_FLATTEN_RETRIES)

    async def test_complete_without_fill_field_counts_the_whole_chunk(self):
        manager = self.manager([65, 65, 0, 0])
        manager._broker_terminal_record.return_value = {"status": "COMPLETE"}
        self.assertTrue(await manager._flatten_to_zero("LEG", reason="test"))
        manager._submit_order_strict.assert_awaited_once()

    async def test_lagging_position_stops_before_another_multi_lot_order(self):
        manager = self.manager([130, 130, 130])
        manager.ENTRY_POSITION_VERIFY_TIMEOUT_SEC = 0.0
        manager._contract_details.return_value = {"lot": 65, "max_qty": 130}
        manager._broker_terminal_record.return_value = {"status": "CANCELED", "fillshares": "65"}
        self.assertFalse(await manager._flatten_to_zero("LEG", reason="test"))
        manager._submit_order_strict.assert_awaited_once()


class EntryTransactionIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_validation_failure_cannot_insert_open_trade_and_rolls_back(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.entering = False
        manager.in_trade = False
        manager.symbol = None
        manager.direction = None
        manager.entry_price = None
        manager.stop_price = None
        manager._watch_symbols = set()
        manager.symbol_fetcher = SimpleNamespace(
            final_symbols={
                "buy": "HEDGE",
                "sell": "SHORT",
                "sell_side": "S",
                "bias": "HH",
                "built_at": time.monotonic(),
            },
            final_symbol="SHORT",
        )
        manager._active_orderlog_row_id = 22
        manager._sell_entry_side = "S"
        manager._buy_orderno = None
        manager._sell_orderno = None
        manager._last_entry_fail_ts = 0.0
        manager._entry_fail_count = 0
        manager._try_acquire_entry_lock = AsyncMock(return_value=True)
        manager._release_entry_lock = AsyncMock()
        manager._has_nonterminal_orders = AsyncMock(return_value=False)
        manager._position_qty = AsyncMock(side_effect=[0, 0])
        manager._place_hedge_seq = AsyncMock(
            return_value=("HEDGE", "SHORT", "BUY-1", "SELL-1")
        )
        manager._validate_entry_execution = AsyncMock(
            side_effect=RuntimeError("quantity mismatch")
        )
        manager._rollback_entry_symbols = AsyncMock(return_value=(True, True))
        manager._set_quarantine = Mock()
        manager._commit_open_trade_atomic = AsyncMock(return_value=99)

        entered = await manager._enter_trade(
            "LONG", 24000.0, {"stop_loss_points": 100.0, "source": "test"}
        )

        self.assertFalse(entered)
        self.assertFalse(manager.in_trade)
        manager._commit_open_trade_atomic.assert_not_awaited()
        manager._rollback_entry_symbols.assert_awaited_once_with(
            "HEDGE", "SHORT", reason="entry_transaction_failed"
        )
        manager._set_quarantine.assert_called_once_with(
            "HEDGE", "SHORT", reason="entry_failed"
        )
        manager._release_entry_lock.assert_awaited_once()
        self.assertFalse(manager.entering)


class _AttemptDb:
    def __init__(self, *, duplicate=False, verify=True):
        self.calls = []
        self.duplicate = duplicate
        self.verify = verify
        self.orderno = None

    async def execute_async(self, sql, params=None, *, fetch=False):
        self.calls.append((sql, params, fetch))
        if "INSERT INTO botdata.order_attempts" in sql:
            return [{"id": 41}]
        if "id<>" in sql:
            return [{"id": 7}] if self.duplicate else []
        if "SET orderno=" in sql:
            if self.duplicate:
                raise RuntimeError("duplicate key value violates unique constraint")
            self.orderno = params[0]
            return [{"orderno": self.orderno if self.verify else "WRONG"}]
        if "SELECT orderno" in sql:
            return [{"orderno": self.orderno if self.verify else "WRONG"}]
        return None


class StrictOrderAcknowledgementTests(unittest.IsolatedAsyncioTestCase):
    def manager(self, response):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.db = _AttemptDb()
        manager.api = SimpleNamespace(
            place_order=Mock(return_value=response),
            searchscrip=Mock(return_value={"values": [{"tsym": "LEG", "token": "77", "ti": "0.05", "ls": "65"}]}),
            get_quotes=Mock(return_value={"stat": "Ok", "tsym": "LEG", "sp1": "100", "bp1": "99", "ti": "0.05", "ls": "65"}),
            get_security_info=Mock(return_value={"stat": "Ok", "tsym": "LEG", "ti": "0.05", "ls": "65", "frzqty": "650"}),
            get_order_book=Mock(return_value=[]),
            get_trade_book=Mock(return_value=[]),
        )
        manager.BROKER_RECONCILE_TIMEOUT_SEC = 0.2
        return manager

    async def submit(self, manager):
        return await manager._submit_order_strict(
            row_id=3, phase="ENTRY", leg="buy", attempt=1,
            side="B", symbol="LEG", qty=65,
        )

    async def test_valid_ack_is_persisted_and_returned(self):
        manager = self.manager({"stat": "Ok", "norenordno": "12345"})
        self.assertEqual(await self.submit(manager), "12345")
        self.assertEqual(manager.db.orderno, "12345")
        manager.api.get_order_book.assert_not_called()
        kwargs = manager.api.place_order.call_args.kwargs
        self.assertEqual((kwargs["price_type"], kwargs["retention"], kwargs["price"]), ("LMT", "IOC", 102.0))
        self.assertNotIn("reduce_only", kwargs)

    async def test_missing_quote_rejects_before_broker_submission(self):
        manager = self.manager({"stat": "Ok", "norenordno": "12345"})
        manager.api.get_quotes.return_value = None
        with self.assertRaisesRegex(RuntimeError, "broker quote unavailable"):
            await self.submit(manager)
        manager.api.place_order.assert_not_called()

    async def test_broker_freeze_rejects_oversize_order_before_submission(self):
        manager = self.manager({"stat": "Ok", "norenordno": "12345"})
        manager.api.get_security_info.return_value["frzqty"] = "65"
        with self.assertRaisesRegex(RuntimeError, "broker freeze"):
            manager._place_broker_order("B", "LEG", qty=130)
        manager.api.place_order.assert_not_called()

    async def test_missing_quote_tick_uses_contract_details(self):
        manager = self.manager({"stat": "Ok", "norenordno": "12345"})
        manager.api.searchscrip.return_value = {"values": [{"tsym": "LEG", "token": "77"}]}
        manager.api.get_quotes.return_value = {"stat": "Ok", "tsym": "LEG", "sp1": "100", "bp1": "99"}
        manager.api.get_security_info = Mock(return_value={"stat": "Ok", "ti": "0.05", "ls": "65", "frzqty": "650"})
        await self.submit(manager)
        manager.api.get_security_info.assert_called_once_with(exchange="NFO", token="77")
        self.assertEqual(manager.api.place_order.call_args.kwargs["price"], 102.0)

    async def test_missing_ack_is_recovered_by_unique_client_reference(self):
        manager = self.manager({"stat": "Ok"})

        async def recover(ref):
            # Echoed back as the order's remarks, so short and plain enough to
            # survive a broker tag limit untouched.
            self.assertRegex(ref, r"^NBEB[0-9a-f]{16}$")
            return "RECOVERED1"

        manager._recover_orderno_by_client_ref = recover
        self.assertEqual(await self.submit(manager), "RECOVERED1")

    async def test_missing_ack_without_recovery_fails_unknown_without_resubmit(self):
        manager = self.manager(None)
        manager._recover_orderno_by_client_ref = AsyncMock(return_value=None)

        with self.assertRaisesRegex(UncertainOrderError, "acknowledgement uncertain"):
            await self.submit(manager)

        manager.api.place_order.assert_called_once()

    async def test_malformed_order_number_fails_closed(self):
        manager = self.manager({"stat": "Ok", "norenordno": "bad order id!"})
        manager._recover_orderno_by_client_ref = AsyncMock(return_value=None)
        with self.assertRaisesRegex(RuntimeError, "acknowledgement uncertain"):
            await self.submit(manager)

    async def test_duplicate_order_number_is_rejected(self):
        manager = self.manager({"stat": "Ok", "norenordno": "12345"})
        manager.db.duplicate = True
        with self.assertRaisesRegex(RuntimeError, "duplicate key"):
            await self.submit(manager)

    async def test_failed_persistence_verification_is_rejected(self):
        manager = self.manager({"stat": "Ok", "norenordno": "12345"})
        manager.db.verify = False
        with self.assertRaisesRegex(UncertainOrderError, "failed to verify persisted"):
            await self.submit(manager)

    async def test_database_failure_after_broker_ack_is_uncertain(self):
        manager = self.manager({"stat": "Ok", "norenordno": "12345"})
        manager.db.verify = False
        with self.assertRaises(UncertainOrderError):
            await self.submit(manager)
        manager.api.place_order.assert_called_once()

    async def test_ambiguous_recovery_returns_none(self):
        manager = self.manager(None)
        manager.api.get_order_book.return_value = [
            {"remarks": "REF", "norenordno": "1"},
            {"remarks": "REF", "norenordno": "2"},
        ]
        self.assertIsNone(await manager._recover_orderno_by_client_ref("REF"))


class RestOrderReconciliationTests(unittest.IsolatedAsyncioTestCase):
    async def test_flatten_attempt_recovers_terminal_state_without_websocket(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager._watch_symbols = set()
        manager.db = SimpleNamespace(execute_async=AsyncMock(side_effect=[
            [{"id": 9, "order_log_id": None, "client_ref": "REF", "phase": "FLATTEN",
              "symbol": "OLD-LEG",
              "leg": "position", "orderno": "ORDER1", "status": "ACKNOWLEDGED"}],
            None,
        ]))
        manager.api = SimpleNamespace(single_order_history=lambda orderno: [
            {"norenordno": orderno, "status": "COMPLETE"}
        ])
        manager._position_qty = AsyncMock(return_value=0)
        manager.BROKER_RECONCILE_TIMEOUT_SEC = 0.2
        self.assertFalse(await manager._reconcile_unresolved_attempts())
        self.assertEqual(manager.db.execute_async.await_args.args[1][0], "COMPLETE")
        self.assertEqual(manager._watch_symbols, {"OLD-LEG"})

    async def test_terminal_flatten_stays_unresolved_until_broker_is_flat(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager._watch_symbols = set()
        attempt = {"id": 9, "order_log_id": None, "client_ref": "REF", "phase": "FLATTEN",
                   "symbol": "OLD-LEG", "leg": "position", "orderno": "ORDER1", "status": "ACKNOWLEDGED"}
        manager.db = SimpleNamespace(execute_async=AsyncMock(return_value=[attempt]))
        manager._broker_terminal_status = AsyncMock(return_value="COMPLETE")
        manager._position_qty = AsyncMock(side_effect=[65, None, 0])

        self.assertFalse(await manager._reconcile_unresolved_attempts())
        self.assertTrue(await manager._reconcile_unresolved_attempts())
        self.assertFalse(await manager._reconcile_unresolved_attempts())
        manager.db.execute_async.assert_awaited_with(
            "UPDATE botdata.order_attempts SET status=%s,updated_at=%s WHERE id=%s;",
            unittest.mock.ANY,
        )
        self.assertEqual(manager.db.execute_async.await_count, 4)

    async def test_lost_websocket_complete_is_rebuilt_from_exact_order_history(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.api = SimpleNamespace(
            single_order_history=lambda orderno: [{
                "norenordno": orderno,
                "status": "COMPLETE",
                "exchordid": "EX-77",
                "fillshares": "65",
                "avgprc": "123.45",
            }]
        )
        manager.db = SimpleNamespace(execute_async=AsyncMock(return_value=None))
        manager.BROKER_RECONCILE_TIMEOUT_SEC = 0.2

        self.assertTrue(await manager._reconcile_exact_order(
            row_id=5, leg="sell", ordno="ORDER77", is_exit=True
        ))

        sql, params = manager.db.execute_async.await_args.args[:2]
        self.assertIn("sell_exit_state", sql)
        self.assertIn("sell_exit_exchordid", sql)
        self.assertEqual(params[0], "COMPLETE")
        self.assertEqual(params[2], "EX-77")
        self.assertEqual(params[3], 65)

    async def test_wrong_order_history_cannot_be_used_as_confirmation(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.api = SimpleNamespace(single_order_history=lambda orderno: [])
        manager.db = SimpleNamespace(execute_async=AsyncMock())
        manager.BROKER_RECONCILE_TIMEOUT_SEC = 0.2

        self.assertFalse(await manager._reconcile_exact_order(
            row_id=5, leg="buy", ordno="ORDER77", is_exit=False
        ))
        manager.db.execute_async.assert_not_awaited()

    async def test_history_for_a_different_order_is_rejected(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.api = SimpleNamespace(single_order_history=lambda orderno: [
            {"norenordno": "OTHER", "status": "COMPLETE", "fillshares": "65"}
        ])
        manager.db = SimpleNamespace(execute_async=AsyncMock())
        manager.BROKER_RECONCILE_TIMEOUT_SEC = 0.2
        self.assertFalse(await manager._reconcile_exact_order(
            row_id=5, leg="buy", ordno="ORDER77", is_exit=False
        ))
        manager.db.execute_async.assert_not_awaited()

    async def test_rebinding_exit_order_clears_prior_confirmation(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.db = SimpleNamespace(execute_async=AsyncMock(return_value=[{"ordno": "NEW"}]))
        await manager._set_leg_exit_orderno(5, "sell", "NEW")
        sql, params = manager.db.execute_async.await_args.args[:2]
        for field in ("sell_exit_state", "time2_exit", "sell_exit_fill_qty", "sell_exit_avg_price"):
            self.assertIn(f"{field} = CASE", sql)
        self.assertEqual(params, ("NEW",) * 6 + (5,))


class ExitSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_unconfirmed_cancel_stops_exit_retries(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.EXIT_MAX_RETRIES = 3
        manager.EXIT_RETRY_BACKOFF_SEC = 0.0
        manager.POLL_INTERVAL_SEC = 0.001
        manager._position_qty = AsyncMock(return_value=-65)
        manager._fetch_order_log_row = AsyncMock(return_value=None)
        manager._submit_order_strict = AsyncMock(return_value="ORDER1")
        manager._set_leg_exit_orderno = AsyncMock()
        manager._wait_leg_status = AsyncMock(side_effect=TimeoutError("update lost"))
        manager._wait_leg_terminal = AsyncMock(return_value=None)
        manager._cancel_order_if_possible = AsyncMock(return_value=False)
        manager._emergency_flatten = AsyncMock()
        manager._contract_details = Mock(return_value={"lot": 65, "max_qty": 650})
        with self.assertRaises(UncertainOrderError):
            await manager._exit_leg_with_retries(
                buy_sym="HEDGE", sell_sym="SHORT", leg="sell", action_side="B",
                symbol="SHORT", after_ts=None, per_try_timeout=0.01, row_id=5,
            )
        manager._submit_order_strict.assert_awaited_once()
        manager._emergency_flatten.assert_not_awaited()

    async def test_unknown_exit_quantity_never_submits_blind_lot_size(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.EXIT_MAX_RETRIES = 1
        manager.EXIT_RETRY_BACKOFF_SEC = 0.0
        manager.POLL_INTERVAL_SEC = 0.001
        manager.EMERGENCY_FLATTEN_RETRIES = 1
        manager.EMERGENCY_FLATTEN_SLEEP_SEC = 0.0
        manager._position_qty = AsyncMock(return_value=None)
        manager._submit_order_strict = AsyncMock()
        manager._emergency_flatten = AsyncMock()

        with self.assertRaisesRegex(RuntimeError, "All exit attempts failed"):
            await manager._exit_leg_with_retries(
                buy_sym="HEDGE", sell_sym="SHORT", leg="sell", action_side="B",
                symbol="SHORT", after_ts=None, per_try_timeout=0.01, row_id=5,
            )

        manager._submit_order_strict.assert_not_awaited()
        manager._emergency_flatten.assert_awaited_once_with("SHORT")


class _TxCursor:
    def __init__(self, *, trade_id=71, order_row=True, trade_row=True):
        self.trade_id = trade_id
        self.order_row = order_row
        self.trade_row = trade_row
        self.fetches = []
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params):
        self.statements.append((sql, params))
        if "INSERT INTO botdata.trade_logs" in sql:
            self.fetches.append({"id": self.trade_id})
        elif "UPDATE botdata.trade_logs" in sql:
            self.fetches.append({"id": self.trade_id} if self.trade_row else None)
        elif "UPDATE botdata.order_logs" in sql:
            self.fetches.append({"id": 9} if self.order_row else None)

    def fetchone(self):
        return self.fetches.pop(0)


class _TxConnection:
    def __init__(self, *, order_row=True, trade_row=True, commit_error=None):
        self.cur = _TxCursor(order_row=order_row, trade_row=trade_row)
        self.commit_error = commit_error
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1
        if self.commit_error:
            raise self.commit_error

    def rollback(self):
        self.rollbacks += 1


class _TxDb:
    def __init__(self, conn, *, recovered=None, recovery_error=None, put_error=None):
        self.conn = conn
        self.recovered = recovered
        self.recovery_error = recovery_error
        self.put_error = put_error
        self.put_calls = []

    async def get_raw_connection_async(self):
        return self.conn

    async def put_raw_connection_async(self, conn, *, close=False):
        self.put_calls.append(close)
        if self.put_error:
            raise self.put_error

    async def execute_async(self, sql, params=None, *, fetch=False):
        if self.recovery_error:
            raise self.recovery_error
        return self.recovered or []


class TradeLogAtomicCommitTests(unittest.IsolatedAsyncioTestCase):
    async def commit(self, manager):
        return await manager._commit_open_trade_atomic(
            9, 24000.0, "LONG", 23900.0, "HEDGE", "SHORT",
            "BUY1", "SELL1", "S",
        )

    async def test_trade_and_order_open_commit_in_one_transaction(self):
        conn = _TxConnection()
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.db = _TxDb(conn)

        self.assertEqual(await self.commit(manager), 71)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(conn.rollbacks, 0)
        self.assertEqual(len(conn.cur.statements), 2)
        self.assertIn("INSERT INTO botdata.trade_logs", conn.cur.statements[0][0])
        self.assertIn("lifecycle_state='OPEN'", conn.cur.statements[1][0])


class ResumedTradeVerificationTests(unittest.IsolatedAsyncioTestCase):
    def manager(self, quantities):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager._resume_requires_verification = True
        manager._resumed_trade_state = "OPEN"
        manager.trade_row_id = 50
        manager.in_trade = True
        manager.lot_size = 65
        manager._flat_while_in_trade_count = 0
        manager._buy_orderno = "B1"
        manager._sell_orderno = "S1"
        manager.direction = "LONG"
        manager.entry_price = 24000.0
        manager.stop_price = 23900.0
        manager.symbol = "SHORT"
        manager._sell_entry_side = "S"
        manager._quarantine_active = False
        manager._quarantine_symbols = None
        manager._watch_symbols = {"HEDGE", "SHORT"}
        manager._position_qty = AsyncMock(side_effect=quantities)
        manager._set_quarantine = Mock()
        manager.db = SimpleNamespace(execute_async=AsyncMock(side_effect=self.db_result))
        return manager

    @staticmethod
    def db_result(sql, params=None, *, fetch=False):
        if "SELECT buy_symbol" in sql:
            return [{"buy_symbol": "HEDGE", "sell_symbol": "SHORT", "sell_entry_side": "S"}]
        return None

    async def test_exact_broker_position_allows_resume(self):
        manager = self.manager([65, -65])
        self.assertTrue(await manager._verify_resumed_trade())
        self.assertFalse(manager._resume_requires_verification)

    async def test_unknown_broker_position_blocks_resume(self):
        manager = self.manager([None, -65])
        self.assertFalse(await manager._verify_resumed_trade())
        self.assertTrue(manager._resume_requires_verification)

    async def test_mismatched_broker_position_quarantines_and_blocks(self):
        manager = self.manager([65, -20])
        self.assertFalse(await manager._verify_resumed_trade())
        manager._set_quarantine.assert_called_once()
        self.assertTrue(manager.in_trade)

    async def test_broker_flat_requires_three_confirmations_then_closes_phantom(self):
        manager = self.manager([0, 0, 0, 0, 0, 0])
        self.assertFalse(await manager._verify_resumed_trade())
        self.assertFalse(await manager._verify_resumed_trade())
        self.assertFalse(await manager._verify_resumed_trade())
        self.assertFalse(manager.in_trade)
        self.assertIsNone(manager.trade_row_id)
        updates = [call.args[0] for call in manager.db.execute_async.await_args_list]
        self.assertTrue(any("RECOVERY_FLAT" in sql for sql in updates))

    async def test_quarantined_persisted_trade_stays_blocked_while_mismatched(self):
        manager = self.manager([65, -20])
        manager._resumed_trade_state = "QUARANTINED"
        self.assertFalse(await manager._verify_resumed_trade())
        self.assertTrue(manager._resume_requires_verification)
        manager._set_quarantine.assert_called_once()
        updates = [call.args[0] for call in manager.db.execute_async.await_args_list]
        self.assertFalse(any("UPDATE" in sql for sql in updates), "already QUARANTINED")

    async def test_quarantined_persisted_trade_resumes_on_exact_broker_match(self):
        # A quarantine that outlived its mismatch must not strand the position
        # - with no exit - across every later restart.
        manager = self.manager([65, -65])
        manager._resumed_trade_state = "QUARANTINED"
        self.assertTrue(await manager._verify_resumed_trade())
        self.assertFalse(manager._resume_requires_verification)
        updates = [call.args[0] for call in manager.db.execute_async.await_args_list]
        self.assertTrue(any("trade_state='OPEN'" in sql and "trade_state='QUARANTINED'" in sql
                            for sql in updates))

    async def test_repeated_mismatch_is_recorded_once(self):
        manager = self.manager([65, -20, 65, -20, 65, -20])
        for _ in range(3):
            self.assertFalse(await manager._verify_resumed_trade())
        writes = [call.args[0] for call in manager.db.execute_async.await_args_list
                  if "trade_state='QUARANTINED'" in call.args[0]]
        self.assertEqual(len(writes), 1)

    async def test_phantom_recovery_forgets_the_held_size(self):
        manager = self.manager([0, 0, 0, 0, 0, 0])
        manager._held_quantity = 20
        manager._index = "SENSEX"
        manager._book = MOMENTUM_BOOK
        for _ in range(3):
            await manager._verify_resumed_trade()
        self.assertFalse(manager.in_trade)
        # A stale held size would size the next entry, on any index, as this one.
        self.assertIsNone(manager._held_quantity)
        self.assertIsNone(manager._index)
        self.assertIsNone(manager._book)


class TradeLogAtomicFailureTests(unittest.IsolatedAsyncioTestCase):
    async def commit(self, manager):
        return await manager._commit_open_trade_atomic(
            9, 24000.0, "LONG", 23900.0, "HEDGE", "SHORT",
            "BUY1", "SELL1", "S",
        )

    async def test_order_row_transition_failure_rolls_back_trade_insert(self):
        conn = _TxConnection(order_row=False)
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.db = _TxDb(conn)

        with self.assertRaisesRegex(RuntimeError, "OPEN transaction rolled back"):
            await self.commit(manager)
        self.assertEqual(conn.commits, 0)
        self.assertEqual(conn.rollbacks, 1)

    async def test_lost_commit_ack_recovers_by_execution_id(self):
        conn = _TxConnection(commit_error=RuntimeError("connection lost after commit"))
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.db = _TxDb(conn, recovered=[{"id": 88}])

        self.assertEqual(await self.commit(manager), 88)
        self.assertEqual(conn.commits, 1)

    async def test_commit_and_recovery_both_unknown_raise_uncertain_not_failed(self):
        conn = _TxConnection(commit_error=RuntimeError("commit response lost"))
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.db = _TxDb(conn, recovery_error=RuntimeError("database unavailable"))

        with self.assertRaises(UncertainTradeCommitError):
            await self.commit(manager)

    async def test_pool_return_failure_after_confirmed_commit_does_not_fail_trade(self):
        conn = _TxConnection()
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.db = _TxDb(conn, put_error=RuntimeError("pool broken"))

        self.assertEqual(await self.commit(manager), 71)


class TradeLogAtomicCloseTests(unittest.IsolatedAsyncioTestCase):
    async def close(self, manager):
        await manager._commit_closed_trade_atomic(order_log_id=9, win=1, exit_price=24100.0)

    def manager(self, conn, **db_kwargs):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.trade_row_id = 71
        manager.db = _TxDb(conn, **db_kwargs)
        return manager

    async def test_trade_and_order_close_commit_atomically(self):
        conn = _TxConnection()
        await self.close(self.manager(conn))
        self.assertEqual(conn.commits, 1)
        self.assertEqual(len(conn.cur.statements), 2)
        self.assertIn("UPDATE botdata.trade_logs", conn.cur.statements[0][0])
        self.assertIn("UPDATE botdata.order_logs", conn.cur.statements[1][0])

    async def test_missing_order_row_rolls_back_trade_close(self):
        conn = _TxConnection(order_row=False)
        with self.assertRaisesRegex(RuntimeError, "CLOSE transaction rolled back"):
            await self.close(self.manager(conn))
        self.assertEqual(conn.rollbacks, 1)

    async def test_lost_close_commit_ack_recovers_when_both_ledgers_closed(self):
        conn = _TxConnection(commit_error=RuntimeError("commit ack lost"))
        await self.close(self.manager(conn, recovered=[{"id": 71}]))

    async def test_unknown_close_commit_raises_uncertain(self):
        conn = _TxConnection(commit_error=RuntimeError("commit ack lost"))
        manager = self.manager(conn, recovery_error=RuntimeError("database unavailable"))
        with self.assertRaises(UncertainTradeCommitError):
            await self.close(manager)


# --------------------------------------------------------------------------- #
#  Live-path hardening: each class pins one defect that a fake broker hid.
# --------------------------------------------------------------------------- #
NO_DATA = {"stat": "Not_Ok", "request_time": "09:15:00 05-10-2026",
           "emsg": 'Error Occurred : 5 "no data"'}


def shoonya_api(payload):
    """A real `ShoonyaApiPy` whose every REST answer is `payload`."""
    from shoonya_api import ShoonyaApiPy
    api = ShoonyaApiPy.__new__(ShoonyaApiPy)
    api.userid = "U1"
    api._token_for_request = lambda: "token"
    api._raw_post = lambda route, values, token, timeout: (200, payload)
    return api


class EmptyBrokerBookTests(unittest.IsolatedAsyncioTestCase):
    """Shoonya answers an empty book with an error, never with `[]`."""

    def test_no_data_is_an_empty_book(self):
        for name in ("get_positions", "get_order_book", "get_trade_book"):
            with self.subTest(route=name):
                self.assertEqual(getattr(shoonya_api(NO_DATA), name)(), [])
        self.assertEqual(shoonya_api(NO_DATA).single_order_history("1"), [])

    def test_any_other_error_stays_unknown(self):
        for payload in ({"stat": "Not_Ok", "emsg": "Server busy"}, {"stat": "Ok"}):
            self.assertIsNone(shoonya_api(payload).get_positions())

    def test_candle_series_keeps_its_contract(self):
        self.assertIsNone(shoonya_api(NO_DATA).get_time_price_series(
            "NSE", "26000", warn_on_error=False))

    async def test_a_flat_account_reads_flat_not_unknown(self):
        # Before: every live entry on an account with no positions today was
        # blocked as "broker quantity unknown" and quarantined for good.
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.api = shoonya_api(NO_DATA)
        manager._position_lookup_blocked_until = 0.0
        self.assertEqual(await manager._position_qty("NIFTY13OCT26P25000"), 0)


class SymbolExchangeRoutingTests(unittest.TestCase):
    def manager(self, security_info):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager._index = None          # flat: no held index to route by
        manager._contract_cache = {}
        manager.api = SimpleNamespace(
            searchscrip=Mock(side_effect=lambda exchange, searchtext: {
                "stat": "Ok", "values": [{"tsym": searchtext, "token": "9"}]}),
            get_security_info=Mock(return_value=security_info),
        )
        return manager

    def test_a_flat_manager_routes_each_symbol_to_its_own_exchange(self):
        manager = self.manager({"stat": "Ok", "ls": "20", "ti": "0.05", "frzqty": "1000"})
        for symbol, exchange in (("SENSEX2610881000PE", "BFO"),
                                 ("NIFTY13OCT26P25000", "NFO"),
                                 ("NIFTYNXT5027OCT26P60000", "NFO")):
            with self.subTest(symbol=symbol):
                manager._contract_details(symbol)
                manager.api.searchscrip.assert_called_with(exchange=exchange, searchtext=symbol)

    def test_longest_root_wins(self):
        self.assertEqual(instruments.of_symbol("NIFTYNXT5027OCT26P60000").name, "NIFTYNXT50")
        self.assertEqual(instruments.of_symbol("NIFTY13OCT26P25000").name, "NIFTY")
        self.assertEqual(instruments.of_symbol("BANKNIFTY27OCT26P50000").name, "BANKNIFTY")
        self.assertIsNone(instruments.of_symbol("LEG"))

    def test_missing_freeze_quantity_slices_at_one_lot(self):
        for frzqty in (None, "", "0", "bad"):
            with self.subTest(frzqty=frzqty):
                info = {"stat": "Ok", "ls": "65", "ti": "0.05"}
                if frzqty is not None:
                    info["frzqty"] = frzqty
                details = self.manager(info)._contract_details("NIFTY13OCT26P25000")
                self.assertEqual((details["lot"], details["max_qty"]), (65, 65))


class LostOrderUpdateTests(unittest.IsolatedAsyncioTestCase):
    """A WebSocket update lost to a reconnect or a race costs seconds, not the timeout."""

    def manager(self, row, history):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.REST_RECONCILE_EVERY_SEC = 0.05
        manager._fetch_order_log_row = AsyncMock(side_effect=lambda row_id: dict(row))
        manager._reconcile_exact_order = AsyncMock(side_effect=history)
        return manager

    async def test_completion_is_read_from_order_history_well_before_the_timeout(self):
        row = {"state1": None, "time1": None, "buy_orderno": "ORD1"}
        placed = ist.now()

        async def history(**_kwargs):
            row.update(state1="COMPLETE", time1=ist.now())
            return True

        manager = self.manager(row, history)
        started = time.monotonic()
        await manager._wait_leg_status(row_id=1, leg="buy", ordno="ORD1", timeout_sec=30.0,
                                       poll_sec=0.01, after_ts=placed)
        self.assertLess(time.monotonic() - started, 2.0)

    async def test_a_stored_order_update_wakes_the_waiter_before_its_next_poll(self):
        import threading
        import trade_manager

        row = {"state1": None, "time1": None, "buy_orderno": "ORD1"}
        placed = ist.now()

        async def history(**_kwargs):
            return False

        def writer():                                   # the order-update writer's thread
            row.update(state1="COMPLETE", time1=ist.now())
            trade_manager.ORDER_UPDATES.notify()

        manager = self.manager(row, history)
        manager.REST_RECONCILE_EVERY_SEC = 60.0
        threading.Timer(0.1, writer).start()
        started = time.monotonic()
        await manager._wait_leg_status(row_id=1, leg="buy", ordno="ORD1", timeout_sec=30.0,
                                       poll_sec=10.0, after_ts=placed)
        self.assertLess(time.monotonic() - started, 2.0)

    async def test_a_dead_order_is_read_from_order_history_well_before_the_timeout(self):
        row = {"sell_exit_state": None, "time2_exit": None, "sell_exit_orderno": "ORD2"}

        async def history(**_kwargs):
            row.update(sell_exit_state="CANCELED")
            return True

        manager = self.manager(row, history)
        started = time.monotonic()
        state = await manager._wait_leg_terminal(row_id=1, leg="sell", ordno="ORD2",
                                                 timeout_sec=30.0, poll_sec=0.01, is_exit=True)
        self.assertEqual(state, "CANCELED")
        self.assertLess(time.monotonic() - started, 2.0)

    async def test_a_failing_history_read_only_waits_for_the_next(self):
        row = {"state1": None, "buy_exchordid": None, "buy_orderno": "ORD3"}
        calls = []

        async def history(**_kwargs):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("broker busy")
            row.update(buy_exchordid="X1")
            return True

        manager = self.manager(row, history)
        await manager._wait_leg_exchordid(row_id=1, leg="buy", ordno="ORD3", timeout_sec=5.0,
                                          poll_sec=0.01, not_ok=manager.NOT_OK_STATES)
        self.assertGreaterEqual(len(calls), 2)

    async def test_history_listed_oldest_first_still_records_the_outcome(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.BROKER_RECONCILE_TIMEOUT_SEC = 0.2
        manager.api = SimpleNamespace(single_order_history=lambda orderno: [
            {"norenordno": orderno, "status": "PENDING"},
            {"norenordno": orderno, "status": "OPEN"},
            {"norenordno": orderno, "status": "CANCELED", "fillshares": "0"},
        ])
        manager.db = SimpleNamespace(execute_async=AsyncMock(return_value=None))
        self.assertTrue(await manager._reconcile_exact_order(
            row_id=5, leg="buy", ordno="ORD4", is_exit=False))
        self.assertEqual(manager.db.execute_async.await_args.args[1][0], "CANCELED")


class StaleOrderIntentTests(unittest.IsolatedAsyncioTestCase):
    """An intent that never reached the broker must not block trading forever."""

    NOW = datetime(2026, 10, 5, 12, 0, tzinfo=ist.IST)

    def manager(self, age_sec, *, books=([], []), orderno=None, status="UNKNOWN"):
        attempt = {"id": 5, "created_at": self.NOW - timedelta(seconds=age_sec),
                   "order_log_id": 3, "client_ref": "NBEB0123456789abcdef", "phase": "ENTRY",
                   "leg": "buy", "symbol": "LEG", "orderno": orderno, "status": status}
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager._watch_symbols = set()
        manager.BROKER_RECONCILE_TIMEOUT_SEC = 0.2
        manager.db = SimpleNamespace(execute_async=AsyncMock(
            side_effect=lambda sql, params=None, fetch=False: [attempt] if "SELECT" in sql else None))
        manager.api = SimpleNamespace(get_order_book=Mock(return_value=books[0]),
                                      get_trade_book=Mock(return_value=books[1]))
        return manager

    async def reconcile(self, manager):
        with patch.object(tm, "_now_ist", return_value=self.NOW), \
                patch.object(tm, "now_ist", return_value=self.NOW.replace(tzinfo=None)):
            return await manager._reconcile_unresolved_attempts()

    def updates(self, manager):
        return [call.args for call in manager.db.execute_async.await_args_list
                if call.args[0].startswith("UPDATE")]

    async def test_absent_from_both_books_after_the_grace_is_closed_not_found(self):
        manager = self.manager(age_sec=120)
        self.assertFalse(await self.reconcile(manager))
        (sql, params), = self.updates(manager)
        self.assertEqual(params[0], "NOT_FOUND")
        self.assertEqual(params[2], 5)

    async def test_a_fresh_intent_is_still_waited_for(self):
        manager = self.manager(age_sec=1)
        self.assertTrue(await self.reconcile(manager))
        self.assertEqual(self.updates(manager), [])

    async def test_an_unreadable_book_is_never_evidence_of_absence(self):
        manager = self.manager(age_sec=120, books=(None, []))
        self.assertTrue(await self.reconcile(manager))
        self.assertEqual(self.updates(manager), [])

    async def test_a_late_found_order_is_bound_not_closed(self):
        manager = self.manager(age_sec=120, books=(
            [{"remarks": "NBEB0123456789abcdef", "norenordno": "ORD9"}], []))
        manager._set_leg_orderno = AsyncMock()
        manager._reconcile_exact_order = AsyncMock(return_value=False)
        manager._fetch_order_log_row = AsyncMock(return_value={"state1": None})
        self.assertTrue(await self.reconcile(manager))
        sql, params = self.updates(manager)[0]
        self.assertIn("status='ACKNOWLEDGED'", sql)
        self.assertEqual(params[0], "ORD9")

    async def test_an_earlier_session_attempt_expires_even_with_an_order_number(self):
        # Yesterday's order history is gone; waiting for it blocked the next
        # morning's carry exit forever.
        manager = self.manager(age_sec=20 * 3600, orderno="ORD7", status="ACKNOWLEDGED")
        manager._reconcile_exact_order = AsyncMock()
        self.assertFalse(await self.reconcile(manager))
        (sql, params), = self.updates(manager)
        self.assertEqual(params[0], "EXPIRED")
        manager._reconcile_exact_order.assert_not_awaited()
        self.assertEqual(manager._watch_symbols, {"LEG"})


class UncertainOpenCommitTests(unittest.IsolatedAsyncioTestCase):
    """Legs whose OPEN commit was lost are neither flattened nor forgotten."""

    def manager(self, **db):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager._uncertain_execution_id = "TR-1"
        manager.db = SimpleNamespace(execute_async=AsyncMock(**db))
        manager._load_open_trade_state = Mock()
        return manager

    async def test_a_landed_commit_is_resumed(self):
        manager = self.manager(return_value=[{"id": 9}])
        self.assertTrue(await manager._resolve_uncertain_open_commit())
        manager._load_open_trade_state.assert_called_once()
        self.assertIsNone(manager._uncertain_execution_id)

    async def test_a_rolled_back_commit_releases_reconciliation(self):
        manager = self.manager(return_value=[])
        self.assertTrue(await manager._resolve_uncertain_open_commit())
        manager._load_open_trade_state.assert_not_called()

    async def test_an_unreadable_ledger_holds_every_action(self):
        manager = self.manager(side_effect=RuntimeError("database restarting"))
        self.assertFalse(await manager._resolve_uncertain_open_commit())
        self.assertEqual(manager._uncertain_execution_id, "TR-1")
        manager.decider = SimpleNamespace(step=AsyncMock())
        manager._reconcile_positions = AsyncMock()
        await manager._cycle_once()
        manager.decider.step.assert_not_awaited()
        manager._reconcile_positions.assert_not_awaited()

    async def test_the_error_carries_the_execution_id(self):
        conn = _TxConnection(commit_error=RuntimeError("commit response lost"))
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.db = _TxDb(conn, recovery_error=RuntimeError("database unavailable"))
        with self.assertRaises(UncertainTradeCommitError) as caught:
            await manager._commit_open_trade_atomic(9, 1.0, "LONG", None, "B", "S", "1", "2", "S")
        self.assertTrue(str(caught.exception.execution_id).startswith("TR-"))


class RestartExitTimingTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_restart_before_the_bell_sends_nothing_until_it(self):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager._resume_requires_verification = True
        manager._resumed_trade_state = "EXIT_PENDING"
        manager._uncertain_execution_id = None
        manager.entry_price, manager.direction = 100.0, "LONG"
        manager._reconcile_unresolved_attempts = AsyncMock(return_value=False)
        manager._held_price_getter = AsyncMock(return_value=110.0)
        manager._exit_trade = AsyncMock()
        day = datetime(2026, 10, 5)
        with patch.object(tm, "now_ist", return_value=day.replace(hour=9, minute=5)):
            await manager._cycle_once()
        manager._exit_trade.assert_not_awaited()
        with patch.object(tm, "now_ist", return_value=day.replace(hour=9, minute=15, second=1)):
            await manager._cycle_once()
        manager._exit_trade.assert_awaited_once_with("restart_exit_recovery", 1, 110.0)


class ReconcileBookTests(unittest.IsolatedAsyncioTestCase):
    def manager(self, positions):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager._last_reconcile_ts = 0.0
        manager._position_lookup_blocked_until = 0.0
        manager.entering = manager.exiting = manager.in_trade = False
        manager._quarantine_active = True
        manager._quarantine_symbols = ("LEG0", "LEG1")
        manager._watch_symbols = {f"LEG{i}" for i in range(12)}
        manager.symbol_fetcher = None
        manager._reconcile_unresolved_attempts = AsyncMock(return_value=False)
        manager._flatten_to_zero = AsyncMock(return_value=True)
        manager.api = SimpleNamespace(get_positions=Mock(return_value=positions))
        return manager

    async def test_one_book_read_serves_every_watched_symbol(self):
        manager = self.manager([])
        await manager._reconcile_positions()
        manager.api.get_positions.assert_called_once()
        self.assertFalse(manager._quarantine_active)

    async def test_stray_cleanup_waits_for_the_bell(self):
        manager = self.manager([{"tsym": "LEG3", "netqty": "-65", "prd": "M"}])
        with patch.object(tm, "now_ist", return_value=datetime(2026, 10, 5, 9, 5)):
            await manager._reconcile_positions()
        manager._flatten_to_zero.assert_not_awaited()
        manager._last_reconcile_ts = 0.0
        with patch.object(tm, "now_ist", return_value=datetime(2026, 10, 5, 9, 20)):
            await manager._reconcile_positions()
        manager._flatten_to_zero.assert_awaited_once_with("LEG3", reason="reconcile")


class ExistingExitOrderTests(unittest.IsolatedAsyncioTestCase):
    """A retried exit reads a settled book before sizing any residual."""

    def manager(self, state):
        manager = TradeManagerLive.__new__(TradeManagerLive)
        manager.EXIT_MAX_RETRIES = 1
        manager.EXIT_RETRY_BACKOFF_SEC = 0.0
        manager.POLL_INTERVAL_SEC = 0.001
        manager.ENTRY_POSITION_VERIFY_TIMEOUT_SEC = 1.0
        manager.ENTRY_POSITION_VERIFY_POLL_SEC = 0.001
        manager._position_qty = AsyncMock(return_value=-65)   # read before the fill landed
        manager._position_book = AsyncMock(side_effect=[{"SHORT": -65}, {"SHORT": -65}, {}])
        manager._fetch_order_log_row = AsyncMock(return_value={
            "sell_exit_orderno": "X1", "sell_exit_state": state, "sell_exit_exchordid": "E1"})
        manager._wait_leg_status = AsyncMock()
        manager._wait_leg_exchordid = AsyncMock()
        manager._submit_order_strict = AsyncMock()
        manager._emergency_flatten = AsyncMock()
        return manager

    async def test_completed_or_completing_order_is_not_sent_again(self):
        for state in ("COMPLETE", "OPEN"):
            with self.subTest(existing=state):
                manager = self.manager(state)
                self.assertTrue(await manager._exit_leg_with_retries(
                    buy_sym="HEDGE", sell_sym="SHORT", leg="sell", action_side="B",
                    symbol="SHORT", after_ts=None, per_try_timeout=0.01, row_id=5,
                ))
                manager._submit_order_strict.assert_not_awaited()
                manager._emergency_flatten.assert_not_awaited()


class LaggingBroker(FakeBroker):
    """A position book that trails each order by `lag` reads, as a real one can."""

    lag = 0
    _stale = None
    _lag_left = 0

    def place_order(self, **kwargs):
        self._stale, self._lag_left = dict(self.positions), self.lag
        return super().place_order(**kwargs)

    def get_positions(self):
        if self._lag_left > 0:
            self._lag_left -= 1
            return [{"tsym": sym, "netqty": qty} for sym, qty in self._stale.items()]
        return super().get_positions()


class LiveRoundTripTests(unittest.IsolatedAsyncioTestCase):
    """The real manager against the fake broker: enter, square off, enter again."""

    def setUp(self):
        self.db = FakeDB()
        self.broker = LaggingBroker(self.db)
        self.intent = {"key": "v3:probe", "book": MOMENTUM_BOOK, "stop_loss_points": 40.0,
                       "source": MOMENTUM_BOOK, "position": "long"}
        service = ScriptedService()
        service.runtime.intent = self.intent
        settings = replace(
            TRADE_SETTINGS, poll_interval_sec=0.01, buy_wait_timeout_sec=1.0,
            sell_wait_timeout_sec=1.0, exchordid_wait_timeout_sec=1.0,
            exit_exchordid_wait_timeout_sec=1.0, exit_retry_backoff_sec=0.01,
            emergency_flatten_sleep_sec=0.01, entry_fail_cooldown_sec=0.0,
        )
        self.manager = TradeManagerLive(
            api=self.broker, db_connector=self.db, symbol_fetcher=FakeSymbols(),
            live_price_getter=lambda: 24_350.0, fresh_price_getter=lambda: 24_350.0,
            strategy_service=service, trade_settings=settings,
        )
        self.manager.ENTRY_POSITION_VERIFY_TIMEOUT_SEC = 2.0
        self.manager.ENTRY_POSITION_VERIFY_POLL_SEC = 0.01

    async def enter(self):
        self.assertTrue(await self.manager._enter_trade("LONG", 24_350.0, dict(self.intent)))

    async def test_a_lagging_book_never_triggers_a_second_exit(self):
        await self.enter()
        self.broker.lag = 5
        await self.manager._exit_trade("carry_open", 1, 24_400.0)
        self.assertEqual(len(self.broker.orders), 4, [(o["symbol"], o["side"]) for o in self.broker.orders])
        self.assertTrue(all(qty == 0 for qty in self.broker.positions.values()), self.broker.positions)
        self.assertFalse(self.manager.in_trade)
        self.assertEqual(self.db.trade_logs[0]["trade_state"], "CLOSED")

    async def test_a_retry_after_a_lost_close_ack_finishes_the_close(self):
        await self.enter()
        real, calls = self.manager._commit_closed_trade_atomic, []

        async def lossy(**kwargs):
            calls.append(kwargs)
            await real(**kwargs)
            if len(calls) == 1:
                raise UncertainTradeCommitError("acknowledgement lost")

        self.manager._commit_closed_trade_atomic = lossy
        with self.assertRaises(UncertainTradeCommitError):
            await self.manager._exit_trade("carry_open", 1, 24_400.0)
        self.assertTrue(self.manager.in_trade)
        await self.manager._exit_trade("carry_open", 1, 24_400.0)
        self.assertFalse(self.manager.in_trade)
        self.assertEqual(self.db.trade_logs[0]["trade_state"], "CLOSED")
        self.assertEqual(self.db.order_logs[0]["lifecycle_state"], "CLOSED")
        self.assertEqual(len(self.broker.orders), 4)

    async def test_square_off_then_enter_again(self):
        await self.enter()
        await self.manager._exit_trade("carry_open", 1, 24_400.0)
        self.assertFalse(self.manager.in_trade)
        self.assertIsNone(self.manager._held_quantity)
        self.intent["key"] = "v3:second"
        await self.enter()
        self.assertTrue(self.manager.in_trade)
        self.assertEqual(len(self.db.trade_logs), 2)
        self.assertEqual(len(self.broker.orders), 6)

    async def test_a_failed_entry_closes_its_ledger_row(self):
        self.broker.reject = {FakeSymbols.BUY_LEG}
        self.assertFalse(await self.manager._enter_trade("LONG", 24_350.0, dict(self.intent)))
        self.assertEqual(self.db.order_logs[0]["lifecycle_state"], "ABORTED")
        self.assertFalse(self.db.trade_logs)

    async def test_every_client_ref_fits_a_short_broker_tag(self):
        await self.enter()
        await self.manager._exit_trade("carry_open", 1, 24_400.0)
        refs = [o["remarks"] for o in self.broker.orders]
        self.assertEqual(len(set(refs)), len(refs))
        for ref in refs:
            self.assertRegex(ref, r"^NB[EXF][BSP][0-9a-f]{16}$")


if __name__ == "__main__":
    unittest.main()
